# Databricks notebook source
# MAGIC %md 
# MAGIC #Import necessary PySpark functions for data manipulation
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.functions import year, month,dayofmonth,dayofweek
from pyspark.sql.types import IntegerType
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import hour
from pyspark.sql.functions import sum, avg, max

# COMMAND ----------

# MAGIC %md 
# MAGIC #This code reads and processes public transport data files by month, handling date conversions, duration calculations, delay categorization, and storage of the processed data.
# MAGIC

# COMMAND ----------


spark.conf.set(
    "fs.azure.account.key.dbstoragelamiaeelhosni.blob.core.windows.net", "vWfglH1IE6wtJdzxrBJJh9LEVcj7ShKFu3DQzApnsTrSQnoiJ+8AgknMwIb0MgrYk0JhYFE1jNVv+AStAHiAEg==")
def GetFilesByMonth(Month):
    spark.conf.set(
    "fs.azure.account.key.dbstoragelamiaeelhosni.blob.core.windows.net", "vWfglH1IE6wtJdzxrBJJh9LEVcj7ShKFu3DQzApnsTrSQnoiJ+8AgknMwIb0MgrYk0JhYFE1jNVv+AStAHiAEg==")

    file_path = f"wasbs://mycontainer@dbstoragelamiaeelhosni.blob.core.windows.net/public_transport_data/raw/Year=2023/{Month}*.csv"
    spark_df = spark.read.format('csv').option('header', True).load(file_path)

    # Add Year Month , Day and convert Date Columns
    spark_df = spark_df.withColumn("Date", col("Date").cast("Date"))
    spark_df = spark_df.withColumn("Year", year(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("Month", month(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("DayOfMonth", dayofmonth(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("DayOfWeek", dayofweek(spark_df["Date"]).cast(IntegerType()))

    # Add Duration Between each Arrival
    spark_df = spark_df.withColumn("Duration_Minutes", ((col("ArrivalTime").cast("timestamp") - col("DepartureTime").cast("timestamp")) / 60).cast("int"))
    spark_df = spark_df.withColumn("Duration_Hours", ((col("ArrivalTime").cast("timestamp") - col("DepartureTime").cast("timestamp")) / 3600).cast("int"))

    # Add rows contains null after calc the Duration time :

    spark_df = spark_df.where(col("Duration_Minutes").isNotNull())

    # Delay Analysis:
    spark_df = spark_df.withColumn("Retard",when(spark_df["Delay"] <= 0, 'Pas de Retard').when(spark_df["Delay"] <= 10, "Retard Court").when(spark_df["Delay"] <= 20, "Retard Moyen").otherwise( 'Long Retard'))
    #Anlytics
    spark_df = spark_df.withColumn("hours_DepartureTime", hour(spark_df["DepartureTime"]))

    spark_df.coalesce(1).write.partitionBy("Year","Month").format("csv").option('header', True).mode("append").save("wasbs://mycontainer@dbstoragelamiaeelhosni.blob.core.windows.net/public_transport_data/processed/")



# COMMAND ----------

# MAGIC %md 
# MAGIC #This code checks for unprocessed files in the "raw" directory and invokes the "GetFilesByMonth" function to process them, ensuring that only two files are processed.
# MAGIC

# COMMAND ----------

raw = "wasbs://mycontainer@dbstoragelamiaeelhosni.blob.core.windows.net/public_transport_data/raw/Year=2023"
processed = "wasbs://mycontainer@dbstoragelamiaeelhosni.blob.core.windows.net/public_transport_data/processed/Year=2023"

processed_count = 0

files_raw = dbutils.fs.ls(raw)
files_processed = [x.name for x in dbutils.fs.ls(processed)]
for r in files_raw:
    if processed_count == 2:
        break
    if r.name not in files_processed:
        GetFilesByMonth(r.name)
        processed_count+=1

# COMMAND ----------

# MAGIC %md 
# MAGIC #Identifying peak and off-peak hours based on passenger counts
# MAGIC

# COMMAND ----------

Analyse des Passagers: Identifier les heures de pointe et hors pointe en fonction du nombre de passagers.
from pyspark.sql.functions import hour

# Group by the hour of departure and count the number of passengers for each hour
passenger_analysis = spark_df.groupBy(hour("DepartureTime").alias("HourOfDay")).agg({"Passengers": "sum"})

# Order the results by passenger count in descending order
passenger_analysis = passenger_analysis.orderBy("sum(Passengers)", ascending=False)

# Show the results
passenger_analysis.show()


# COMMAND ----------

# MAGIC %md 
# MAGIC # Calculating average delay, average passenger count, and total trips for each route.
# MAGIC

# COMMAND ----------

Analyse des Itinéraires: Calculer le retard moyen, le nombre moyen de passagers et le nombre total de voyages pour chaque itinéraire.


# Group by l'itinéraire (Route)
route_analysis = spark_df.groupBy("Route").agg(
    {"Delay": "avg", "Passengers": "avg", "Route": "count"}
).withColumnRenamed("avg(Delay)", "AverageDelay").withColumnRenamed("avg(Passengers)", "AveragePassengers").withColumnRenamed("count(Route)", "TotalTrips")

# Afficher l'analyse des itinéraires
route_analysis.show()


# COMMAND ----------


