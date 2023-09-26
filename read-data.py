# Databricks notebook source
# MAGIC %md 
# MAGIC #imports
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.functions import year, month,dayofmonth,dayofweek
from pyspark.sql.types import IntegerType
from datetime import datetime
from pyspark.sql.functions import col

# COMMAND ----------

spark.conf.set(
"fs.azure.account.key.dbstoragelamiaeelhoqni.blob.core.windows.net", "1vw5+qnniUd6TK3I1FBRKUp1vWbtdmuLLrp5fHNlgJpdC/C35Le7kq9MvPZ8ski6uQDw12kT0fbZ+AStZiP1OQ==")

spark_df = spark.read.format('csv').option('header', True).load("wasbs://mycontainer@dbstoragelamiaeelhoqni.blob.core.windows.net/public_transport_data/raw/*.csv")

display(spark_df)

# COMMAND ----------



#Add columns year, month, day, and day of the week
#spark_df  = spark.createDataFrame(spark_df, ["Date"])
spark_df = spark_df.withColumn("Date", col("Date").cast("Date"))
spark_df = spark_df.withColumn("Year", year(spark_df["Date"]).cast(IntegerType()))
spark_df = spark_df.withColumn("Month", month(spark_df["Date"]).cast(IntegerType()))
spark_df = spark_df.withColumn("DayOfMonth", dayofmonth(spark_df["Date"]).cast(IntegerType()))
spark_df = spark_df.withColumn("DayOfWeek", dayofweek(spark_df["Date"]).cast(IntegerType()))

display(spark_df)



# COMMAND ----------


#spark_df.select('DepartureTime','ArrivalTime').show()

spark_df = spark_df.withColumn("Duration (M)", ((col("ArrivalTime").cast("timestamp") - col("DepartureTime").cast("timestamp")) / 60).cast("int"))
spark_df = spark_df.withColumn("Duration (H)", ((col("ArrivalTime").cast("timestamp") - col("DepartureTime").cast("timestamp")) / 3600).cast("int"))


display(spark_df)

# COMMAND ----------


# Assuming you have a DataFrame named "spark_df" with "Duration (M)" column
# Define the categorization logic using "when" function
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
spark_df = spark_df.withColumn("DelayCategory", 
    when(col("Duration (M)") <= 0, "Pas de Retard")
    .when((col("Duration (M)") > 0) & (col("Duration (M)") <= 10), "Retard Court")
    .when((col("Duration (M)") > 10) & (col("Duration (M)") <= 20), "Retard Moyen")
    .when(col("Duration (M)") > 20, "Long Retard")
    .otherwise("Unknown"))

# Show the DataFrame with the new "DelayCategory" column
display(spark_df)
