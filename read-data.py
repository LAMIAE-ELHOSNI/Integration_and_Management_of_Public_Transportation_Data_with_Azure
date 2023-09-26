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

#Connection configuration
spark.conf.set(
"fs.azure.account.key.yassineessadidatalakeg2.blob.core.windows.net", "gWYEfszXt9mbYAwRbZP0hE3Bo1rZUFJoFw71LWPsENPoEPb5CzWeN28ukbQV6/o3vm6mlyg31lim+ASt3uGX5A==")

spark_df = spark.read.format('csv').option('header', True).load("wasbs://data@yassineessadidatalakeg2.blob.core.windows.net/public_transport_data/raw/*.csv")

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
