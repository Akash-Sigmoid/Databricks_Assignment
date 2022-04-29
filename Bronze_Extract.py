# Databricks notebook source
# MAGIC %run
# MAGIC ./Logger

# COMMAND ----------

import pandas
from pandas import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,DateType

epidemiology_data = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv")
demographics_data = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/demographics.csv")
health_data = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/health.csv")
hospitalizations_data = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/hospitalizations.csv")
mobility_data = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/mobility.csv")
vaccinations_data = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/vaccinations.csv")
epidemiology_df = spark.createDataFrame(epidemiology_data).withColumn("date",col("date").cast(DateType()))
demographics_df = spark.createDataFrame(demographics_data)
health_df = spark.createDataFrame(health_data)
hospitalizations_df = spark.createDataFrame(hospitalizations_data).withColumn("date",col("date").cast(DateType()))
mobility_df = spark.createDataFrame(mobility_data).withColumn("date",col("date").cast(DateType()))
vaccinations_df = spark.createDataFrame(vaccinations_data).withColumn("date",col("date").cast(DateType()))
vaccinations_df.show(5)

# COMMAND ----------

path="dbfs:/FileStore/Akash/Bronze/Epidemiology"
epidemiology_df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}') 

# COMMAND ----------

path="dbfs:/FileStore/Akash/Bronze/Demographics"
df= demographics_df.dropDuplicates()
df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

path="dbfs:/FileStore/Akash/Bronze/Health"
df= health_df.dropDuplicates()
df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

path="dbfs:/FileStore/Akash/Bronze/Hospitalizations"
hospitalizations_df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

path="dbfs:/FileStore/Akash/Bronze/Mobility"
mobility_df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

try:
    path="dbfs:/FileStore/Akash/Bronze/Vaccinations"
    vaccinations_df.write.format('delta') \
       .mode('overwrite') \
       .option('header','true') \
       .option('overwriteSchema', 'true') \
       .save(f'{path}')

except exception as err:
    logger.error(err)
logging.shutdown()

# COMMAND ----------

dffs = spark.read.format('delta') \
    .option('header','true') \
    .load("dbfs:/FileStore/Akash/Silver/Covid_aggregate")


logger.info("Number of records:"+str(dffs.count()))

# COMMAND ----------

# MAGIC %run
# MAGIC ./logging
