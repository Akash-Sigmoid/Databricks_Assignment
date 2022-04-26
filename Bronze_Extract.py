# Databricks notebook source
# Reading list of Columns from csv file

import pandas
from pandas import *
epidemiology_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv",header=None,nrows=1).iloc[0]
demographics_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/demographics.csv",header=None,nrows=1).iloc[0]
health_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/health.csv",header=None,nrows=1).iloc[0]
hospitalizations_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/hospitalizations.csv",header=None,nrows=1).iloc[0]
mobility_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/mobility.csv",header=None,nrows=1).iloc[0]
vaccinations_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/vaccinations.csv",
usecols=[0,1,2,3,4,5,6,7],header=None,nrows=1).iloc[0]

# COMMAND ----------

# Reading data from BigQuery Aggregated Table in dataframe data

path="dbfs:/FileStore/Akash/Bronze/Epidemiology"
data = spark.read \
  .format("bigquery") \
  .option('table', 'bigquery-public-data.covid19_open_data.covid19_open_data') \
  .option('header','true') \
  .load()

# Writing Epidemiology data in Delta table

df= data.select(*epidemiology_col)
df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}') 

# COMMAND ----------

# Storing Demographics data in Delta table

path="dbfs:/FileStore/Akash/Bronze/Demographics"
df= data.select(*demographics_col).dropDuplicates()
df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

#Storing Health data in Delta Table

path="dbfs:/FileStore/Akash/Bronze/Health"
df= data.select(*health_col).dropDuplicates()
df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

# Storing Hospitalizations data in Delta table

path="dbfs:/FileStore/Akash/Bronze/Hospitalizations"
df= data.select(*hospitalizations_col)
df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

# Storing Mobility data in Delta table

path="dbfs:/FileStore/Akash/Bronze/Mobility"
df= data.select(*mobility_col)
df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

# Storing Vaccinations data in Delta table

path="dbfs:/FileStore/Akash/Bronze/Vaccinations"
df= data.select(*vaccinations_col)
df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')
df.show()

# COMMAND ----------

dffs = spark.read.format('delta') \
    .option('header','true') \
    .load("dbfs:/FileStore/Akash/Bronze/Hospitalizations")

dffs.show()
dffs.count()
