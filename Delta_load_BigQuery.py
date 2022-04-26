# Databricks notebook source
# Appending new records in Bigquery table from dataframe having only yesterday's data

from datetime import date,timedelta
import pyspark.sql.functions as f
from delta.tables import *
deltaCovidData = DeltaTable.forPath(spark, '/FileStore/Akash/Silver/Covid_aggregate')
df=deltaCovidData.toDF()
today=date.today()
yesterday=today-timedelta(days=1)
df=df.filter((f.col("date") == yesterday))

df.write \
  .format("bigquery") \
  .mode("append") \
  .option("temporaryGcsBucket","rb-databricks-temp") \
  .option("partitionType","MONTH") \
  .option("partitionField","date") \
  .option("clusteredFields", "location_key")\
  .save("akash_singh.covid_aggregate")
