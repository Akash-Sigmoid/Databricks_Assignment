# Databricks notebook source
# Creating BigQuery table with data in aggregated silver layer table. It is partitioned on field date and clustered by location_key

from delta.tables import *
deltaCovidData = DeltaTable.forPath(spark, '/FileStore/Akash/Silver/Covid_aggregate')
df=deltaCovidData.toDF()
df.write \
  .format("bigquery") \
  .mode("overwrite") \
  .option("temporaryGcsBucket","rb-databricks-temp") \
  .option("partitionType","MONTH") \
  .option("partitionField","date") \
  .option("clusteredFields", "location_key")\
  .save("akash_singh.covid_aggregate")

# COMMAND ----------


