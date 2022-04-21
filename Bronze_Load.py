# Databricks notebook source
df = spark.read \
  .format("bigquery") \
  .load("bigquery-public-data.covid19_open_data.covid19_open_data")

df[['location_key']].head(5)

# COMMAND ----------


