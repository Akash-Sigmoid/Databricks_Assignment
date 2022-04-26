# Databricks notebook source
import pandas
from pandas import *
from datetime import date,timedelta
import pyspark.sql.functions as f
from delta.tables import *

#Buffer table is mapped to silver layer(1:1) but if any transformation is done to any table it should be applied to buffer table

path = "dbfs:/FileStore/Akash/Silver/Buffer"
deltaBuffer = DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Buffer')
deltaBuffer_df = deltaBuffer.toDF()
deltaBuffer_df.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

# COMMAND ----------

# Creating DataFrame from buffer delta table

today=date.today()
yesterday=today-timedelta(days=1)

deltaCovidData = DeltaTable.forPath(spark, '/FileStore/Akash/Silver/Covid_aggregate')
deltaBuffer = DeltaTable.forPath(spark, '/FileStore/Akash/Silver/Buffer')
df= deltaBuffer.toDF()
#deltaCovidData = deltaCovidData.toDF()

#df=deltaCovidData.filter((f.col("date") == today) | (f.col("date") == yesterday))





# COMMAND ----------

#Merging all the updates to Aggregated Table Covid_aggregate

deltaCovidData.alias("target").merge(df.alias("source"),'target.date = source.date and target.location_key=source.location_key') \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll()\
  .execute()
