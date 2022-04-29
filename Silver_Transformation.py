# Databricks notebook source
# MAGIC %run
# MAGIC ./Logger

# COMMAND ----------

# Storing all delta tables in DataFrame

from delta.tables import *
deltaEpidemiology= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Epidemiology')
deltaDemographics= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Demographics')
deltaHospitalizations = DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Hospitalizations')
deltaHealth= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Health')
deltaMobility= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Mobility')
deltaVaccinations= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Vaccinations')
Epidemiology_df= deltaEpidemiology.toDF()
Demographics_df= deltaDemographics.toDF()
Hospitalizations_df = deltaHospitalizations.toDF()
Health_df = deltaHealth.toDF()
Mobility_df = deltaMobility.toDF()
Vaccinations_df = deltaVaccinations.toDF()




# COMMAND ----------

#joining all DataFrame's to create aggregated dataFrame

from pyspark.sql.functions import col
Aggregate_demography_Health_df = Demographics_df.join(Health_df,["location_key"],"inner").dropDuplicates()
Aggregated_df=Epidemiology_df.join(Hospitalizations_df,["date", "location_key",],"inner").join(Mobility_df,["date","location_key"], \
"inner").join(Vaccinations_df, ["date","location_key"], "inner").join(Aggregate_demography_Health_df,["location_key"],"inner")

# COMMAND ----------

#Creating Aggregated table in Silver layer using bronze layer tables
try:
    path = "dbfs:/FileStore/Akash/Silver/Covid_aggregate"
    Aggregated_df.write.format('delta') \
       .mode('overwrite') \
       .option('header','true') \
       .option('overwriteSchema', 'true') \
       .partitionBy("date") \
       .save(f'{path}')
except exception as err:
    logger.error(err)
logging.shutdown()

# COMMAND ----------

spark.sql("DROP TABLE  IF EXISTS akash_singh.covid_19_data")
spark.sql("CREATE TABLE akash_singh.covid_19_data USING DELTA LOCATION '/FileStore/Akash/Silver/Covid_aggregate'")
spark.sql("OPTIMIZE akash_singh.covid_19_data ZORDER BY (location_key)")
