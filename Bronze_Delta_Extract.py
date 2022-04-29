# Databricks notebook source
# MAGIC %run
# MAGIC ./Logger

# COMMAND ----------

import pandas
from pandas import *
from datetime import date,timedelta
import pyspark.sql.functions as f
from delta.tables import *
epidemiology_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv",header=None,nrows=1).iloc[0]
today=date.today()
yesterday=today-timedelta(days=1)
path = "dbfs:/FileStore/Akash/Bronze/Buffer"

# Reading Today's and yesterday's data from bigquery table by pusing filter to it

data = spark.read \
  .format("bigquery")   \
  .option('table', 'bigquery-public-data.covid19_open_data.covid19_open_data') \
  .option("filter",f"date = '{today}' or date = '{yesterday}'") \
  .option('header','true') \
  .load()

# Storing Today's and yesterday's data from bigquery table in BUffer table

data.write.format('delta') \
   .mode('overwrite') \
   .option('header','true') \
   .option('overwriteSchema', 'true') \
   .save(f'{path}')

deltaBuffer= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Buffer')

# COMMAND ----------

#Merging and inserting updated todays and yeterday's data in Epidemiology delta table

epidemiology_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv",header=None,nrows=1).iloc[0]
dfffs= data.select(*epidemiology_col)
deltaEpidemiology= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Epidemiology')
try:
    deltaEpidemiology.alias("target").merge(dfffs.alias("source"),'target.date = source.date and target.location_key=source.location_key') \
      .whenMatchedUpdate(set =
        {
          "date": "source.date",
          "location_key" : "source.location_key",
          "new_confirmed": "source.new_confirmed",
          "new_deceased": "source.new_deceased",
          "new_recovered": "source.new_recovered",
          "new_tested": "source.new_tested",
          "cumulative_confirmed": "source.cumulative_confirmed",
          "cumulative_deceased" : "source.cumulative_deceased",
          "cumulative_recovered" : "source.cumulative_recovered",
          "cumulative_tested" : "source.cumulative_tested"
        }
      ) \
      .whenNotMatchedInsert(values =
        {
          "date": "source.date",
          "location_key": "source.location_key",
          "new_confirmed": "source.new_confirmed",
          "new_deceased": "source.new_deceased",
          "new_recovered": "source.new_recovered",
          "new_tested": "source.new_tested",
          "cumulative_confirmed": "source.cumulative_confirmed",
          "cumulative_deceased": "source.cumulative_deceased",
          "cumulative_recovered": "source.cumulative_recovered",
          "cumulative_tested": "source.cumulative_tested"
        }
      ) \
      .execute()
except exception as err:
    logger.error(err)
logging.shutdown()

# COMMAND ----------

#Merging and inserting updated todays and yeterday's data in Demographics delta table

demographics_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/demographics.csv",header=None,nrows=1).iloc[0]
dfffs= data.select(*demographics_col).dropDuplicates()
deltaDemographics= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Demographics')

deltaDemographics.alias("target").merge(dfffs.alias("source"),'target.location_key=source.location_key') \
  .whenMatchedUpdate(set =
    {
      "location_key" : "source.location_key",
      "population": "source.population",
      "population_male": "source.population_male",
      "population_female": "source.population_female",
      "population_rural": "source.population_rural",
      "population_urban": "source.population_urban",
      "population_largest_city" : "source.population_largest_city",
      "population_clustered" : "source.population_clustered",
      "population_density" : "source.population_density",
      "human_development_index" : "source.human_development_index",
      "population_age_00_09" : "source.population_age_00_09",
      "population_age_10_19" : "source.population_age_10_19",
      "population_age_20_29" : "source.population_age_20_29",
      "population_age_30_39" : "source.population_age_30_39",
      "population_age_40_49" : "source.population_age_40_49",
      "population_age_50_59" : "source.population_age_50_59",
      "population_age_60_69" : "source.population_age_60_69",
      "population_age_70_79" : "source.population_age_70_79",
      "population_age_80_and_older" : "source.population_age_80_and_older"
        
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "location_key" : "source.location_key",
      "population": "source.population",
      "population_male": "source.population_male",
      "population_female": "source.population_female",
      "population_rural": "source.population_rural",
      "population_urban": "source.population_urban",
      "population_largest_city" : "source.population_largest_city",
      "population_clustered" : "source.population_clustered",
      "population_density" : "source.population_density",
      "human_development_index" : "source.human_development_index",
      "population_age_00_09" : "source.population_age_00_09",
      "population_age_10_19" : "source.population_age_10_19",
      "population_age_20_29" : "source.population_age_20_29",
      "population_age_30_39" : "source.population_age_30_39",
      "population_age_40_49" : "source.population_age_40_49",
      "population_age_50_59" : "source.population_age_50_59",
      "population_age_60_69" : "source.population_age_60_69",
      "population_age_70_79" : "source.population_age_70_79",
      "population_age_80_and_older" : "source.population_age_80_and_older"
    }
  ) \
  .execute()

# COMMAND ----------

#Merging and inserting updated todays and yeterday's data in Health delta table

health_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/health.csv",header=None,nrows=1).iloc[0]
dfffs= data.select(*health_col).dropDuplicates()
deltaHealth= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Health')

deltaHealth.alias("target").merge(dfffs.alias("source"),'target.location_key=source.location_key') \
  .whenMatchedUpdate(set =
    {
      "location_key": "source.location_key",
      "life_expectancy" : "source.life_expectancy",
      "smoking_prevalence": "source.smoking_prevalence",
      "diabetes_prevalence": "source.diabetes_prevalence",
      "infant_mortality_rate": "source.infant_mortality_rate",
      "adult_male_mortality_rate": "source.adult_male_mortality_rate",
      "adult_female_mortality_rate": "source.adult_female_mortality_rate",
      "comorbidity_mortality_rate": "source.comorbidity_mortality_rate",
      "hospital_beds_per_1000" : "source.hospital_beds_per_1000",
      "nurses_per_1000" : "source.nurses_per_1000",
      "physicians_per_1000" : "source.physicians_per_1000",
      "health_expenditure_usd" : "source.health_expenditure_usd",
      "out_of_pocket_health_expenditure_usd" : "source.out_of_pocket_health_expenditure_usd"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "location_key": "source.location_key",
      "life_expectancy" : "source.life_expectancy",
      "smoking_prevalence": "source.smoking_prevalence",
      "diabetes_prevalence": "source.diabetes_prevalence",
      "infant_mortality_rate": "source.infant_mortality_rate",
      "adult_male_mortality_rate": "source.adult_male_mortality_rate",
      "adult_female_mortality_rate": "source.adult_female_mortality_rate",
      "comorbidity_mortality_rate": "source.comorbidity_mortality_rate",
      "hospital_beds_per_1000" : "source.hospital_beds_per_1000",
      "nurses_per_1000" : "source.nurses_per_1000",
      "physicians_per_1000" : "source.physicians_per_1000",
      "health_expenditure_usd" : "source.health_expenditure_usd",
      "out_of_pocket_health_expenditure_usd" : "source.out_of_pocket_health_expenditure_usd"
    }
  ) \
  .execute()

# COMMAND ----------

#Merging and inserting updated todays and yeterday's data in Hospitalizations delta table

hospitalizations_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/hospitalizations.csv",header=None,nrows=1).iloc[0]
dfffs= data.select(*hospitalizations_col)
deltaHospitalizations = DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Hospitalizations')

deltaHospitalizations.alias("target").merge(dfffs.alias("source"),'target.date == source.date and target.location_key = source.location_key') \
  .whenMatchedUpdate(set =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "new_hospitalized_patients": "source.new_hospitalized_patients",
      "cumulative_hospitalized_patients": "source.cumulative_hospitalized_patients",
      "current_hospitalized_patients": "source.current_hospitalized_patients",
      "new_intensive_care_patients": "source.new_intensive_care_patients",
      "cumulative_intensive_care_patients": "source.cumulative_intensive_care_patients",
      "current_intensive_care_patients": "source.current_intensive_care_patients",
      "new_ventilator_patients" : "source.new_ventilator_patients",
      "cumulative_ventilator_patients" : "source.cumulative_ventilator_patients",
      "current_ventilator_patients" : "source.current_ventilator_patients"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "new_hospitalized_patients": "source.new_hospitalized_patients",
      "cumulative_hospitalized_patients": "source.cumulative_hospitalized_patients",
      "current_hospitalized_patients": "source.current_hospitalized_patients",
      "new_intensive_care_patients": "source.new_intensive_care_patients",
      "cumulative_intensive_care_patients": "source.cumulative_intensive_care_patients",
      "current_intensive_care_patients": "source.current_intensive_care_patients",
      "new_ventilator_patients" : "source.new_ventilator_patients",
      "cumulative_ventilator_patients" : "source.cumulative_ventilator_patients",
      "current_ventilator_patients" : "source.current_ventilator_patients"
    }
  ) \
  .execute()

# COMMAND ----------

#Merging and inserting updated todays and yeterday's data in Mobility delta table

mobility_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/mobility.csv",header=None,nrows=1).iloc[0]
dfffs= data.select(*mobility_col)
deltaMobility= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Mobility')

deltaMobility.alias("target").merge(dfffs.alias("source"),'target.date = source.date and target.location_key=source.location_key') \
  .whenMatchedUpdate(set =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "mobility_retail_and_recreation": "source.mobility_retail_and_recreation",
      "mobility_grocery_and_pharmacy": "source.mobility_grocery_and_pharmacy",
      "mobility_parks": "source.mobility_parks",
      "mobility_transit_stations": "source.mobility_transit_stations",
      "mobility_workplaces": "source.mobility_workplaces",
      "mobility_residential" : "source.mobility_residential"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "mobility_retail_and_recreation": "source.mobility_retail_and_recreation",
      "mobility_grocery_and_pharmacy": "source.mobility_grocery_and_pharmacy",
      "mobility_parks": "source.mobility_parks",
      "mobility_transit_stations": "source.mobility_transit_stations",
      "mobility_workplaces": "source.mobility_workplaces",
      "mobility_residential" : "source.mobility_residential"
    }
  ) \
  .execute()

# COMMAND ----------

#Merging and inserting updated todays and yeterday's data in vaccinations delta table

vaccinations_col = pandas.read_csv("https://storage.googleapis.com/covid19-open-data/v3/vaccinations.csv",
usecols=[0,1,2,3,4,5,6,7],header=None,nrows=1).iloc[0]
dfffs= data.select(*vaccinations_col)
deltaVaccinations= DeltaTable.forPath(spark, '/FileStore/Akash/Bronze/Vaccinations')

deltaVaccinations.alias("target").merge(dfffs.alias("source"),'target.date = source.date and target.location_key = source.location_key') \
  .whenMatchedUpdate(set =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "new_persons_vaccinated": "source.new_persons_vaccinated",
      "cumulative_persons_vaccinated": "source.cumulative_persons_vaccinated",
      "new_persons_fully_vaccinated": "source.new_persons_fully_vaccinated",
      "cumulative_persons_fully_vaccinated": "source.cumulative_persons_fully_vaccinated",
      "new_vaccine_doses_administered": "source.new_vaccine_doses_administered",
      "cumulative_vaccine_doses_administered" : "source.cumulative_vaccine_doses_administered"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "date": "source.date",
      "location_key" : "source.location_key",
      "new_persons_vaccinated": "source.new_persons_vaccinated",
      "cumulative_persons_vaccinated": "source.cumulative_persons_vaccinated",
      "new_persons_fully_vaccinated": "source.new_persons_fully_vaccinated",
      "cumulative_persons_fully_vaccinated": "source.cumulative_persons_fully_vaccinated",
      "new_vaccine_doses_administered": "source.new_vaccine_doses_administered",
      "cumulative_vaccine_doses_administered" : "source.cumulative_vaccine_doses_administered"
    }
  ) \
  .execute()

# COMMAND ----------


