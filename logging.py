# Databricks notebook source
dbutils.fs.mv('file:'+name,'/FileStore/Akash/log'+name)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists pipeline_logging;
# MAGIC create table if not exists pipeline_logging
# MAGIC Using text options(path '/FileStore/Akash/log/*', header=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pipeline_logging
