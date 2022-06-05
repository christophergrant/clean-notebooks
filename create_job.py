# Databricks notebook source
from backend import jobs
jobs.display_widgets(spark, dbutils)

# COMMAND ----------

data = jobs.get_widget_values(dbutils)

# COMMAND ----------

jobs.process(data, spark, dbutils)
