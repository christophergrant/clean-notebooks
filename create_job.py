# Databricks notebook source
from backend import jobs
jobs.process(dbutils, spark)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

jobs.display_widgets(spark, dbutils)
