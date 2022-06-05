# Databricks notebook source
from backend import jobs
jobs.display_widgets(spark, dbutils)

# COMMAND ----------

jobs.process(spark, dbutils)
