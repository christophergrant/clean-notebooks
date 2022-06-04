# Databricks notebook source
from backend import jobs
jobs.process(dbutils, spark)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

jobs.display_widgets(spark, dbutils)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from control.events order by timestamp desc limit 30

# COMMAND ----------


