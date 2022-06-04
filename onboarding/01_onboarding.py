# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text(name="gcs_bucket", defaultValue="gs://")

# COMMAND ----------

gcs_bucket = dbutils.widgets.get("gcs_bucket").rstrip("/")

# COMMAND ----------

control_tables = ["runs", "approval_status", "code", "data", "jobs", "event_status"]
for table in control_tables:
    spark.sql(f"CREATE TABLE IF NOT EXISTS control.{table} USING DELTA LOCATION '{gcs_bucket}/control/{table}'")

# COMMAND ----------


