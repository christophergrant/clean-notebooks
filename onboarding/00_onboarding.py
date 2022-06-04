# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text(name="gcs_bucket", defaultValue="gs://")
dbutils.widgets.text(name="participant_name", defaultValue="snap")

# COMMAND ----------

gcs_bucket = dbutils.widgets.get("gcs_bucket").rstrip("/")
participant = dbutils.widgets.get("participant_name")

# COMMAND ----------

import re
assert bool(re.match(r"^gs:\/\/.+", gcs_bucket)), "bucket must be from Google storage, fully qualified like gs://<bucket>"
dirs_to_create = ["control", "output", "input", "cluster-logs", "code", "audit-logs"]
for d in dirs_to_create:
  print(f"Creation status for: {gcs_bucket}/{d}", dbutils.fs.mkdirs(f"{gcs_bucket}/{d}"))

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {participant} LOCATION '{gcs_bucket}/input'")
spark.sql(f"CREATE DATABASE IF NOT EXISTS control LOCATION '{gcs_bucket}/control'")
spark.sql(f"CREATE TABLE IF NOT EXISTS control.events (`event_type` STRING,`uuid` STRING,`payload` STRING,`timestamp` TIMESTAMP,`user` STRING,`notebook` STRING,`hostname` STRING,`api_version` STRING) USING delta LOCATION '{gcs_bucket}/control/events'")
