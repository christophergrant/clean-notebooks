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

# DBTITLE 1,For v1.1
from delta.tables import DeltaTable

code_table_columns = DeltaTable.forName(spark, "control.code").toDF().columns
if 'default_parameters' not in code_table_columns:
    alter_sql = f"ALTER TABLE control.code ADD COLUMNS (default_parameters string)"
    print(f"adding new column via DDL: {alter_sql}")
    spark.sql(alter_sql)
else:
    print("control.code already has the default_parameters column")

# COMMAND ----------

runs_table_columns = DeltaTable.forName(spark, "control.runs").toDF().columns
if 'error_trace' not in runs_table_columns:
    alter_sql = f"ALTER TABLE control.runs ADD COLUMNS (error STRING, error_trace STRING)"
    print(f"adding new column via DDL: {alter_sql}")
    spark.sql(alter_sql)
else:
    print("control.runs already has the error_trace column")
