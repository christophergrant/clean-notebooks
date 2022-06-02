# Databricks notebook source
bucket = "gs://snap-local-test"
assert bucket and bucket != ""
destination = f"{bucket}/control/events"

# COMMAND ----------

from backend import data
data.display_widgets(dbutils)

# COMMAND ----------

vals = data.get_widget_values(dbutils)

# COMMAND ----------

data.validate_widget_values(vals, dbutils, spark)
enriched_vals = data.enrich_widget_values(vals, spark)
data.form_event_and_send_to_control(enriched_vals, destination, dbutils, spark) and displayHTML("<h1>Request sent.</h1>")

# COMMAND ----------


