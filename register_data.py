# Databricks notebook source
bucket = ""
destination = f"{bucket}/control/events"

# COMMAND ----------

from cleanroom.notebooks import data
data.display_widgets(dbutils)

# COMMAND ----------

vals = data.get_widget_values(dbutils)

# COMMAND ----------

data.validate_widget_values(vals, dbutils, spark)
enriched_vals = data.enrich_widget_values(vals, spark)
data.form_event_and_send_to_control(enriched_vals, destination, dbutils, spark) and displayHTML("<h1>Request sent.</h1>")

# COMMAND ----------


