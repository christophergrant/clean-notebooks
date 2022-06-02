# Databricks notebook source
bucket = ""
destination = f"{bucket}/control/events"

# COMMAND ----------

from backend import jobs
jobs.display_widgets(dbutils)
data = jobs.get_widget_values(dbutils)

# COMMAND ----------

validated_data = jobs.validate_widget_values(data)
enriched_data = jobs.enrich_widget_values(validated_data)
jobs.form_event_and_send_to_control(enriched_data, destination, dbutils, spark) and displayHTML("<h1>Request sent.</h1>")

# COMMAND ----------


