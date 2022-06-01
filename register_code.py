# Databricks notebook source
bucket = ""
destination = f"{bucket}/control/events"

# COMMAND ----------

from cleanroom.notebooks import code
code.display_widgets(dbutils)

# COMMAND ----------

vals = code.get_widget_values(dbutils)

# COMMAND ----------

code.validate_widget_values(vals)
enriched_vals = code.enrich_widget_values(vals)
code.form_event_and_send_to_control(enriched_vals, destination, dbutils, spark) and displayHTML("<h1>Request sent.</h1>")

# COMMAND ----------


