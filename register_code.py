# Databricks notebook source
from backend import code
code.display_widgets(dbutils)

# COMMAND ----------

from backend import code
vals = code.get_widget_values(dbutils)

# COMMAND ----------

code.validate_widget_values(vals)
enriched_vals = code.enrich_widget_values(vals)
code.form_event_and_send_to_control(enriched_vals, dbutils, spark) and displayHTML("<h1>Request sent.</h1>")
