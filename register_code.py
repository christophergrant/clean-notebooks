# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from backend import code
code.display_widgets(dbutils)
vals = code.get_widget_values(dbutils)

# COMMAND ----------

if vals['code_name'] != '':
    code.validate_widget_values(vals)
    enriched_vals = code.enrich_widget_values(vals)
    code.form_event_and_send_to_control(enriched_vals, dbutils, spark) and displayHTML("<h1>Request sent.</h1>")
