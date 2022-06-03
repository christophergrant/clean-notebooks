# Databricks notebook source
from backend import approvals
approvals.display_widgets(spark, dbutils)
data = approvals.get_widget_values(dbutils)

# COMMAND ----------

validated_data = approvals.validate_widget_values(data, dbutils, spark)
enriched_data = approvals.enrich_widget_values(validated_data, spark)
approvals.form_event_and_send_to_control(enriched_data, dbutils, spark) and displayHTML("<h1>Request sent.</h1>")

# COMMAND ----------


