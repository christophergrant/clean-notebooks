# Databricks notebook source
bucket = ""
destination = f"{bucket}/control/events"

# COMMAND ----------

from backend import approvals
approvals.display_widgets(dbutils)
data = approvals.get_widget_values(dbutils)

# COMMAND ----------

validated_data = approvals.validate_widget_values(data)
enriched_data = approvals.enrich_widget_values(validated_data)
approvals.form_event_and_send_to_control(enriched_data, destination, dbutils, spark) and displayHTML("<h1>Request sent.</h1>")

# COMMAND ----------

dbutils.widgets.
