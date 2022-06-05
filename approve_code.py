# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from backend import approvals
data = None
try:
    data = approvals.get_widget_values(dbutils)
except Exception as e:
    if not "InputWidgetNotDefined" in str(e): print(e) 

# COMMAND ----------

from backend import approvals
try:
    if (data != None):
        validated_data = approvals.validate_widget_values(data, dbutils, spark)
        enriched_data = approvals.enrich_widget_values(validated_data, spark)
        approvals.form_event_and_send_to_control(enriched_data, dbutils, spark) and displayHTML("<h1>Request sent.</h1>") 
except Exception as e:
    if not "InputWidgetNotDefined" in str(e): print(e) 

# COMMAND ----------

df = approvals.display_widgets(spark, dbutils)

# COMMAND ----------

# DBTITLE 1,Review pending request details
display(df)
