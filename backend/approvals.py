from backend import common

# display_widgets
def display_widgets(spark, dbutils):
    dbutils.widgets.removeAll()
    dbutils.widgets.dropdown(name="01_action", defaultValue="REVIEWING", choices=['REVIEWING', 'APPROVE', 'REJECT'])
    dbutils.widgets.text(name="02_code_name", defaultValue="")
    dbutils.widgets.tex(name="03_reason", defaultValue="INSERT", choices=["INSERT", "UPDATE", "DELETE"])

def get_widget_values(dbutils):
    widget_names = ["action", "description", "action"]
    return common.get_widget_values(widget_names, dbutils)

def validate_widget_values(d, dbutils, spark):
    return d

def enrich_widget_values(d, spark):
    return d

def form_event_and_send_to_control(d, destination, dbutils, spark):
    return common.form_event_and_send_to_control(d, "DATA", destination, dbutils, spark)
