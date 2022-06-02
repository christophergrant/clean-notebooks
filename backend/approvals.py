from backend import common

# display_widgets
def display_widgets(spark, dbutils):
    dbutils.widgets.removeAll()
    dbutils.widgets.dropdown(name="00_action", defaultValue="REVIEWING", choices=['REVIEWING', 'APPROVE', 'REJECT'])
    dbutils.widgets.text(name="01_code_name", defaultValue="")
    dbutils.widgets.text(name="02_reason", defaultValue="")

def get_widget_values(dbutils):
    widget_names = ["action", "code_name", "reason"]
    return common.get_widget_values(widget_names, dbutils)

def validate_widget_values(d, dbutils, spark):
    return d

def enrich_widget_values(d, spark):
    return d

def form_event_and_send_to_control(d, destination, dbutils, spark):
    return common.form_event_and_send_to_control(d, "DATA", destination, dbutils, spark)
