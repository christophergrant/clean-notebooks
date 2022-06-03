from backend import common
import json

# display_widgets
def display_widgets(dbutils, spark):
    dbutils.widgets.removeAll()
    dbutils.widgets.text(name="00_job_name", defaultValue="")
    code_choices = spark.sql("SELECT * FROM control.code")
    dbutils.widgets.dropdown(name="01_code_name", defaultValue="INSERT", choices=["INSERT", "UPDATE",
                                                                                  "DELETE"])
    dbutils.widgets.text(name="02_parameters", defaultValue="{}")
    dbutils.widgets.dropdown(name="03_compute", defaultValue="Small",
                             choices=["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"])

def get_widget_values(dbutils):
    widget_names = ["job_name", "code_name", "parameters", "compute"]
    return common.get_widget_values(widget_names, dbutils)

def validate_widget_values(d, dbutils, spark):
    assert d["job_name"] not in ("", None), "job_name cannot be empty"
    try:
        json.loads(d["parameters"])
    except:
        raise AssertionError("'parameters' JSON must be valid")

def enrich_widget_values(d):
    return d

def form_event_and_send_to_control(d, dbutils, spark):
    return common.form_event_and_send_to_control(d, "JOB", dbutils, spark)

