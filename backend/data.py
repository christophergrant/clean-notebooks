import common
import re

# display_widgets
def display_widgets(dbutils):
    dbutils.widgets.removeAll()
    dbutils.widgets.text(name="00_data_metastore_name", defaultValue="")
    dbutils.widgets.text(name="01_description", defaultValue="")
    dbutils.widgets.dropdown(name="02_action", defaultValue="INSERT", choices=["INSERT", "UPDATE", "DELETE"])

def get_widget_values(dbutils):
    widget_names = ["data_metastore_name", "description", "action"]
    return common.get_widget_values(widget_names, dbutils)

def validate_widget_values(d, dbutils, spark):
    assert d["data_metastore_name"] not in ("", None) and bool(re.match(r"\w+\.\w+", d[
        "data_metastore_name"])), "data_metastore_name cannot be empty and must be of form <database>.<table name>"
    assert spark._jsparkSession.catalog().tableExists(d["data_metastore_name"]), "table must exist"
   # assert spark.sql(f"describe extended {d['data_metastore_name']}").filter("col_name = 'Type'").collect()[0].asDict()["data_type"] == "EXTERNAL", "Table must be unmanaged/external. See here: https://docs.databricks.com/data/tables.html#managed-and-unmanaged-tables" # shouldn't require external on the participant workspaces
    return d

def enrich_widget_values(d, spark):
    details = spark.sql("DESCRIBE DETAIL snap.test2").collect()[0].asDict()
    d["location"] = details["location"]
    d["format"] = details["format"]
    return d


def form_event_and_send_to_control(d, destination, dbutils, spark):
    return common.form_event_and_send_to_control(d, "DATA", destination, dbutils, spark)
