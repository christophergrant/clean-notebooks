from pyspark.sql import SparkSession
import json
from uuid import uuid4
from datetime import datetime
import pandas as pd
import cleanroom_dbutils as dbutils

dbutils = dbutils.get_dbutils()

spark = SparkSession.builder.getOrCreate()

def get_widget_values(widget_names, dbutils) -> dict:
    d = dict()
    for i, widget_name in enumerate(widget_names):
      padded_name = f"{str(i).zfill(2)}_{widget_name}"
      d[widget_name] = dbutils.widgets.get(padded_name)
    return d


def form_event_and_send_to_control(d, event_type, destination, dbutils, spark):
    j = json.dumps(d)
    row = {"event_type": event_type,
           "uuid": str(uuid4()),
           "payload": j,
           "ts": datetime.now().isoformat(),
           "user": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user'),
           "notebook": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
           "hostname": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply(
               'browserHostName'),
           "api_version": "1"
           }
    spark.createDataFrame(pd.DataFrame.from_dict([row])).write.format("delta").mode("append").save(destination)
    return True