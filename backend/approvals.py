from backend import common
import json
from datetime import datetime
import time
from delta.tables import *

def get_open_code_reviews(spark, dbutils):
    choices = []
    sql = f"""
      with events_in_flight as (
        select uuid as id, max(timestamp) as max_ts
          from control.events
         where event_type in ('CODE_REQUEST_REJECTED', 'CODE_REQUEST_APPROVED')
         group by uuid
      )
      select c.id, c.name, c.notebook_hash, c.participant_name, c.description, 
             c.default_compute, c.submitted_timestamp, c.notebook_language
        from control.approval_status a
       inner join control.code c on c.id = a.code_id
       left outer join events_in_flight f on f.id = a.code_id 
       where a.approval_status = "CODE_REQUEST_PENDING_APPROVAL"
         and (a.approval_timestamp > f.max_ts or f.max_ts is null)
       order by c.submitted_timestamp asc
    """

    df = spark.sql(sql)
    pending = df.collect()

    if (len(pending) == 0):
        dbutils.displayHTML("""
                <div style="color:red;font-size:28pt">There are no pending code requests. Please check back later.</div>
            """)
        return ([""], df)
    else:
        choices = [x.name for x in pending]
        return (choices, df)
    

# display_widgets
def display_widgets(spark, dbutils):
    dbutils.widgets.dropdown(name="00_action", defaultValue="REVIEWING", choices=['REVIEWING', 'APPROVE', 'REJECT'])
    (code_choices, df) = get_open_code_reviews(spark, dbutils)
    dbutils.widgets.multiselect(name="01_code_name", defaultValue=code_choices[0], choices=code_choices)
    dbutils.widgets.text(name="02_reason", defaultValue="")
    return df

def get_widget_values(dbutils):
    widget_names = ["action", "code_name", "reason"]
    return common.get_widget_values(widget_names, dbutils)

def validate_widget_values(d, dbutils, spark):
    return d

def enrich_widget_values(d, spark):
    return d

def send_to_control(code_id, d, event_type, dbutils, spark):
    j = json.dumps(d)
    row = {"event_type": event_type,
           "uuid": code_id,
           "payload": j,
           "timestamp": datetime.now(),
           "user": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user'),
           "notebook": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
           "hostname": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply(
               'browserHostName'),
           "api_version": "1"
           }
    schema = DeltaTable.forName(spark, "control.events").toDF().schema
    spark.createDataFrame([row], schema).write.format("delta").mode("append").saveAsTable("control.events")
    return True

def form_event_and_send_to_control(d, dbutils, spark):
    if ("REVIEWING") == d['action']:
        return False
    if ("REJECT") == d['action'] and "" == d['reason']:
        dbutils.displayHTML("""
                <div style="color:red;font-size:28pt">Please enter a reason when rejecting code requests.</div>
            """)
        return False
    code_list = dbutils.widgets.get("01_code_name").split(",")
    action = dbutils.widgets.get("00_action")
    event_type = "CODE_REQUEST_REJECTED"
    if (action == "APPROVE"):
        event_type = "CODE_REQUEST_APPROVED"
    for code_name in code_list:
        if (code_name != ""):
            id = spark.sql(f"""select id from control.code where name='{code_name}' """).collect()[0][0]
            send_to_control(id, d['reason'], event_type, dbutils, spark)
    return True
        
