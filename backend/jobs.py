from backend import common
import json

schedule_to_value = {
    "ad-hoc": "ad-hoc",
    "daily": "0 0 6 * * ? *", # 6AM every day
    "hourly": "0 0 * * * ? *",
    "weekly": "0 0 6 * * 1 *" # 6AM every Sunday
}

def get_approved_codes(spark, dbutils):
    choices = []
    sql = f"""
      select c.id, c.name, c.notebook_hash, c.participant_name, c.description, 
             c.default_compute, c.submitted_timestamp, c.notebook_language
        from control.approval_status a
       inner join control.code c on c.id = a.code_id
       where a.approval_status = "CODE_REQUEST_APPROVED"
       order by c.name asc
    """

    df = spark.sql(sql)
    pending = df.collect()

    if (len(pending) == 0):
        dbutils.displayHTML("""
                <div style="color:red;font-size:28pt">There are no approved code requests. Please check back later.</div>
            """)
        return [""]
    else:
        choices = [x.name for x in pending]
        return choices   
    

# display_widgets
def display_widgets(spark, dbutils):
    dbutils.widgets.removeAll()
    dbutils.widgets.text(name="00_job_name", defaultValue="")


    code_choices = get_approved_codes(spark, dbutils)

    dbutils.widgets.dropdown(name="01_code_name", defaultValue=code_choices[0], choices=code_choices)
    dbutils.widgets.text(name="02_parameters", defaultValue="{}")
    dbutils.widgets.dropdown(name="03_compute", defaultValue="Small",
                             choices=["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"])
    dbutils.widgets.dropdown(name="04_schedule", defaultValue="ad-hoc", choices=["ad-hoc", "hourly", "daily", "weekly"])

def get_widget_values(dbutils):
    widget_names = ["job_name", "code_name", "parameters", "compute", "schedule"]
    return common.get_widget_values(widget_names, dbutils)

def validate_widget_values(d, dbutils, spark):
    if (d["job_name"] in ("", None)):
        dbutils.displayHTML("""
                <div style="color:red;font-size:28pt">Please enter a job name</div>
            """)
        raise AssertionError("job_name cannot be empty")
    special_characters = """"'!@#$%^&*()+?=,<>/"""
    s = d["job_name"]
    if any(c in special_characters for c in s):
        dbutils.displayHTML(f"""
                <div style="color:red;font-size:28pt">Please enter a job name without using special characters {special_characters}</div>
            """)
        raise Exception(f"Job name should not have any of theses special characters {special_characters}")

    try:
        json.loads(d["parameters"])
    except:
        dbutils.displayHTML(f"""
                <div style="color:red;font-size:28pt">Please check the parameters value. The 'parameters' JSON must be valid"</div>
            """)
        raise AssertionError("'parameters' JSON must be valid")
    return d

def enrich_widget_values(d, spark):
    d['schedule'] = schedule_to_value[d['schedule']]
    return d

def form_event_and_send_to_control(d, dbutils, spark):
    return common.form_event_and_send_to_control(d, "JOB", dbutils, spark)

def process(dbutils, spark):
    try:
        data = get_widget_values(dbutils)
        validated_data = validate_widget_values(data, dbutils, spark)
        enriched_data = enrich_widget_values(validated_data, spark)
        form_event_and_send_to_control(enriched_data, dbutils, spark) and dbutils.displayHTML("<h1>Request sent.</h1>")    
    except Exception as e:
        if not "InputWidgetNotDefined" in str(e): print(e) 