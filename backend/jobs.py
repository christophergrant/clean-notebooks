from backend import common
from delta.tables import *
import json
from pyspark.sql.functions import col

schema = "control"

def get_approved_codes(spark, dbutils):
    choices = []
    sql = f"""
      select distinct c.name
        from {schema}.approval_status a
       inner join {schema}.code c on c.id = a.code_id
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
    
def show_default_parameters(spark, dbutils, code_name):
    rows = list()
    if (code_name != ""):
        code_table_columns = DeltaTable.forName(spark, "control.code").toDF().columns
        if ("default_parameters" in code_table_columns):
            df_code = DeltaTable.forName(spark, f"{schema}.code").toDF()
            df_code = df_code.select(["default_parameters", "name"]).where(
                (col("name") == code_name) & (col("default_parameters") != ""))
            code_records = df_code.collect()
            if (len(code_records) > 0):
                param_string = code_records[0][0]
                param_dict = json.loads(param_string)
                rows.append(param_dict)
                df_params = spark.createDataFrame(rows)
                param_html = df_params.toPandas().to_html(index=False)
                dbutils.displayHTML(f"""
                    <table border=1>
                        <tr>
                            <th>Default Parameters for {code_name}</th>
                        </tr>
                        <tr>
                            <td>{param_html}</td>
                        </tr>
                    </table>
                """)


# display_widgets
def display_widgets(spark, dbutils):
    dbutils.widgets.removeAll()
    dbutils.widgets.text(name="00_job_name", defaultValue="")


    code_choices = get_approved_codes(spark, dbutils)

    dbutils.widgets.dropdown(name="01_code_name", defaultValue=code_choices[0], choices=code_choices)
    dbutils.widgets.text(name="02_parameters", defaultValue="{}")
    dbutils.widgets.dropdown(name="03_compute", defaultValue="Small",
                             choices=["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"])
    
    code_name = dbutils.widgets.get("01_code_name")
    show_default_parameters(spark, dbutils, code_name)
                

def get_widget_values(dbutils):
    widget_names = ["job_name", "code_name", "parameters", "compute"] 
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
    d['schedule'] = "ad-hoc"
    return d

def form_event_and_send_to_control(d, dbutils, spark):
    return common.form_event_and_send_to_control(d, "JOB", dbutils, spark)

def process(data, spark, dbutils):
    try:
        validated_data = validate_widget_values(data, dbutils, spark)
        enriched_data = enrich_widget_values(validated_data, spark)
        form_event_and_send_to_control(enriched_data, dbutils, spark) and dbutils.displayHTML("<h1>Request sent.</h1>")    
    except Exception as e:
        if not "InputWidgetNotDefined" in str(e): print(e) 