from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import get_config
from databricks_cli.sdk.service import WorkspaceService
from requests.exceptions import HTTPError
import json
from hashlib import sha256

import common

# display_widgets
def display_widgets(dbutils):
    dbutils.widgets.removeAll()
    dbutils.widgets.text(name="00_code_name", defaultValue="")
    dbutils.widgets.text(name="01_description", defaultValue="")
    dbutils.widgets.text(name="02_path_to_notebook", defaultValue="")
    dbutils.widgets.dropdown(name="03_action", defaultValue="INSERT", choices=["INSERT", "UPDATE", "DELETE"])
    dbutils.widgets.text(name="04_default_parameters", defaultValue="{}")
    dbutils.widgets.dropdown(name="05_default_compute", defaultValue="SMALL", choices=["SMALL", "MEDIUM", "LARGE"])
    dbutils.widgets.dropdown(name="06_who_can_run", defaultValue="ONLY_US", choices=["ONLY_US", "ANYONE"])


def get_widget_values(dbutils):
    widget_names = ["code_name", "description", "path_to_notebook", "action", "default_parameters", "default_compute", "who_can_run"]
    #widget_names = ["job_name", "path_to_notebook", "parameters", "compute_size"]
    return common.get_widget_values(widget_names, dbutils)

def validate_widget_values(d):
    def notebook_exists(path) -> bool:
        ws = WorkspaceService(_get_api_client(get_config()))
        flag = False
        try:
            status = ws.get_status(path)
            if status["object_type"] == "NOTEBOOK":
                flag = True
        except HTTPError as e:
            print(e.response.text)
        return flag

    assert notebook_exists(
        d["path_to_notebook"]), f"path must exist and point to a single notebook, got {d['path_to_notebook']}"
    try:
        json.loads(d["default_parameters"])
    except:
        raise AssertionError("JSON must be valid")
    return d

def enrich_widget_values(d):
    def get_notebook_contents(path) -> bytes:
        ws = WorkspaceService(_get_api_client(get_config()))
        export = ws.export_workspace(path)
        return export["content"]

    def get_notebook_language(path) -> str:
        ws = WorkspaceService(_get_api_client(get_config()))
        return ws.get_status(path)["language"]

    d["notebook_contents"] = get_notebook_contents(d["path_to_notebook"])
    d["notebook_contents_hash"] = sha256(d["notebook_contents"].encode("utf-8")).hexdigest()
    d["notebook_language"] = get_notebook_language(d["path_to_notebook"])
    return d

def form_event_and_send_to_control(d, destination, dbutils, spark):
    return common.form_event_and_send_to_control(d, "CODE", destination, dbutils, spark)

