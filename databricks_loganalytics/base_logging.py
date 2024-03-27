import json
from databricks_loganalytics.write_log import post_data


def log_console_output(workspace_id: str, workspace_key: str, message: str):
    msg = json.dumps(message)
    return post_data(workspace_id, workspace_key, msg)
