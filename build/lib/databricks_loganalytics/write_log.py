import requests
from databricks_loganalytics.build_signature import build_signature
import datetime
import os


def post_data(workspace_id: str, workspace_key: str, message: str):
    method = "POST"
    content_type = "application/json"
    resource = "/api/logs"
    rfc1123date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    content_length = len(message)
    signature = build_signature(
        workspace_id,
        workspace_key,
        rfc1123date,
        content_length,
        method,
        content_type,
        resource,
    )
    uri = (
        "https://"
        + workspace_id
        + ".ods.opinsights.azure.com"
        + resource
        + "?api-version=2016-04-01"
    )

    headers = {
        "content-type": content_type,
        "Authorization": signature,
        "Log-Type": os.environ.get('LOG_TABLE_NAME'),
        "x-ms-date": rfc1123date,
    }

    response = requests.post(uri, data=message, headers=headers)
    if response.status_code >= 200 and response.status_code <= 299:
        return "Accepted"
    else:
        print(f"Response code: {response.status_code}")
        print(f"Response: {response.content}")
        return "Not Accepted"
