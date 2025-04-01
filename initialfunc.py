from google import genai
from google.genai import types
import base64
import functions_framework
import os
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")

@functions_framework.cloud_event
def send_to_gemini_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]
    mime_type = data["contentType"]


    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Mime-Type: {mime_type}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location="us-central1",
    )

    document1 = types.Part.from_uri(
        file_uri=f"gs://{bucket}/{name}",
        mime_type=mime_type,
    )
    fileresponse = ""
    #TODO: add the rest of the gemini code asuming the text returned is fileresponse

    client = bigquery.Client()
    
    timestamp = datetime.now().isoformat()

    data = [{"filepath": f"gs://{bucket}/{name}", "details": fileresponse, "timestamp": timestamp}]

    errors = client.insert_rows_json(
      f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", data)
    print(errors)