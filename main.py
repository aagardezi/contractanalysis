import functions_framework
import os

import base64
import vertexai
from vertexai.generative_models import GenerativeModel, Part, FinishReason
import vertexai.preview.generative_models as generative_models
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")


# Triggered by a change in a storage bucket
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

    vertexai.init(project="genaillentsearch", location="us-central1")
    model = GenerativeModel(
      "gemini-1.5-flash-001",generation_config={"response_mime_type": "application/json"}
    )

    file1 = Part.from_uri(mime_type=mime_type, uri=f"gs://{bucket}/{name}")
    text1 = """Can you act like a legal advisor and analyse this contract and classify the contract. 
    Make sure you assess the entire document before you make the decision. 
    Make the output a json with a field: contract_type. Also include in the output json parties with roles; 
    value of agreement with currency;  summary of termination clauses; confidentiality provisions; material adverse change. 
    The json response should match the following structure
    {\"contract_type\": \"\",
    \"parties\":[\"name\": \"\",\"role\": \"\"],
    \"value\": \"\",
    \"termination_clauses\": \"\",
    \"confidentiality_provisions\": \"\"}"""

    responses = model.generate_content(
      [file1, text1],
      generation_config=generation_config,
      safety_settings=safety_settings,
      stream=False,
    )

    fileresponse = ''  
    # for response in responses:
    #   print(response.text)
    #   if response.text == "```":
    #     continue
    #   if response.text == "json":
    #     continue
    #   fileresponse += response.text
    fileresponse = responses.candidates[0].content.parts[0].text
    
    client = bigquery.Client()

    # Set the project ID and dataset ID.
    # project_id = "genaillentsearch"
    # dataset_id = "contractanalysis"

    # # Set the table ID.
    # table_id = "contractdetail"

    timestamp = datetime.utcnow().isoformat()

    data = [{"filepath": f"gs://{bucket}/{name}", "details": fileresponse, "timestamp": timestamp}]

    errors = client.insert_rows_json(
      f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", data)
    print(errors)


generation_config = {
    "max_output_tokens": 8192,
    "temperature": 1,
    "top_p": 0.95,
    "response_mime_type": "application/json"
}

safety_settings = {
    generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
}
    
