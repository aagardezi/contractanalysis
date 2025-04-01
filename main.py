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

    text1 = types.Part.from_text(text="""Can you act like a legal advisor and analyse this contract and classify the contract. 
    Make sure you assess the entire document before you make the decision. 
    Make the output a json with a field: contract_type. Also include in the output json parties with roles; 
    value of agreement with currency;  summary of termination clauses; confidentiality provisions; material adverse change. Can you infer material adverse change based on the context of the entire contract and summarise it. Also include a summary of the either contract/document.""")
    si_text1 = """You are a legal advisor who is doing a through analysis of the documents/contracts provided. Make sure your response is based on the entire document/contract you are analysing and only that, nothing else. Make sure you process the entire document/contract before you respond. If anything is unclear please point it out in the response."""

    model = "gemini-2.0-flash-001"
    contents = [
        types.Content(
            role="user",
            parts=[
                document1,
                text1
            ]
        )
    ]

    generate_content_config = types.GenerateContentConfig(
        temperature = 1,
        top_p = 0.95,
        max_output_tokens = 8192,
        response_modalities = ["TEXT"],
        safety_settings = [types.SafetySetting(
            category="HARM_CATEGORY_HATE_SPEECH",
            threshold="OFF"
        ),types.SafetySetting(
            category="HARM_CATEGORY_DANGEROUS_CONTENT",
            threshold="OFF"
        ),types.SafetySetting(
            category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
            threshold="OFF"
        ),types.SafetySetting(
            category="HARM_CATEGORY_HARASSMENT",
            threshold="OFF"
        )],
        response_mime_type = "application/json",
        response_schema = {"type":"OBJECT","properties":{"contract_type":{"type":"STRING"},"value":{"type":"NUMBER"},"termination_clauses":{"type":"STRING"},"confidentiality_provisions":{"type":"STRING"},"material_adverse_change":{"type":"STRING"},"summary":{"type":"STRING"},"parties":{"type":"ARRAY","items":{"type":"OBJECT","properties":{"name":{"type":"STRING","nullable":"FALSE"},"role":{"type":"STRING","nullable":"FALSE"}}}}},"required":["contract_type","value","termination_clauses","confidentiality_provisions","parties","material_adverse_change","summary"]},
        system_instruction=[types.Part.from_text(text=si_text1)],
    )

    response = client.models.generate_content(model = model, contents = contents, config = generate_content_config)

    fileresponse = response.text
    
    client = bigquery.Client()
    
    timestamp = datetime.now().isoformat()

    data = [{"filepath": f"gs://{bucket}/{name}", "details": fileresponse, "timestamp": timestamp}]

    errors = client.insert_rows_json(
      f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", data)
    print(errors)