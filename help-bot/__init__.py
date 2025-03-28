import logging
import azure.functions as func
import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        req_body = req.get_json()
        message = req_body.get('message')
        
        if not message:
            return func.HttpResponse(
                "Please provide a message in the request body",
                status_code=400
            )
    except ValueError:
        return func.HttpResponse(
            "Please pass a valid JSON in the request body with 'message' field",
            status_code=400
        )
    
    # Get Language Service API key from environment variable
    subscription_key = os.environ.get("LANGUAGE_SERVICE_KEY")
    if not subscription_key:
        return func.HttpResponse(
            "LANGUAGE_SERVICE_KEY environment variable is not configured",
            status_code=500
        )
    
    # Communicate with the Language service
    try:
        response = query_language_service(subscription_key, message)
        
        # Check if we got a default answer (indicated by confidenceScore of 0.0)
        is_default_answer = False
        if response.get("answers") and response["answers"][0].get("confidenceScore") == 0.0:
            is_default_answer = True
            
        return func.HttpResponse(
            json.dumps({
                "response": response,
                "is_default_answer": is_default_answer,
                "answer_text": response.get("answers", [{}])[0].get("answer", "No answer available")
            }),
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error communicating with Language service: {str(e)}")
        return func.HttpResponse(
            f"Error communicating with Language service: {str(e)}",
            status_code=500
        )

def query_language_service(subscription_key, question):
    # Language service endpoint
    url = "https://promptmenuqna.cognitiveservices.azure.com/language/:query-knowledgebases"
    
    # Request parameters
    params = {
        "projectName": "promptmenuhelp",
        "api-version": "2021-10-01",
        "deploymentName": "production"
    }
    
    # Request headers
    headers = {
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Content-Type": "application/json"
    }
    
    # Request payload
    payload = {
        "top": 3,
        "question": question,
        "includeUnstructuredSources": True,
        "confidenceScoreThreshold": 0.3,
        "answerSpanRequest": {
            "enable": True,
            "topAnswersWithSpan": 1,
            "confidenceScoreThreshold": 0.5
        }
    }
    
    # Send request to Language service
    response = requests.post(url, headers=headers, params=params, json=payload)
    
    if response.status_code != 200:
        raise Exception(f"Failed to query Language service: {response.text}")
    
    # Process the response
    response_data = response.json()
    
    # Return the full response for more detailed processing if needed
    # This allows the client to handle default answers differently if desired
    return response_data