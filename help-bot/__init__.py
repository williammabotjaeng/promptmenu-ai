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
        conversation_id = req_body.get('conversation_id')
        watermark = req_body.get('watermark')
        
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
    
    # Get Direct Line secret from environment variable
    direct_line_secret = os.environ.get("DIRECT_LINE_SECRET")
    if not direct_line_secret:
        return func.HttpResponse(
            "DIRECT_LINE_SECRET environment variable is not configured",
            status_code=500
        )
    
    # Communicate with the bot
    try:
        bot_responses, new_conversation_id, new_watermark = send_to_bot(
            direct_line_secret, message, conversation_id, watermark
        )
        
        return func.HttpResponse(
            json.dumps({
                "responses": bot_responses,
                "conversation_id": new_conversation_id,
                "watermark": new_watermark
            }),
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error communicating with bot: {str(e)}")
        return func.HttpResponse(
            f"Error communicating with bot: {str(e)}",
            status_code=500
        )

def send_to_bot(direct_line_secret, message, conversation_id=None, watermark=None):
    base_url = "https://directline.botframework.com/v3/directline"
    headers = {
        "Authorization": f"Bearer {direct_line_secret}",
        "Content-Type": "application/json"
    }
    
    # Start or continue conversation
    if not conversation_id:
        response = requests.post(f"{base_url}/conversations", headers=headers)
        if response.status_code != 201:
            raise Exception(f"Failed to create conversation: {response.text}")
        
        data = response.json()
        conversation_id = data["conversationId"]
        watermark = None
    
    # Send message
    payload = {
        "type": "message",
        "from": {"id": "user1"},
        "text": message
    }
    
    send_response = requests.post(
        f"{base_url}/conversations/{conversation_id}/activities",
        headers=headers,
        json=payload
    )
    
    if send_response.status_code != 200 and send_response.status_code != 201:
        raise Exception(f"Failed to send message: {send_response.text}")
    
    # Wait for a short time to allow the bot to process the message
    import time
    time.sleep(2)
    
    # Get response
    get_response = requests.get(
        f"{base_url}/conversations/{conversation_id}/activities",
        headers=headers,
        params={"watermark": watermark} if watermark else {}
    )
    
    if get_response.status_code != 200:
        raise Exception(f"Failed to get activities: {get_response.text}")
    
    activities = get_response.json().get("activities", [])
    new_watermark = get_response.json().get("watermark")
    
    # Filter for bot responses that came after our message
    bot_responses = []
    user_message_found = False
    
    for activity in activities:
        # If we find our user message, start collecting bot responses after it
        if activity.get("from", {}).get("id") == "user1" and activity.get("text") == message:
            user_message_found = True
            continue
        
        # Collect bot responses that come after our message
        if user_message_found and activity.get("from", {}).get("id") != "user1":
            bot_responses.append(activity.get("text", ""))
    
    return bot_responses, conversation_id, new_watermark