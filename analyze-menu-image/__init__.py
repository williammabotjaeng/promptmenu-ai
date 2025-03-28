# __init__.py
import logging
import azure.functions as func
import os
import json
import re
import datetime
import uuid
from azure.core.credentials import AzureKeyCredential
from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.vision.imageanalysis.models import VisualFeatures
from openai import AzureOpenAI
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function for menu image analysis.')
    
    try:
        # Try to import dotenv and load from .env file
        try:
            from dotenv import load_dotenv
            load_dotenv()
            logging.info("Loaded environment variables from .env file")
        except ImportError:
            logging.info("python-dotenv not installed, using environment variables directly")
        except Exception as e:
            logging.warning(f"Could not load .env file: {str(e)}")
        
        # Check if the request contains a file upload
        if not req.files:
            return func.HttpResponse(
                json.dumps({"error": "No file uploaded. Please upload a file using multipart/form-data."}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Get the uploaded file
        file_data = req.files.get('file')
        if not file_data:
            return func.HttpResponse(
                json.dumps({"error": "No file found with the key 'file'. Please ensure your form uses 'file' as the field name."}),
                status_code=400,
                mimetype="application/json"
            )
        
        file_name = file_data.filename
        file_content = file_data.read()
        
        # Extract user info and dietary preferences from form data
        user_info = {}
        metadata = {}
        dietary_restrictions = []
        health_conditions = []
        
        # Process form fields
        for key in req.form:
            if key != 'file':
                # User info fields
                if key in ['owner', 'displayName', 'fullName', 'email', 'userId', 'restaurant']:
                    user_info[key] = req.form[key]
                # Dietary restrictions
                elif key == 'dietary_restrictions':
                    dietary_restrictions = req.form[key].split(',')
                # Health conditions
                elif key == 'health_conditions':
                    health_conditions = req.form[key].split(',')
                # Metadata fields
                else:
                    metadata[key] = req.form[key]
        
        # Get Computer Vision credentials
        vision_endpoint = os.environ.get("VISION_ENDPOINT")
        vision_key = os.environ.get("VISION_KEY")
        
        # Get OpenAI credentials
        openai_endpoint = os.environ.get("OPENAI_ENDPOINT") 
        openai_key = os.environ.get("OPENAI_KEY")
        openai_deployment = os.environ.get("OPENAI_DEPLOYMENT", "gpt-35-turbo")
        
        # Get Blob Storage credentials
        blob_connection_string = os.environ.get("BLOB_STORAGE_CONNECTION_STRING")
        container_name = os.environ.get("BLOB_CONTAINER_NAME", "menu-images")
        
        # Check for missing required configuration
        missing_config = []
        if not vision_endpoint:
            missing_config.append("VISION_ENDPOINT")
        if not vision_key:
            missing_config.append("VISION_KEY")
        if not openai_endpoint:
            missing_config.append("OPENAI_ENDPOINT")
        if not openai_key:
            missing_config.append("OPENAI_KEY")
        if not blob_connection_string:
            missing_config.append("BLOB_STORAGE_CONNECTION_STRING")
        
        if missing_config:
            error_message = f"Missing required configuration: {', '.join(missing_config)}"
            logging.error(error_message)
            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=500,
                mimetype="application/json"
            )
        
        # Upload to blob storage
        blob_client, blob_url = upload_to_blob_storage(
            blob_connection_string, 
            container_name, 
            file_name, 
            file_content,
            metadata
        )
        
        # Generate SAS URL for the blob
        sas_url = generate_sas_url(
            blob_connection_string,
            container_name,
            blob_client.blob_name
        )
        
        # Step 1: Analyze the menu image with Computer Vision
        vision_results = analyze_menu_image(vision_endpoint, vision_key, file_content)
        
        # Step 2: Get dish analysis and dietary advice from OpenAI
        openai_results = get_dietary_advice(
            openai_endpoint, 
            openai_key, 
            openai_deployment,
            vision_results, 
            dietary_restrictions, 
            health_conditions
        )
        
        # Combine all results
        analysis_results = {
            "vision_analysis": vision_results,
            "dietary_analysis": openai_results
        }
        
        # Save to database
        db_response = save_menu_analysis_to_db(
            blob_client.blob_name,
            blob_url,
            sas_url,
            user_info,
            metadata,
            dietary_restrictions,
            health_conditions,
            analysis_results
        )
        
        # Prepare the response
        response_data = {
            "message": "Menu image analysis completed successfully",
            "blob_name": blob_client.blob_name,
            "blob_url": blob_url,
            "document_key": db_response["document_key"],
            "document_id": db_response["id"],
            "dish_name": vision_results.get("dish_name", "Unknown dish"),
            "analysis": openai_results
        }
        
        return func.HttpResponse(
            json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )
    
    except ValueError as ve:
        logging.error(f"Invalid request format: {str(ve)}")
        return func.HttpResponse(
            json.dumps({"error": f"Invalid request format: {str(ve)}"}),
            status_code=400,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"An unexpected error occurred: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

def upload_to_blob_storage(connection_string, container_name, file_name, file_content, metadata=None):
    """
    Upload a file to Azure Blob Storage
    """
    # Generate a unique file name with timestamp
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    # Extract file extension
    file_extension = os.path.splitext(file_name)[1] if '.' in file_name else ''
    # Create a unique blob name
    blob_name = f"menu_{timestamp}{file_extension}"
    
    # Initialize the BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Get the container client - create if it doesn't exist
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()
    
    # Get the blob client
    blob_client = container_client.get_blob_client(blob_name)
    
    # Upload the file
    blob_client.upload_blob(file_content, overwrite=True, metadata=metadata)
    
    # Get the blob URL
    blob_url = blob_client.url
    
    return blob_client, blob_url

def generate_sas_url(connection_string, container_name, blob_name):
    """
    Generate a SAS URL for accessing the blob
    """
    # Parse the connection string to get account information
    account_dict = {item.split('=', 1)[0]: item.split('=', 1)[1] for item in connection_string.split(';') if '=' in item}
    account_name = account_dict.get('AccountName')
    account_key = account_dict.get('AccountKey')
    
    if not account_name or not account_key:
        raise ValueError("Could not extract account name and key from connection string")
    
    # Calculate token expiry time (1 hour from now)
    expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    
    # Create SAS token with read permission
    sas_token = generate_blob_sas(
        account_name=account_name,
        account_key=account_key,
        container_name=container_name,
        blob_name=blob_name,
        permission=BlobSasPermissions(read=True),
        expiry=expiry
    )
    
    # Construct the full SAS URL
    sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    
    return sas_url

def analyze_menu_image(vision_endpoint, vision_key, image_data):
    """
    Analyze a menu image using Azure AI Vision Image Analysis 4.0
    """
    # Initialize the Vision client
    client = ImageAnalysisClient(
        endpoint=vision_endpoint,
        credential=AzureKeyCredential(vision_key)
    )
    
    # Define visual features to analyze
    visual_features = [
        VisualFeatures.TAGS,       # Get tags/labels for the image
        VisualFeatures.OBJECTS,    # Identify objects in the image
        VisualFeatures.CAPTION,    # Get a caption describing the image
        VisualFeatures.READ,       # Extract text from the image (menu items, descriptions)
    ]
    
    try:
        # Analyze the image
        result = client.analyze(
            image_data=image_data,
            visual_features=visual_features,
            language="en"
        )
        
        # Process the results to identify the most likely food item
        food_tags = []
        dish_name = ""
        menu_text = ""
        
        # Extract tags related to food
        food_related_categories = ["food", "cuisine", "dish", "meal", "ingredient", "dessert", "fruit", "vegetable", "meat"]
        if result.tags:
            for tag in result.tags.list:
                if any(category in tag.name.lower() for category in food_related_categories):
                    food_tags.append({"name": tag.name, "confidence": tag.confidence})
        
        # Sort food tags by confidence
        food_tags.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Get the dish name from high-confidence food tags or caption
        if food_tags and food_tags[0]["confidence"] > 0.7:
            dish_name = food_tags[0]["name"]
        elif result.caption and "food" in result.caption.text.lower():
            # Extract the main subject from the caption
            caption_text = result.caption.text.lower()
            # Remove common phrases like "a photo of", "an image of", etc.
            caption_text = re.sub(r'(a|an) (photo|picture|image) of', '', caption_text).strip()
            caption_text = re.sub(r'a plate of', '', caption_text).strip()
            dish_name = caption_text
        
        # Extract text from the image (menu description)
        if result.read and result.read.blocks:
            for block in result.read.blocks:
                for line in block.lines:
                    menu_text += line.text + "\n"
        
        # If we have menu text but no dish name, try to extract it from the menu text
        if menu_text and not dish_name:
            # Look for capitalized text that might be a dish name
            lines = menu_text.split('\n')
            for line in lines:
                if re.match(r'^[A-Z][a-zA-Z\s]+$', line.strip()):
                    dish_name = line.strip()
                    break
        
        # Format the results
        vision_results = {
            "dish_name": dish_name,
            "caption": result.caption.text if result.caption else "",
            "caption_confidence": result.caption.confidence if result.caption else 0,
            "food_tags": food_tags,
            "menu_text": menu_text,
            "objects": [{"name": obj.tags[0].name, "confidence": obj.tags[0].confidence} for obj in result.objects.list] if result.objects else []
        }
        
        return vision_results
    
    except Exception as e:
        logging.error(f"Error analyzing image with Vision: {str(e)}")
        return {"error": str(e)}

def get_dietary_advice(openai_endpoint, openai_key, openai_deployment, vision_results, dietary_restrictions, health_conditions):
    """
    Get dietary advice using Azure OpenAI
    """
    # Initialize the OpenAI client
    openai_client = AzureOpenAI(
        azure_endpoint=openai_endpoint,
        api_key=openai_key,
        api_version="2023-05-15"
    )
    
    # Prepare the prompt
    dish_name = vision_results.get("dish_name", "Unknown dish")
    food_tags = vision_results.get("food_tags", [])
    food_tags_text = ", ".join([tag["name"] for tag in food_tags[:5]])
    menu_text = vision_results.get("menu_text", "")
    
    # Create a list of dietary restrictions if any
    restrictions_text = ""
    if dietary_restrictions:
        restrictions_text = "Dietary restrictions: " + ", ".join(dietary_restrictions)
        
    # Create a list of health conditions if any
    conditions_text = ""
    if health_conditions:
        conditions_text = "Health conditions: " + ", ".join(health_conditions)
    
    # Create the prompt for OpenAI
    prompt = f"""
    You are a nutrition expert analyzing a food item from a menu.

    The food item appears to be: {dish_name}

    Additional food tags identified: {food_tags_text}

    Text extracted from the menu:
    {menu_text}

    {restrictions_text}
    {conditions_text}

    Please provide:
    1. A brief description of this dish
    2. Likely ingredients (list the main ingredients)
    3. Estimated calorie count (provide a range)
    4. Nutritional information (protein, carbs, fat estimates)
    5. Dietary considerations (is it vegetarian, vegan, gluten-free, etc.)
    6. Health considerations (how this dish might affect someone with the mentioned health conditions)
    7. Recommendations (whether to eat it, portion control advice, etc.)

    Format your response as JSON with the following structure:
    {{"description": "...", "ingredients": ["...", "..."], "calories": "...", "nutrition": {{"protein": "...", "carbs": "...", "fat": "..."}}, "dietary_info": "...", "health_warnings": "...", "recommendations": "..."}}
    """
    
    try:
        # Call OpenAI
        response = openai_client.chat.completions.create(
            model=openai_deployment,
            messages=[
                {"role": "system", "content": "You are a nutrition expert providing detailed food analysis in JSON format."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=800,
            response_format={"type": "json_object"}
        )
        
        # Extract and parse the JSON response
        content = response.choices[0].message.content
        analysis_result = json.loads(content)
        
        # Add some metadata
        analysis_result["dish_analyzed"] = dish_name
        analysis_result["analysis_timestamp"] = datetime.datetime.utcnow().isoformat()
        
        return analysis_result
        
    except Exception as e:
        logging.error(f"Error getting dietary advice from OpenAI: {str(e)}")
        return {
            "error": str(e),
            "dish_analyzed": dish_name,
            "analysis_timestamp": datetime.datetime.utcnow().isoformat()
        }

def save_menu_analysis_to_db(blob_name, blob_url, sas_url, user_info, metadata, 
                             dietary_restrictions, health_conditions, analysis_results):
    """
    Save the menu analysis to the database
    """
    # Create timestamp
    timestamp = datetime.datetime.utcnow()
    
    # Get dish name from analysis
    dish_name = "unknown_dish"
    if "vision_analysis" in analysis_results and "dish_name" in analysis_results["vision_analysis"]:
        dish_name = analysis_results["vision_analysis"]["dish_name"]
    dish_name = convert_to_snake_case(dish_name)
    
    # Get username if available
    username = "unknown"
    if user_info:
        if user_info.get("displayName"):
            username = user_info.get("displayName")
        elif user_info.get("fullName"):
            username = user_info.get("fullName")
        elif user_info.get("owner"):
            username = user_info.get("owner")
        
        username = convert_to_snake_case(username)
    
    # Create a document key
    doc_key = f"menu_{dish_name}_{username}_{timestamp.strftime('%Y%m%d%H%M%S')}"
    
    # Create a document record with all analysis data
    record = {
        "_id": str(uuid.uuid4()),
        "document_key": doc_key,
        "blob_name": blob_name,
        "blob_url": blob_url,
        "sas_url": sas_url,
        "upload_timestamp": timestamp.isoformat(),
        "dish_name": analysis_results.get("vision_analysis", {}).get("dish_name", "Unknown dish"),
        "user_info": user_info or {},
        "metadata": metadata or {},
        "dietary_restrictions": dietary_restrictions or [],
        "health_conditions": health_conditions or [],
        "analysis_results": analysis_results
    }
    
    # If we have nutritional info, add it at the top level for easier querying
    if "dietary_analysis" in analysis_results and isinstance(analysis_results["dietary_analysis"], dict):
        dietary = analysis_results["dietary_analysis"]
        if "calories" in dietary:
            record["calories"] = dietary["calories"]
        if "nutrition" in dietary and isinstance(dietary["nutrition"], dict):
            record["nutrition"] = dietary["nutrition"]
        if "dietary_info" in dietary:
            record["dietary_info"] = dietary["dietary_info"]
        if "health_warnings" in dietary:
            record["health_warnings"] = dietary["health_warnings"]
    
    # Connect to the database and save
    try:
        # Get Cosmos DB/MongoDB connection
        cosmos_db_connection_string = os.environ.get("COSMOS_DB_CONNECTION_STRING")
        database_name = os.environ.get("DATABASE_NAME", "MenuDatabase")
        container_name = os.environ.get("CONTAINER_NAME", "MenuAnalysis")
        
        # Connect to database
        client = MongoClient(
            cosmos_db_connection_string,
            socketTimeoutMS=60000,
            connectTimeoutMS=60000
        )
        
        # Get database and collection
        database = client[database_name]
        collection = database[container_name]
        
        # Insert document
        result = collection.insert_one(record)
        
        return {
            "id": str(result.inserted_id),
            "document_key": doc_key,
            "status": "saved"
        }
    
    except Exception as e:
        logging.error(f"Error saving to database: {str(e)}")
        # Return partial success so front-end can still show results
        return {
            "id": "error-saving",
            "document_key": doc_key,
            "status": "analysis-completed-but-not-saved",
            "error": str(e)
        }

def convert_to_snake_case(text):
    """
    Convert a display name or text to snake_case format
    Example: "John Doe" -> "john_doe"
    """
    # Replace spaces and special characters with underscores
    s1 = re.sub(r'[^\w\s]', '_', str(text))
    # Replace one or more spaces with a single underscore
    s2 = re.sub(r'\s+', '_', s1)
    # Convert to lowercase
    return s2.lower()