# __init__.py
import logging
import azure.functions as func
import os
import json
import datetime
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function for media blob upload and document analysis.')
    
    try:
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
        
        # Get any form data as metadata
        metadata = {}
        for key in req.form:
            if key != 'file':
                metadata[key] = req.form[key]
        
        # Get Document Intelligence credentials
        endpoint = os.environ.get("DOCUMENT_INTELLIGENCE_ENDPOINT")
        key = os.environ.get("DOCUMENT_INTELLIGENCE_KEY")
        
        # Get Blob Storage credentials
        blob_connection_string = os.environ.get("BLOB_STORAGE_CONNECTION_STRING")
        container_name = os.environ.get("BLOB_CONTAINER_NAME", "documents")
        
        # Check for missing required configuration
        missing_config = []
        if not endpoint:
            missing_config.append("DOCUMENT_INTELLIGENCE_ENDPOINT")
        if not key:
            missing_config.append("DOCUMENT_INTELLIGENCE_KEY")
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
        
        # Analyze the document
        result = analyze_document(endpoint, key, sas_url)
        
        # Prepare the response
        response_data = {
            "message": "Document processed successfully",
            "blob_name": blob_client.blob_name,
            "blob_url": blob_url,
            "sas_url": sas_url,
            "analysis_results": format_analysis_results(result)
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
    blob_name = f"{os.path.splitext(file_name)[0]}_{timestamp}{file_extension}"
    
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

def analyze_document(endpoint, key, document_url):
    """
    Analyze a document using Azure Document Intelligence (Form Recognizer)
    """
    # Initialize the Document Analysis Client
    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )
    
    # Start the document analysis (this uses the prebuilt-document model)
    poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-document", document_url)
    
    # Wait for the analysis to complete and get the result
    result = poller.result()
    
    return result

def format_analysis_results(result):
    """
    Format the analysis results for easier consumption
    """
    # Extract key-value pairs
    key_value_pairs = {}
    for kv_pair in result.key_value_pairs:
        if kv_pair.key and kv_pair.value:
            key_value_pairs[kv_pair.key.content] = kv_pair.value.content
    
    # Extract tables (if any)
    tables = []
    for table in result.tables:
        table_data = {
            "row_count": table.row_count,
            "column_count": table.column_count,
            "cells": []
        }
        
        for cell in table.cells:
            table_data["cells"].append({
                "row_index": cell.row_index,
                "column_index": cell.column_index,
                "text": cell.content,
                "row_span": cell.row_span,
                "column_span": cell.column_span
            })
        
        tables.append(table_data)
    
    # Extract document type if available
    document_type = None
    if hasattr(result, 'document_type') and result.document_type:
        document_type = result.document_type
    
    # Extract entities if available
    entities = []
    if hasattr(result, 'entities'):
        for entity in result.entities:
            entities.append({
                "category": entity.category,
                "content": entity.content
            })
    
    # Prepare the formatted results
    formatted_results = {
        "key_value_pairs": key_value_pairs,
        "tables": tables,
        "pages": result.page_count,
        "languages": [language.locale for language in result.languages],
        "document_type": document_type,
        "entities": entities
    }
    
    return formatted_results