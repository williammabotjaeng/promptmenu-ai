# __init__.py
import logging
import azure.functions as func
import os
import json
import re
import datetime
import uuid
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function for receipt and bill analysis.')
    
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
        
        # Extract user info from form data
        user_info = {}
        metadata = {}
        
        # Process form fields
        for key in req.form:
            if key != 'file':
                # User info fields
                if key in ['owner', 'displayName', 'fullName', 'email', 'userId', 'restaurant']:
                    user_info[key] = req.form[key]
                # Metadata fields
                else:
                    metadata[key] = req.form[key]
        
        # Get Document Intelligence credentials
        endpoint = os.environ.get("DOCUMENT_INTELLIGENCE_ENDPOINT")
        key = os.environ.get("DOCUMENT_INTELLIGENCE_KEY")
        
        # Get Blob Storage credentials
        blob_connection_string = os.environ.get("BLOB_STORAGE_CONNECTION_STRING")
        container_name = os.environ.get("BLOB_CONTAINER_NAME", "receipts")
        
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
        
        # Extract raw documents
        raw_documents = None
        if hasattr(result, 'documents') and result.documents:
            # Print raw documents for debugging
            print(f"Result documents: {result.documents}")
            
            # Extract raw document data as a list of dictionaries
            raw_docs_list = []
            for doc in result.documents:
                doc_dict = {}
                # Get doc_type and confidence if available
                doc_dict["doc_type"] = doc.doc_type if hasattr(doc, "doc_type") else None
                doc_dict["confidence"] = doc.confidence if hasattr(doc, "confidence") else None
                
                # Extract fields if available
                if hasattr(doc, "fields"):
                    fields_dict = {}
                    for field_name, field in doc.fields.items():
                        # Create field data dictionary
                        field_dict = {
                            "value_type": field.value_type if hasattr(field, "value_type") else None,
                            "confidence": field.confidence if hasattr(field, "confidence") else None
                        }
                        
                        # Extract value based on value_type
                        if hasattr(field, "value_type"):
                            if field.value_type == "string" and hasattr(field, "value_string"):
                                field_dict["value"] = field.value_string
                            elif field.value_type == "number" and hasattr(field, "value_number"):
                                field_dict["value"] = field.value_number
                            elif field.value_type == "integer" and hasattr(field, "value_integer"):
                                field_dict["value"] = field.value_integer
                            elif field.value_type == "date" and hasattr(field, "value_date"):
                                field_dict["value"] = str(field.value_date)
                            elif field.value_type == "time" and hasattr(field, "value_time"):
                                field_dict["value"] = str(field.value_time)
                            elif field.value_type == "phoneNumber" and hasattr(field, "value_phone_number"):
                                field_dict["value"] = field.value_phone_number
                            elif field.value_type == "selectionMark" and hasattr(field, "value_selection_mark"):
                                field_dict["value"] = field.value_selection_mark
                            elif field.value_type == "countryRegion" and hasattr(field, "value_country_region"):
                                field_dict["value"] = field.value_country_region
                            elif field.value_type == "array" and hasattr(field, "value_array"):
                                # Handle arrays (like items in a receipt)
                                items_list = []
                                for item in field.value_array:
                                    if hasattr(item, "value_type") and item.value_type == "object" and hasattr(item, "value_object"):
                                        item_dict = {}
                                        for item_field_name, item_field in item.value_object.items():
                                            # Include field content and value
                                            item_dict[item_field_name] = {
                                                "content": item_field.content if hasattr(item_field, "content") else None
                                            }
                                            
                                            # Get the typed value
                                            if hasattr(item_field, "value_type"):
                                                value_attr = f"value_{item_field.value_type}"
                                                if hasattr(item_field, value_attr):
                                                    value = getattr(item_field, value_attr)
                                                    # Handle date objects
                                                    if hasattr(value, "isoformat"):
                                                        item_dict[item_field_name]["value"] = value.isoformat()
                                                    else:
                                                        item_dict[item_field_name]["value"] = value
                                        
                                        items_list.append(item_dict)
                                
                                field_dict["items"] = items_list
                        
                        # Add content when available
                        if hasattr(field, "content"):
                            field_dict["content"] = field.content
                        
                        fields_dict[field_name] = field_dict
                    
                    doc_dict["fields"] = fields_dict
                
                raw_docs_list.append(doc_dict)
            
            raw_documents = raw_docs_list
        
        # Save to database
        db_response = save_raw_documents_to_db(
            blob_client.blob_name,
            blob_url,
            sas_url,
            user_info,
            metadata,
            raw_documents
        )
        
        # Determine receipt type for response
        receipt_type = "unknown"
        if raw_documents and len(raw_documents) > 0:
            doc_type = raw_documents[0].get("doc_type")
            if doc_type == "receipt":
                receipt_type = "receipt"
                # Check if it might be a restaurant bill
                if "fields" in raw_documents[0]:
                    fields = raw_documents[0]["fields"]
                    # Check for restaurant indicators
                    if "MerchantName" in fields:
                        merchant = fields["MerchantName"].get("value", "").lower() if isinstance(fields["MerchantName"].get("value"), str) else ""
                        if any(keyword in merchant for keyword in ["restaurant", "cafe", "bar", "grill"]):
                            receipt_type = "restaurant_bill"
                    # Check for tip field (common in restaurant bills)
                    if "Tip" in fields or "ServiceCharge" in fields:
                        receipt_type = "restaurant_bill"
            elif doc_type == "invoice":
                receipt_type = "invoice"
        
        # Prepare the response
        response_data = {
            "message": "Receipt processed successfully",
            "blob_name": blob_client.blob_name,
            "blob_url": blob_url,
            "document_key": db_response["document_key"],
            "document_id": db_response["id"],
            "receipt_type": receipt_type,
            "raw_document_count": len(raw_documents) if raw_documents else 0
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
    blob_name = f"receipt_{timestamp}{file_extension}"
    
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
    Analyze a document using Azure Document Intelligence
    """
    # Initialize the Document Intelligence Client
    document_analysis_client = DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )
    
    # Log available methods for debugging
    methods = [method for method in dir(document_analysis_client) if method.startswith('begin_analyze')]
    logging.info(f"Available analyze methods: {methods}")
    
    # Try to analyze with different model types based on availability, prioritizing receipt models
    try:
        # First try the prebuilt-receipt model (best for receipts)
        analyze_request = AnalyzeDocumentRequest(url_source=document_url)
        poller = document_analysis_client.begin_analyze_document(
            "prebuilt-receipt", analyze_request
        )
        logging.info("Using prebuilt-receipt model")
    except Exception as e1:
        logging.warning(f"Receipt model failed: {str(e1)}")
        try:
            # Try the prebuilt-invoice model (good for restaurant bills)
            analyze_request = AnalyzeDocumentRequest(url_source=document_url)
            poller = document_analysis_client.begin_analyze_document(
                "prebuilt-invoice", analyze_request
            )
            logging.info("Using prebuilt-invoice model")
        except Exception as e2:
            logging.warning(f"Invoice model failed: {str(e2)}")
            try:
                # Try the prebuilt-document model
                analyze_request = AnalyzeDocumentRequest(url_source=document_url)
                poller = document_analysis_client.begin_analyze_document(
                    "prebuilt-document", analyze_request
                )
                logging.info("Using prebuilt-document model")
            except Exception as e3:
                logging.warning(f"Document model failed: {str(e3)}")
                # Fall back to layout model
                analyze_request = AnalyzeDocumentRequest(url_source=document_url)
                poller = document_analysis_client.begin_analyze_document(
                    "prebuilt-layout", analyze_request
                )
                logging.info("Using prebuilt-layout model")
    
    # Wait for the analysis to complete and get the result
    result = poller.result()
    
    return result

def save_raw_documents_to_db(blob_name, blob_url, sas_url, user_info, metadata, raw_documents):
    """
    Save the raw documents directly to the database without complex processing
    """
    # Create timestamp
    timestamp = datetime.datetime.utcnow()
    
    # Generate a simple document key
    receipt_type = "receipt"
    if raw_documents and len(raw_documents) > 0 and raw_documents[0].get("doc_type"):
        if raw_documents[0]["doc_type"] == "receipt":
            receipt_type = "receipt"
        elif raw_documents[0]["doc_type"] == "invoice":
            receipt_type = "invoice"
    
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
    
    doc_key = f"{receipt_type}_{username}_{timestamp.strftime('%Y%m%d%H%M%S')}"
    
    # Create a document record with raw data
    record = {
        "_id": str(uuid.uuid4()),
        "document_key": doc_key,
        "blob_name": blob_name,
        "blob_url": blob_url,
        "sas_url": sas_url,  # Store SAS URL (note: this will expire)
        "upload_timestamp": timestamp.isoformat(),
        "receipt_type": receipt_type,
        "user_info": user_info or {},
        "metadata": metadata or {},
        "raw_documents": raw_documents or []
    }
    
    # Extract some key information from raw documents if available
    if raw_documents and len(raw_documents) > 0 and "fields" in raw_documents[0]:
        fields = raw_documents[0]["fields"]
        
        # Extract merchant name
        if "MerchantName" in fields:
            merchant_info = fields["MerchantName"]
            if "value" in merchant_info:
                record["merchant"] = merchant_info["value"]
        elif "VendorName" in fields:
            vendor_info = fields["VendorName"]
            if "value" in vendor_info:
                record["merchant"] = vendor_info["value"]
        
        # Extract total
        if "Total" in fields:
            total_info = fields["Total"]
            if "value" in total_info:
                record["total"] = total_info["value"]
        elif "InvoiceTotal" in fields:
            total_info = fields["InvoiceTotal"]
            if "value" in total_info:
                record["total"] = total_info["value"]
        
        # Extract date
        if "TransactionDate" in fields:
            date_info = fields["TransactionDate"]
            if "value" in date_info:
                record["date"] = date_info["value"]
        elif "InvoiceDate" in fields:
            date_info = fields["InvoiceDate"]
            if "value" in date_info:
                record["date"] = date_info["value"]
    
    # Connect to the database and save
    try:
        # Get Cosmos DB/MongoDB connection
        load_dotenv()
        cosmos_db_connection_string = os.environ.get("COSMOS_DB_CONNECTION_STRING")
        database_name = os.environ.get("DATABASE_NAME", "ReceiptDatabase")
        container_name = os.environ.get("CONTAINER_NAME", "Receipts")
        
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
        raise

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