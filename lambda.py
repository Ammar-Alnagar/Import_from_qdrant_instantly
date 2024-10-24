import os
import json
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
import numpy as np
import uuid
from google.oauth2 import service_account
from googleapiclient.discovery import build

def init_google_client():
    """Initialize Google Drive client with service account"""
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=['https://www.googleapis.com/auth/drive.metadata.readonly']
    )
    return build('drive', 'v3', credentials=credentials)

def init_qdrant_client():
    """Initialize Qdrant client"""
    return QdrantClient(
        url=os.environ['QDRANT_URL'],
        api_key=os.environ['QDRANT_API_KEY']
    )

def generate_vector(file_name):
    """Generate a deterministic vector based on filename"""
    np.random.seed(hash(file_name) % (2**32))
    return np.random.random(1536).tolist()

def lambda_handler(event, context):
    try:
        # Initialize clients
        drive_service = init_google_client()
        qdrant_client = init_qdrant_client()
        
        # Get collection name from event
        collection_name = (
            event.get('pathParameters', {}).get('collection_name') or
            event.get('queryStringParameters', {}).get('username') or
            'default_collection'
        )
        
        # Sanitize collection name
        collection_name = ''.join(c for c in collection_name.lower() if c.isalnum() or c == '_')
        if not collection_name[0].isalpha():
            collection_name = 'c_' + collection_name
        
        # Ensure collection exists
        collections = qdrant_client.get_collections().collections
        if not any(c.name == collection_name for c in collections):
            qdrant_client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=1536, distance=Distance.COSINE)
            )
        
        # Get existing files
        existing_points = qdrant_client.scroll(
            collection_name=collection_name,
            limit=10000,
            with_payload=True
        )[0]
        existing_files = {point.payload["file_name"] for point in existing_points}
        
        # Fetch Drive files
        results = drive_service.files().list(
            pageSize=100,
            fields="files(id, name)"
        ).execute()
        drive_files = results.get('files', [])
        
        # Process new files
        new_points = []
        for file in drive_files:
            if file['name'] not in existing_files:
                new_points.append(PointStruct(
                    id=str(uuid.uuid4()),
                    vector=generate_vector(file['name']),
                    payload={"file_name": file['name']}
                ))
        
        # Insert new files
        if new_points:
            qdrant_client.upsert(
                collection_name=collection_name,
                points=new_points
            )
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'status': 'success',
                'new_files_added': len(new_points),
                'collection_name': collection_name,
                'message': f"Sync completed: {len(new_points)} new files added"
            })
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")  # This will go to CloudWatch logs
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'status': 'error',
                'error': str(e)
            })
        }