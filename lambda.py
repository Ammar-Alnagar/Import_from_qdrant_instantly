import os
import pickle
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from google.auth.transport.requests import Request
import numpy as np
import uuid
from dotenv import load_dotenv
import time
from datetime import timedelta
import re

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
load_dotenv()

# OAuth configuration
CLIENT_CONFIG = {
    "web": {
        "client_id": os.getenv('GOOGLE_CLIENT_ID'),
        "project_id": os.getenv('GOOGLE_PROJECT_ID'),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": os.getenv('GOOGLE_CLIENT_SECRET'),
        "redirect_uris": ["http://localhost:8080/"]
    }
}

class DriveToQdrantApp:
    def __init__(self):
        self.qdrant = QdrantClient(
            url=os.getenv('QDRANT_URL'),
            api_key=os.getenv('QDRANT_API_KEY')
        )
        self.operation_times = {}

    def sanitize_collection_name(self, name: str) -> str:
        """Sanitize the collection name to meet Qdrant requirements."""
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        if not sanitized[0].isalpha():
            sanitized = 'c_' + sanitized
        return sanitized[:64]  # Limit length to 64 characters

    def format_time_delta(self, seconds):
        if seconds < 1:
            return f"{seconds * 1000:.0f}ms"
        elif seconds < 60:
            return f"{seconds:.1f}s"
        else:
            return str(timedelta(seconds=int(seconds)))

    def start_timer(self, operation):
        self.operation_times[operation] = time.time()

    def end_timer(self, operation):
        if operation in self.operation_times:
            duration = time.time() - self.operation_times[operation]
            del self.operation_times[operation]
            return duration
        return 0

    async def get_existing_files(self, collection_name):
        self.start_timer("fetch_existing")
        try:
            points = self.qdrant.scroll(
                collection_name=collection_name,
                limit=10000,
                with_payload=True,
                with_vectors=False
            )[0]
            existing_files = {point.payload["file_name"] for point in points}
            return existing_files
        except Exception as e:
            raise Exception(f"Error fetching existing files: {str(e)}")
        finally:
            self.end_timer("fetch_existing")

    async def handle_collection(self, collection_name):
        self.start_timer("collection_handle")
        try:
            collections = self.qdrant.get_collections().collections
            exists = any(c.name == collection_name for c in collections)

            if exists:
                existing_files = await self.get_existing_files(collection_name)
                return True, existing_files

            self.qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=1536, distance=Distance.COSINE)
            )
            return True, set()

        except Exception as e:
            raise Exception(f"Collection handling error: {str(e)}")
        finally:
            self.end_timer("collection_handle")

    def google_auth(self):
        self.start_timer("auth")
        try:
            creds = None
            token_file = 'token.pickle'

            if os.path.exists(token_file):
                with open(token_file, 'rb') as token:
                    creds = pickle.load(token)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_config(CLIENT_CONFIG, SCOPES)
                    creds = flow.run_local_server(port=8080)

                with open(token_file, 'wb') as token:
                    pickle.dump(creds, token)

            return creds
        except Exception as e:
            raise Exception(f"Authentication error: {str(e)}")
        finally:
            self.end_timer("auth")

    async def fetch_drive_files(self):
        self.start_timer("drive_fetch")
        try:
            creds = self.google_auth()
            service = build('drive', 'v3', credentials=creds)
            results = service.files().list(
                pageSize=10, fields="files(id, name)").execute()
            items = results.get('files', [])

            if not items:
                return []
            return items
        except Exception as e:
            raise Exception(f"Error fetching drive files: {str(e)}")
        finally:
            self.end_timer("drive_fetch")

    def generate_vector(self, file_name):
        np.random.seed(hash(file_name) % (2**32))
        return np.random.random(1536).tolist()

    async def insert_into_qdrant(self, files, collection_name, existing_files):
        self.start_timer("qdrant_insert")
        try:
            points = []
            new_files_count = 0

            for file in files:
                if file['name'] not in existing_files:
                    vector = self.generate_vector(file['name'])
                    point = PointStruct(
                        id=str(uuid.uuid4()),
                        vector=vector,
                        payload={"file_name": file['name']}
                    )
                    points.append(point)
                    new_files_count += 1

            if points:
                self.qdrant.upsert(
                    collection_name=collection_name,
                    points=points
                )
                return True, new_files_count
            return True, 0
        except Exception as e:
            raise Exception(f"Failed to sync to Qdrant: {str(e)}")
        finally:
            self.end_timer("qdrant_insert")

    async def run_sync(self, collection_name: str) -> dict:
        self.start_timer("total")
        try:
            sanitized_collection_name = self.sanitize_collection_name(collection_name)
            
            success, existing_files = await self.handle_collection(sanitized_collection_name)
            files = await self.fetch_drive_files()
            
            if files:
                success, new_files_count = await self.insert_into_qdrant(files, sanitized_collection_name, existing_files)
                total_time = self.format_time_delta(self.end_timer('total'))
                
                return {
                    "status": "success",
                    "collection_name": sanitized_collection_name,
                    "new_files_added": new_files_count,
                    "total_time": total_time,
                    "message": f"Sync completed: {new_files_count} new files added"
                }
            
            return {
                "status": "success",
                "collection_name": sanitized_collection_name,
                "new_files_added": 0,
                "total_time": self.format_time_delta(self.end_timer('total')),
                "message": "No files found in Drive"
            }
            
        except Exception as e:
            self.end_timer('total')
            raise Exception(str(e))

# Initialize the DriveToQdrantApp instance
drive_app = DriveToQdrantApp()

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    """
    try:
        # Handle path parameters (e.g., /sync/{username})
        if 'pathParameters' in event and event['pathParameters'] is not None:
            collection_name = event['pathParameters'].get('collection_name', 'default_collection')

        # Handle query parameters (e.g., /sync?username=johndoe)
        elif 'queryStringParameters' in event and event['queryStringParameters'] is not None:
            collection_name = event['queryStringParameters'].get('username', 'default_collection')
        
        # Fallback if no collection_name or username is provided
        else:
            collection_name = 'default_collection'

        # Run sync process
        sync_result = drive_app.run_sync(collection_name)

        return {
            'statusCode': 200,
            'body': json.dumps(sync_result)
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
