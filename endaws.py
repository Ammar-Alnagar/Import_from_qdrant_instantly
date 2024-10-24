# app/main.py
import os
import pickle
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
from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any
import re
import logging
from mangum import Mangum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

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

# Response Models
class SuccessResponse(BaseModel):
    status: str = "success"
    collection_name: str
    new_files_added: int
    total_time: str
    message: str

class ErrorResponse(BaseModel):
    status: str = "error"
    error_code: str
    message: str
    details: Dict[str, Any] = None

app = FastAPI(
    title="Drive to Qdrant API",
    description="API for syncing Google Drive files to Qdrant collections",
    version="1.0.0"
)

class DriveToQdrantApp:
    def __init__(self):
        self.qdrant = QdrantClient(
            url=os.getenv('QDRANT_URL'),
            api_key=os.getenv('QDRANT_API_KEY')
        )
        self.operation_times = {}

    def sanitize_collection_name(self, name: str) -> str:
        """Sanitize the collection name to meet Qdrant requirements"""
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        if not sanitized[0].isalpha():
            sanitized = 'c_' + sanitized
        return sanitized[:64]

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
            logger.error(f"Error fetching existing files: {e}")
            raise HTTPException(
                status_code=500,
                detail={"error_code": "FETCH_ERROR", "message": str(e)}
            )
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
            logger.error(f"Collection handling error: {e}")
            raise HTTPException(
                status_code=500,
                detail={"error_code": "COLLECTION_ERROR", "message": str(e)}
            )
        finally:
            self.end_timer("collection_handle")

    def google_auth(self):
        self.start_timer("auth")
        try:
            creds = None
            token_file = '/tmp/token.pickle' if os.getenv('AWS_LAMBDA_FUNCTION_NAME') else 'token.pickle'

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
            logger.error(f"Authentication error: {e}")
            raise HTTPException(
                status_code=401,
                detail={"error_code": "AUTH_ERROR", "message": str(e)}
            )
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
            logger.error(f"Error fetching drive files: {e}")
            raise HTTPException(
                status_code=500,
                detail={"error_code": "DRIVE_ERROR", "message": str(e)}
            )
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
            logger.error(f"Failed to sync to Qdrant: {e}")
            raise HTTPException(
                status_code=500,
                detail={"error_code": "SYNC_ERROR", "message": str(e)}
            )
        finally:
            self.end_timer("qdrant_insert")

    async def run_sync(self, user_name: str) -> Dict[str, Any]:
        self.start_timer("total")
        try:
            collection_name = self.sanitize_collection_name(user_name)
            
            success, existing_files = await self.handle_collection(collection_name)
            files = await self.fetch_drive_files()
            
            if files:
                success, new_files_count = await self.insert_into_qdrant(files, collection_name, existing_files)
                total_time = self.format_time_delta(self.end_timer('total'))
                
                return SuccessResponse(
                    collection_name=collection_name,
                    new_files_added=new_files_count,
                    total_time=total_time,
                    message=f"Sync completed: {new_files_count} new files added"
                )
            
            return SuccessResponse(
                collection_name=collection_name,
                new_files_added=0,
                total_time=self.format_time_delta(self.end_timer('total')),
                message="No files found in Drive"
            )
            
        except Exception as e:
            self.end_timer('total')
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(
                status_code=500,
                detail={"error_code": "UNKNOWN_ERROR", "message": str(e)}
            )

# Initialize the DriveToQdrantApp instance
drive_app = DriveToQdrantApp()

@app.get("/sync/{username}")
async def sync_drive_to_qdrant(
    username: str = Path(..., min_length=1, max_length=64, regex="^[a-zA-Z0-9_-]+$")
):
    """
    Sync Google Drive files to Qdrant collection for a specific user
    
    Parameters:
    - username: String to be used as the collection name (from URL path)
    
    Returns:
    - JSON with sync results including status and statistics
    """
    try:
        result = await drive_app.run_sync(username)
        return JSONResponse(content=result.dict())
    except HTTPException as e:
        error_response = ErrorResponse(
            error_code=e.detail.get("error_code", "UNKNOWN_ERROR"),
            message=e.detail.get("message", str(e)),
            details=e.detail
        )
        return JSONResponse(
            status_code=e.status_code,
            content=error_response.dict()
        )

@app.get("/health")
async def health_check():
    """Health check endpoint for AWS"""
    return {"status": "healthy"}

# Create handler for AWS Lambda
handler = Mangum(app)