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

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
load_dotenv()

# OAuth configuration (replace with actual values)
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

# Hardcoded collection name
COLLECTION_NAME = "my_default_collection"


class DriveToQdrantApp:
    def __init__(self):
        # Initialize Qdrant client with API key
        self.qdrant = QdrantClient(
            url=os.getenv('QDRANT_URL'),
            api_key=os.getenv('QDRANT_API_KEY')
        )

        # Timer variables
        self.operation_times = {}

    def format_time_delta(self, seconds):
        """Format time delta in a human-readable format"""
        if seconds < 1:
            return f"{seconds * 1000:.0f}ms"
        elif seconds < 60:
            return f"{seconds:.1f}s"
        else:
            return str(timedelta(seconds=int(seconds)))

    def start_timer(self, operation):
        """Start timing an operation"""
        self.operation_times[operation] = time.time()

    def end_timer(self, operation):
        """End timing an operation and return the duration"""
        if operation in self.operation_times:
            duration = time.time() - self.operation_times[operation]
            del self.operation_times[operation]
            return duration
        return 0

    def get_existing_files(self, collection_name):
        """Get list of existing file names in the collection"""
        self.start_timer("fetch_existing")
        try:
            points = self.qdrant.scroll(
                collection_name=collection_name,
                limit=10000,
                with_payload=True,
                with_vectors=False
            )[0]
            existing_files = {point.payload["file_name"] for point in points}
            print(f"Fetched existing files in {self.format_time_delta(self.end_timer('fetch_existing'))}")
            return existing_files
        except Exception as e:
            self.end_timer("fetch_existing")
            print(f"Error fetching existing files: {e}")
            return set()

    def handle_collection(self, collection_name):
        """Check if the collection exists, and create it if not."""
        self.start_timer("collection_handle")
        try:
            collections = self.qdrant.get_collections().collections
            exists = any(c.name == collection_name for c in collections)

            if exists:
                print(f"Collection exists. Fetching existing files...")
                existing_files = self.get_existing_files(collection_name)
                print(f"Collection check: {self.format_time_delta(self.end_timer('collection_handle'))}")
                return True, existing_files

            # Create the collection if it doesn't exist
            self.qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=1536, distance=Distance.COSINE)
            )
            print(f"Collection created in {self.format_time_delta(self.end_timer('collection_handle'))}")
            return True, set()

        except Exception as e:
            self.end_timer("collection_handle")
            print(f"Collection handling error: {e}")
            return False, set()

    def cleanup_token(self):
        """Remove the token file if it exists."""
        token_file = 'token.pickle'
        try:
            if os.path.exists(token_file):
                os.remove(token_file)
                print("Token file cleaned up successfully")
        except Exception as e:
            print(f"Error cleaning up token file: {e}")

    def google_auth(self):
        """Authenticate the user via Google OAuth."""
        self.start_timer("auth")
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

        print(f"Authentication: {self.format_time_delta(self.end_timer('auth'))}")
        return creds

    def fetch_drive_files(self):
        """Fetch a list of files from Google Drive."""
        self.start_timer("drive_fetch")
        creds = self.google_auth()
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            pageSize=10, fields="files(id, name)").execute()
        items = results.get('files', [])

        fetch_time = self.format_time_delta(self.end_timer('drive_fetch'))
        print(f"Drive fetch: {fetch_time}")

        if not items:
            print("No files found.")
            return []
        return items

    def generate_vector(self, file_name):
        """Generate a random vector for a file name."""
        np.random.seed(hash(file_name) % (2**32))
        return np.random.random(1536).tolist()

    def insert_into_qdrant(self, files, collection_name, existing_files):
        """Insert new files into the Qdrant collection."""
        self.start_timer("qdrant_insert")
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
            try:
                self.qdrant.upsert(
                    collection_name=collection_name,
                    points=points
                )
                print(f"Qdrant insert: {self.format_time_delta(self.end_timer('qdrant_insert'))}")
                return True, new_files_count
            except Exception as e:
                self.end_timer("qdrant_insert")
                print(f"Failed to sync to Qdrant: {e}")
                return False, 0
        self.end_timer("qdrant_insert")
        return True, 0

    def run(self):
        """Main workflow to handle syncing files from Google Drive to Qdrant."""
        self.start_timer("total")
        collection_name = COLLECTION_NAME
        print(f"Using collection: {collection_name}")

        success, existing_files = self.handle_collection(collection_name)
        if not success:
            print("Failed to handle collection.")
            self.cleanup_token()
            return

        print("Fetching files from Google Drive...")
        try:
            files = self.fetch_drive_files()
            if files:
                print("Syncing to Qdrant...")
                success, new_files_count = self.insert_into_qdrant(files, collection_name, existing_files)

                total_time = self.format_time_delta(self.end_timer('total'))

                if success:
                    if new_files_count > 0:
                        print(f"Sync completed in {total_time}: {new_files_count} new files added.")
                    else:
                        print("No new files to add.")
                else:
                    print("Sync failed.")
        except Exception as e:
            print(f"Error syncing: {e}")
            self.cleanup_token()


if __name__ == '__main__':
    app = DriveToQdrantApp()
    app.run()
