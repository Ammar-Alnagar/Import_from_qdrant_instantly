import os
import pickle
import tkinter as tk
from tkinter import messagebox, ttk
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from google.auth.transport.requests import Request
import numpy as np
import uuid
from dotenv import load_dotenv

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
load_dotenv()
# OAuth configuration (replace with actual values)
CLIENT_CONFIG = {
            "installed": {
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
        self.window = tk.Tk()
        self.window.title("Google Drive to Qdrant Sync")
        self.window.geometry("400x300")
        
        # Initialize Qdrant client with API key
        self.qdrant = QdrantClient(
            url=os.getenv('QDRANT_URL'),
            api_key=os.getenv('QDRANT_API_KEY')
        )
        
        # Create UI
        self.create_ui()

    def create_ui(self):
        # Create and pack a frame for better organization
        main_frame = ttk.Frame(self.window, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Collection name label and entry
        collection_label = ttk.Label(main_frame, text="Collection Name:")
        collection_label.pack(pady=(0, 5))

        self.collection_entry = ttk.Entry(main_frame, width=30)
        self.collection_entry.pack(pady=(0, 20))
        self.collection_entry.insert(0, "")  # Default value

        # Sync button
        self.sync_button = ttk.Button(
            main_frame,
            text="Sign in with Google and Sync",
            command=self.handle_sync,
            width=25
        )
        self.sync_button.pack(pady=10)

        # Status label
        self.status_label = ttk.Label(main_frame, text="")
        self.status_label.pack(pady=10)

    def create_collection(self, collection_name):
        try:
            self.qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=128, distance=Distance.COSINE)
            )
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                return True  # Collection already exists, which is fine
            print(f"Collection creation error: {e}")
            return False

    def cleanup_token(self):
        token_file = 'token.pickle'
        try:
            if os.path.exists(token_file):
                os.remove(token_file)
                print("Token file cleaned up successfully")
        except Exception as e:
            print(f"Error cleaning up token file: {e}")

    def google_auth(self):
        creds = None
        token_file = 'token.pickle'
        
        # Load credentials from 'token.pickle' if they exist
        if os.path.exists(token_file):
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)

        # If credentials are not valid, refresh them or prompt the user to log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_config(CLIENT_CONFIG, SCOPES)
                creds = flow.run_local_server(port=8080)

            # Save the credentials for future use
            with open(token_file, 'wb') as token:
                pickle.dump(creds, token)

        return creds

    def fetch_drive_files(self):
        creds = self.google_auth()
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            pageSize=10, fields="files(id, name)").execute()
        items = results.get('files', [])
        if not items:
            messagebox.showinfo("Google Drive", "No files found.")
            return []
        else:
            return items

    def generate_vector(self, file_name):
        np.random.seed(hash(file_name) % (2**32))
        return np.random.random(128).tolist()

    def insert_into_qdrant(self, files, collection_name):
        points = []
        for file in files:
            vector = self.generate_vector(file['name'])
            point = PointStruct(
                id=str(uuid.uuid4()),
                vector=vector,
                payload={"file_name": file['name']}
            )
            points.append(point)
        
        try:
            self.qdrant.upsert(
                collection_name=collection_name,
                points=points
            )
            return True
        except Exception as e:
            messagebox.showerror("Error", f"Failed to sync to Qdrant: {str(e)}")
            return False

    def handle_sync(self):
        collection_name = self.collection_entry.get().strip()
        if not collection_name:
            messagebox.showerror("Error", "Please enter a collection name.")
            return

        self.status_label.config(text="Creating collection...")
        if not self.create_collection(collection_name):
            messagebox.showerror("Error", "Failed to create collection.")
            self.status_label.config(text="Failed to create collection")
            self.cleanup_token()
            return

        self.status_label.config(text="Fetching files...")
        try:
            files = self.fetch_drive_files()
            if files:
                self.status_label.config(text="Syncing to Qdrant...")
                if self.insert_into_qdrant(files, collection_name):
                    self.status_label.config(text="Sync completed successfully!")
                    messagebox.showinfo("Success", f"Files synced to collection '{collection_name}' successfully!")
            else:
                self.status_label.config(text="No files found")
        except Exception as e:
            self.status_label.config(text="Error occurred")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
        finally:
            # Clean up token file regardless of success or failure
            self.cleanup_token()

if __name__ == "__main__":
    app = DriveToQdrantApp()
    app.window.mainloop()