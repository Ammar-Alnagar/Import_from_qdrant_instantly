import os
import pickle
import tkinter as tk
from tkinter import messagebox
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
        self.window.geometry("400x200")
        
        # Initialize Qdrant client with API key
        self.qdrant = self.qdrant = QdrantClient(
            url=os.getenv('QDRANT_URL'),
            api_key=os.getenv('QDRANT_API_KEY')
        )

        
        # Create collection if it doesn't exist
        try:
            self.qdrant.create_collection(
                collection_name="drive_files",
                vectors_config=VectorParams(size=128, distance=Distance.COSINE)
            )
        except Exception as e:
            print(f"Collection creation error: {e}")
        
        # Create UI
        self.create_ui()

    def create_ui(self):
        self.sync_button = tk.Button(
            self.window,
            text="Sign in with Google",
            command=self.handle_sync,
            height=2,
            width=20
        )
        self.sync_button.pack(pady=20)

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
                # Use a fixed port like 8080
                creds = flow.run_local_server(port=8080)  # Fixed port to maintain the same URI

            # Save the credentials to 'token.pickle' for future use
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

    def insert_into_qdrant(self, files):
        points = []
        for file in files:
            vector = self.generate_vector(file['name'])
            point = PointStruct(
                id=str(uuid.uuid4()),  # Generate a UUID for the point ID
                vector=vector,
                payload={"file_name": file['name']}
            )
            points.append(point)
        
        try:
            self.qdrant.upsert(
                collection_name="drive_files",
                points=points
            )
            messagebox.showinfo("Qdrant Sync", "Files synced to Qdrant successfully!")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to sync to Qdrant: {str(e)}")

    def handle_sync(self):
        try:
            files = self.fetch_drive_files()
            if files:
                self.insert_into_qdrant(files)
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

if __name__ == "__main__":
    app = DriveToQdrantApp()
    app.window.mainloop()
