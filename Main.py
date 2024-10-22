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
import time
from datetime import datetime, timedelta

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
        self.window.geometry("400x350")  # Made window slightly taller for time display
        
        # Initialize Qdrant client with API key
        self.qdrant = QdrantClient(
            url=os.getenv('QDRANT_URL'),
            api_key=os.getenv('QDRANT_API_KEY')
        )
        
        # Timer variables
        self.start_time = None
        self.operation_times = {}
        
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

        # Time label
        self.time_label = ttk.Label(main_frame, text="")
        self.time_label.pack(pady=5)

    def format_time_delta(self, seconds):
        """Format time delta in a human-readable format"""
        if seconds < 1:
            return f"{seconds*1000:.0f}ms"
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
            self.time_label.config(text=f"Fetched existing files in {self.format_time_delta(self.end_timer('fetch_existing'))}")
            return existing_files
        except Exception as e:
            self.end_timer("fetch_existing")
            print(f"Error fetching existing files: {e}")
            return set()

    def handle_collection(self, collection_name):
        self.start_timer("collection_handle")
        try:
            collections = self.qdrant.get_collections().collections
            exists = any(c.name == collection_name for c in collections)
            
            if exists:
                self.status_label.config(text="Collection exists, fetching existing files...")
                existing_files = self.get_existing_files(collection_name)
                self.time_label.config(text=f"Collection check: {self.format_time_delta(self.end_timer('collection_handle'))}")
                return True, existing_files
            
            self.qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=128, distance=Distance.COSINE)
            )
            self.time_label.config(text=f"Collection created in {self.format_time_delta(self.end_timer('collection_handle'))}")
            return True, set()
            
        except Exception as e:
            self.end_timer("collection_handle")
            print(f"Collection handling error: {e}")
            return False, set()

    def cleanup_token(self):
        token_file = 'token.pickle'
        try:
            if os.path.exists(token_file):
                os.remove(token_file)
                print("Token file cleaned up successfully")
        except Exception as e:
            print(f"Error cleaning up token file: {e}")

    def google_auth(self):
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

        self.time_label.config(text=f"Authentication: {self.format_time_delta(self.end_timer('auth'))}")
        return creds

    def fetch_drive_files(self):
        self.start_timer("drive_fetch")
        creds = self.google_auth()
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            pageSize=10, fields="files(id, name)").execute()
        items = results.get('files', [])
        
        fetch_time = self.format_time_delta(self.end_timer('drive_fetch'))
        self.time_label.config(text=f"Drive fetch: {fetch_time}")
        
        if not items:
            messagebox.showinfo("Google Drive", "No files found.")
            return []
        return items

    def generate_vector(self, file_name):
        np.random.seed(hash(file_name) % (2**32))
        return np.random.random(128).tolist()

    def insert_into_qdrant(self, files, collection_name, existing_files):
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
                self.time_label.config(text=f"Qdrant insert: {self.format_time_delta(self.end_timer('qdrant_insert'))}")
                return True, new_files_count
            except Exception as e:
                self.end_timer("qdrant_insert")
                messagebox.showerror("Error", f"Failed to sync to Qdrant: {str(e)}")
                return False, 0
        self.end_timer("qdrant_insert")
        return True, 0

    def handle_sync(self):
        self.start_timer("total")
        collection_name = self.collection_entry.get().strip()
        if not collection_name:
            messagebox.showerror("Error", "Please enter a collection name.")
            return

        self.status_label.config(text="Checking collection...")
        success, existing_files = self.handle_collection(collection_name)
        if not success:
            messagebox.showerror("Error", "Failed to handle collection.")
            self.status_label.config(text="Failed to handle collection")
            self.cleanup_token()
            return

        self.status_label.config(text="Fetching files from Drive...")
        try:
            files = self.fetch_drive_files()
            if files:
                self.status_label.config(text="Syncing to Qdrant...")
                success, new_files_count = self.insert_into_qdrant(files, collection_name, existing_files)
                
                total_time = self.format_time_delta(self.end_timer('total'))
                
                if success:
                    if new_files_count > 0:
                        status_msg = (
                            f"Sync completed in {total_time}:\n"
                            f"- {new_files_count} new files added\n"
                            f"- {len(existing_files)} existing files kept\n"
                            f"- Total files: {len(existing_files) + new_files_count}"
                        )
                        self.status_label.config(text=f"Sync completed: {new_files_count} new files added")
                        messagebox.showinfo("Success", status_msg)
                    else:
                        status_msg = (
                            f"Operation completed in {total_time}\n"
                            "No new files to add\n"
                            f"Collection already contains {len(existing_files)} files"
                        )
                        self.status_label.config(text="No new files to add")
                        messagebox.showinfo("Success", status_msg)
            else:
                self.end_timer('total')
                self.status_label.config(text="No files found in Drive")
        except Exception as e:
            self.end_timer('total')
            self.status_label.config(text="Error occurred")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
        finally:
            self.cleanup_token()

if __name__ == "__main__":
    app = DriveToQdrantApp()
    app.window.mainloop()