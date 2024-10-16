import os
import pickle
import tkinter as tk
from tkinter import messagebox
import webbrowser
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
import threading
import secrets
import base64

# OAuth configuration
GOOGLE_OAUTH_CONFIG = {
    "web": {
        "client_id": "YOUR_CLIENT_ID",
        "project_id": "YOUR_PROJECT_ID",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "YOUR_CLIENT_SECRET",
        "redirect_uris": ["http://localhost:8080"]
    }
}

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Parse the callback URL
            parsed = urllib.parse.urlparse(self.path)
            query_params = urllib.parse.parse_qs(parsed.query)
            
            # Store the auth code
            if 'code' in query_params:
                self.server.auth_code = query_params['code'][0]
                self.server.state = query_params.get('state', [None])[0]
                
                # Send success response
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                success_html = """
                <html>
                    <body style="text-align: center; padding-top: 50px;">
                        <h2>Authentication Successful!</h2>
                        <p>You can close this window and return to the application.</p>
                        <script>setTimeout(function() { window.close(); }, 3000);</script>
                    </body>
                </html>
                """
                self.wfile.write(success_html.encode())
            else:
                raise Exception("No authorization code received")
                
            # Stop the server
            threading.Thread(target=self.server.shutdown).start()
            
        except Exception as e:
            self.send_error(400, str(e))
            threading.Thread(target=self.server.shutdown).start()

    def log_message(self, format, *args):
        # Suppress logging
        pass

class DriveToQdrantApp:
    def __init__(self):
        self.window = tk.Tk()
        self.window.title("Google Drive to Qdrant Sync")
        self.window.geometry("400x200")
        
        # Initialize Qdrant client
        self.qdrant = QdrantClient(host="localhost", port=6333)
        
        # Create collection if it doesn't exist
        try:
            self.qdrant.create_collection(
                collection_name="drive_files",
                vectors_config=VectorParams(size=128, distance=Distance.COSINE)
            )
        except Exception:
            pass
        
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
        
        self.status_label = tk.Label(
            self.window,
            text="Click button to sign in with Google",
            wraplength=350
        )
        self.status_label.pack(pady=10)
    
    def get_google_credentials(self):
        # Generate a random state value for security
        state = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('utf-8')
        
        # Create a Flow instance
        flow = Flow.from_client_config(
            GOOGLE_OAUTH_CONFIG,
            scopes=SCOPES,
            redirect_uri="http://localhost:8080"
        )
        
        # Generate authorization URL
        auth_url, _ = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='select_account',  # This forces the account selection screen
            state=state
        )
        
        # Start local server to receive the OAuth callback
        server = HTTPServer(('localhost', 8080), OAuthCallbackHandler)
        server.auth_code = None
        server.state = None
        
        # Open browser for authentication
        webbrowser.open(auth_url)
        
        # Update status
        self.status_label.config(text="Waiting for Google authentication...")
        
        # Wait for the callback
        server.serve_forever()
        
        # Verify state and get auth code
        if server.state != state:
            raise Exception("State mismatch. Possible security issue.")
        
        if not server.auth_code:
            raise Exception("Failed to get authorization code")
        
        # Exchange auth code for credentials
        flow.fetch_token(code=server.auth_code)
        
        # Save credentials
        creds = flow.credentials
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
        
        return creds

    def fetch_drive_files(self, service):
        results = []
        page_token = None
        
        while True:
            try:
                response = service.files().list(
                    pageSize=1000,
                    fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
                    pageToken=page_token
                ).execute()
                
                files = response.get('files', [])
                results.extend(files)
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                    
            except Exception as e:
                self.status_label.config(text=f"Error fetching files: {str(e)}")
                return []
                
        return results
    
    def store_in_qdrant(self, files):
        for file in files:
            vector = [0] * 128  # Placeholder vector
            
            try:
                self.qdrant.upsert(
                    collection_name="drive_files",
                    points=[PointStruct(
                        id=hash(file['id']),
                        vector=vector,
                        payload={
                            "name": file['name'],
                            "mime_type": file['mimeType'],
                            "modified_time": file['modifiedTime'],
                            "size": file.get('size', '0'),
                            "drive_id": file['id']
                        }
                    )]
                )
            except Exception as e:
                print(f"Error storing file {file['name']}: {str(e)}")
    
    def handle_sync(self):
        try:
            self.status_label.config(text="Starting Google Sign-In...")
            creds = self.get_google_credentials()
            
            service = build('drive', 'v3', credentials=creds)
            
            self.status_label.config(text="Fetching files from Google Drive...")
            files = self.fetch_drive_files(service)
            
            if files:
                self.status_label.config(text=f"Storing {len(files)} files in Qdrant...")
                self.store_in_qdrant(files)
                self.status_label.config(text="Sync completed successfully!")
                self.sync_button.config(text="Sync Again")
            else:
                self.status_label.config(text="No files found in Google Drive")
                
        except Exception as e:
            messagebox.showerror("Error", str(e))
            self.status_label.config(text="Sync failed. See error message.")
    
    def run(self):
        self.window.mainloop()

if __name__ == "__main__":
    app = DriveToQdrantApp()
    app.run()