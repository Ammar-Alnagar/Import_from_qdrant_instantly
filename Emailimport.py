import imaplib
import email
from email.header import decode_header
from qdrant_client import QdrantClient
import tkinter as tk
from tkinter import messagebox
import time
import cohere  # Cohere SDK
from dotenv import load_dotenv
import os
load_dotenv()
# Initialize Cohere client (replace 'your-api-key' with your actual API key)
co = cohere.Client(os.getenv('COHERE_API_KEY'))

# Step 1: Connect to email via IMAP
def get_emails(server, email_user, email_pass, label="INBOX", batch_size=10000):
    try:
        mail = imaplib.IMAP4_SSL(server)
        mail.login(email_user, email_pass)
        mail.select(label)
        
        # Fetch email IDs
        result, data = mail.search(None, 'ALL')
        email_ids = data[0].split()
        
        # Fetch emails in batches
        for i in range(0, len(email_ids), batch_size):
            batch = email_ids[i:i + batch_size]
            emails = []
            for e_id in batch:
                result, msg_data = mail.fetch(e_id, "(RFC822)")
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])
                        emails.append(msg)
            yield emails

        mail.logout()
    except Exception as e:
        messagebox.showerror("Error", f"Failed to fetch emails: {str(e)}")

# Step 2: Process and Vectorize emails using Cohere
def process_emails(emails):
    vectors = []
    for msg in emails:
        subject = decode_header(msg["subject"])[0][0]
        if isinstance(subject, bytes):
            subject = subject.decode()
        # Get email body
        body = get_email_body(msg)
        email_text = f"Subject: {subject}\nBody: {body}"
        
        # Vectorize using Cohere
        embedding = co.embed(texts=[email_text]).embeddings[0]
        vectors.append(embedding)
    return vectors

def get_email_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                return part.get_payload(decode=True).decode()
    else:
        return msg.get_payload(decode=True).decode()

# Step 3: Store in Qdrant
def store_in_qdrant(vectors, client, batch_size=10000):
    collection_name = "emails"
    for i in range(0, len(vectors), batch_size):
        batch_vectors = vectors[i:i + batch_size]
        client.insert(
            collection_name=collection_name,
            vectors=batch_vectors,
            payload=[{"email_id": i} for i in range(len(batch_vectors))]
        )

# Mapping of email providers to their IMAP servers
EMAIL_SERVERS = {
    "Gmail": "imap.gmail.com",
    "Outlook": "imap-mail.outlook.com",
    "Yahoo": "imap.mail.yahoo.com",
    "iCloud": "imap.mail.me.com"
}

# Example Usage:
def start_processing(email_user, email_pass, selected_server):
    try:
        # Qdrant client setup
        client = QdrantClient(
            url=os.getenv('QDRANT_URL'),
            api_key=os.getenv('QDRANT_API_KEY')
        )

        server = EMAIL_SERVERS[selected_server]  # Get the IMAP server based on user selection
        batch_size = 20000  # Adjust based on your needs

        # Process the emails in batches
        for emails_batch in get_emails(server, email_user, email_pass, batch_size=batch_size):
            start_time = time.time()
            print(f"Processing {len(emails_batch)} emails...")
            
            # Step 2: Preprocess and Vectorize emails
            vectors = process_emails(emails_batch)
            
            # Step 3: Store the vectors in Qdrant
            store_in_qdrant(vectors, client, batch_size=batch_size)
            
            print(f"Batch processed and stored in {(time.time() - start_time) / 60:.2f} minutes.")
        
        messagebox.showinfo("Success", "Emails processed and stored successfully.")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to process emails: {str(e)}")

# GUI Part
def create_gui():
    def on_submit():
        email_user = email_entry.get()
        email_pass = password_entry.get()
        selected_server = server_var.get()

        if not email_user or not email_pass or not selected_server:
            messagebox.showwarning("Input Error", "Please fill in all fields.")
        else:
            start_processing(email_user, email_pass, selected_server)

    # Create window
    root = tk.Tk()
    root.title("Email Processor")

    # Email input
    tk.Label(root, text="Email:").grid(row=0, column=0, padx=10, pady=10)
    email_entry = tk.Entry(root, width=40)
    email_entry.grid(row=0, column=1, padx=10, pady=10)

    # Password input
    tk.Label(root, text="Password:").grid(row=1, column=0, padx=10, pady=10)
    password_entry = tk.Entry(root, show="*", width=40)
    password_entry.grid(row=1, column=1, padx=10, pady=10)

    # Email server dropdown menu
    tk.Label(root, text="Email Provider:").grid(row=2, column=0, padx=10, pady=10)
    server_var = tk.StringVar(root)
    server_var.set("Gmail")  # Set the default option

    server_menu = tk.OptionMenu(root, server_var, *EMAIL_SERVERS.keys())
    server_menu.grid(row=2, column=1, padx=10, pady=10)

    # Submit button
    submit_button = tk.Button(root, text="Start", command=on_submit)
    submit_button.grid(row=3, column=1, padx=10, pady=10)

    # Start the GUI loop
    root.mainloop()

if __name__ == "__main__":
    create_gui()
