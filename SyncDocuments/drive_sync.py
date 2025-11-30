"""
Google Drive synchronization utilities.
Retrieves files from Google Drive and checks which documents are new.
"""

from __future__ import annotations

import io
import os
from pathlib import Path
from typing import List, Dict, Optional

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

if __package__ in {None, ""}:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from SyncDocuments.config import settings


def connect_drive():
    """
    Connect to Google Drive API.
    
    Returns:
        Google Drive service object
    """
    creds = None
    
    # Load existing token if available
    if settings.google_drive_token.exists():
        creds = Credentials.from_authorized_user_file(
            str(settings.google_drive_token), 
            settings.google_drive_scopes
        )
    
    # If no valid credentials, authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not settings.google_drive_credentials.exists():
                raise FileNotFoundError(
                    f"Credentials file not found: {settings.google_drive_credentials}"
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(settings.google_drive_credentials),
                settings.google_drive_scopes
            )
            creds = flow.run_local_server(port=0)
        
        # Save credentials for next run
        settings.google_drive_token.parent.mkdir(parents=True, exist_ok=True)
        with open(settings.google_drive_token, "w") as token:
            token.write(creds.to_json())
    
    # Build and return service
    service = build("drive", "v3", credentials=creds)
    return service


def list_files_in_folder(service, folder_id: str) -> List[Dict]:
    """
    List all files in a Google Drive folder.
    
    Args:
        service: Google Drive service object
        folder_id: ID of the folder to list files from
        
    Returns:
        List of file dictionaries with id, name, and mimeType
    """
    query = f"'{folder_id}' in parents and trashed=false"
    
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType, modifiedTime)"
    ).execute()
    
    files = results.get("files", [])
    return files


def download_file(service, file_id: str, filename: str) -> Path:
    """
    Download a file from Google Drive.
    
    Args:
        service: Google Drive service object
        file_id: ID of the file to download
        filename: Name to save the file as
        
    Returns:
        Path to the downloaded file
    """
    # Ensure download directory exists
    settings.download_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = settings.download_dir / filename
    
    # Download the file
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(str(file_path), 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    return file_path


def get_existing_pdf_ids(supabase_client, log_callback=None) -> set:
    """
    Get set of PDF IDs that already exist in the database.
    Queries Supabase to extract unique pdf_id values from the metadata JSONB field.
    
    The pdf_id is stored in the metadata JSONB field as: metadata->>'pdf_id'
    This function queries all rows and extracts unique pdf_id values.
    
    Args:
        supabase_client: Supabase client object
        log_callback: Optional callback function for logging messages
        
    Returns:
        Set of existing PDF IDs (filename without .pdf extension)
    """
    def log(message: str):
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    try:
        log("📊 Querying Supabase for existing PDF IDs from metadata...")
        
        # Query Supabase to get all rows with metadata
        # We select all rows and extract pdf_id from the JSONB metadata field
        # The metadata field is JSONB, so we can access pdf_id directly
        response = supabase_client.table(settings.supabase_table).select("metadata").execute()
        
        existing_ids = set()
        total_rows = len(response.data) if response.data else 0
        log(f"  Found {total_rows} row(s) in database")
        
        for row in response.data:
            metadata = row.get("metadata")
            if metadata:
                # Handle both dict and JSONB formats
                # Supabase typically returns JSONB as a dict in Python
                if isinstance(metadata, dict):
                    pdf_id = metadata.get("pdf_id")
                else:
                    # If metadata is a string, try to parse it
                    import json
                    try:
                        metadata_dict = json.loads(metadata) if isinstance(metadata, str) else metadata
                        pdf_id = metadata_dict.get("pdf_id") if isinstance(metadata_dict, dict) else None
                    except (json.JSONDecodeError, TypeError):
                        pdf_id = None
                
                if pdf_id:
                    # Ensure pdf_id is a string and add to set
                    # This will automatically handle duplicates (set only stores unique values)
                    existing_ids.add(str(pdf_id))
        
        log(f"  Extracted {len(existing_ids)} unique PDF ID(s) from database")
        if existing_ids:
            log(f"  Existing PDF IDs: {', '.join(sorted(list(existing_ids))[:10])}{'...' if len(existing_ids) > 10 else ''}")
        
        return existing_ids
    except Exception as e:
        log(f"⚠️  Warning: Could not fetch existing PDF IDs from Supabase: {e}")
        import traceback
        traceback.print_exc()
        return set()


def filter_new_pdfs(
    drive_files: List[Dict],
    existing_pdf_ids: set,
    log_callback=None
) -> List[Dict]:
    """
    Filter out PDFs that already exist in the database.
    Compares the filename (without .pdf extension) with pdf_id values from Supabase metadata.
    
    Args:
        drive_files: List of files from Google Drive
        existing_pdf_ids: Set of PDF IDs that already exist in database (from metadata.pdf_id)
        log_callback: Optional callback function for logging messages
        
    Returns:
        List of new PDF files (not in database)
    """
    def log(message: str):
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    new_files = []
    
    for file in drive_files:
        # Only process PDF files
        if not file.get("name", "").lower().endswith(".pdf"):
            continue
        
        # Extract PDF ID from filename (remove .pdf extension)
        # This should match the pdf_id stored in Supabase metadata
        pdf_id = Path(file["name"]).stem
        
        # Check if this PDF is already in the database by comparing with existing pdf_ids
        if pdf_id not in existing_pdf_ids:
            new_files.append(file)
            log(f"✅ New file found: {file['name']}")
        else:
            log(f"⏭️  Skipping {file['name']} (pdf_id '{pdf_id}' already exists in database)")
    
    return new_files

