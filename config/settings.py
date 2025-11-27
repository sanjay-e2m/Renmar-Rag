import os

# Google Drive scopes
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Google Drive Folder ID
FOLDER_ID = "1XnkW5Mi0UrBM4_pbUF32N8LZPMio3jCX"

# Path to credentials
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials", "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "credentials", "token.json")

# Download directory
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")
