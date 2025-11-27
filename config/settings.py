import os
from dotenv import load_dotenv
load_dotenv()
# Google Drive scopes
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Google Drive Folder ID
FOLDER_ID = os.getenv("FOLDER_ID")

# Path to credentials
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials", "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "credentials", "token.json")

# Download directory
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")
print('hello')
