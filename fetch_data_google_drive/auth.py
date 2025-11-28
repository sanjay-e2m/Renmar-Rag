import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from config.settings import SCOPES, CREDENTIALS_PATH, TOKEN_PATH


def connect_drive():
    creds = None

    # If token exists, load it
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    # If no valid token → authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save token
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())

    # Connect to Google Drive
    service = build("drive", "v3", credentials=creds)
    return service
