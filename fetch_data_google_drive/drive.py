from googleapiclient.http import MediaIoBaseDownload
import io
import os
from config.settings import DOWNLOAD_DIR


def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents"

    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType)"
    ).execute()

    files = results.get("files", [])
    return files


def download_file(service, file_id, filename):
    file_path = os.path.join(DOWNLOAD_DIR, filename)


    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    return file_path
