from utils.auth import connect_drive
from utils.drive import list_files_in_folder, download_file
from utils.file_reader import read_pdf, read_txt, read_docx
from config.settings import FOLDER_ID

def process_file(file_path):
    if file_path.endswith(".pdf"):
        return read_pdf(file_path)

    elif file_path.endswith(".txt"):
        return read_txt(file_path)

    elif file_path.endswith(".docx"):
        return read_docx(file_path)

    else:
        return "Unsupported file format."


def main():
    # Authenticate
    service = connect_drive()
    print("Connected to Google Drive!")

    # List files
    print("\nFetching files...")
    files = list_files_in_folder(service, FOLDER_ID)

    for file in files:
        print(f"\nFound: {file['name']} ({file['mimeType']})")

        # Download file
        local_path = download_file(service, file["id"], file["name"])
        print(f"Downloaded to: {local_path}")

        # Read content
        content = process_file(local_path)
        print("\n--- File Content Start ---\n")
        print(content)
        print("\n--- File Content End ---\n")


if __name__ == "__main__":
    main()
