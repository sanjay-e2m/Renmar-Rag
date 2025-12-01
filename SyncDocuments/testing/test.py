"""
Console-based test script for the document sync pipeline.
"""

import sys
from pathlib import Path

# Add project root to path (go up two levels from testing/ to project root)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from SyncDocuments.pipeline import sync_documents
from SyncDocuments.config import settings


def main():
    """Main test function."""
    print("="*80)
    print("🧪 DOCUMENT SYNC PIPELINE - TEST SCRIPT")
    print("="*80)
    print()
    
    # Get folder ID from user
    print("Please provide the Google Drive folder ID to sync.")
    print("You can find the folder ID in the Google Drive URL:")
    print("  Example: https://drive.google.com/drive/folders/FOLDER_ID_HERE")
    print()
    
    folder_id = input("Enter Google Drive folder ID: ").strip()
    
    if not folder_id:
        print("❌ Error: Folder ID cannot be empty")
        return
    
    print()
    print("="*80)
    print(f"📁 Folder ID: {folder_id}")
    print("="*80)
    print()
    
    # Validate configuration
    try:
        settings.validate()
        print("✅ Configuration validated")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("\nPlease check your .env file and ensure:")
        print("  - GEMINI_API_KEY is set")
        print("  - SUPABASE_URL is set")
        print("  - SUPABASE_ANON_KEY is set")
        print("  - Google Drive credentials.json exists in credentials/ folder")
        return
    
    print()
    print("Starting sync pipeline...")
    print()
    
    # Run the sync pipeline
    try:
        results = sync_documents(folder_id)
        
        print()
        print("="*80)
        print("✅ TEST COMPLETED")
        print("="*80)
        print(f"Results: {results}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

