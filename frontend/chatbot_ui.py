"""
Streamlit-based frontend UI for the chatbot with semantic search and memory.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
from generation.chatbot import create_chatbot
from SyncDocuments.streamlit_pipeline import sync_documents_streamlit
from SyncDocuments.drive_sync import get_existing_pdf_ids
from SyncDocuments.config import settings
from supabase import create_client

# Page configuration
st.set_page_config(
    page_title="Business Analytics Chatbot",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply dark theme via Streamlit config
st.markdown("""
    <style>
    /* Override Streamlit default theme */
    .stApp {
        background-color: #1e1e1e;
    }
    </style>
""", unsafe_allow_html=True)

# Custom CSS for dark theme UI
st.markdown("""
    <style>
    /* Main page background */
    .main .block-container {
        background-color: #1e1e1e;
        color: #ffffff;
    }
    
    /* Header styling */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #64b5f6;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    /* Chat message containers */
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        color: #ffffff;
    }
    
    /* User message styling - dark blue background */
    .user-message {
        background-color: #2d3a4b;
        border-left: 4px solid #64b5f6;
        color: #ffffff;
    }
    
    /* Bot message styling - dark green background */
    .bot-message {
        background-color: #2d4a3b;
        border-left: 4px solid #66bb6a;
        color: #ffffff;
    }
    
    /* Button styling */
    .stButton>button {
        width: 100%;
        background-color: #4CAF50;
        color: #ffffff;
        font-weight: bold;
        border: none;
    }
    .stButton>button:hover {
        background-color: #66bb6a;
        color: #ffffff;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #2d2d2d;
    }
    
    [data-testid="stSidebar"] * {
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] div,
    [data-testid="stSidebar"] span {
        color: #ffffff !important;
    }
    
    /* Text color fixes */
    .stMarkdown {
        color: #ffffff;
    }
    
    .stMarkdown p, .stMarkdown div, .stMarkdown span {
        color: #ffffff !important;
    }
    
    /* Input field styling */
    .stTextInput>div>div>input {
        background-color: #2d2d2d;
        color: #ffffff;
    }
    
    /* Chat input styling */
    .stChatInput>div>div>input {
        background-color: #2d2d2d;
        color: #ffffff;
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        color: #ffffff;
        background-color: #2d2d2d;
    }
    
    /* Footer text */
    .footer-text {
        text-align: center;
        color: #b0b0b0;
    }
    
    /* General text color */
    p, div, span, label {
        color: #ffffff !important;
    }
    
    /* Slider styling */
    .stSlider {
        color: #ffffff;
    }
    
    /* Chat message bubbles from Streamlit */
    [data-testid="stChatMessage"] {
        background-color: transparent;
    }
    
    [data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] {
        color: #ffffff;
    }
    
    /* Divider styling */
    hr {
        border-color: #444444;
    }
    
    /* JSON viewer in expander */
    .stJson {
        background-color: #2d2d2d;
        color: #ffffff;
    }
    
    /* Log container styling */
    .log-container {
        background-color: #1a1a1a;
        border: 1px solid #444444;
        border-radius: 0.5rem;
        padding: 1rem;
        max-height: 500px;
        overflow-y: auto;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        color: #ffffff;
    }
    
    .log-line {
        margin: 0.25rem 0;
        padding: 0.25rem;
        white-space: pre-wrap;
        word-wrap: break-word;
    }
    
    /* Cancel button styling */
    .cancel-button {
        background-color: #f44336 !important;
    }
    .cancel-button:hover {
        background-color: #d32f2f !important;
    }
    
    /* Ensure chat input stays at bottom */
    .stChatInput {
        position: sticky;
        bottom: 0;
        background-color: #1e1e1e;
        padding: 1rem 0;
        margin-top: 1rem;
        z-index: 100;
        border-top: 1px solid #444444;
    }
    
    /* Custom scrollbar for chat messages container */
    [data-testid="stVerticalBlock"] [data-testid="element-container"]:has([data-testid="stChatMessage"]) {
        scrollbar-width: thin;
        scrollbar-color: #64b5f6 #2d2d2d;
    }
    
    /* Ensure messages don't overflow */
    [data-testid="stChatMessage"] {
        margin-bottom: 1rem;
    }
    
    /* Smooth scrolling for chat container */
    [data-testid="stVerticalBlock"] {
        scroll-behavior: smooth;
    }
    
    /* Thinking/loading indicator styling */
    .thinking-indicator {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.75rem 1rem;
        color: #64b5f6;
        font-style: italic;
    }
    
    .thinking-dots {
        display: inline-flex;
        gap: 0.25rem;
    }
    
    .thinking-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background-color: #64b5f6;
        animation: thinking-pulse 1.4s ease-in-out infinite;
    }
    
    .thinking-dot:nth-child(2) {
        animation-delay: 0.2s;
    }
    
    .thinking-dot:nth-child(3) {
        animation-delay: 0.4s;
    }
    
    @keyframes thinking-pulse {
        0%, 60%, 100% {
            opacity: 0.3;
            transform: scale(0.8);
        }
        30% {
            opacity: 1;
            transform: scale(1);
        }
    }
    
    /* Prevent layout shift */
    .chat-message-container {
        min-height: 50px;
    }
    
    /* Auto-scroll to bottom */
    .chat-messages-container {
        scroll-behavior: smooth;
    }
    
    /* Ensure spinner is visible */
    .stSpinner > div {
        border-top-color: #64b5f6 !important;
    }
    
    /* Better spacing for messages */
    [data-testid="stChatMessage"] {
        padding: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize session state
if "chatbot" not in st.session_state:
    st.session_state.chatbot = None
if "messages" not in st.session_state:
    st.session_state.messages = []
if "initialized" not in st.session_state:
    st.session_state.initialized = False
if "sync_running" not in st.session_state:
    st.session_state.sync_running = False
if "sync_cancelled" not in st.session_state:
    st.session_state.sync_cancelled = False
if "sync_logs" not in st.session_state:
    st.session_state.sync_logs = []
if "sync_results" not in st.session_state:
    st.session_state.sync_results = None
if "existing_pdfs" not in st.session_state:
    st.session_state.existing_pdfs = []
if "checked_existing" not in st.session_state:
    st.session_state.checked_existing = False
if "processing" not in st.session_state:
    st.session_state.processing = False


def initialize_chatbot(top_k: int = 2):
    """Initialize the chatbot if not already done."""
    if st.session_state.chatbot is None:
        try:
            with st.spinner("Initializing chatbot..."):
                st.session_state.chatbot = create_chatbot(top_k=top_k)
                st.session_state.initialized = True
                st.success("Chatbot initialized successfully!")
        except Exception as e:
            st.error(f"Error initializing chatbot: {str(e)}")
            st.info("Please make sure your .env file has GEMINI_API_KEY, SUPABASE_URL, and SUPABASE_ANON_KEY set.")
            return False
    return True


def clear_chat():
    """Clear chat history and memory."""
    if st.session_state.chatbot:
        st.session_state.chatbot.clear_memory()
    st.session_state.messages = []
    st.rerun()


# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    top_k = st.slider(
        "Number of documents to retrieve",
        min_value=1,
        max_value=10,
        value=2,
        help="More documents provide more context but may be slower"
    )
    
    if st.button("🔄 Reinitialize Chatbot"):
        st.session_state.chatbot = None
        st.session_state.initialized = False
        initialize_chatbot(top_k=top_k)
    
    st.divider()
    
    st.header("📊 Chat Info")
    st.write(f"Messages in history: {len(st.session_state.messages)}")
    
    if st.button("🗑️ Clear Chat History"):
        clear_chat()
    
    st.divider()
    
    st.header("ℹ️ About")
    st.markdown("""
    This chatbot uses:
    - **Semantic Search** to find relevant documents
    - **Google Gemini** for generating answers
    - **LangChain Memory** for conversation context
    
    Ask questions about your business documents!
    """)


# Create tabs for Chat and Document Sync
tab1, tab2 = st.tabs(["💬 Chat", "📥 Document Sync"])

# ==================== CHAT TAB ====================
with tab1:
    st.markdown('<div class="main-header">🤖 Business Analytics Chatbot</div>', unsafe_allow_html=True)

    # Initialize chatbot on first load
    if not st.session_state.initialized:
        initialize_chatbot(top_k=top_k)

    # Display chat history in a scrollable container
    # Calculate dynamic height based on message count
    num_messages = len(st.session_state.messages)
    # Use a smaller container when there are fewer messages
    if num_messages == 0:
        container_height = 400
    elif num_messages <= 3:
        container_height = 450
    elif num_messages <= 6:
        container_height = 500
    else:
        container_height = 550
    
    messages_container = st.container(height=container_height)
    
    with messages_container:
        # Display chat history
        if not st.session_state.messages:
            # Show welcome message if no messages
            with st.chat_message("assistant"):
                st.markdown("""
                <div class="chat-message bot-message">
                    👋 Welcome! I'm your Business Analytics Chatbot. 
                    Ask me questions about your business documents, and I'll help you find the information you need.
                </div>
                """, unsafe_allow_html=True)
        else:
            for message in st.session_state.messages:
                if message["role"] == "user":
                    with st.chat_message("user"):
                        st.markdown(f'<div class="chat-message user-message">{message["content"]}</div>', unsafe_allow_html=True)
                else:
                    with st.chat_message("assistant"):
                        st.markdown(f'<div class="chat-message bot-message">{message["content"]}</div>', unsafe_allow_html=True)
                        # Show context info if available
                        if "context_info" in message and message["context_info"]:
                            with st.expander("📄 View Retrieved Documents"):
                                for idx, info in enumerate(message["context_info"], 1):
                                    st.markdown(f"**Document {idx}**")
                                    st.json(info)
                                    if idx < len(message["context_info"]):
                                        st.divider()
    
    # Chat input - always visible below messages container
    if prompt := st.chat_input("Ask a question about your business documents..."):
        # Ensure chatbot is initialized
        if not st.session_state.initialized or st.session_state.chatbot is None:
            if not initialize_chatbot(top_k=top_k):
                st.stop()
        
        # Add user message to chat immediately
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Show the user message immediately in the container
        with messages_container:
            with st.chat_message("user"):
                st.markdown(f'<div class="chat-message user-message">{prompt}</div>', unsafe_allow_html=True)
            
            # Show thinking indicator immediately
            with st.chat_message("assistant"):
                with st.spinner("🤔 Thinking..."):
                    # Process the message
                    try:
                        # Get bot response
                        result = st.session_state.chatbot.chat(prompt)
                        answer = result["answer"]
                        context_docs = result.get("context_docs", [])
                        
                        # Prepare context info
                        context_info = []
                        for doc in context_docs:
                            context_info.append({
                                "doc_id": doc.metadata.get("doc_id", "N/A"),
                                "pdf_id": doc.metadata.get("pdf_id", "N/A"),
                                "page_no": doc.metadata.get("page_no", "N/A"),
                                "similarity": round(doc.metadata.get("similarity", 0), 4) if doc.metadata.get("similarity") else "N/A",
                                "content_preview": doc.page_content[:200] + "..." if len(doc.page_content) > 200 else doc.page_content
                            })
                        
                        # Add bot message to chat history
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": answer,
                            "context_info": context_info
                        })
                        
                    except Exception as e:
                        error_msg = f"Error: {str(e)}"
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": error_msg
                        })
        
        # Rerun to display the new messages properly
        st.rerun()



# ==================== DOCUMENT SYNC TAB ====================
with tab2:
    st.markdown('<div class="main-header">📥 Document Sync from Google Drive</div>', unsafe_allow_html=True)
    
    st.markdown("""
    ### Sync Documents from Google Drive
    
    This tool will:
    1. Connect to your Google Drive
    2. List all PDF files in the specified folder
    3. Check which documents are new (not in database)
    4. Download and process new PDFs
    5. Create summaries and embeddings
    6. Store them in the vector database
    
    **Note:** Only new documents will be processed. Existing documents are skipped.
    """)
    
    st.divider()
    
    # Folder ID input
    folder_id = st.text_input(
        "Google Drive Folder ID",
        placeholder="Enter the folder ID from Google Drive URL",
        help="You can find the folder ID in the Google Drive URL: https://drive.google.com/drive/folders/FOLDER_ID_HERE",
        key="folder_id_input"
    )
    
    # Check existing PDFs when folder_id is provided
    if folder_id and folder_id.strip():
        # Reset checked status if folder_id changed
        if "last_checked_folder" not in st.session_state or st.session_state.last_checked_folder != folder_id:
            st.session_state.checked_existing = False
            st.session_state.existing_pdfs = []
            st.session_state.last_checked_folder = folder_id
        
        # Check existing PDFs button
        col_check1, col_check2 = st.columns([2, 1])
        
        with col_check1:
            check_existing = st.button("🔍 Check Existing PDFs in Database", type="secondary")
        
        with col_check2:
            refresh_existing = st.button("🔄 Refresh", help="Refresh the list of existing PDFs")
        
        if check_existing or refresh_existing:
            try:
                with st.spinner("Checking database for existing PDFs..."):
                    supabase_client = create_client(settings.supabase_url, settings.supabase_key)
                    existing_pdf_ids = get_existing_pdf_ids(supabase_client, log_callback=None)
                    st.session_state.existing_pdfs = sorted(list(existing_pdf_ids))
                    st.session_state.checked_existing = True
                    st.success(f"✅ Found {len(st.session_state.existing_pdfs)} existing PDF(s) in database")
                    st.rerun()
            except Exception as e:
                st.error(f"❌ Error checking database: {str(e)}")
                st.session_state.existing_pdfs = []
                st.session_state.checked_existing = False
    
    # Display existing PDFs if checked
    if st.session_state.checked_existing:
        st.divider()
        st.subheader("📚 Existing PDFs in Database")
        
        if len(st.session_state.existing_pdfs) > 0:
            # Show count
            st.info(f"**{len(st.session_state.existing_pdfs)} PDF(s)** already exist in the database. These will be skipped during sync.")
            
            # Show PDFs in expandable section
            with st.expander(f"📋 View All {len(st.session_state.existing_pdfs)} Existing PDFs", expanded=False):
                # Create columns for better display
                num_cols = 3
                cols = st.columns(num_cols)
                
                for idx, pdf_id in enumerate(st.session_state.existing_pdfs):
                    with cols[idx % num_cols]:
                        st.code(pdf_id, language=None)
        else:
            st.success("✅ No PDFs found in database. All documents in the folder will be new and will be synced.")
    
    st.divider()
    
    # Sync buttons
    if folder_id and not st.session_state.checked_existing:
        st.warning("⚠️ Please check existing PDFs first to see what's already in the database.")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        start_sync = st.button(
            "🚀 Sync New Documents", 
            type="primary", 
            disabled=st.session_state.sync_running or not folder_id,
            help="This will sync only new documents that are not in the database"
        )
    
    with col2:
        cancel_sync = st.button("🛑 Cancel Sync", disabled=not st.session_state.sync_running)
    
    # Handle cancel button
    if cancel_sync and st.session_state.sync_running:
        st.session_state.sync_cancelled = True
        st.session_state.sync_running = False
        st.session_state.sync_logs.append("⚠️ Sync cancelled by user")
        st.rerun()
    
    # Handle start sync
    if start_sync and folder_id and not st.session_state.sync_running:
        st.session_state.sync_running = True
        st.session_state.sync_cancelled = False
        st.session_state.sync_logs = []
        st.session_state.sync_results = None
        st.rerun()
    
    # Run sync if it's supposed to be running
    if st.session_state.sync_running and folder_id:
        # Create log callback
        def log_callback(message: str):
            st.session_state.sync_logs.append(message)
        
        # Create cancel check function
        def check_cancel() -> bool:
            return st.session_state.sync_cancelled
        
        # Show progress indicator
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_display = st.empty()
        
        # Update log display periodically
        def update_logs():
            if st.session_state.sync_logs:
                log_text = "\n".join(st.session_state.sync_logs[-30:])  # Show last 30 lines
                log_display.markdown(f'<div class="log-container">{log_text}</div>', unsafe_allow_html=True)
        
        status_text.info("🔄 Syncing documents... This may take a while. Click Cancel to stop.")
        update_logs()
        
        # Run sync synchronously
        try:
            results = sync_documents_streamlit(
                folder_id=folder_id,
                log_callback=log_callback,
                check_cancel=check_cancel
            )
            st.session_state.sync_results = results
            st.session_state.sync_running = False
            progress_bar.progress(100)
            status_text.success("✅ Sync completed!")
            update_logs()
        except Exception as e:
            st.session_state.sync_logs.append(f"❌ Error: {str(e)}")
            st.session_state.sync_running = False
            status_text.error(f"❌ Sync failed: {str(e)}")
            update_logs()
        
        st.rerun()
    
    # Display sync status
    if st.session_state.sync_running:
        st.info("🔄 Sync in progress... Click Cancel to stop.")
    
    # Display logs (if not currently running, show static logs)
    if st.session_state.sync_logs and not st.session_state.sync_running:
        st.divider()
        st.subheader("📋 Sync Logs")
        
        # Create log container
        log_text = "\n".join(st.session_state.sync_logs)
        st.markdown(f'<div class="log-container">{log_text}</div>', unsafe_allow_html=True)
    
    # Display results
    if st.session_state.sync_results:
        st.divider()
        st.subheader("📊 Sync Results")
        
        results = st.session_state.sync_results
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Files", results.get("total_files", 0))
        with col2:
            st.metric("New Files", results.get("new_files", 0))
        with col3:
            st.metric("Processed", results.get("processed", 0))
        with col4:
            st.metric("Failed", results.get("failed", 0))
        
        if results.get("errors"):
            with st.expander("❌ Errors", expanded=False):
                for error in results["errors"]:
                    st.error(error)

# Footer
st.markdown("---")
st.markdown(
    "<div class='footer-text'>Powered by LangChain, Google Gemini, and Supabase</div>",
    unsafe_allow_html=True
)

