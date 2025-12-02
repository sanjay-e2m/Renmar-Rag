# Renmar RAG Platform

Business-analytics RAG stack that pulls PDFs from Google Drive, converts each page
to Gemini-ready summaries, stores embeddings inside Supabase, and exposes both CLI
and Streamlit chat frontends with conversational memory.

---

## Features

- Automated Google Drive sync (`SyncDocuments/pipeline.py`) with duplicate detection,
  PDF downloads, PyMuPDF image generation, Gemini page summaries, Supabase storage,
  and automatic cleanup of downloaded assets once each file is indexed.
- Streamlit-friendly pipeline wrapper (`SyncDocuments/streamlit_pipeline.py`) with
  live logging, cancellation support, and integration into the chatbot UI.
- Supabase vector store integration via custom LangChain embeddings that use
  Gemma/Hugging Face models (`supabase_pipeline/langchain_gemma_embeddings.py`).
- Conversational chatbot (`generation/chatbot.py`) with semantic search, Gemini
  responses, and chat history fallbacks.
- Streamlit UI (`frontend/chatbot_ui.py`) that lets you trigger new sync jobs,
  inspect collection stats, and chat over the indexed reports.
- Standalone PDF converter utilities (`pdf_converter/`) for debugging or tooling.

---

## Project Structure

```
SyncDocuments/        # Drive sync, PDF processing, Supabase storage
generation/           # Semantic search + Gemini chatbot logic
frontend/             # Streamlit UI for sync + chat
pdf_converter/        # Low-level PDF→image helpers
supabase_pipeline/    # Shared embedding + config utilities
data/                 # Downloaded PDFs + generated summaries (temporary)
temp/                 # Ephemeral artifacts (images, docstore cache)
```

---

## Prerequisites

- **Python 3.12** (required for the entire codebase)
- Google Cloud project with Drive API enabled (place OAuth
  `credentials/credentials.json` and generated `credentials/token.json`)
- Supabase project with a vector table (default `page_summaries`)
- Gemini API access
- Optional: Hugging Face token for private Gemma variants, Pinecone account if
  you experiment with `retrieval/test.py`

---

## Environment Variables

Copy `.env_example` to `.env` and fill in your secrets.

Key variables the code expects:

- `FOLDER_ID` – target Google Drive folder
- `GEMINI_API_KEY` (+ optional `GEMINI_MODEL`)
- `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_TABLE`, `SUPABASE_QUERY_FN`,
  `SUPABASE_MATCH_THRESHOLD`
- `EMBEDDING_MODEL`, `HUGGINGFACE_HUB_TOKEN`, `HF_TOKEN`, `BATCH_SIZE`
- `RETRIEVER_TOP_K`
- Optional Pinecone keys for experiments

The pipeline uses `python-dotenv`, so running scripts from the repo root will
automatically load `.env`.

---

## Installation

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install --upgrade pip
pip install -r requirements.txt
```

Make sure the folders referenced in `SyncDocuments/config.py` exist and contain
the proper credentials.

---

## Document Sync Pipeline

Use the CLI test harness to manually sync a Drive folder:

```bash
python SyncDocuments/testing/test.py
```

What happens per document:

1. Download the PDF into `data/downloaded/`.
2. Convert each page to PNG (PyMuPDF) and send base64 data to Gemini for
   summarization (`SyncDocuments/pdf_processor.py`).
3. Store the per-page summaries + embeddings inside Supabase (`SyncDocuments/vector_store.py`).
4. Delete the downloaded PDF, generated summary JSON, and temp images so the repo
   stays lean.

Errors are logged per file and the script prints a sync summary at the end.

### Streamlit-compatible Sync

`SyncDocuments/streamlit_pipeline.py` exposes the same workflow with callbacks for
logging and cancellation. The Streamlit UI uses this to show progress bars and let
users stop a run mid-way.

---

## Chatbot & Semantic Search

### CLI Chatbot

```bash
python generation/chatbot.py
```

- Uses Supabase semantic search (`generation/semantic_search.py`) to fetch the top
  `top_k` pages.
- Builds a Gemini prompt containing context documents plus the most recent chat
  turns (history text is now explicitly referenced inside the system instructions).
- Returns the assistant answer and prints how many documents were retrieved.

### Streamlit UI

```bash
streamlit run frontend/chatbot_ui.py
```

Capabilities:

- Trigger a new sync job from the sidebar by pasting a Drive folder ID.
- View Supabase stats (number of PDFs, pages, last sync, etc.).
- Chat with the Gemini-backed assistant in a dark-themed interface with
  conversation history, clear, and export controls.

---

## PDF Converter Utilities

The `pdf_converter/` package contains helper scripts in case you want to debug
PDF rendering outside the full pipeline:

```bash
python pdf_converter/pdf_to_images.py
```

Adjust DPI / format or call `convert_pdf_to_images_pymupdf` from your own scripts.

---

## Troubleshooting

- `ModuleNotFoundError: SyncDocuments` – ensure you run commands from the repo root
  so package-relative imports resolve. The included scripts call `sys.path.insert`
  to assist, but using the root virtualenv shell avoids surprises.
- `GEMINI_API_KEY` / Supabase errors – confirm `.env` matches `.env_example`. The
  scripts call `settings.validate()` before running heavy work.
- Drive auth prompts – the first `connect_drive()` run launches a local server for
  OAuth; a `credentials/token.json` file will be stored for subsequent runs.
- Large temp folders – automatic cleanup is performed after each successful sync,
  but if a run crashes you can manually clear `data/downloaded/`, `data/summaries/`,
  and `temp/pdf_images/`.

---

## Contributing

1. Fork / create a feature branch.
2. Keep scripts portable (no absolute paths) and document new env vars in
   `.env_example`.
3. Run linting/tests relevant to your changes.
4. Submit a PR with a concise summary and screenshots if UI changes were made.

---

Happy syncing and chatting! If you run into issues or have feature ideas,
open an issue or ping the team. :)
