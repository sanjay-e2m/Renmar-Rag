# Excel RAG Pipeline Flow

## Overview
This document describes the complete flow of the Excel RAG Pipeline system.

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         INPUT FOLDER                            │
│                    (data/input/*.xlsx)                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Excel Files
                              ▼
        ┌─────────────────────────────────────────┐
        │      PARALLEL PROCESSING                │
        └─────────────────────────────────────────┘
                  │                    │
                  │                    │
        ┌─────────▼─────────┐  ┌───────▼──────────────┐
        │  TEXT PROCESSOR   │  │  DATAFRAME PROCESSOR │
        │  (Excel → .txt)   │  │  (Excel → CSV)       │
        └─────────┬─────────┘  └───────┬──────────────┘
                  │                    │
                  │                    │
        ┌─────────▼─────────┐  ┌───────▼──────────────┐
        │   OUTPUT FOLDER   │  │  PREPROCESSED FOLDER │
        │ (data/output/*.txt)│  │(data/preprocessed/*.csv)│
        └───────────────────┘  └──────────────────────┘
```

## Step-by-Step Flow

### 1. Initial Setup
- Excel files are placed in `data/input/` folder
- System initializes processors and LLM components

### 2. Processing Phase (When "Process All Excel Files" is clicked)

#### 2.1 Text Extraction (Parallel)
- **Processor**: `ExcelToTextProcessor`
- **Input**: Excel files from `data/input/`
- **Process**:
  - Reads each Excel file
  - Extracts all sheets
  - Converts to markdown format
  - Saves as `.txt` files
- **Output**: `.txt` files in `data/output/` folder
- **Example**: `Irby_Jan_ report.xlsx` → `Irby_Jan_ report.txt`

#### 2.2 DataFrame Processing (Parallel)
- **Processor**: `ExcelToDataFrameProcessor`
- **Input**: Excel files from `data/input/`
- **Process**:
  - Reads Excel files
  - Removes unwanted columns
  - Handles null values
  - Converts data types
  - Processes special characters (arrows, etc.)
  - Saves as preprocessed CSV files
- **Output**: `.csv` files in `data/preprocessed/` folder
- **Example**: `Irby_Jan_ report.xlsx` → `Irby_Jan_ report.csv`

### 3. Query Phase (When user asks a question)

#### 3.1 File Routing
- **Component**: `FileRouter`
- **Process**:
  - User asks a question
  - LLM analyzes the question
  - Matches question to appropriate Excel file
  - Returns the matched file name

#### 3.2 Query Execution
- **Component**: `QueryExecutor`
- **Process**:
  1. **Check for Preprocessed File**:
     - Checks if CSV exists in `data/preprocessed/`
     - If exists: Load directly
     - If not: Process Excel file first, then load
  
  2. **Load Context**:
     - Loads `.txt` file from `data/output/` as context
     - Uses this context for query generation
  
  3. **Generate Pandas Query**:
     - **Component**: `QueryGenerator`
     - Takes user question + DataFrame info + context
     - LLM generates pandas query
     - Validates syntax before execution
  
  4. **Execute Query**:
     - Executes pandas query on DataFrame
     - Returns result DataFrame/Series
  
  5. **Format Response**:
     - **Component**: `ResponseFormatter`
     - Takes result DataFrame + user question
     - LLM formats into user-friendly answer
     - Returns formatted response

### 4. Frontend Display
- **Framework**: Streamlit
- **Display**:
  - Shows formatted answer
  - Displays insights and highlights
  - Shows generated pandas query (optional)
  - Shows raw data (optional)

## File Structure

```
excel_rag_pipeline_pandas/
├── data/
│   ├── input/              # Excel files go here
│   │   ├── file1.xlsx
│   │   └── file2.xlsx
│   ├── output/             # Text files (.txt) go here
│   │   ├── file1.txt
│   │   └── file2.txt
│   └── preprocessed/       # CSV files (.csv) go here
│       ├── file1.csv
│       └── file2.csv
├── src/
│   ├── processors/
│   │   ├── excel_to_text.py        # Excel → .txt
│   │   └── excel_to_dataframe.py   # Excel → CSV
│   ├── routing/
│   │   ├── file_router.py           # Routes queries to files
│   │   └── query_executor.py       # Executes queries
│   ├── llm/
│   │   ├── query_generator.py      # Generates pandas queries
│   │   └── response_formatter.py   # Formats responses
│   └── pipeline/
│       └── main_pipeline.py        # Main orchestrator
└── app.py                           # Streamlit UI
```

## Key Components

### ExcelToTextProcessor
- Converts Excel files to formatted text/markdown
- Uses LlamaParse if available, falls back to pandas
- Saves `.txt` files for context in queries

### ExcelToDataFrameProcessor
- Converts Excel files to preprocessed DataFrames
- Cleans data, handles nulls, converts types
- Saves `.csv` files for fast loading

### FileRouter
- Uses LLM to determine which Excel file a query belongs to
- Analyzes question intent and file names
- Returns matched file name

### QueryExecutor
- Orchestrates query execution
- Checks for preprocessed files
- Loads context from `.txt` files
- Generates and executes pandas queries
- Handles errors and retries

### QueryGenerator
- Converts natural language to pandas queries
- Uses DataFrame info and context
- Validates syntax before execution

### ResponseFormatter
- Formats query results into user-friendly answers
- Uses LLM to generate insights and summaries
- Returns structured response

## Usage Flow

1. **Setup**: Place Excel files in `data/input/`
2. **Process**: Click "Process All Excel Files" in Streamlit
3. **Query**: Ask questions in natural language
4. **Results**: System automatically:
   - Routes to correct file
   - Loads preprocessed data
   - Generates pandas query
   - Executes query
   - Formats response
   - Displays answer

## Benefits

- **Efficient**: Preprocessed files loaded instantly
- **Smart**: Automatic file routing
- **Fast**: Parallel processing
- **Robust**: Error handling and retries
- **User-Friendly**: Natural language queries
