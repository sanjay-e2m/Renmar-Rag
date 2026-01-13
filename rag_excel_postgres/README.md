# SQL-RAG Agentic Pipeline with LangGraph

A sophisticated agentic SQL-RAG (Retrieval-Augmented Generation) system built with LangGraph that converts natural language questions into SQL queries and retrieves data from PostgreSQL. The system features intelligent query formatting, retry mechanisms, and conversation history management.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Workflow Diagram](#workflow-diagram)
- [How It Works](#how-it-works)
- [Components](#components)
- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Configuration](#configuration)

## 🎯 Overview

This system uses **LangGraph** to create an agentic workflow that:

1. **Accepts** natural language questions from users
2. **Formats** queries intelligently (fixes spelling, corrects client names)
3. **Generates** SQL queries using LLM (Groq)
4. **Validates** SQL for security and correctness
5. **Executes** queries on PostgreSQL database
6. **Formats** results into natural language answers
7. **Stores** conversation history for context

The system implements a **3-tier retry mechanism**:
- **Attempt 1**: Direct SQL generation from original query
- **Attempt 2**: Format query, then generate SQL
- **Attempt 3**: Reformat query with error context, then generate SQL

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         User Interface                           │
│                    (main_pipeline.py)                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LangGraph Agent Workflow                      │
│                         (agent/graph.py)                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ Query        │   │ Query        │   │ Query        │
│ Formatter    │   │ Generator    │   │ Executor     │
│              │   │              │   │              │
│ - Spelling   │   │ - Schema     │   │ - PostgreSQL │
│ - Grammar    │   │ - Context    │   │ - Execution  │
│ - Client     │   │ - History    │   │ - Results    │
│   Names      │   │ - Examples   │   │              │
└──────────────┘   └──────────────┘   └──────────────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Conversation Manager                         │
│              - History Storage                                   │
│              - Client List Retrieval                            │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                          │
│              - reports_master table                              │
│              - conversation_history table                        │
└─────────────────────────────────────────────────────────────────┘
```

## 🔄 Workflow Diagram

### Complete Agentic Workflow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        START: User Query                             │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  validate_input_node  │
                    │  - Validate query     │
                    │  - Store original     │
                    │  - Load session       │
                    │  - NO formatting yet  │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  generate_sql_node    │
                    │  - Use original query │
                    │  - Generate SQL       │
                    │  - Schema context     │
                    │  - Client list        │
                    │  - Conversation hist  │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  validate_sql_node    │
                    │  - Security checks    │
                    │  - Syntax validation  │
                    │  - Table verification │
                    └───────────┬───────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
            SQL Valid?              SQL Invalid?
                    │                       │
                    ▼                       ▼
        ┌───────────────────┐   ┌───────────────────┐
        │ execute_sql_node  │   │ error_handler     │
        │ - Execute query   │   │ - Handle error    │
        │ - Check rows       │   │ - End workflow    │
        └───────────┬───────┘   └───────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
   Success?              Failed/0 rows?
        │                       │
        ▼                       ▼
┌───────────────┐   ┌──────────────────────────┐
│ format_answer │   │ format_and_retry_node    │
│ - Format      │   │ - Format original query  │
│   results     │   │ - Reset SQL state        │
└───────┬───────┘   └───────────┬──────────────┘
        │                       │
        │                       ▼
        │           ┌───────────────────────┐
        │           │ generate_sql_node     │
        │           │ (with formatted)      │
        │           └───────────┬───────────┘
        │                       │
        │                       ▼
        │           ┌───────────────────────┐
        │           │ validate_sql_node     │
        │           └───────────┬───────────┘
        │                       │
        │                       ▼
        │           ┌───────────────────────┐
        │           │ execute_sql_node      │
        │           └───────────┬───────────┘
        │                       │
        │           ┌───────────┴───────────┐
        │           │                       │
        │      Success?              Failed?
        │           │                       │
        │           ▼                       ▼
        │   ┌───────────────┐   ┌──────────────────────────┐
        │   │ format_answer │   │ reformat_and_retry_node  │
        │   └───────┬───────┘   │ - Reformat with errors   │
        │           │           │ - Reset SQL state        │
        │           │           └───────────┬──────────────┘
        │           │                       │
        │           │                       ▼
        │           │           ┌───────────────────────┐
        │           │           │ generate_sql_node      │
        │           │           │ (with reformatted)    │
        │           │           └───────────┬───────────┘
        │           │                       │
        │           │                       ▼
        │           │           ┌───────────────────────┐
        │           │           │ validate_sql_node     │
        │           │           └───────────┬───────────┘
        │           │                       │
        │           │                       ▼
        │           │           ┌───────────────────────┐
        │           │           │ execute_sql_node      │
        │           │           └───────────┬───────────┘
        │           │                       │
        │           │           ┌───────────┴───────────┐
        │           │           │                       │
        │           │      Success?              Failed?
        │           │           │                       │
        │           │           ▼                       ▼
        │           │   ┌───────────────┐   ┌──────────────────┐
        │           │   │ format_answer │   │ error_handler    │
        │           │   └───────┬───────┘   │ - Final error    │
        │           │           │           │ - End workflow   │
        │           │           │           └──────────────────┘
        │           │           │
        └───────────┴───────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │ save_conversation_node│
        │ - Store SUCCESSFUL    │
        │   query version       │
        │ - Save answer         │
        └───────────┬───────────┘
                    │
                    ▼
                ┌───────┐
                │  END  │
                └───────┘
```

### Query Version Storage Logic

```
┌─────────────────────────────────────────────────────────────┐
│          Which Query Version Gets Stored?                   │
└─────────────────────────────────────────────────────────────┘

Attempt 1 (formatting_attempt = 0):
  Original Query → SQL → Execute → Success?
    │                                    │
    └─ YES → Store ORIGINAL query ───────┘
    └─ NO  → Go to Attempt 2

Attempt 2 (formatting_attempt = 1):
  Format Query → SQL → Execute → Success?
    │                                    │
    └─ YES → Store FORMATTED query ──────┘
    └─ NO  → Go to Attempt 3

Attempt 3 (formatting_attempt = 2):
  Reformat Query → SQL → Execute → Success?
    │                                    │
    └─ YES → Store REFORMATTED query ───┘
    └─ NO  → Store last attempted query
```

## 🔍 How It Works

### Step-by-Step Process

#### 1. **Input Validation** (`validate_input_node`)
- Receives user's natural language question
- Validates query is not empty
- Generates or retrieves session ID
- Loads conversation history (last 3 turns)
- **Important**: Does NOT format query on first attempt
- Stores original query for reference

#### 2. **SQL Generation - Attempt 1** (`generate_sql_node`)
- Uses **original query** (no formatting)
- Passes to `QueryGenerator` with:
  - Database schema context
  - Available client list
  - Recent conversation history (top 3)
  - SQL query examples
- LLM generates SQL query

#### 3. **SQL Validation** (`validate_sql_node`)
- Security checks (blocks DROP, DELETE, etc.)
- Syntax validation (must start with SELECT)
- Table verification (must reference reports_master)
- Routes to execution if valid, error handler if invalid

#### 4. **SQL Execution - Attempt 1** (`execute_sql_node`)
- Executes SQL query on PostgreSQL
- Converts DataFrame to list of dicts (for LangGraph serialization)
- **Checks for 0 rows** - marks as failed if empty
- Returns results or error

#### 5. **Retry Logic - Attempt 2** (`format_and_retry_node`)
- **Triggered if**: Execution failed OR returned 0 rows
- Formats original query using `QueryFormatter`:
  - Fixes spelling mistakes
  - Corrects grammar
  - **Corrects client names** using database client list
  - Standardizes terminology
- Resets SQL generation state
- Routes back to `generate_sql_node` with formatted query

#### 6. **Retry Logic - Attempt 3** (`reformat_and_retry_node`)
- **Triggered if**: Attempt 2 also failed
- Reformats query with **error context**:
  - Original query
  - Previous formatted query
  - Execution error details
  - SQL generation errors
- Uses `reformat_query` method with detailed prompt
- Routes back to `generate_sql_node` with reformatted query

#### 7. **Answer Formatting** (`format_answer_node`)
- **Triggered if**: Execution succeeded
- Formats query results using LLM
- Creates natural language answer
- Includes insights and summaries

#### 8. **Conversation Storage** (`save_conversation_node`)
- **Stores the SUCCESSFUL query version**:
  - If Attempt 1 succeeded → stores original query
  - If Attempt 2 succeeded → stores formatted query
  - If Attempt 3 succeeded → stores reformatted query
- Saves to `conversation_history` table
- Updates state with conversation history

## 🧩 Components

### Core Components

#### 1. **Query Formatter** (`llm/query_formatter.py`)
- **Purpose**: Cleans and formats user queries
- **Features**:
  - Basic cleaning (whitespace, punctuation)
  - LLM-based formatting (spelling, grammar)
  - **Client name correction** using database client list
  - Schema-aware formatting
- **Methods**:
  - `format_query()`: First-time formatting
  - `reformat_query()`: Error-aware reformatting

#### 2. **Query Generator** (`llm/query_generator.py`)
- **Purpose**: Generates SQL from natural language
- **Context Sources**:
  - Database schema (detailed table/column descriptions)
  - Client list (unique clients from database)
  - Conversation history (top 3 recent turns)
  - SQL examples (optimized for token efficiency)
- **Model**: Groq (llama-3.1-8b-instant)

#### 3. **Query Executor** (`llm/query_executor.py`)
- **Purpose**: Executes SQL on PostgreSQL
- **Features**:
  - Connection management
  - Error handling
  - Result formatting
  - DataFrame conversion

#### 4. **Conversation Manager** (`postgres_insert_create/conversation_manager.py`)
- **Purpose**: Manages conversation history
- **Features**:
  - Save conversations
  - Retrieve recent conversations
  - Get unique client list
  - Format context for LLM

#### 5. **LangGraph Agent** (`agent/`)
- **State Management** (`state.py`): TypedDict for agent state
- **Nodes** (`nodes.py`): Individual workflow steps
- **Edges** (`edges.py`): Conditional routing logic
- **Graph** (`graph.py`): Workflow construction and compilation

### Database Schema

#### `reports_master` Table
- Stores keyword ranking reports
- Key columns:
  - `client_name`: Client identifier
  - `year`, `month`: Time dimensions
  - `keyword`: Search keyword
  - `search_volume`: Search volume metric
  - `current_ranking`: Current ranking position
  - `change`: Ranking change (positive = improved)

#### `conversation_history` Table
- Stores conversation turns
- Columns:
  - `id`: Primary key
  - `session_id`: Session identifier
  - `user_query`: User's question (successful version)
  - `system_response`: System's answer
  - `created_at`: Timestamp

## 📦 Installation

### Prerequisites

```bash
# Python 3.8+
python --version

# PostgreSQL database
# Groq API key
```

### Setup

1. **Clone the repository**
```bash
cd /Users/dhyeybhimani/Desktop/RAG_v01
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Set up environment variables**
```bash
cp .env_example .env
# Edit .env with your credentials:
# - GROQ_API_KEY=your_groq_api_key
# - GROQ_MODEL=llama-3.1-8b-instant
# - DB_HOST=localhost
# - DB_PORT=5432
# - DB_NAME=your_database
# - DB_USER=your_user
# - DB_PASSWORD=your_password
```

4. **Set up database tables**
```bash
# Create reports_master table
python rag_excel_postgres/postgres_insert_create/create_table.py

# Create conversation_history table
python rag_excel_postgres/postgres_insert_create/create_conversation_table.py
```

## 🚀 Usage

### Basic Usage

```python
from rag_excel_postgres.frontend.main_pipeline import MainPipeline

# Initialize pipeline
pipeline = MainPipeline()

# Process a question
result = pipeline.process_user_question(
    user_question="Show me top 5 keywords with highest search volume for efg in December 2025",
    session_id=None,  # Creates new session
    format_answer=True,
    show_sql=True
)

# Display result
pipeline.display_result(result)
```

### Using the Agent Directly

```python
from rag_excel_postgres.agent import create_agent_graph

# Create agent
agent = create_agent_graph()

# Invoke with query
result = agent.invoke(
    user_query="Show me top 5 keywords with highest search volume for efg in December 2025",
    session_id="my_session_123"
)

# Access results
print(result['formatted_answer'])
print(result['generated_sql'])
print(result['query_result'])
```

### Interactive CLI

```bash
python rag_excel_postgres/frontend/main_pipeline.py
```

### Example Usage Script

```bash
python rag_excel_postgres/agent/example_usage.py
```

## ✨ Features

### 1. **Intelligent Query Formatting**
- Automatic spelling correction
- Grammar fixing
- Client name correction using database reference
- Schema-aware formatting

### 2. **3-Tier Retry Mechanism**
- **Attempt 1**: Direct SQL generation (fastest)
- **Attempt 2**: Format query then generate SQL
- **Attempt 3**: Reformat with error context

### 3. **Context-Aware SQL Generation**
- Database schema context
- Client list reference
- Conversation history (top 3 turns)
- SQL examples

### 4. **Smart Conversation Storage**
- Stores the query version that succeeded
- Tracks formatting attempts
- Maintains conversation context

### 5. **Comprehensive Error Handling**
- SQL validation
- Execution error handling
- Retry logic
- User-friendly error messages

### 6. **Debug Logging**
- Detailed debug output at each step
- Query version tracking
- Execution status logging
- Storage decision logging

## ⚙️ Configuration

### Environment Variables

```env
# Groq Configuration
GROQ_API_KEY=your_api_key_here
GROQ_MODEL=llama-3.1-8b-instant

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
```

### Model Configuration

The system uses Groq's `llama-3.1-8b-instant` model by default. You can change this in:
- Environment variable: `GROQ_MODEL`
- Or pass directly: `MainPipeline(groq_model="your-model")`

### Token Optimization

The prompt is optimized for token efficiency:
- Concise schema description
- Limited query examples (5 key examples)
- Compact conversation history (3 turns, 150 char previews)
- Streamlined client list format

## 📊 Example Queries

### Simple Queries
```
"Show me top 5 keywords with highest search volume for efg in December 2025"
"How many keywords are tracked for abc in March 2025?"
"What is the total search volume for xyz in May 2024?"
```

### Complex Queries
```
"Compare total search volume for efg between December 2025 and November 2025"
"Show keywords that exist in both March and April 2025 for abc"
"Keywords with search volume > 500 AND ranking < 20 for efg in December 2025"
```

### Queries with Misspellings (Auto-corrected)
```
"Show me top 5 keywords with highesttt searchd volume for efggg in December 2025"
# System will correct: "highesttt" → "highest", "searchd" → "search", "efggg" → "efg"
```

## 🔧 Troubleshooting

### Common Issues

1. **"Request too large for model"**
   - The prompt is optimized, but if you add more context, you may hit token limits
   - Solution: Reduce conversation history limit or simplify schema

2. **"No module named 'rag_excel_postgres'"**
   - Solution: Ensure project root is in `sys.path` (handled automatically in entry points)

3. **"DataFrame serialization error"**
   - Solution: DataFrames are automatically converted to list of dicts (already handled)

4. **"Query returned 0 rows"**
   - This triggers retry mechanism automatically
   - System will format/reformat query and retry

## 📝 File Structure

```
rag_excel_postgres/
├── agent/
│   ├── __init__.py
│   ├── graph.py          # LangGraph workflow construction
│   ├── nodes.py          # Individual workflow nodes
│   ├── edges.py          # Conditional routing logic
│   ├── state.py          # Agent state schema
│   ├── tools.py          # Agent tools wrapper
│   └── example_usage.py  # Usage examples
├── llm/
│   ├── query_generator.py    # SQL generation from NL
│   ├── query_executor.py     # SQL execution
│   ├── query_formatter.py    # Query formatting/reformatting
│   ├── query_pipeline.py     # Legacy pipeline
│   └── schema.py             # Database schema context
├── postgres_insert_create/
│   ├── conversation_manager.py   # Conversation history management
│   ├── create_table.py           # Create reports_master
│   └── create_conversation_table.py  # Create conversation_history
├── frontend/
│   └── main_pipeline.py      # Main entry point
└── README.md                  # This file
```

## 🎓 Key Concepts

### LangGraph
- **StateGraph**: Manages stateful workflow
- **Nodes**: Individual processing steps
- **Edges**: Conditional routing between nodes
- **Checkpointing**: Saves state for conversation persistence

### Agentic Workflow
- **Planning**: Determines which steps to take
- **Execution**: Performs actions (SQL generation, execution)
- **Self-Correction**: Retries with improved queries
- **Context Management**: Maintains conversation history

### SQL-RAG
- **Retrieval**: Gets schema, client list, conversation history
- **Augmentation**: Adds context to LLM prompt
- **Generation**: Creates SQL query
- **Execution**: Runs query and formats results

## 📚 Additional Resources

- [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)
- [Groq API Documentation](https://console.groq.com/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 🤝 Contributing

When modifying the workflow:
1. Update state schema if adding new fields
2. Update nodes if changing processing logic
3. Update edges if changing routing
4. Update this README if changing workflow

## 📄 License

[Your License Here]

---

**Built with using LangGraph, Groq, and PostgreSQL**
