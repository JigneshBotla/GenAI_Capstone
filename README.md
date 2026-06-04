### Our project is live on : https://genaicapstone-deco.streamlit.app

# Deco — Data Engineering AI Assistant & Observability Platform

## 📋 Team 3 Project Specifications

Deco is a premium, RAG-powered, agentic Data Engineering (DE) assistant and observability platform built on a hybrid architecture. It combines a dynamic Snowflake-based metadata schema catalog, live data lineage tracing, real-time pipeline monitoring, automated data quality (DQ) validations, incident diagnosis, and sandboxed natural-language-to-SQL querying. 

Deco resolves information fragmentation by unifying access to:
* **Codebases & Documentation (Unstructured)**: Airflow DAGs, dbt models, architecture markdown files, and compliance policies.
* **Data Catalogs & Lineage (Structured)**: Table details, column definitions, PII tags, masking rules, and dependency trees.
* **Orchestration & Operational Logs (Observability)**: Live execution runs, SLA/SLO compliance histories, and pipeline error log diagnostics.

---

## 🏗️ System Architecture & Data Flow

Deco uses a hybrid architecture that splits data and metadata between a scalable cloud-native data warehouse (**Snowflake**) and a semantic vector store (**ChromaDB**). This design balances deterministic catalog lookups with semantic documentation queries.


#### 📊 Text-Based Platform Flow Overview:
```text
                  +-------------------------------------------------+
                  |      Snowflake Data Warehouse (Capstone_DB)     |
                  +-------------------------------------------------+
                    │ (Bronze Schema)                  │
                    ▼                                  ▼
             [bronze.raw_users]              [bronze.raw_transactions]
                    │                                  │
          ┌─────────┴─────────┐              ┌─────────┴─────────┐
          │ (Valid ID)        │ (Null ID)    │ (Valid Tx ID)     │ (Null Tx ID)
          ▼                   ▼              ▼                   ▼
    [staging.stg_users]  [stg_users_     [staging.stg_     [stg_transactions_
    (Cleansed, Hashed,   quarantine]     transactions]        quarantine]
     Masked Phone)       (Quarantined)   (ABS Amount)        (Quarantined)
          │                                      │
          └─────────────────┬────────────────────┘
                            ▼
             [marts.fct_user_transactions] (Completed Spend Analytics)
                            │
                            ▼
             [marts.fct_user_churn] (Churn Classification Segmentations)
```

```text
                  +─────────────────────────────────────────────────+
                  |              Deco Core Agent Services           |
                  +─────────────────────────────────────────────────+
                                           ▲
                                           │ (Reads & Executes Queries)
                     ┌─────────────────────┼─────────────────────┐
                     ▼                     ▼                     ▼
             [ChromaDB Vector]     [Snowflake Metadata]   [AWS Bedrock Nova]
             (Semantic search      (Lineage, schemas,     (Orchestration &
             code & docs)           SLA/SLOs, runs log)    NL-to-SQL engine)
                                           ▲
                                           │
                                   [Streamlit UI app.py]
```

### Medallion Schemas Breakdown
1. **`BRONZE` (Ingestion)**: Direct replication of raw transactional data. Retains dirty records.
2. **`STAGING` (Silver)**: Conformed and cleansed layer. Critical errors are routed to quarantine tables. Email fields are anonymized using SHA-256 with a secret salt.
3. **`MARTS` (Gold)**: Aggregated analytical facts used for downstream BI reporting and churn predictions.
4. **`PUBLIC` (Catalog & Observability)**: Governance data storing active table properties, column details, line-level lineage, pipeline run execution history, and SLA thresholds.

---

## 🛠️ Technology Stack

| Component | Technology / Library | Role in Architecture |
|---|---|---|
| **Data Warehouse** | Snowflake | Hosts the Medallion schemas, data catalog, lineage map, and observability tables. |
| **Vector Database** | ChromaDB | Stores embeddings of dbt SQL files, Airflow DAGs, and markdown documentation. |
| **Embeddings** | SentenceTransformer (`all-MiniLM-L6-v2`) | Embedded local vector generator for codebase documentation. |
| **LLM Backend** | AWS Bedrock (Amazon Nova Lite) | Performs orchestration loops, tool calling, log analysis, and NL-to-SQL translation. |
| **Agent Observability** | Langfuse SDK | Monitors and traces Bedrock generation parameters, latency, and tool-span execution. |
| **Frontend UI** | Streamlit | Provides a premium, dark-themed control panel and interactive chat interface. |

---

## ⚙️ Data Quality & Quarantine Pipeline

When ETL transformations execute, conformed boundaries are enforced on raw data:

| Target Table | Source Table | Rules Enforced | Action Taken on Violation |
|---|---|---|---|
| `staging.stg_users` | `bronze.raw_users` | `id IS NOT NULL` | Conformed & clean load |
| `staging.stg_users_quarantine` | `bronze.raw_users` | `id IS NULL` | Quarantined with reason |
| `staging.stg_users` | `bronze.raw_users` | `email` or `country_code` is `NULL` | Warning logged; email hashed, country code kept conformed |
| `staging.stg_transactions` | `bronze.raw_transactions` | `amount_usd < 0` | Validated; negative amount flipped to positive using `ABS()` |
| `staging.stg_transactions_quarantine` | `bronze.raw_transactions` | `transaction_id IS NULL` | Quarantined with reason |
| `marts.fct_user_transactions` | `staging.stg_transactions` | `transaction_status = 'completed'` | Aggregated per-user rolling spend metric |
| `marts.fct_user_churn` | `staging.stg_users` & `marts.fct_user_transactions` | `last_active_timestamp > 30 days` | Sets `churn_status = 'CHURNED'` else `'ACTIVE'` |

### Data Masking & Anonymization Rules
* **Email Anonymization**: Primary email addresses in `staging.stg_users` are encrypted using `SHA2(LOWER(email) || 'SUPER_SECRET_SALT_123', 256)`.
* **Phone Masking**: Phone numbers are obfuscated keeping country code and the last 4 digits: `CONCAT('+', COALESCE(country_code, 'XX'), '-XXX-XXX-', RIGHT(phone_number, 4))`.

---

## 🔍 RAG (Retrieval-Augmented Generation) Feature

To answer "why" configurations or transformations exist, Deco uses **Retrieval-Augmented Generation** over project documentation and code files:

### 1. Ingestion & Advanced Chunking Strategy
* **Semantic Chunking (Prose)**: Markdown documents are split into parent chunks based on semantic shifts. The system tokenizes prose into sentences, generates embeddings for each sentence using the local SentenceTransformer model, and computes the cosine distance between consecutive sentences. Boundary splits are generated where the distance exceeds a dynamic threshold:
  $$\text{Threshold} = \text{Mean Distance} + 0.8 \times \text{Standard Deviation}$$
  Sentences are then grouped between these boundaries to form semantic parent chunks.
* **Parent-Child Chunking**: To optimize semantic retrieval, Deco indexes granular child chunks while returning the broader parent context to the LLM:
  * **Markdown Prose**: Parent chunks are sliced into child chunks using overlapping sentence windows (window size of 2 sentences, step size of 1 sentence).
  * **Codebase Files**: Code scripts are sliced into 30-line parent chunks (with 5 lines overlap) and 10-line child chunks (with 3 lines overlap).
  * **Metadata Binding**: Only child chunks are embedded and registered in ChromaDB. Each child record's metadata contains the `parent_id`, `parent_text` block, and `chunk_type = 'child'`.

### 2. Hybrid Search & Reciprocal Rank Fusion (RRF) Querying
When an engineer asks a general architectural question, the agent triggers the `search_codebase_and_docs` tool:
1. **Vector Retrieval**: Queries ChromaDB for the top 20 nearest neighbor child chunks using cosine similarity.
2. **Keyword Retrieval (BM25)**: Evaluates all child chunks against the query tokens using a local BM25 scoring algorithm.
3. **Reciprocal Rank Fusion (RRF)**: Fuses both rankings using RRF with parameter $k = 60$:
   $$Score_{RRF}(d) = \sum_{m \in \{\text{Vector}, \text{BM25}\}} \frac{1}{60 + \text{Rank}_{m}(d)}$$
4. **Parent Context Retrieval**: The top 3 chunks sorted by RRF score are selected. The system retrieves the `parent_text` associated with each child chunk, de-duplicates by `parent_id`, and feeds the final parent contexts to the LLM.


---

## 🤖 Deco Agent Core & Tool Capabilities

The agent loop is orchestrated via the AWS Bedrock Converse API using **Amazon Nova Lite** (`amazon.nova-lite-v1:0`), equipped with tools that act as standard interface endpoints.

### Agent-to-System Protocol (MCP Pattern)
Deco's tool registration framework operates similarly to the **Model Context Protocol (MCP)**. By defining rigid JSON schemas for parameters and binding them to deterministic database execution wrappers, the LLM is decoupled from direct database drivers:

```
[User Input] ➔ [Bedrock Agent Core]
                       │ (Tool Call Specification)
                       ▼
            [Deco MCP-like Interface]
            ├── get_table_schema ────────► (Snowflake public.columns)
            ├── get_table_lineage ───────► (Snowflake public.lineage)
            ├── get_pipeline_history ────► (Snowflake public.pipeline_runs)
            ├── get_failed_run_diagnosis ─► (Local Log Files)
            ├── trigger_dq_check ────────► (Snowflake Mutation & Run Logger)
            ├── nl2sql ──────────────────► (Snowflake Query Runner)
            └── search_codebase_and_docs ─► (ChromaDB Vector Q&A)
```

### Available Tools Specification
1. **`search_codebase_and_docs`**: Performs RAG semantic searches across ChromaDB (documentation, design records, dbt models, DAG configurations).
2. **`get_table_schema`**: Retrieves definitions, data types, descriptions, PII flags, and masking policies from Snowflake `public.columns`.
3. **`get_table_lineage`**: Fetches upstream sources and downstream consumers dynamically from Snowflake `public.lineage`.
4. **`get_pipeline_history`**: Queries Snowflake `public.pipeline_runs` and computes recent run statistics and SLO compliance.
5. **`get_failed_run_diagnosis`**: Inspects pipeline execution error trace logs and synthesizes immediate, actionable troubleshooting recommendations.
6. **`trigger_data_quality_check`**: Runs programmatic data validation checks (integrity constraints, uniqueness tests, PII validation) on a Snowflake table, logs execution metrics, and updates table records.
7. **`nl2sql`**: Converts natural language requests into a clean Snowflake SQL `SELECT` statement, validates it against a security blocklist, executes the query against Snowflake, and displays tabular results.

### Security-Conformed SQL Sandbox (`nl2sql`)
To prevent injection attacks, the query generator enforces strict security boundaries:
* **DDL/DML Filter**: Standardizes tokens to check for and block query keywords including `DELETE`, `TRUNCATE`, `INSERT`, `UPDATE`, `DROP`, `CREATE`, `ALTER`, `REPLACE`, `MERGE`, `GRANT`, `REVOKE`, `UPSERT`, `EXEC`, and `EXECUTE`.
* **Read-Only Enforcer**: Regex parsing guarantees that only queries beginning with `SELECT` or `WITH` can be executed against Snowflake.

---

## 🖥️ Streamlit Frontend Dashboard UI

### 1. Control Room Sidebar
* **AWS Bedrock Configuration**: Inputs to dynamically specify AWS credentials (`AWS Access Key ID`, `AWS Secret Access Key`, and `AWS Region`).
* **Active Connectivity Badges**: Real-time indicator displaying the status of AWS Bedrock.
* **Manual Data Quality Trigger**: A dropdown menu dynamically pulling registered tables from Snowflake, enabling engineers to manually trigger test assertions.
* **Cache Controller**: Global refresh trigger to pull updated table statistics, execution histories, and lineage maps from Snowflake.

### 2. Main Navigation Tabs

#### Tab A: DE Platform Control Panel
* **🗂️ Data Catalog**: Displays interactive metadata cards indicating schema names, conformed row counts, and data sizes. Includes collapsible drawers showing full column lists, descriptions, and color-coded `🚨 PII` compliance badges.
* **🔗 Data Lineage**: A responsive node chart visualizer mapping the flow of data through the Medallion stages (Bronze $\rightarrow$ Silver/Staging $\rightarrow$ Gold/Marts). Selecting a specific table dynamically builds an isolated upstream/downstream dependency diagram.
* **⚡ Operations & SLOs**: Displays live SLA tracking status blocks, daily target completion times, duration breaches, and tabular pipeline execution logs colored by status (`SUCCESS` / `FAILED`).

#### Tab B: Chat with Deco
* **Real-time Chat Window**: Conversation area with scroll buffers and quick-prompt suggestion pills for schema searches, PII validations, incident troubleshooting, and lineage traces.
* **Deco's Thinking Expander**: A collapsible dropdown rendering real-time thoughts and active tool calls (parameters, target execution, and output text) to prevent flooding the main chat container.
