import os
import sqlite3
import json

def setup_directories():
    directories = [
        "mock_data",
        "mock_data/docs",
        "mock_data/codebase",
        "mock_data/codebase/dbt/models/staging",
        "mock_data/codebase/dbt/models/marts",
        "mock_data/codebase/airflow/dags",
        "mock_data/logs"
    ]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    print("Created mock directories.")

def setup_docs():
    # 1. Architecture overview
    architecture_overview = """# architecture_overview.md

Welcome to the internal system architecture and design documentation for "Deco" — the RAG-powered Data Engineering AI Assistant and Observability Platform. This reference is designed to assist engineers in understanding our storage layers, routing topologies, and semantic systems.

## 1. What Deco Is and the Problem It Solves

Data platform operations often suffer from critical information fragmentation. When debugging a failing ingestion task or verifying regulatory compliance, engineers must typically hop between disparate environments:
* **Codebases**: Stored in Git repositories containing dbt transformation scripts and Apache Airflow DAG workflows.
* **Data Catalogs**: Catalog schemas, line-level lineage lists, and compliance data tags.
* **Execution Logs**: Historical error files, run duration metrics, and SLA status monitors.

Deco resolves this fragmentation by unifying all of these developer-facing views into a single, interactive control room. It acts as an agentic co-pilot that helps teams query schema definitions, trace lineage dependencies, inspect system health logs, perform sandbox-restricted SQL database queries, and trigger active data quality validation checks directly from a Streamlit conversational portal.

## 2. Hybrid System Architecture (Snowflake + ChromaDB)

Deco uses a hybrid architecture combining a deterministic database management system (Snowflake) with a semantic search engine (ChromaDB). 

```
                                  +-----------------------+
                                  |     Streamlit UI      |
                                  +-----------+-----------+
                                              |
                                              v
                                  +-----------+-----------+
                                  |   Deco Agent Core     |
                                  | (AWS Bedrock Nova)    |
                                  +-----+-----------+-----+
                                        |           |
                        +---------------+           +---------------+
                        |                                           |
                        v                                           v
            +-----------+-----------+                   +-----------+-----------+
            |  Vector Search DB     |                   |  Lakehouse Compute    |
            |  (ChromaDB Local)     |                   | (Snowflake Database)  |
            +-----------+-----------+                   +-----------+-----------+
                        |                                           |
                        v                                           v
            * Codebase files (.sql, .py)                 * Medallion data tables
            * Markdown design records                    * Catalog & Lineage tables
            * Technical documentation                    * Pipeline & SLA logs
```

* **Snowflake (Lakehouse Compute)**: Snowflake acts as our primary datastore and query engine. It hosts all structured production data tables, data quality logs, schemas, lineage logs, and execution times. Using Snowflake guarantees deterministic, real-time retrievals of critical schemas and performance stats.
* **ChromaDB (Vector Database)**: ChromaDB acts as our semantic knowledge retriever. It stores embedded chunks of unstructured repository assets: dbt transformation code blocks, DAG structures, architectural decision records (ADRs), and markdown docs. Chunks are indexed using the `all-MiniLM-L6-v2` SentenceTransformer model.
* **Why They Complement Each Other**: If a junior engineer queries: *"Which table holds transactions, and what is its validation lineage?"*, the agent runs a semantic query against ChromaDB to find architectural context (from markdown documentation) while concurrently executing a structured query against Snowflake catalog tables (`public.columns` and `public.lineage`) to fetch the exact schema definition and upstream dependency paths.

## 3. End-to-End Medallion Data Flow

The platform implements the standard Medallion pattern (Bronze -> Silver -> Gold). Data flows along two parallel pipelines:

### User Profiles Pipeline
1. **Bronze Layer (`bronze.raw_users`)**: Ingests raw, raw-structured user details directly from PostgreSQL database replicas. Dirty records, duplicate IDs, and null rows are retained here.
2. **Silver Layer (`staging.stg_users` & `staging.stg_users_quarantine`)**: Re-maps columns to standard types, enforces email hashing with a dynamic salt, and masks phone numbers.
   * If a row contains a valid, non-null ID, it is routed to `staging.stg_users`.
   * If the `id` column is null, the row is routed to `staging.stg_users_quarantine` along with a reason tag.
3. **Gold Layer (`marts.fct_user_churn`)**: Combines conformed user records from `staging.stg_users` with transactional facts from `marts.fct_user_transactions` to classify users as `ACTIVE` or `CHURNED`.

### Financial Transactions Pipeline
1. **Bronze Layer (`bronze.raw_transactions`)**: Receives transaction records from payment processor API dumps. Negative values and null transaction IDs are retained.
2. **Silver Layer (`staging.stg_transactions` & `staging.stg_transactions_quarantine`)**: Standardizes currency fields, filters out sandbox tests, and converts negative amounts.
   * If `transaction_id` is null, the row is routed to `staging.stg_transactions_quarantine`.
   * If a row has a negative amount, the absolute value is computed via `ABS(amount_usd)`.
3. **Gold Layer (`marts.fct_user_transactions` & `marts.fct_user_churn`)**:
   * `marts.fct_user_transactions` aggregates lifetime transaction counts, total USD spend, and last active timestamps grouped by user.
   * `marts.fct_user_churn` utilizes these transactional aggregates to analyze active retention metrics.

## 4. Medallion Schemas Breakdown

Every schema inside the `Capstone_DB` Snowflake database has a dedicated responsibility:

| Schema | Target Audience | Role & Operations | Primary Tables |
|---|---|---|---|
| `BRONZE` | Data Ingestion | Append-only raw replication. Retains duplicate rows, missing fields, and bad formatting for auditing. | `raw_users`, `raw_transactions` |
| `STAGING` | Data Engineers | Cleanses, standardizes types, hashes PII, applies data quality rules, and routes invalid rows to quarantine. | `stg_users`, `stg_users_quarantine`, `stg_transactions`, `stg_transactions_quarantine` |
| `MARTS` | BI Analysts / ML | Highly optimized business logic aggregates. Enforces unique grains and aggregates. | `fct_user_transactions`, `fct_user_churn` |
| `PUBLIC` | Governance / Agent | Catalog schemas, line-level lineage lists, SLA targets, execution run logs, and data quality check histories. | `tables`, `columns`, `lineage`, `pipeline_runs`, `pipeline_slo` |

## 5. Agent Service Interactions & Bedrock Orchestration

The Deco assistant is driven by AWS Bedrock utilizing the `amazon.nova-lite-v1:0` model. The services coordinate as follows:
1. **User Request**: The user submits a query via the Streamlit frontend.
2. **Bedrock Routing**: The agent decides if it needs semantic information (searching ChromaDB) or structured data (querying Snowflake via tools).
3. **Tool Execution**:
   * If a semantic search is needed, the agent invokes `search_codebase_and_docs` which calls ChromaDB.
   * If schema details are needed, the agent invokes `get_table_schema` which queries `public.columns`.
   * If lineage maps are requested, the agent invokes `get_table_lineage` which queries `public.lineage`.
   * If analytical stats are requested, the agent invokes `nl2sql` which compiles and runs a query directly against Snowflake database schemas.
4. **Synthesis & Frontend rendering**: The agent receives the tools' responses, isolates its reasoning within `<thinking>` tags (rendered as a collapsible dropdown in Streamlit), and outputs the clean markdown text to the user.
5. **Observability Tracking**: Every model execution step and tool latency span is reported to Langfuse via the Langfuse SDK for performance tracking.

## 6. Stack Selection Rationale

* **Snowflake**: Selected for structured operational data, conformed schemas, and catalog metadata. It provides secure DDL/DML sandboxes and fast analytical queries.
* **ChromaDB**: Chosen as a lightweight, persistent local vector database to index and search unstructured developer assets (dbt, markdown documents, DAG files).
* **AWS Bedrock Nova**: Selected as the LLM backend for its robust tool-calling Converse API, low latency, and parsing speeds.
* **SentenceTransformer (`all-MiniLM-L6-v2`)**: Used as a local, free embedding model to convert repository files into 384-dimensional vectors without external API calls.
* **Langfuse**: Chosen to trace agent execution paths, map latency bottlenecks, and capture tool execution errors.
"""

    # 2. Design decisions - Data Governance Policy
    data_governance_policy = """# data_governance_policy.md

Compliance and data governance represent core components of the Deco platform. This document defines our Data Quality (DQ) pipeline rules, email anonymization logic, phone number masking structures, PII metadata audits, and quarantine architectures.

## 1. Data Quality (DQ) Pipeline Rules

To guarantee conformed values enter downstream analytical datasets, specific validation actions are triggered during SQL-based ETL runs in Snowflake. The table below documents the exact rule constraints and violation routing:

| Source Table | Target Table | Validating Rule Constraint | Action Taken on Violation / Logic |
|---|---|---|---|
| `bronze.raw_users` | `staging.stg_users` | `id IS NOT NULL` | Conformed clean record is loaded. |
| `bronze.raw_users` | `staging.stg_users_quarantine` | `id IS NULL` | Record is quarantined with a reason tag of `'Critical: Null user_id (id)'` and a quarantined timestamp. |
| `bronze.raw_users` | `staging.stg_users` | `email` or `country_code` is NULL | Warning is logged. Email is hashed (if null, remains null), and country code is standard-mapped (defaults to `'XX'` during phone masking and `'NONE'` for empty fields). |
| `bronze.raw_transactions` | `staging.stg_transactions` | `amount_usd >= 0` | Standard load. Negative amount values are auto-corrected using absolute value: `ABS(amount_usd)`. |
| `bronze.raw_transactions` | `staging.stg_transactions_quarantine` | `transaction_id IS NULL` | Record is quarantined with reason tag `'Critical: Null transaction_id (transaction_id)'`. |
| `staging.stg_transactions` | `marts.fct_user_transactions` | `transaction_status = 'completed'` | Filters out failing payments and test transactions. Only completed payments are aggregated per user. |
| `staging.stg_users` & `marts.fct_user_transactions` | `marts.fct_user_churn` | `DATEDIFF('day', last_active_timestamp, CURRENT_DATE()) <= 30` | If the last active date is more than 30 days ago (or is null), `churn_status` is marked as `'CHURNED'`. Otherwise, it is flagged as `'ACTIVE'`. |

## 2. Email Anonymization (Hashing)

Personally Identifiable Information (PII) must be obscured to comply with CCPA and GDPR constraints before analysts can query the tables.
* **Exact Hashing Formula**: 
  ```sql
  SHA2(LOWER(email) || 'SUPER_SECRET_SALT_123', 256)
  ```
  *(Note: In the active python code base implementation, this static salt value is loaded dynamically from the `EMAIL_HASH_SALT` environment variable to prevent secret leakage).*
* **Why Salting is Used**: Plain SHA-256 hashes are vulnerable to rainbow table dictionary attacks where common email addresses can be brute-forced. Appending a secret "salt" (or Pepper) prior to hashing ensures that the resulting string cannot be mapped back to its original plain text without possession of the system salt.
* **Where This Happens**: This masking is applied inside the staging layer when transforming data from `bronze.raw_users` into `staging.stg_users`. The output is loaded into the `hashed_email` column.

## 3. Phone Number Masking

Phone numbers are masked to balance analytical utility with consumer privacy.
* **Exact Masking Formula**:
  ```sql
  CONCAT('+', COALESCE(country_code, 'XX'), '-XXX-XXX-', RIGHT(phone_number, 4))
  ```
* **Why This Logic is Applied**: It preserves the regional context (the country code) and the final 4 digits of the phone number. Preserving this metadata lets customer service trace payment receipts and geo-segment analytics without exposing the full phone digits.
* **Where This Happens**: This transformation is executed during staging ETL loads from `bronze.raw_users` to the `masked_phone` column in `staging.stg_users`.

## 4. PII Metadata Tagging and Catalog Surfacing

Governance audits require tracking where PII is stored. 
* **Metadata Table Registry**: The Snowflake metadata table `public.columns` maintains a registry of column classifications. For every column, it tracks:
  - `is_pii` (BOOLEAN): Flagged as `TRUE` if the field contains raw or masked PII.
  - `pii_type` (VARCHAR): Classified as `NAME`, `EMAIL`, `PHONE`, or `NONE`.
  - `masking_policy` (VARCHAR): Details the anonymization status (e.g. `'SHA-256 with Pepper Salt'`, `'Masked: keeping code + last 4 digits'`, or `'Raw (Restricted Access)'`).
* **UI Catalog Surface**: The Streamlit interface queries this table dynamically to display schema tables. When users click on a table in the Catalog tab, columns tagged as PII are highlighted with alert indicators alongside their corresponding masking policies.

## 5. Purpose of Quarantine Tables

Rather than dropping raw rows that fail validation checks (e.g., null identifiers), the platform routes them to dedicated quarantine tables: `staging.stg_users_quarantine` and `staging.stg_transactions_quarantine`.
* **Lineage & Audit trails**: Retaining failed rows ensures that the raw data volume in Bronze matches the combined volume of staging clean rows and quarantined rows.
* **Debugging**: Data engineers can query quarantine tables to identify upstream extraction failures (e.g., API parser bugs generating null IDs).
* **Reprocessing**: Once upstream ingestion pipelines are corrected, quarantined rows can be reprocessed without rebuilding the entire historical Bronze catalog.
"""

    # 3. Codebase reference
    codebase_reference = """# codebase_reference.md

This codebase reference serves as an engineering manual for our dbt models, Apache Airflow workflows, and vector indexing operations. It provides precise descriptions of the underlying SQL queries, ingestion schedules, and chunking parameters.

## 1. Staging dbt Models (Silver Layer)

Staging models ingest raw fields from Bronze replicas, execute cleaning routines, and separate malformed rows into quarantine tables.

### `stg_users`
* **Source Table**: `bronze.raw_users`
* **Transformation Logic**:
  * Filtering: Filters out rows where the primary key `id` is null (`WHERE id IS NOT NULL`).
  * Email Hashing: Applies lowercase standardization and hashes values:
    ```sql
    SHA2(LOWER(email) || '{salt}', 256)
    ```
  * Phone Masking: Preserves region code and last 4 digits:
    ```sql
    CONCAT('+', country_code, '-XXX-XXX-', RIGHT(phone_number, 4))
    ```
  * Country Standardizing: Converts country strings to uppercase via `UPPER(country_code)`.
* **Output Columns**:
  - `user_id` (TEXT): Conformed primary customer identifier.
  - `hashed_email` (TEXT): Salted hash of user's email.
  - `masked_phone` (TEXT): Obfuscated customer phone contact.
  - `country` (TEXT): Standardized uppercase country code.
  - `created_timestamp` (TIMESTAMP): Standardized customer creation timestamp.
  - `updated_timestamp` (TIMESTAMP): Standardized customer profile update timestamp.
* **Purpose**: Provides conformed, privacy-compliant user demographics for analytical marts.

### `stg_users_quarantine`
* **Source Table**: `bronze.raw_users`
* **Transformation Logic**: Filters exclusively for critical validation failures where `id` is missing (`WHERE id IS NULL`). Hardcodes the reason text `'Critical: Null user_id (id)'`.
* **Output Columns**:
  - `id` (TEXT): Raw primary customer index (always null here).
  - `first_name` (TEXT), `last_name` (TEXT), `email` (TEXT), `phone_number` (TEXT), `country_code` (TEXT): Raw unmasked demographic details.
  - `created_at` (TIMESTAMP), `updated_at` (TIMESTAMP): Ingestion timestamps.
  - `quarantine_reason` (TEXT): Reason for quarantine routing.
  - `quarantined_at` (TIMESTAMP): Current execution timestamp.
* **Purpose**: Holds broken raw records to facilitate debugging and ingestion metrics tracking.

### `stg_transactions`
* **Source Table**: `bronze.raw_transactions`
* **Transformation Logic**:
  * Filtering: Rejects rows with null `transaction_id` (`WHERE transaction_id IS NOT NULL`). Rejects status values matching `'test_payment'`.
  * Correction: Multiplies negative value amounts by -1 using absolute value conversion: `ABS(CAST(amount_usd AS DECIMAL(18, 2)))`.
* **Output Columns**:
  - `transaction_id` (TEXT): Standardized primary transaction ID.
  - `user_id` (TEXT): Cleaned foreign key mapping users.
  - `transaction_amount_usd` (DECIMAL): Auto-corrected payment value in USD.
  - `transaction_status` (TEXT): Lowercase transaction status code.
  - `payment_method` (TEXT): Consolidated billing method string.
  - `transaction_timestamp` (TIMESTAMP): Standardized charge timestamp.
* **Purpose**: Represents cleaned, conformed transactional ledger entries ready for financial aggregation.

### `stg_transactions_quarantine`
* **Source Table**: `bronze.raw_transactions`
* **Transformation Logic**: Filters exclusively for raw transactions that lack identifiers (`WHERE transaction_id IS NULL`). Hardcodes the reason text `'Critical: Null transaction_id (transaction_id)'`.
* **Output Columns**:
  - `transaction_id` (TEXT): Always null here.
  - `user_id` (TEXT), `amount_usd` (NUMERIC), `status` (TEXT), `payment_method` (TEXT), `transaction_time` (TIMESTAMP): Raw transaction attributes.
  - `quarantine_reason` (TEXT): Reason for quarantine.
  - `quarantined_at` (TIMESTAMP): Timestamp of quarantine routing.
* **Purpose**: Isolates raw transaction records missing transaction identifiers.

---

## 2. Marts dbt Models (Gold Layer)

Marts aggregate transaction details and demographic segments to compute metrics for reporting and retention analytics.

### `fct_user_transactions`
* **Source Models**: `staging.stg_transactions`
* **Aggregation Logic**: Grouped by `user_id`, filtering for `'completed'` statuses. Aggregates lifetime transaction count (`COUNT(*)`), total spend (`SUM(transaction_amount_usd)`), and latest activity (`MAX(transaction_timestamp)`).
* **Grain of Table**: One row per unique user who completed a purchase.
* **Business Purpose**: Tracks purchasing behavior and flags user activity updates.

### `fct_user_churn`
* **Source Models**: Joins `staging.stg_users` (demographics) with `marts.fct_user_transactions` (spend aggregates) on `user_id`.
* **Aggregation Logic**: Uses the last active date to check active retention status:
  ```sql
  CASE 
      WHEN last_active_timestamp IS NULL THEN 'CHURNED'
      WHEN DATEDIFF('day', last_active_timestamp, CURRENT_DATE()) > 30 THEN 'CHURNED'
      ELSE 'ACTIVE'
  END AS churn_status
  ```
* **Grain of Table**: One row per unique conformed user.
* **Business Purpose**: Serves as the primary source table for churn predictions and customer segment marketing hook reports.

---

## 3. Airflow DAG Workflow & Execution Logging

Our orchestration pipelines run via Apache Airflow DAGs.

* **DAG Naming Conventions**:
  * Operational Pipelines: Scheduled production pipelines are suffixed with `_pipeline` (e.g. `user_analytics_pipeline`).
  * Data Quality Runs: Pipelines triggered programmatically to run data quality checks are prefixed with `dq_etl_` followed by the target table name (e.g. `dq_etl_bronze.raw_users`).
* **Task Dependency Chain**:
  1. `extract_users_from_rds` & `extract_transactions_from_stripe` (Extract/Ingest raw tables)
  2. `load_s3_to_snowflake_bronze` (Bulk-copy raw files into Bronze schema)
  3. `dbt_run_transformations` (Build view/table stages in Staging and Marts)
  4. `dbt_test_validations` (Assert uniqueness and data quality bounds)
* **Execution Logs mapping to Snowflake**:
  When a DAG begins and finishes, execution details are logged to the `public.pipeline_runs` table in Snowflake.
* **Triggering pipeline_run Logs**:
  * Daily scheduled runs (e.g., at 2:00 AM UTC).
  * Manual actions triggered through Deco's control sidebar or by tools like `trigger_data_quality_check`.
  * Logs record: `run_id`, `pipeline_name`, `status` (`SUCCESS`/`FAILED`), `start_time`, `end_time`, `duration_sec`, `error_message`, and the host file `log_path`.

---

## 4. ChromaDB Ingestion and Advanced indexing Pipeline

Deco utilizes an advanced Retrieval-Augmented Generation (RAG) indexing and querying architecture to ensure highly precise, context-grounded answers.

* **Scanned Folders**: The pipeline scans `mock_data/docs` (markdown files) and `mock_data/codebase` (dbt SQL models, Airflow DAG python scripts).
* **Semantic Chunking (Prose)**: Markdown documents are split into parent chunks based on semantic shifts. The system tokenizes prose into sentences, generates embeddings for each sentence using the local SentenceTransformer model, and computes the cosine distance between consecutive sentences. Boundary splits are generated where the distance exceeds a dynamic threshold:
  $$\text{Threshold} = \text{Mean Distance} + 0.8 \times \text{Standard Deviation}$$
  Sentences are then grouped between these boundaries to form semantic parent chunks.
* **Parent-Child Chunking Strategy**:
  To optimize semantic retrieval, Deco indexes granular child chunks while returning the broader parent context to the LLM:
  * **Markdown Prose**: Parent chunks are sliced into child chunks using overlapping sentence windows (window size of 2 sentences, step size of 1 sentence).
  * **Codebase Files**: Code scripts are sliced into 30-line parent chunks (with 5 lines overlap) and 10-line child chunks (with 3 lines overlap).
  * **Metadata Binding**: Only child chunks are embedded and registered in ChromaDB. Each child record's metadata contains the `parent_id`, `parent_text` block, and `chunk_type = 'child'`.
* **Hybrid Search & Reciprocal Rank Fusion (RRF)**:
  Queries execute a parallel hybrid retrieval flow:
  1. **Vector Retrieval**: Queries ChromaDB for the top 20 nearest neighbor child chunks using cosine similarity.
  2. **Keyword Retrieval (BM25)**: Evaluates all child chunks against the query tokens using a local BM25 scoring algorithm.
  3. **Reciprocal Rank Fusion (RRF)**: Fuses both rankings using RRF with parameter $k = 60$:
     $$Score_{RRF}(d) = \sum_{m \in \{\text{Vector}, \text{BM25}\}} \frac{1}{60 + \text{Rank}_{m}(d)}$$
  4. **Parent Context Retrieval**: The top 3 chunks sorted by RRF score are selected. The system retrieves the `parent_text` associated with each child chunk, de-duplicates by `parent_id`, and feeds the final parent contexts to the LLM.
"""

    # 4. Tool API reference
    tool_api_reference = """# tool_api_reference.md

This tool and API reference manual documents the tool definitions, parameter definitions, safety constraints, and framework schemas that define the Deco agent execution cycle.

## 1. Deco Agent Tools

Deco coordinates operations by routing natural language intents into a series of tool executions. The seven tools integrated into the agent loop are detailed below:

### `search_codebase_and_docs`
* **Purpose**: Performs RAG semantic searches across our vectorized documentation and codebase directories to extract design context.
* **Input Parameters**:
  - `query` (string, required): Semantic search query phrase to resolve.
* **Backend System**: Queries ChromaDB vector store collection `deco_knowledge_base`.
* **Returns**: Returns the top 3 semantically relevant code or documentation text chunks. The agent uses this output to formulate descriptive architectural explanations.
* **Example Query**: *"Where is the hashing logic for email fields documented?"*

### `get_table_schema`
* **Purpose**: Retrieves schema column definitions, data types, descriptions, PII flags, and masking policies for a target table.
* **Input Parameters**:
  - `table_id` (string, required): Full table path name (e.g. `staging.stg_users`).
* **Backend System**: Queries the Snowflake metadata table `public.columns` and `public.tables`.
* **Returns**: A structured JSON object containing table stats and column properties. The agent formats this into schema tables for the user.
* **Example Query**: *"What are the data types and PII tags of staging.stg_users?"*

### `get_table_lineage`
* **Purpose**: Traces upstream data sources and downstream consumer tables for a target table.
* **Input Parameters**:
  - `table_id` (string, required): The target table path (e.g. `marts.fct_user_churn`).
  - `direction` (string, optional): Upstream source or downstream consumer path to trace (`'upstream'`, `'downstream'`, or `'both'`).
* **Backend System**: Queries the Snowflake dependency lineage table `public.lineage`.
* **Returns**: JSON mapping upstream tables, lineage types, and downstream target names. The agent parses this to present dependency paths.
* **Example Query**: *"Show me the upstream data lineage for marts.fct_user_churn."*

### `get_pipeline_history`
* **Purpose**: Retrieves recent execution runs and evaluates SLO compliance targets.
* **Input Parameters**:
  - `limit` (integer, optional): Number of recent pipeline runs to return (defaults to 10).
* **Backend System**: Queries Snowflake tables `public.pipeline_runs` and `public.pipeline_slo`.
* **Returns**: Recent run durations, statuses (`SUCCESS`/`FAILED`), and SLO compliance percentages. The agent synthesizes this to identify latency anomalies.
* **Example Query**: *"What is the recent run history and SLO compliance status for our pipeline?"*

### `get_failed_run_diagnosis`
* **Purpose**: Analyzes local execution log files to diagnose pipeline incidents and suggest resolutions.
* **Input Parameters**:
  - `run_id` (string/integer, required): ID of the failed execution run to diagnose.
* **Backend System**: Parses the local log file corresponding to the run ID path stored in `public.pipeline_runs.log_path`.
* **Returns**: The raw traceback and error lines from the log file. The agent synthesizes this log output to generate troubleshooting recommendations.
* **Example Query**: *"Run 1002 failed. Can you read the logs and diagnose what broke?"*

### `trigger_data_quality_check`
* **Purpose**: Runs a data quality check suite against a target table in Snowflake, inserts the result run log, and returns the check outcomes.
* **Input Parameters**:
  - `table_id` (string, required): Target database table path to test.
* **Backend System**: Queries table properties, registers a pipeline run status entry in `public.pipeline_runs`, and writes diagnostic assertions to local log files.
* **Returns**: A structured summary detailing total rows, null columns, and validation outcomes. The agent displays this to confirm data integrity.
* **Example Query**: *"Deco, run a manual data quality check on staging.stg_users."*

### `nl2sql`
* **Purpose**: Translates natural language questions into conformed SQL SELECT queries, executes them in Snowflake, and returns the query results.
* **Input Parameters**:
  - `nl_query` (string, required): The natural language query question to execute.
  - `query` (string, optional): A pre-compiled SQL query string if generated by LLM reasoning.
* **Backend System**: Translates the natural language question to a SQL string and executes it against Snowflake schemas (`BRONZE`, `STAGING`, `MARTS`, `PUBLIC`).
* **Returns**: A formatted pandas dataframe table containing the query outputs.
* **Example Query**: *"How many conformed records are in marts.fct_user_churn grouped by churn_status?"*

---

## 2. SQL Sandbox Security Enforcements

The `nl2sql` tool executes queries dynamically. To prevent database corruption and restrict access to query operations, a two-layer security sandbox is enforced:

1. **Blocked Keyword Blacklist**:
   Prior to executing any query, the tool runs a check. If the SQL query contains any of the following DDL/DML keywords, it is rejected:
   * Data Mutation/Deletions: `DELETE`, `TRUNCATE`, `INSERT`, `UPDATE`, `UPSERT`, `MERGE`, `REPLACE`
   * Schema Modifications: `DROP`, `CREATE`, `ALTER`
   * Access Controls: `GRANT`, `REVOKE`
   * Code Executions: `EXEC`, `EXECUTE`
2. **Read-Only SELECT Regex Enforcement**:
   The query string must match a strict regex check ensuring the string begins with a read-only token:
   * Must begin with `SELECT` or a Common Table Expression (`WITH`) syntax.
   * Multiple query execution is blocked; only single statements are allowed.

---

## 3. Decoupled Tool Registration Pattern

Deco uses a decoupled tool registration pattern that resembles the Model Context Protocol (MCP):
* **Schema Definition**: Each tool is registered as a JSON schema specifying the parameter names, data types, descriptions, and required keys.
* **Execution Mapping**: When Bedrock resolves a tool-call intent, it returns the tool name and parameter arguments. The agent core acts as a router, matching the tool name to a local python method wrapper.
* **Decoupling Driver**: This pattern keeps LLM reasoning decoupled from the underlying database driver. The model does not manage connections or execute SQL strings directly; instead, it outputs structured JSON parameters that python execution wrappers execute.

---

## 4. Bedrock Converse API Loop & Observability

* **Converse Loop**: The interaction uses the AWS Bedrock Converse API. When a user submits a query, the agent parses the response. If the model outputs a `toolUse` block, the agent runs the corresponding python wrapper, appends the tool result to the conversation history, and calls the Converse API again. This loop runs until the model returns a final text output.
* **Default LLM Model**: `amazon.nova-lite-v1:0` (with fallback to `amazon.nova-pro-v1:0` for complex reasoning).
* **Langfuse Tracing**: The Langfuse SDK wraps both the Converse API and the local python tool wrappers. Each run maps parent spans (user prompt) to child spans (individual tool latency, SQL query execution times, token counts) to trace agent costs and query performance.

---

## 5. Streamlit Frontend Integration

The Streamlit control panel integrates with our Snowflake metadata tables and agent tools:

* **Data Catalog Tab**: Powered by `MetadataHelper.get_all_tables()` and `MetadataHelper.get_table_details()`. Displays dynamic table sizes (MB) and row counts by reading `public.tables` in Snowflake.
* **Data Lineage Tab**: Powered by `MetadataHelper.get_lineage()`. It traces upstream and downstream nodes by querying the `public.lineage` table.
* **Operations & SLOs Tab**: Powered by `MetadataHelper.get_pipeline_history()` and `MetadataHelper.get_pipeline_slo_compliance()`. It calculates success rates and checks SLA thresholds.
* **Chat with Deco Tab**: Integrates with the `AgentCore` loop, rendering the conversation log. Uses a custom parser to extract and render the model's `<thinking>` tags in a collapsible expander.
* **DQ Trigger Sidebar**: Provides a dropdown to select a target table and trigger a test run. This calls `MetadataHelper.run_dq_etl_pipeline()`, which runs validation logic, inserts records to `public.pipeline_runs`, and surfaces outcomes in the chat.
"""

    # Write files
    with open("mock_data/docs/architecture_overview.md", "w") as f:
        f.write(architecture_overview)
    with open("mock_data/docs/data_governance_policy.md", "w") as f:
        f.write(data_governance_policy)
    with open("mock_data/docs/codebase_reference.md", "w") as f:
        f.write(codebase_reference)
    with open("mock_data/docs/tool_api_reference.md", "w") as f:
        f.write(tool_api_reference)
        
    print("Created mock markdown docs.")

def setup_codebase():
    # 1. dbt staging: stg_users.sql
    stg_users_sql = """-- stg_users.sql
-- Cleans and hashes raw user profile information from bronze.raw_users.
-- Implements SHA-256 hashing for compliance with ADR PII standards.

WITH raw_source AS (
    SELECT
        id AS user_id,
        first_name,
        last_name,
        email,
        phone_number,
        country_code,
        created_at,
        updated_at
    FROM {{ source('bronze', 'raw_users') }}
)

SELECT
    user_id,
    -- Hashing PII for GDPR compliance (ADR-002)
    SHA2(LOWER(email) || 'f8c3d9b1e5a26748c9d0e1f2b3a4c5d6') AS hashed_email,
    -- Masking phone number (keeps country code and last 4 digits)
    CONCAT('+', country_code, '-XXX-XXX-', RIGHT(phone_number, 4)) AS masked_phone,
    UPPER(country_code) AS country,
    CAST(created_at AS TIMESTAMP) AS created_timestamp,
    CAST(updated_at AS TIMESTAMP) AS updated_timestamp
FROM raw_source
"""

    # 2. dbt staging: stg_transactions.sql
    stg_transactions_sql = """-- stg_transactions.sql
-- Cleans transaction records from bronze.raw_transactions.
-- Casts price values, extracts calendar attributes, and filters test payments.

WITH raw_source AS (
    SELECT
        transaction_id,
        user_id,
        amount_usd,
        status,
        payment_method,
        transaction_time
    FROM {{ source('bronze', 'raw_transactions') }}
)

SELECT
    transaction_id,
    user_id,
    CAST(amount_usd AS DECIMAL(18, 2)) AS transaction_amount_usd,
    LOWER(status) AS transaction_status,
    payment_method,
    CAST(transaction_time AS TIMESTAMP) AS transaction_timestamp
FROM raw_source
WHERE LOWER(status) != 'test_payment'
"""

    # 3. dbt marts: fct_user_transactions.sql
    fct_user_transactions_sql = """-- fct_user_transactions.sql
-- Aggregates daily transaction statistics per user.
-- Depends on stg_transactions.

WITH transactions AS (
    SELECT
        user_id,
        transaction_amount_usd,
        transaction_timestamp
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_status = 'completed'
)

SELECT
    user_id,
    COUNT(transaction_amount_usd) AS lifetime_transaction_count,
    SUM(transaction_amount_usd) AS lifetime_spend_usd,
    MAX(transaction_timestamp) AS last_active_timestamp
FROM transactions
GROUP BY user_id
"""

    # 4. dbt marts: fct_user_churn.sql
    fct_user_churn_sql = """-- fct_user_churn.sql
-- Defines customer status: active vs. inactive (churned).
-- Joins cleaned user metadata with transaction histories.
-- Must enforce unique constraint on user_id.

WITH users AS (
    SELECT
        user_id,
        hashed_email,
        country
    FROM {{ ref('stg_users') }}
),

activity AS (
    SELECT
        user_id,
        lifetime_transaction_count,
        lifetime_spend_usd,
        last_active_timestamp
    FROM {{ ref('fct_user_transactions') }}
)

SELECT
    u.user_id,
    u.hashed_email,
    u.country,
    COALESCE(a.lifetime_transaction_count, 0) AS tx_count,
    COALESCE(a.lifetime_spend_usd, 0.0) AS spend_amount,
    a.last_active_timestamp,
    CASE 
        -- Flag as churned if no transaction in past 30 days
        WHEN a.last_active_timestamp IS NULL THEN 'CHURNED'
        WHEN DATEDIFF('day', a.last_active_timestamp, CURRENT_DATE()) > 30 THEN 'CHURNED'
        ELSE 'ACTIVE'
    END AS churn_status
FROM users u
LEFT JOIN activity a ON u.user_id = a.user_id
"""

    # 5. Airflow DAG: user_analytics_dag.py
    airflow_dag_py = """# user_analytics_dag.py
# Ingests user profiles and event data, then runs dbt transformation pipeline.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['de-alerts@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def extract_users_from_rds():
    # Extracts fresh accounts from transaction database RDS
    # Custom ETL code logic
    print("Starting ingestion: Connecting to db.production.local ...")
    # Simulate DB fetching
    print("Ingested 14,812 rows. Writing to S3 bronze bucket...")

def extract_transactions_from_stripe():
    # Ingests transaction events from Stripe external API
    print("Querying Stripe endpoints...")
    print("Ingested 4,921 Stripe rows. Copying into S3 bronze...")

with DAG(
    'user_analytics_pipeline',
    default_args=default_args,
    description='Nightly ingestion and dbt marts rebuild pipeline for Customer Churn models',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['churn', 'users', 'stripe']
) as dag:

    ingest_users = PythonOperator(
        task_id='extract_users_from_rds',
        python_callable=extract_users_from_rds
    )

    ingest_transactions = PythonOperator(
        task_id='extract_transactions_from_stripe',
        python_callable=extract_transactions_from_stripe
    )

    snowflake_load_raw = SnowflakeOperator(
        task_id='load_s3_to_snowflake_bronze',
        snowflake_conn_id='snowflake_prod',
        sql="COPY INTO bronze.raw_users FROM @s3_bronze_stage/users/;"
    )

    dbt_run = BashOperator(
        task_id='dbt_run_transformations',
        bash_command='dbt run --profiles-dir . --project-dir ./dbt'
    )

    dbt_test = BashOperator(
        task_id='dbt_test_validations',
        bash_command='dbt test --profiles-dir . --project-dir ./dbt'
    )

    # Dependency routing
    [ingest_users, ingest_transactions] >> snowflake_load_raw >> dbt_run >> dbt_test
"""

    with open("mock_data/codebase/dbt/models/staging/stg_users.sql", "w") as f:
        f.write(stg_users_sql)
    with open("mock_data/codebase/dbt/models/staging/stg_transactions.sql", "w") as f:
        f.write(stg_transactions_sql)
    with open("mock_data/codebase/dbt/models/marts/fct_user_transactions.sql", "w") as f:
        f.write(fct_user_transactions_sql)
    with open("mock_data/codebase/dbt/models/marts/fct_user_churn.sql", "w") as f:
        f.write(fct_user_churn_sql)
    with open("mock_data/codebase/airflow/dags/user_analytics_dag.py", "w") as f:
        f.write(airflow_dag_py)
        
    print("Created mock codebase files (dbt + Airflow).")

def setup_logs():
    success_log = """2026-05-31T02:00:00Z [INFO] Starting Airflow Task Instance: customer_churn_dag.extract_users_from_rds
2026-05-31T02:00:01Z [INFO] Connecting to database host: db.production.local, port: 5432, database: prod_rds
2026-05-31T02:00:03Z [INFO] Extract query executed. Fetched 14,812 rows.
2026-05-31T02:00:04Z [INFO] Uploaded csv file to s3://company-datalake-bronze/users/date=2026-05-31/data.csv
2026-05-31T02:00:05Z [INFO] Task extract_users_from_rds finished. Status: SUCCESS
2026-05-31T02:00:05Z [INFO] Starting Airflow Task Instance: customer_churn_dag.load_s3_to_snowflake_bronze
2026-05-31T02:00:06Z [INFO] Executing COPY INTO bronze.raw_users in Snowflake warehouse PROD_WH...
2026-05-31T02:00:09Z [INFO] Snowflake COPY completed. 14,812 rows inserted.
2026-05-31T02:00:10Z [INFO] Starting Airflow Task Instance: customer_churn_dag.dbt_run_transformations
2026-05-31T02:00:11Z [INFO] Concurrency: 4 threads
2026-05-31T02:00:12Z [INFO] 1 of 4 START staging.stg_users ......................... [RUN]
2026-05-31T02:00:13Z [INFO] 2 of 4 START staging.stg_transactions .................. [RUN]
2026-05-31T02:00:15Z [INFO] 1 of 4 PASS staging.stg_users .......................... [OK in 2.92s]
2026-05-31T02:00:16Z [INFO] 2 of 4 PASS staging.stg_transactions ................... [OK in 3.12s]
2026-05-31T02:00:17Z [INFO] 3 of 4 START marts.fct_user_transactions ............... [RUN]
2026-05-31T02:00:19Z [INFO] 3 of 4 PASS marts.fct_user_transactions ................ [OK in 2.05s]
2026-05-31T02:00:20Z [INFO] 4 of 4 START marts.fct_user_churn ...................... [RUN]
2026-05-31T02:00:22Z [INFO] 4 of 4 PASS marts.fct_user_churn ....................... [OK in 1.95s]
2026-05-31T02:00:23Z [INFO] Done. PASS=4 WARN=0 ERROR=0 SKIP=0
2026-05-31T02:00:24Z [INFO] Starting Airflow Task Instance: customer_churn_dag.dbt_test_validations
2026-05-31T02:00:25Z [INFO] Running test unique_stg_users_user_id... [PASS]
2026-05-31T02:00:26Z [INFO] Running test not_null_stg_users_user_id... [PASS]
2026-05-31T02:00:27Z [INFO] Running test unique_fct_user_churn_user_id... [PASS]
2026-05-31T02:00:28Z [INFO] Running test relationships_fct_user_churn_user_id_stg_users_user_id... [PASS]
2026-05-31T02:00:29Z [INFO] All 4 tests passed. dbt run validated successfully!
2026-05-31T02:00:30Z [INFO] DAG user_analytics_pipeline completed. Status: SUCCESS
"""

    failed_dbt_log = """2026-05-31T04:15:00Z [INFO] Starting Airflow Task Instance: customer_churn_dag.dbt_run_transformations
2026-05-31T04:15:02Z [INFO] Concurrency: 4 threads
2026-05-31T04:15:03Z [INFO] 1 of 4 START staging.stg_users ......................... [RUN]
2026-05-31T04:15:04Z [INFO] 2 of 4 START staging.stg_transactions .................. [RUN]
2026-05-31T04:15:06Z [INFO] 1 of 4 PASS staging.stg_users .......................... [OK in 2.12s]
2026-05-31T04:15:07Z [INFO] 2 of 4 PASS staging.stg_transactions ................... [OK in 3.41s]
2026-05-31T04:15:08Z [INFO] 3 of 4 START marts.fct_user_transactions ............... [RUN]
2026-05-31T04:15:10Z [INFO] 3 of 4 PASS marts.fct_user_transactions ................ [OK in 2.11s]
2026-05-31T04:15:11Z [INFO] 4 of 4 START marts.fct_user_churn ...................... [RUN]
2026-05-31T04:15:13Z [ERROR] Database error: Duplicate entry 'USR_94821' for key 'fct_user_churn.primary'
2026-05-31T04:15:13Z [ERROR] 4 of 4 ERROR marts.fct_user_churn ..................... [ERROR in 1.87s]
2026-05-31T04:15:14Z [INFO] Done. PASS=3 WARN=0 ERROR=1 SKIP=0
2026-05-31T04:15:14Z [FATAL] Pipeline run failed due to compilation or execution error: Marts unique key violation.
Traceback (most recent call last):
  File "/opt/airflow/dags/customer_churn_dag.py", line 87, in trigger_dbt_run
    raise Exception("dbt transformations failed: Marts unique key violation inside marts.fct_user_churn")
Exception: dbt transformations failed: Marts unique key violation inside marts.fct_user_churn
"""

    failed_rds_log = """2026-05-31T02:00:00Z [INFO] Starting Airflow Task Instance: customer_churn_dag.extract_users_from_rds
2026-05-31T02:00:01Z [INFO] Connecting to database host: db.production.local, port: 5432, database: prod_rds
2026-05-31T02:00:16Z [WARNING] Retrying database connection (attempt 1/3)...
2026-05-31T02:00:31Z [WARNING] Retrying database connection (attempt 2/3)...
2026-05-31T02:00:46Z [WARNING] Retrying database connection (attempt 3/3)...
2026-05-31T02:01:01Z [ERROR] Failed to establish connection after 3 attempts.
Traceback (most recent call last):
  File "/opt/airflow/dags/customer_churn_dag.py", line 47, in extract_users_from_rds
    conn = psycopg2.connect(host="db.production.local", user="airflow_svc", password="***")
  File "/usr/local/lib/python3.10/site-packages/psycopg2/__init__.py", line 122, in connect
    conn = _connect(dsn, connection_factory=connection_factory, async_=async_)
psycopg2.OperationalError: connection to server at "db.production.local" (10.0.4.12), port 5432 failed: Connection timed out
	Is the server running on that host and accepting TCP/IP connections?
2026-05-31T02:01:02Z [FATAL] Task failed. Exiting with error code 1.
"""

    with open("mock_data/logs/run_1001_success.log", "w") as f:
        f.write(success_log)
    with open("mock_data/logs/run_1002_failed.log", "w") as f:
        f.write(failed_dbt_log)
    with open("mock_data/logs/run_1003_failed.log", "w") as f:
        f.write(failed_rds_log)
        
    print("Created mock log files.")

def setup_sqlite():
    db_path = "mock_data/metadata.db"
    
    # Remove existing db if any
    if os.path.exists(db_path):
        os.remove(db_path)
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Create tables table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tables (
        table_id TEXT PRIMARY KEY,
        schema_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        description TEXT,
        row_count INTEGER,
        size_bytes INTEGER
    )
    """)

    # 2. Create columns table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS columns (
        table_id TEXT NOT NULL,
        column_name TEXT NOT NULL,
        data_type TEXT NOT NULL,
        description TEXT,
        is_pii BOOLEAN NOT NULL CHECK (is_pii IN (0, 1)),
        pii_type TEXT,
        masking_policy TEXT,
        PRIMARY KEY (table_id, column_name),
        FOREIGN KEY (table_id) REFERENCES tables(table_id)
    )
    """)

    # 3. Create lineage table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS lineage (
        source_table TEXT NOT NULL,
        target_table TEXT NOT NULL,
        lineage_type TEXT NOT NULL,
        PRIMARY KEY (source_table, target_table)
    )
    """)

    # 4. Create pipeline_runs table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        pipeline_name TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
        start_time TEXT NOT NULL,
        end_time TEXT,
        duration_sec INTEGER,
        error_message TEXT,
        log_path TEXT
    )
    """)

    # 5. Create pipeline_slo table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_slo (
        pipeline_name TEXT PRIMARY KEY,
        sla_target_time TEXT NOT NULL,
        max_duration_sec INTEGER NOT NULL,
        slo_percentage_target REAL NOT NULL
    )
    """)

    # ------------------ SEEDING DATA ------------------

    # Seeding Tables
    tables = [
        ("bronze.raw_users", "bronze", "raw_users", "Raw ingestion replica of postgres customer registrations database.", 153821, 24192081),
        ("bronze.raw_transactions", "bronze", "raw_transactions", "Raw transactions dump from Stripe API payments integration.", 952048, 142093848),
        ("staging.stg_users", "staging", "stg_users", "Cleaned user demographics and profiles. Hashes primary email fields.", 153812, 19283011),
        ("staging.stg_transactions", "staging", "stg_transactions", "Standardized currency conversions and transaction attributes. Ignores payments marked as test.", 948201, 98203810),
        ("marts.fct_user_transactions", "marts", "fct_user_transactions", "Fact table containing rolling aggregate metrics on user transaction activities.", 120482, 12038410),
        ("marts.fct_user_churn", "marts", "fct_user_churn", "Analyzes user activity to flag inactive accounts exceeding 30 days as churned. Primary reporting table.", 120482, 14028310)
    ]
    cursor.executemany("INSERT INTO tables VALUES (?, ?, ?, ?, ?, ?)", tables)

    # Seeding Columns
    columns = [
        # bronze.raw_users
        ("bronze.raw_users", "id", "TEXT", "Primary identifier for users.", 0, "NONE", None),
        ("bronze.raw_users", "first_name", "TEXT", "First name of customer.", 1, "NAME", "Raw (Restricted Access)"),
        ("bronze.raw_users", "last_name", "TEXT", "Last name of customer.", 1, "NAME", "Raw (Restricted Access)"),
        ("bronze.raw_users", "email", "TEXT", "Primary email address of user.", 1, "EMAIL", "Raw (Restricted Access)"),
        ("bronze.raw_users", "phone_number", "TEXT", "User phone number.", 1, "PHONE", "Raw (Restricted Access)"),
        ("bronze.raw_users", "country_code", "TEXT", "ISO country code.", 0, "NONE", None),
        ("bronze.raw_users", "created_at", "TIMESTAMP", "Creation timestamp in postgres.", 0, "NONE", None),
        ("bronze.raw_users", "updated_at", "TIMESTAMP", "Last update timestamp.", 0, "NONE", None),
        
        # bronze.raw_transactions
        ("bronze.raw_transactions", "transaction_id", "TEXT", "Stripe payment identifier.", 0, "NONE", None),
        ("bronze.raw_transactions", "user_id", "TEXT", "Foreign key to user account.", 0, "NONE", None),
        ("bronze.raw_transactions", "amount_usd", "NUMERIC", "Transaction billing value in USD.", 0, "NONE", None),
        ("bronze.raw_transactions", "status", "TEXT", "Payment processor response.", 0, "NONE", None),
        ("bronze.raw_transactions", "payment_method", "TEXT", "Method used: credit card, apple pay, etc.", 0, "NONE", None),
        ("bronze.raw_transactions", "transaction_time", "TIMESTAMP", "Time of charge.", 0, "NONE", None),

        # staging.stg_users
        ("staging.stg_users", "user_id", "TEXT", "Conformed primary customer index.", 0, "NONE", None),
        ("staging.stg_users", "hashed_email", "TEXT", "Hashed secure email identifier for downstream matches.", 1, "EMAIL", "SHA-256 with Pepper Salt"),
        ("staging.stg_users", "masked_phone", "TEXT", "Obfuscated customer contact.", 1, "PHONE", "Masked: keeping code + last 4 digits"),
        ("staging.stg_users", "country", "TEXT", "Standardized uppercase country string.", 0, "NONE", None),
        ("staging.stg_users", "created_timestamp", "TIMESTAMP", "Standardized user creation timestamp.", 0, "NONE", None),
        ("staging.stg_users", "updated_timestamp", "TIMESTAMP", "Standardized profile update timestamp.", 0, "NONE", None),

        # staging.stg_transactions
        ("staging.stg_transactions", "transaction_id", "TEXT", "Standardized primary transaction ID.", 0, "NONE", None),
        ("staging.stg_transactions", "user_id", "TEXT", "Cleaned foreign key reference to user.", 0, "NONE", None),
        ("staging.stg_transactions", "transaction_amount_usd", "DECIMAL(18,2)", "Standardized amount in USD decimal format.", 0, "NONE", None),
        ("staging.stg_transactions", "transaction_status", "TEXT", "Lowercase transaction response code.", 0, "NONE", None),
        ("staging.stg_transactions", "payment_method", "TEXT", "Consolidated billing method.", 0, "NONE", None),
        ("staging.stg_transactions", "transaction_timestamp", "TIMESTAMP", "Standardized charge timestamp.", 0, "NONE", None),

        # marts.fct_user_transactions
        ("marts.fct_user_transactions", "user_id", "TEXT", "Primary index key (one per user).", 0, "NONE", None),
        ("marts.fct_user_transactions", "lifetime_transaction_count", "INTEGER", "Aggregated completed purchases count.", 0, "NONE", None),
        ("marts.fct_user_transactions", "lifetime_spend_usd", "DECIMAL(18,2)", "Aggregated gross user spend in USD.", 0, "NONE", None),
        ("marts.fct_user_transactions", "last_active_timestamp", "TIMESTAMP", "Timestamp of the user's latest completed charge.", 0, "NONE", None),

        # marts.fct_user_churn
        ("marts.fct_user_churn", "user_id", "TEXT", "Primary key for customer analysis.", 0, "NONE", None),
        ("marts.fct_user_churn", "hashed_email", "TEXT", "Anonymized user email for marketing segment hooks.", 1, "EMAIL", "Inherited SHA-256 Mask"),
        ("marts.fct_user_churn", "country", "TEXT", "User registration location.", 0, "NONE", None),
        ("marts.fct_user_churn", "tx_count", "INTEGER", "Total transaction count.", 0, "NONE", None),
        ("marts.fct_user_churn", "spend_amount", "DECIMAL(18,2)", "Total aggregated lifetime spending.", 0, "NONE", None),
        ("marts.fct_user_churn", "last_active_timestamp", "TIMESTAMP", "Most recent activity recorded.", 0, "NONE", None),
        ("marts.fct_user_churn", "churn_status", "TEXT", "Flag: 'ACTIVE' (activity <= 30d ago) or 'CHURNED' (activity > 30d ago).", 0, "NONE", None)
    ]
    cursor.executemany("INSERT INTO columns VALUES (?, ?, ?, ?, ?, ?, ?)", columns)

    # Seeding Lineage
    lineage = [
        ("bronze.raw_users", "staging.stg_users", "Direct"),
        ("bronze.raw_transactions", "staging.stg_transactions", "Direct"),
        ("staging.stg_transactions", "marts.fct_user_transactions", "Aggregated"),
        ("staging.stg_users", "marts.fct_user_churn", "Direct"),
        ("marts.fct_user_transactions", "marts.fct_user_churn", "Direct")
    ]
    cursor.executemany("INSERT INTO lineage VALUES (?, ?, ?)", lineage)

    # Seeding Pipeline SLOs
    slos = [
        ("user_analytics_pipeline", "08:00:00", 3600, 99.0)
    ]
    cursor.executemany("INSERT INTO pipeline_slo VALUES (?, ?, ?, ?)", slos)

    # Seeding Pipeline Runs
    runs = [
        (1001, "user_analytics_pipeline", "SUCCESS", "2026-05-29T02:00:00Z", "2026-05-29T02:00:30Z", 30, None, "mock_data/logs/run_1001_success.log"),
        (1002, "user_analytics_pipeline", "FAILED", "2026-05-30T04:15:00Z", "2026-05-30T04:15:14Z", 14, "Marts unique key violation inside marts.fct_user_churn", "mock_data/logs/run_1002_failed.log"),
        (1003, "user_analytics_pipeline", "FAILED", "2026-05-31T02:00:00Z", "2026-05-31T02:01:02Z", 62, "Database extraction timed out after 3 retries.", "mock_data/logs/run_1003_failed.log")
    ]
    cursor.executemany("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", runs)

    conn.commit()
    conn.close()
    print("Created and seeded SQLite metadata database.")

if __name__ == "__main__":
    setup_directories()
    setup_docs()
    setup_codebase()
    setup_logs()
    setup_sqlite()
    print("Mock environment setup successfully!")
