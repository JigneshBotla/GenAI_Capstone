# architecture_overview.md

Welcome to the internal system architecture and design documentation for "Deco" — the RAG-powered Data Engineering AI Assistant and Observability Platform. This reference is designed to assist engineers in understanding our storage layers, routing topologies, and semantic systems.

## 1. What Deco Is and the Problem It Solves

Data platform operations often suffer from critical information fragmentation. When debugging a failing ingestion task or verifying regulatory compliance, engineers must typically hop between disparate environments:
* **Codebases**: Stored in Git repositories containing dbt transformation scripts and Apache Airflow DAG workflows.
* **Data Catalogs**: Catalog schemas, line-level lineage, and compliance data tags.
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
