# Capstone Project: Deco - Data Engineering AI Assistant & Observability Platform

Deco is a premium, RAG-powered, agentic Data Engineering assistant and observability platform built fully on Snowflake. It combines dynamic metadata schema catalogs, live data lineage tracing, real-time pipeline monitoring, automated data quality validations, incident diagnosis, and conformed natural-language-to-SQL querying.

---

## 🎯 Project Specification & Core Goals
The project represents a production-grade data platform engineering control room featuring:
1. **Medallion Data Architecture**: Layered pipeline (Bronze → Silver/Staging → Gold/Marts) deployed directly in Snowflake.
2. **Data Quality & Quarantine Rules**: Active data validation routing to salvage valid rows and quarantine invalid entries.
3. **Dynamic Observation Metadata**: Programmatic metric compilation tracking table size and row count changes dynamically.
4. **Agentic Co-pilot (Deco)**: An AI assistant backed by AWS Bedrock (Nova Lite/Pro) that executes dynamic tool-calling loops.
5. **Interactive UI Control Panel**: A Streamlit dashboard with dedicated tabs separating metadata catalogs, lineage maps, SLA statuses, and a conversational chat.

---

## 🏗️ System Architecture & Datastore

```mermaid
graph TD
    subgraph Snowflake Data Warehouse (Capstone_DB)
        subgraph Bronze Schema (Ingestion)
            B1[bronze.raw_users]
            B2[bronze.raw_transactions]
        end
        
        subgraph Staging Schema (Silver Cleaned)
            S1[staging.stg_users]
            S2[staging.stg_users_quarantine]
            S3[staging.stg_transactions]
            S4[staging.stg_transactions_quarantine]
        end
        
        subgraph Marts Schema (Gold Analytics)
            M1[marts.fct_user_transactions]
            M2[marts.fct_user_churn]
        end
        
        subgraph Public Schema (Catalog & Observability)
            P1[public.tables]
            P2[public.columns]
            P3[public.lineage]
            P4[public.pipeline_runs]
            P5[public.pipeline_slo]
        end
    end
    
    subgraph Core Agent Services
        V1[(ChromaDB Vector Store)]
        A1[AgentCore Runner]
        B3[AWS Bedrock Nova]
    end

    subgraph User Interface
        UI[Streamlit App.py]
    end

    B1 --> S1 & S2
    B2 --> S3 & S4
    S3 --> M1
    S1 & M1 --> M2
    
    A1 --> P1 & P2 & P3 & P4 & P5
    A1 --> V1
    A1 --> B3
    UI --> A1
```

### Medallion Schemas Breakdown
* **`BRONZE` (Ingestion)**: Direct replication of raw transactional data. Retains dirty records.
* **`STAGING` (Silver)**: Conformed and cleansed layer. Critical errors are routed to quarantine tables. Email fields are anonymized using SHA-256 with secret salt.
* **`MARTS` (Gold)**: Aggregated analytical facts used for downstream BI reporting and churn predictions.
* **`PUBLIC` (Catalog & Observability)**: Governance data storing active table properties, column details, line-level lineage, pipeline run execution history, and SLA thresholds.

---

## ⚙️ Data Quality & Quarantine Pipeline
When ETL transformations execute, conformed boundaries are enforced on raw data:

| Target Table | Source Table | Rules Enforced | Action taken on violation |
|---|---|---|---|
| `staging.stg_users` | `bronze.raw_users` | `id IS NOT NULL` | Conformed & clean load |
| `staging.stg_users_quarantine` | `bronze.raw_users` | `id IS NULL` | Quarantined with reason |
| `staging.stg_users` | `bronze.raw_users` | `email` or `country_code` is NULL | Warning logged; email hashed, country code kept conformed |
| `staging.stg_transactions` | `bronze.raw_transactions` | `amount_usd < 0` | Validated; negative amount flipped to positive using `ABS()` |
| `staging.stg_transactions_quarantine` | `bronze.raw_transactions` | `transaction_id IS NULL` | Quarantined with reason |

---

## 🤖 Deco Agent Core & Tool Capabilities
Deco orchestrates operations by resolving natural language input into a sequence of tool execution loops:

### Available Agent Tools
1. **`search_codebase_and_docs`**: Performs RAG semantic searches across ChromaDB (documentation, design records, dbt sql files).
2. **`get_table_schema`**: Retrieves definitions, types, descriptions, PII flags, and masking policies from Snowflake `public.columns`.
3. **`get_table_lineage`**: Fetches upstream sources and downstream consumers dynamically from Snowflake `public.lineage`.
4. **`get_pipeline_history`**: Queries Snowflake `public.pipeline_runs` and evaluates SLO compliance rates.
5. **`get_failed_run_diagnosis`**: Analyzes host error log paths and provides immediate incident recommendations.
6. **`trigger_data_quality_check`**: Runs an active programmatic assertion check suite against staging/marts tables, logs results in Snowflake, and returns status details.
7. **`nl2sql`**: Translates natural language questions to conformed Snowflake queries, executes them, and prints results tables.

---

## ⚡ Performance & Grounding Optimizations
To achieve sub-second response times and prevent hallucinations, we implemented key optimizations:

1. **Async Monitoring Logs**: Removed blocking synchronous network calls (`self.langfuse.flush()`) from the agent execution path, saving 1–2 seconds of latency per turn.
2. **Nova Thinking Separation**: Created a parser method (`_parse_thinking_and_content`) that isolates model reasoning inside `<thinking>` tags from the final text answer. Thoughts are neatly displayed inside Streamlit dropdowns, keeping the main chat clear.
3. **Snowflake Model Grounding**: Provided full schema listings and a direct database system context in the agent's system instructions to block SQLite/catalog hallucinations.
4. **Tool Parameter Resiliency**: Enabled `tool_nl2sql` to accept both `nl_query` and `query` argument names to handle LLM reasoning variations gracefully.

---

## 🖥️ Streamlit Frontend Dashboard UI
The app uses a premium custom dark-themed design with styled glassmorphism cards and contains:
* **Control Room Sidebar**: AWS Bedrock configuration keys, connectivity indicators, and a manual target selector to trigger active Data Quality test runs.
* **Data Catalog Tab**: Interactive cards showing table row counts, storage size (MB), and a collapsible table with column schemas, descriptions, and PII tags.
* **Data Lineage Tab**: Upstream and downstream dependencies flowchart colored by Medallion schema level.
* **Operations & SLOs Tab**: Operational observability logs showing SLA thresholds, recent pipeline success rates, duration breaches, and execution status lists.
* **Chat with Deco Tab**: Real-time agent chat featuring suggetion prompt pills, dynamic loaders, and collapsible thought/tool execution process expanders.
