# 📊 Deco - Capstone Project Presentation Deck Outline (10 Slides)

This document provides a highly structured, content-complete outline for your 10-slide presentation deck. Each slide is designed to address the Capstone assessment criteria (Innovation, Technical Execution, Business Value, and Clarity).

---

## 🛝 Slide 1: Title & Project Hook
* **Slide Title**: Deco — RAG-Powered Data Engineering Co-pilot
Team members: Pawan Singh, Botla Jignesh, Challa Arun, Vineet Rana
Date : June 5th, 2026
* **Subtitle**: Streamlining Codebase Q&A, Catalog Exploration, and Observability through Hybrid GenAI
* **Core Visuals**: Clean modern layout, a high-tech logo, and a visual link to your live Streamlit interface.
* **Talking Points**:
  * Introduce Deco: An intelligent assistant tailored specifically to reduce operational overhead for modern Data Platform teams.
  * Emphasize the Medallion Pattern (Bronze -> Silver -> Gold) and the integration of structured metadata with unstructured code.
  * Highlight the project tech stack: Snowflake, dbt, ChromaDB, Streamlit, and AWS Bedrock (Amazon Nova Pro).

---

## 🛝 Slide 2: The Data Engineer's Fragmented Workflow (Problem Statement)
* **Slide Title**: The Cost of Information Fragmentation
* **Subtitle**: Context-Switching in Incident Response and Governance Compliance
* **Core Visuals**: A split diagram or flow showing a data engineer jumping between:
  1. *GitHub/dbt* (Code Q&A)
  2. *Data Catalog* (Lineage and PII rules)
  3. *Airflow Orchestrator* (Logs and Failed DAG runs)
* **Talking Points**:
  * Modern data platform teams waste up to 30% of their operational time hunting down metadata.
  * Navigating governance compliance (GDPR/CCPA phone/email hashing) is highly manual and error-prone.
  * Incident response (MTTR) is slow due to fragmented logs across disjointed systems.

---



* **Slide Title**: Deco Co-pilot: Unified Interface for DE Teams
* **Subtitle**: Combining Vector Retrieval with Deterministic Metadata Processing
* **Core Visuals**: A three-pillar architecture diagram showing:
  * **ChromaDB**: Semantic search over codebase comments & architecture docs.
  * **SQLite Catalog**: 100% accurate column-level schema lookups & lineage traces.
  * **Active Bedrock Orchestration**: Intelligent tool routing.
* **Talking Points**:
  * Explain why naive RAG fails: LLMs hallucinate schema names, column types, and data lineage maps.
  * Describe Deco's Hybrid approach: Vector store handles the "why" (design decisions), and the SQL metadata layer handles the "what" (precise schemas, lineage, and run states) deterministically.

---

## 🛝 Slide 4: System Architecture & Data Flow
* **Slide Title**: Behind the Scenes: Deco's Technical Architecture
* **Subtitle**: Seamless Data Ingestion, Retrieval, and Agentic Execution
* **Core Visuals**: A schematic showing:
  * Streamlit UI <--> Agent Core (AWS Bedrock / local fallback)
  * Agent Core querying ChromaDB (vector) and SQLite (catalog/logs)
  * Agent Core executing the python DQ test suite.
* **Talking Points**:
  * Detail the ingestion pipeline: Markdown ADR docs and dbt files are chunked and embedded locally using `all-MiniLM-L6-v2`.
  * Highlight that all data fits locally, keeping the application 100% responsive, secure, and private.
  * Outline the tool definitions supplied to Amazon Bedrock Nova models (lite/pro/micro).

---

## 🛝 Slide 5: Unstructured Knowledge & Codebase Q&A (RAG)
* **Slide Title**: Demystifying Code & Design Choices
* **Subtitle**: Semantic Codebase Q&A with ChromaDB
* **Core Visuals**: A screenshot or code block displaying the dbt model comment headers and the corresponding semantic search results for: *"Why do we hash email fields?"*
* **Talking Points**:
  * Discuss how Deco helps onboard new engineers: they can immediately ask about platform architecture (e.g. Bronze/Silver/Gold flow) or staging filters.
  * Highlight the integration of dbt comments directly into the vector index, ensuring developer documentation lives alongside the model code.

---

## 🛝 Slide 6: Precision Data Catalog & PII Governance
* **Slide Title**: 100% Accurate Data Catalog & Governance
* **Subtitle**: Tracking PII Classifications and Masking Rules Programmatically
* **Core Visuals**: A styled schema table from your app displaying `staging.stg_users` with columns, PII markers (`🚨 PII`), and masking rules (`SHA-256 with Pepper`).
* **Talking Points**:
  * Point out the risk of hallucinations in corporate compliance: we cannot afford LLMs guessing which columns are PII.
  * Show how Deco invokes `get_table_schema` to pull exact data types, Descriptions, and security policies from the catalog database.

---

## 🛝 Slide 7: Interactive Lineage & Medallion Dependency Tracing
* **Slide Title**: Tracking Data Flow from Ingestion to Analytics
* **Subtitle**: Fully Traceable Table and Column-level Lineage
* **Core Visuals**: A tree chart from the Streamlit UI tracing:
  `raw_users` / `raw_transactions` --> `stg_users` / `stg_transactions` --> `fct_user_transactions` --> `fct_user_churn`
* **Talking Points**:
  * Lineage is crucial when evaluating downstream impact before modifying a column.
  * Deco traces upstreams and downstreams instantly using standard SQLite queries.
  * Integrates conformed structures, making it clear where business metrics like `churn_status` originate.

---

## 🛝 Slide 8: Pipeline Health, Observability & Incident Diagnosis
* **Slide Title**: Proactive Troubleshooting & SLO Adherence
* **Subtitle**: Zeroing in on Failures and SLA Violations Automatically
* **Core Visuals**: A screenshot showing a simulated failed run (e.g., PostgreSQL connection timeout or dbt duplicate entry violation) alongside Deco's auto-generated incident diagnosis.
* **Talking Points**:
  * Deco continuously monitors run histories and computes active SLO compliance percentages.
  * When a pipeline fails, the engineer simply asks: *"My pipeline broke. What happened?"*
  * Deco extracts the exact execution logs, isolates the Python/SQL exception traceback, and synthesizes immediate, actionable remediation steps.

---

## 🛝 Slide 9: Agentic Action: Data Quality Assertions (Track 2)
* **Slide Title**: Moving Beyond Passive RAG: Agentic Execution
* **Subtitle**: On-Demand Schema Validations, Non-Null Checks, and PII Verification
* **Core Visuals**: A clean visual block displaying the newly logged `SUCCESS` run in the observability table, triggered in real-time by Deco.
* **Talking Points**:
  * Explain the agentic action: Data quality is validated on-demand!
  * When the user triggers a validation check, Deco dynamically connects to the database, performs uniqueness and null checks, evaluates SHA-256 compliance, writes a persistent log, and commits the execution run to the metadata DB.
  * This shifts the assistant from a passive Q&A chatbot to an active, operational team member.

---

## 🛝 Slide 10: Technical Innovation, Limits & Future Roadmap
* **Slide Title**: Innovation Highlights & Production Strategy
* **Subtitle**: Extensibility and Moving to Enterprise Scale
* **Core Visuals**: A roadmap graphic highlighting Snowflake integration, Datadog hooks, and active alerting.
* **Talking Points**:
  * **Innovation**: Standard RAG is combined with deterministic SQL routing and free local embeddings, keeping Bedrock invocation costs extremely low.
  * **Limitations**: Current models operate on mock SQLite datasets.
  * **Future Roadmap**:
    1. Integrate live Snowflake tags & dbt cloud semantic manifest parsing.
    2. Expand quality assertions using Great Expectations or Soda SQL.
    3. Hook into Apache Airflow's REST API to trigger real DAG retries.
