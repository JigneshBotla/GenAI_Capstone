# RAG-Powered Data Engineering Assistant ("Deco") - Project Progress

This document tracks the planning, architecture, implementation status, and completed milestones for the RAG-Powered Data Engineering (DE) Assistant.

---

## 📋 Project Overview & Problem Statement

Data engineers frequently struggle with information fragmentation. To troubleshoot a failing pipeline, understand lineage, verify PII compliance, or retrieve design choices, they must hop across multiple systems:
* **Code repositories** (dbt models, Airflow DAGs)
* **Documentation wikis** (Confluence, Markdown docs)
* **Data catalogs** (table schemas, PII tags, lineage diagrams)
* **Orchestration / Observability logs** (Airflow, Datadog)

### The Solution: "Deco"
Deco is a unified conversational assistant built with a hybrid approach:
1. **Semantic Vector Search (ChromaDB)** for unstructured system documentation and codebase Q&A.
2. **Deterministic SQL/JSON Metadata Queries** for precise schemas, lineage, and PII verification.
3. **Agentic Tool Orchestration (using AWS Bedrock - Amazon Nova Pro)** to dynamically trigger data quality checks, trace dependencies, and inspect active system health.
4. **Streamlit UI** providing an executive dashboard alongside the interactive chat experience.

---

## 🏗️ Proposed Architecture

```
                  +----------------------------------+
                  |      Streamlit UI (Chat +        |
                  |     Interactive Dashboards)      |
                  +-----------------+----------------+
                                    |
                                    v
                  +-----------------+----------------+
                  |         Agentic Router           |
                  +--------+--------+-------+--------+
                           |        |       |
      +--------------------+        |       +-------------------+
      |                             |                           |
      v                             v                           v
+-----+------+               +------+-----+               +-----+------+
| Vector DB  |               | Metadata DB|               | Monitoring |
| (ChromaDB) |               |  (SQLite)  |               |  (Mock API)|
+-----+------+               +------+-----+               +-----+------+
      |                             |                           |
      v                             v                           v
• Pipeline Docs              • Table Schemas              • Run Statuses
• Code Comments              • Lineage Maps               • Error Logs
• Design Decisions           • PII Tags                   • SLO Metrics
```

---

## 🎯 Implementation Roadmap & To-Do List

- [x] **Step 1: Mock Data Generation** <!-- id: step_1 -->
  - Generate a mock codebase (dbt project and Airflow DAGs).
  - Create pipeline documentation, architecture files, and run logs.
  - Structure a data catalog mapping tables, columns, PII tags, and lineage.
- [x] **Step 2: Database Initialization (ChromaDB + SQLite)** <!-- id: step_2 -->
  - Build script to parse codebase and docs, generate embeddings, and load into ChromaDB.
  - Setup SQLite metadata database with schema, lineage, and PII information.
  - Implement helper functions for structured querying.
- [x] **Step 3: Agentic Orchestration Layer** <!-- id: step_3 -->
  - Configure AWS Bedrock integration with Amazon Nova Pro (`amazon.nova-pro-v1:0`).
  - Implement LLM-based tool-calling capabilities (schemas, lineage, logs, quality checks).
  - Add the core agentic action: dynamic Data Quality check runner.
- [x] **Step 4: Streamlit Frontend UI** <!-- id: step_4 -->
  - Create interactive chat interface.
  - Build real-time visualization widgets (lineage graphs, pipeline run status, schema explorer).
  - Optimize layout for a premium, sleek dark-themed experience.
- [x] **Step 5: Testing & Validation** <!-- id: step_5 -->
  - Create a Python notebook or validation script to test all tools and agent capabilities.
  - Document system limitations and potential production upgrades.
- [x] **Step 6: Deliverables & Presentation Deck** <!-- id: step_6 -->
  - Generate template / outline for the 10-slide presentation deck.
  - Package workspace files for submission.
- [x] **Step 7: Enhancement Iterations (New Chat UI & Agent Observability)** <!-- id: step_7 -->
  - Maintain conversation history to allow continuous QA flow.
  - Integrate a secure `nl2sql` query tool with robust DDL/DML restrictions.
  - Design a live collapsible expander dropdown for Deco's thought logs and tool executions in Streamlit.
  - Pivot UI layout from split-screen columns to main top-level navigation tabs.
  - Instrument Langfuse SDK tracing for full observability of agent actions and Bedrock interactions.

---

## 📓 Completed Steps Log

### 🟢 Milestone 0: Project Initiation & Architecture Sign-off
* **Date**: May 31, 2026
* **Details**: Defined the problem statement, outlined the hybrid architecture (RAG + SQL metadata), selected AWS Bedrock (Amazon Nova Pro) as the primary LLM backend, and created the project roadmap.

### 🟢 Milestone 1: Mock Environment & Data Assets Created
* **Date**: May 31, 2026
* **Details**: Created standard directory tree. Setup mock architecture overview docs, governance ADRs, dbt transformations codebase (`stg_users.sql`, `fct_user_churn.sql`), Airflow python DAG definitions, realistic failed/success run logs, and initialized/seeded a SQLite metadata database (`metadata.db`) containing complete schemas, column types, PII tracking classifications, and dependencies (lineage mappings).

### 🟢 Milestone 2: Databases Initialized & ChromaDB Vector Store Loaded
* **Date**: May 31, 2026
* **Details**: Formulated a unified datastore package `database_manager.py` with SQL and Vector engines. Built the persistent local ChromaDB vector store. Embedded and indexed 13 documentation chunks and 5 dbt/Airflow codebase scripts using a free, local SentenceTransformer embedding function (`all-MiniLM-L6-v2`), validated successful retrieval behavior via diagnostic semantic queries.

### 🟢 Milestone 3: Bedrock Agentic Loop & Tool-Calling Engine Implemented
* **Date**: May 31, 2026
* **Details**: Created the agent core runner `agent_core.py`. Equipped the agent with AWS Bedrock tool-calling specifications matching Amazon Nova Pro's native Converse API schema. Structured tools to bridge into SQLite catalog lookups, lineage trees, operational log parsing, and vector Q&A. Coded the core active agentic action `trigger_data_quality_check` which dynamically reviews validations and updates the monitoring database. Added a high-fidelity local keyword-based reasoning fallback which was successfully validated using programmatic dry runs (`test_agent.py`).

### 🟢 Milestone 4: Premium Streamlit Dashboard & Chat UI Developed
* **Date**: May 31, 2026
* **Details**: Programmed a premium executive UI in `app.py` utilizing responsive split-column layout grids. Constructed styled catalog viewports with dropdown schema inspectors, modeled Medallion lineage paths in glassmorphic containers, and calculated key SLO metrics alongside recent pipeline histories. Enabled interactive chat modules with scroll buffers, visual prompt chips, and a sidebar action control which directly executes data quality checks and injects results dynamically into active conversation.

### 🟢 Milestone 5: Programmatic Validation Suite & Notebook Built
* **Date**: May 31, 2026
* **Details**: Compiled a cell-by-cell Jupyter Notebook validation suite `validation_notebook.ipynb` documenting system integrations. Included structural schemas queries, PII tag classifications, lineage maps, RAG semantic searches, operational diagnosis, and programmatically validated database mutation (state changes) triggered by agentic data quality actions.

### 🟢 Milestone 6: Capstone Presentation Deck Structuring
* **Date**: May 31, 2026
* **Details**: Outlined the complete 10-slide presentation deck layout in `PRESENTATION_DECK_OUTLINE.md`. Mapped out title hooks, segmented problem statements, decoupled hybrid designs, system data flows, governance definitions, incident log diagnostics, active agentic validators, technical roadmap extensions, speaking notes, and visual requirements to hit a high-quality defense outcome.

### 🟢 Milestone 7: Advanced Capabilities, UI Enhancements & Langfuse Tracing
* **Date**: June 4, 2026
* **Details**: Integrated full multi-turn conversation history for chat context preservation. Implemented a sandbox-safe `nl2sql` execution engine that automatically token-checks and blocks DDL/DML queries. Redesigned UI to present Home Dashboard and Chat components inside dedicated main tabs. Integrated a live, non-obtrusive expander dropdown rendering real-time thoughts and tool execution flows. Configured Langfuse observability to trace and monitor LLM calls and tool spans in production. Dynamically populated the manual quality assertions dropdown by querying all conformed tables directly from the database catalog instead of using a hardcoded list.
