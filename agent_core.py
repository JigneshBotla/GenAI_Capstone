import boto3
import json
import datetime
import os
from dotenv import load_dotenv
from database_manager import MetadataHelper, VectorStoreHelper

# Load environment variables
load_dotenv()


class AgentCore:
    """Core Agent class that manages tool registration, AWS Bedrock orchestration (Nova Pro), and the tool execution loop."""
    
    def __init__(self, region_name="us-east-1", aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, model_id=None):
        self.region_name = region_name
        
        # Setup Langfuse credentials in environment
        os.environ["LANGFUSE_PUBLIC_KEY"] = "pk-lf-18f10c0e-458e-4120-8e82-2656ba2e2172"
        os.environ["LANGFUSE_SECRET_KEY"] = "sk-lf-3be7acf8-ef72-412a-b7ad-7dd9dd581443"
        os.environ["LANGFUSE_HOST"] = "https://us.cloud.langfuse.com"
        
        try:
            from langfuse import Langfuse
            self.langfuse = Langfuse()
        except Exception as e:
            print(f"Warning: Failed to initialize Langfuse: {e}")
            self.langfuse = None
        
        # Initialize helper libraries
        self.metadata_helper = MetadataHelper()
        self.vector_helper = VectorStoreHelper()
        
        # Configure AWS Bedrock runtime client
        # Read from arguments first, otherwise let boto3 find it in env/credentials file
        params = {"region_name": region_name}
        if aws_access_key_id:
            params["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            params["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            params["aws_session_token"] = aws_session_token
            
        try:
            self.bedrock_client = boto3.client("bedrock-runtime", **params)
            self.has_aws = True
        except Exception as e:
            print(f"Warning: Failed to initialize AWS Bedrock client: {e}")
            self.bedrock_client = None
            self.has_aws = False

        # Model identifier — can be overridden at construction time
        self.model_id = model_id if model_id else "amazon.nova-lite-v1:0"

        # Register tools mapping
        self.tools_map = {
            "search_codebase_and_docs": self.tool_search_codebase_and_docs,
            "get_table_schema": self.tool_get_table_schema,
            "get_table_lineage": self.tool_get_table_lineage,
            "get_pipeline_history": self.tool_get_pipeline_history,
            "get_failed_run_diagnosis": self.tool_get_failed_run_diagnosis,
            "trigger_data_quality_check": self.tool_trigger_data_quality_check,
            "nl2sql": self.tool_nl2sql
        }

        # Bedrock JSON tool specification
        self.bedrock_tools_spec = [
            {
                "toolSpec": {
                    "name": "search_codebase_and_docs",
                    "description": "Performs semantic vector search across all data engineering pipeline documentation, system designs, architectural decision records (ADRs), and python/sql codebase files. Use this for general Q&A about architecture or design decisions.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The natural language semantic query to search for."
                                }
                            },
                            "required": ["query"]
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "get_table_schema",
                    "description": "Retrieves the exact database schema, descriptions, and PII tags for a given table ID in the data catalog. Prevents hallucinations for schemas.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "table_id": {
                                    "type": "string",
                                    "description": "The fully qualified table ID, e.g. 'staging.stg_users' or 'marts.fct_user_churn'."
                                }
                            },
                            "required": ["table_id"]
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "get_table_lineage",
                    "description": "Traces the upstream dependencies or downstream consumer tables for a target table. Helps understand how tables depend on each other.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "table_id": {
                                    "type": "string",
                                    "description": "The fully qualified table ID, e.g. 'marts.fct_user_churn'."
                                },
                                "direction": {
                                    "type": "string",
                                    "enum": ["upstream", "downstream", "both"],
                                    "description": "The lineage direction to trace. Default is 'both'."
                                }
                            },
                            "required": ["table_id"]
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "get_pipeline_history",
                    "description": "Fetches the recent history of all pipeline runs, including statuses (SUCCESS/FAILED), start times, durations, and error messages.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "limit": {
                                    "type": "integer",
                                    "description": "Number of runs to return (default is 5)."
                                }
                            }
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "get_failed_run_diagnosis",
                    "description": "Retrieves logs and error details of the most recent failed pipeline run and automatically summarizes what broke and how to fix it.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {}
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "trigger_data_quality_check",
                    "description": "ACTIVE AGENTIC ACTION: Programmatically executes a data quality verification check (schema validation, non-null assertions, boundary checks, and PII masking conformance validation) on a target table. Logs the results to the monitoring DB.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "table_id": {
                                    "type": "string",
                                    "description": "Table to validate, e.g., 'staging.stg_users' or 'marts.fct_user_churn'."
                                }
                            },
                            "required": ["table_id"]
                        }
                    }
                }
            }
            ,
            {
                "toolSpec": {
                    "name": "nl2sql",
                    "description": "Translates a user's natural language question (e.g., 'how many tables are there', 'how many transactions are there') into a Snowflake SELECT query, executes it, and returns the query and results. The input must be a natural language question, NOT raw SQL. Do NOT write SQL yourself when calling this tool.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "nl_query": {
                                    "type": "string",
                                    "description": "The natural language question describing the needed query. Do NOT write SQL here; write a natural language question."
                                }
                            },
                            "required": ["nl_query"]
                        }
                    }
                }
            }
        ]

    # --- Tool Implementations ---
    
    def tool_search_codebase_and_docs(self, query):
        results = self.vector_helper.query(query, n_results=3)
        if not results:
            return "No matching documentation or codebase snippets found."
        
        output = "### Vector Store Search Results:\n"
        for i, r in enumerate(results):
            output += f"\n**Result {i+1}** [Source File: `{r['metadata']['source_file']}`, Type: `{r['metadata']['file_type']}`]:\n"
            output += f"```\n{r['content']}\n```\n"
        return output

    def tool_get_table_schema(self, table_id):
        details = self.metadata_helper.get_table_details(table_id)
        if not details:
            return f"Table `{table_id}` was not found in the data catalog database."
        
        t = details["table"]
        cols = details["columns"]
        
        output = f"### Table: `{t['table_id']}`\n"
        output += f"* **Schema**: `{t['schema_name']}`\n"
        output += f"* **Description**: {t['description']}\n"
        output += f"* **Row Count**: {t['row_count']:,}\n"
        output += f"* **Size**: {t['size_bytes'] / (1024*1024):.2f} MB\n\n"
        
        output += "| Column Name | Data Type | Description | PII Tagged? | Masking Policy |\n"
        output += "| --- | --- | --- | --- | --- |\n"
        for c in cols:
            pii_flag = "🚨 YES" if c["is_pii"] else "✅ No"
            policy = c["masking_policy"] if c["masking_policy"] else "N/A"
            output += f"| `{c['column_name']}` | `{c['data_type']}` | {c['description']} | {pii_flag} | {policy} |\n"
            
        return output

    def tool_get_table_lineage(self, table_id, direction="both"):
        lin = self.metadata_helper.get_lineage(table_id, direction)
        
        output = f"### Data Lineage for `{table_id}`\n\n"
        
        if direction in ("upstream", "both"):
            output += "**Upstream Sources (Where data comes from):**\n"
            if lin["upstream"]:
                for u in lin["upstream"]:
                    output += f"* Upstream Table: `{u['source_table']}` (Type: {u['lineage_type']})\n"
            else:
                output += "* None (This is a root ingest source)\n"
            output += "\n"
            
        if direction in ("downstream", "both"):
            output += "**Downstream Consumers (Who reads this data):**\n"
            if lin["downstream"]:
                for d in lin["downstream"]:
                    output += f"* Downstream Table: `{d['target_table']}` (Type: {d['lineage_type']})\n"
            else:
                output += "* None (This is a final analytical endpoint)\n"
                
        return output

    def tool_get_pipeline_history(self, limit=5):
        runs = self.metadata_helper.get_pipeline_runs(limit=limit)
        slos = self.metadata_helper.get_pipeline_slo_compliance()
        
        output = "### 📊 Active Pipeline Operational Status\n\n"
        
        output += "**Service Level Objective (SLO) Health:**\n"
        for s in slos:
            status_emoji = "🟢" if s["slo_adherence_status"] == "HIGHLY COMPLIANT" else "🔴"
            output += f"* Pipeline `{s['pipeline_name']}`: {status_emoji} **{s['slo_adherence_status']}**\n"
            output += f"  - Daily SLA Target Completion: `{s['sla_target_completion_time']} UTC`\n"
            output += f"  - Successful Runs Rate (Last 5): `{s['success_rate_percent']:.1f}%` (Goal: >= 99%)\n"
            output += f"  - Runtime Violations (Last 5): {s['duration_violations_count']} runs exceeded `{s['max_duration_allowed_sec']}s` limit.\n\n"
            
        output += "**Recent Ingestion & Transformation Runs:**\n"
        output += "| Run ID | Pipeline | Status | Start Time | Duration | Error Info |\n"
        output += "| --- | --- | --- | --- | --- | --- |\n"
        for r in runs:
            status_str = "🟢 SUCCESS" if r["status"] == "SUCCESS" else "🔴 FAILED"
            err = r["error_message"] if r["error_message"] else "-"
            output += f"| `{r['run_id']}` | `{r['pipeline_name']}` | {status_str} | {r['start_time']} | {r['duration_sec']}s | {err} |\n"
            
        return output

    def tool_get_failed_run_diagnosis(self):
        failed_run = self.metadata_helper.get_latest_failed_run()
        if not failed_run:
            return "Excellent news! There are no failed pipeline runs in the execution logs."
        
        log_path = failed_run["log_path"]
        
        # Read the mock log file if it exists
        log_content = ""
        if os.path.exists(log_path):
            with open(log_path, "r") as f:
                log_content = f.read()
        else:
            log_content = "Log file not found on disks."
            
        output = f"### 🔍 Log Analysis & Incident Diagnosis (Run `#{failed_run['run_id']}`)\n"
        output += f"* **Failing Pipeline**: `{failed_run['pipeline_name']}`\n"
        output += f"* **Error Summary**: `{failed_run['error_message']}`\n"
        output += f"* **Failed Timestamp**: `{failed_run['start_time']}`\n\n"
        output += "**Extracted Run Logs:**\n"
        output += f"```text\n{log_content}\n```\n\n"
        output += "**Automated Deco Recommendations:**\n"
        
        if "Duplicate entry" in failed_run["error_message"] or "Marts unique key" in failed_run["error_message"]:
            output += "1. **Root Cause**: A dbt transformation failed because of a primary key collision. Downstream model `fct_user_churn` enforces uniqueness on `user_id`, but duplicate users were ingested.\n"
            output += "2. **Immediate Mitigation**: Run a data quality integrity check on `staging.stg_users` to locate duplicates. Apply deduplication logic using standard ROW_NUMBER() in staging transformations.\n"
        elif "Connection timed out" in log_content:
            output += "1. **Root Cause**: Extraction task `extract_users_from_rds` timed out after 3 retries trying to contact `db.production.local`.\n"
            output += "2. **Immediate Mitigation**: Verify network route from ECS/Airflow task runner to the Postgres database. Check if the database instance was down or undergoing an active maintenance window.\n"
        else:
            output += "1. **Root Cause**: General run failure.\n"
            output += "2. **Immediate Mitigation**: Trigger a manual pipeline run and inspect host execution metrics.\n"
            
        return output

    def tool_trigger_data_quality_check(self, table_id):
        """Airflow-style orchestration trigger: generate fresh data batch with DQ issues,
        run full ETL through staging/quarantine/marts, log the pipeline run."""

        if not table_id.startswith("bronze."):
            return f"❌ Data Quality ETL can only be triggered on bronze source tables. Got: `{table_id}`"

        result = self.metadata_helper.run_dq_etl_pipeline(table_id)

        status     = result.get("status", "UNKNOWN")
        run_id     = result.get("run_id", "N/A")
        findings   = result.get("dq_findings", {})
        duration   = result.get("duration_sec", 0)
        err        = result.get("error_message")
        batch_size = findings.get("batch_size", 0)
        status_icon = "✅" if status == "SUCCESS" else "❌"

        output  = f"### {status_icon} DQ ETL Pipeline Triggered: `{table_id}`\n"
        output += f"| Field | Value |\n|---|---|\n"
        output += f"| **Run ID** | `#{run_id}` |\n"
        output += f"| **Status** | **{status}** |\n"
        output += f"| **Duration** | {duration}s |\n"
        output += f"| **Batch Size** | {batch_size:,} new records ingested |\n\n"

        if table_id == "bronze.raw_users":
            null_id  = findings.get("null_user_id", 0)
            null_em  = findings.get("null_email", 0)
            null_cc  = findings.get("null_country_code", 0)
            staged   = findings.get("passed_to_staging", 0)
            quar     = findings.get("quarantined", 0)
            output += "**🔬 DQ Findings — `bronze.raw_users`:**\n\n"
            output += "| Rule | Severity | Records Found | Action Taken |\n|---|---|---|---|\n"
            output += f"| Null `user_id` | 🔴 Critical | **{null_id}** | → Quarantined |\n"
            output += f"| Null `email` | 🟡 High | **{null_em}** | → Passed to Staging |\n"
            output += f"| Null `country_code` | 🟡 Medium | **{null_cc}** | → Passed to Staging |\n"
            output += f"\n**ETL Summary:** `{staged:,}` records → `staging.stg_users` | `{quar:,}` records → `staging.stg_users_quarantine`\n"

        elif table_id == "bronze.raw_transactions":
            null_tx  = findings.get("null_transaction_id", 0)
            neg_amt  = findings.get("negative_amounts", 0)
            staged   = findings.get("passed_to_staging", 0)
            quar     = findings.get("quarantined", 0)
            output += "**🔬 DQ Findings — `bronze.raw_transactions`:**\n\n"
            output += "| Rule | Severity | Records Found | Action Taken |\n|---|---|---|---|\n"
            output += f"| Null `transaction_id` | 🔴 Critical | **{null_tx}** | → Quarantined |\n"
            output += f"| Negative `amount_usd` | 🟡 High | **{neg_amt}** | → ABS() fix applied |\n"
            output += f"\n**ETL Summary:** `{staged:,}` records → `staging.stg_transactions` | `{quar:,}` records → `staging.stg_transactions_quarantine`\n"

        if err:
            output += f"\n> ⚠️ **Pipeline Error:** {err}\n"

        output += "\n> 📊 Switch to the **Operations & SLOs** tab and click **🔄 Refresh** to see updated DQ metrics and run history.\n"
        return output


    def translate_nl_to_sql(self, nl_query):
        """Translates natural language to Snowflake SQL using Bedrock Nova Pro."""
        if not self.has_aws or not self.bedrock_client:
            raise ValueError("AWS credentials not configured. Cannot run Bedrock Nova Pro model.")

        system_prompt = """You are a Snowflake SQL translation expert.
Your ONLY job is to translate the user's natural language request into a single valid 
Snowflake SELECT query against Capstone_DB.

CRITICAL OUTPUT RULES:
- Output ONLY the raw SQL query. No explanations, no markdown, no comments, no tool calls.
- Never say you cannot answer. Never suggest the user run the query themselves.
- Never call any tools or functions. Never search for anything.
- If the request is ambiguous, make the most reasonable assumption and write the SQL.

QUERY RULES:
1. Only write SELECT or WITH queries.

2. STRICT TABLE USAGE: You MUST only query tables explicitly listed in the DATABASE SCHEMA 
   below. Never invent, guess, or use any table name not in this list.

3. SMART TABLE RESOLUTION: Users may refer to tables using informal, partial, or different 
   names. You must analyse the intent and map it to the correct listed table using these rules:

   LAYER RESOLUTION EXAMPLES:
   - "nulls", "missing values", "raw data", "landing data"
     → bronze layer: Capstone_DB.bronze.raw_users or Capstone_DB.bronze.raw_transactions
     Reason: Raw/dirty data with nulls lives in bronze before cleaning.

   - "clean users", "valid users", "user list", "customers"
     → Capstone_DB.staging.stg_users
     Reason: Cleaned and conformed user records live in staging.

   - "rejected", "bad records", "quarantined", "failed validation"
     → Capstone_DB.staging.stg_users_quarantine or Capstone_DB.staging.stg_transactions_quarantine
     Reason: Records that failed ETL checks are routed to quarantine tables.

   - "spend", "lifetime spend", "most spent", "top spenders", "transaction summary"
     → Capstone_DB.marts.fct_user_transactions
     Reason: Aggregated per-user spend lives in the marts layer.

   - "churned", "inactive users", "active users", "churn status"
     → Capstone_DB.marts.fct_user_churn
     Reason: Churn classification per user lives in the marts layer.

   - "pipeline", "runs", "job status", "ETL status", "failed runs", "success runs"
     → Capstone_DB.public.pipeline_runs
     Reason: Pipeline execution history lives here.

   - "SLA", "SLO", "pipeline targets", "max duration"
     → Capstone_DB.public.pipeline_slo
     Reason: Pipeline SLA/SLO targets are defined here.

   - "schema info", "table info", "catalog", "metadata", "what columns"
     → Capstone_DB.public.tables or Capstone_DB.public.columns
     Reason: Data catalog metadata lives in public schema.

   - "lineage", "dependencies", "source of", "upstream", "downstream"
     → Capstone_DB.public.lineage

4. CONFIRMATION RULE: If the user's request is too vague to confidently resolve to a single 
   listed table — even after applying the layer resolution rules above — do NOT guess or 
   fabricate a table. Instead, respond with a plain English question asking the user to 
   clarify which table or data they mean, referencing the available options.
   Example: "Did you mean spend data from marts.fct_user_transactions, or raw transaction 
   records from bronze.raw_transactions?"

5. Always use fully qualified table names with the Capstone_DB prefix
   (e.g. Capstone_DB.bronze.raw_users, Capstone_DB.marts.fct_user_transactions).
   Never use unqualified names like 'users', 'transactions', or 'runs'.

6. String comparisons are case-sensitive in Snowflake.
   pipeline_runs.status uses uppercase: 'SUCCESS' or 'FAILED'.

7. bronze.raw_users uses column 'id' (not 'user_id'). Staging/marts tables use 'user_id'.

DATABASE SCHEMA (Capstone_DB) — ONLY THESE TABLES MAY BE QUERIED:
- bronze.raw_users (id, first_name, last_name, email, phone_number, country_code, created_at, updated_at)
- bronze.raw_transactions (transaction_id, user_id, amount_usd, status, payment_method, transaction_time)
- staging.stg_users (user_id, hashed_email, masked_phone, country, created_timestamp, updated_timestamp)
- staging.stg_users_quarantine (id, first_name, last_name, email, phone_number, country_code, created_at, updated_at, quarantine_reason, quarantined_at)
- staging.stg_transactions (transaction_id, user_id, transaction_amount_usd, transaction_status, payment_method, transaction_timestamp)
- staging.stg_transactions_quarantine (transaction_id, user_id, amount_usd, status, payment_method, transaction_time, quarantine_reason, quarantined_at)
- marts.fct_user_transactions (user_id, lifetime_transaction_count, lifetime_spend_usd, last_active_timestamp)
- marts.fct_user_churn (user_id, hashed_email, country, tx_count, spend_amount, last_active_timestamp, churn_status)
- public.tables (table_id, schema_name, table_name, description, row_count, size_bytes)
- public.columns (table_id, column_name, data_type, description, is_pii, pii_type, masking_policy)
- public.lineage (source_table, target_table, lineage_type)
- public.pipeline_runs (run_id, pipeline_name, status, start_time, end_time, duration_sec, error_message, log_path)
- public.pipeline_slo (pipeline_name, sla_target_time, max_duration_sec, slo_percentage_target)
"""
        
        try:
            response = self.bedrock_client.converse(
                modelId=self.model_id,
                messages=[{"role": "user", "content": [{"text": nl_query}]}],
                system=[{"text": system_prompt}],
                inferenceConfig={"temperature": 0.0, "maxTokens": 500}
            )
            sql_text = response["output"]["message"]["content"][0]["text"].strip()
            if "```" in sql_text:
                sql_text = sql_text.replace("```sql", "").replace("```", "").strip()
            return sql_text
        except Exception as e:
            print(f"Error translating NL to SQL: {e}")
            raise e

    def tool_nl2sql(self, nl_query=None, query=None):
        target_query = nl_query if nl_query is not None else query
        if not target_query:
            return "Error: No query provided to nl2sql tool."
            
        try:
            sql_query = self.translate_nl_to_sql(target_query)
        except Exception as e:
            return f"Error translating NL to SQL: {e}"

        import re
        clean_query = re.sub(r'(--.*)|(/\*[\s\S]*?\*/)', '', sql_query).strip()
        if not clean_query:
            return "Error: Generated query is empty."
            
        words = set(re.findall(r'\b\w+\b', clean_query.lower()))
        restricted_keywords = {
            'delete', 'truncate', 'insert', 'update', 'drop', 'create', 
            'alter', 'replace', 'merge', 'grant', 'revoke', 'upsert', 'exec', 'execute'
        }
        
        intersect = words.intersection(restricted_keywords)
        if intersect:
            return f"Security Error: Generated query contains restricted keywords: {', '.join(intersect)}"
        
        first_word_match = re.match(r'^\s*(\w+)', clean_query)
        if not first_word_match or first_word_match.group(1).lower() not in ('select', 'with'):
            return f"Security Error: Only SELECT or WITH queries are permitted.\nGenerated SQL: `{sql_query}`"
            
        results = self.metadata_helper._execute_query(sql_query)
        if isinstance(results, dict) and "error" in results:
            return f"Database Error: {results['error']}\nGenerated SQL: `{sql_query}`"
        
        if not results:
            return f"Generated SQL Query:\n```sql\n{sql_query}\n```\n\nQuery executed successfully. Result: 0 rows returned."
            
        columns = list(results[0].keys())
        output = f"Generated SQL Query:\n```sql\n{sql_query}\n```\n\n"
        output += "### Custom Query Execution Results:\n\n"
        output += "| " + " | ".join(columns) + " |\n"
        output += "| " + " | ".join(["---"] * len(columns)) + " |\n"
        for row in results:
            vals = [str(row[col]) if row[col] is not None else "NULL" for col in columns]
            output += "| " + " | ".join(vals) + " |\n"
            
        return output

    def _parse_thinking_and_content(self, text):
        import re
        thinking_match = re.search(r'<thinking>(.*?)</thinking>', text, re.DOTALL)
        if thinking_match:
            thought_content = thinking_match.group(1).strip()
            final_content = re.sub(r'<thinking>.*?</thinking>', '', text, flags=re.DOTALL).strip()
            return thought_content, final_content
        else:
            return "", text.strip()

    # --- Agent Execution Core Loop ---
    
    def run_agent(self, user_prompt, chat_history=None, callback=None):
        """Orchestrates agent execution. Attempts AWS Bedrock (Nova Pro/Lite) tool loop."""
        if not self.has_aws or not self.bedrock_client:
            raise ValueError("AWS Bedrock client is not initialized. Please verify your credentials.")
            
        trace_ctx = None
        if hasattr(self, "langfuse") and self.langfuse:
            try:
                trace_ctx = self.langfuse.start_as_current_observation(
                    as_type="span",
                    name="deco-agent-run",
                    input={"user_prompt": user_prompt}
                )
                trace_ctx.__enter__()
            except Exception as e:
                print(f"Warning: Langfuse trace failed to initialize: {e}")
                trace_ctx = None

        try:
            # Construct system instructions
            system_prompt = """You are "Deco", an advanced AI Data Engineering Co-pilot. Your purpose is to assist data platform engineers.

DATABASE SYSTEM CONTEXT:
Our platform is built on Snowflake (database name: Capstone_DB). All data catalog tables, pipeline runs, and SLOs are stored in Snowflake.
The conformed tables in Snowflake (Capstone_DB) are:
- bronze.raw_users (id, first_name, last_name, email, phone_number, country_code, created_at, updated_at) - Raw user registrations.
- bronze.raw_transactions (transaction_id, user_id, amount_usd, status, payment_method, transaction_time) - Raw transactions.
- staging.stg_users (user_id, hashed_email, masked_phone, country, created_timestamp, updated_timestamp) - Cleaned conformed users.
- staging.stg_users_quarantine (id, first_name, last_name, email, phone_number, country_code, created_at, updated_at, quarantine_reason, quarantined_at) - Quarantined user records.
- staging.stg_transactions (transaction_id, user_id, transaction_amount_usd, transaction_status, payment_method, transaction_timestamp) - Cleaned transactions.
- staging.stg_transactions_quarantine (transaction_id, user_id, amount_usd, status, payment_method, transaction_time, quarantine_reason, quarantined_at) - Quarantined transaction records.
- marts.fct_user_transactions (user_id, lifetime_transaction_count, lifetime_spend_usd, last_active_timestamp) - User activity summaries.
- marts.fct_user_churn (user_id, hashed_email, country, tx_count, spend_amount, last_active_timestamp, churn_status) - User churn segments.
- public.tables (table_id, schema_name, table_name, description, row_count, size_bytes) - Data catalog tables.
- public.columns (table_id, column_name, data_type, description, is_pii, pii_type, masking_policy) - Column definitions.
- public.lineage (source_table, target_table, lineage_type) - Upstream/downstream table lineages.
- public.pipeline_runs (run_id, pipeline_name, status, start_time, end_time, duration_sec, error_message, log_path) - Execution logs.
- public.pipeline_slo (pipeline_name, sla_target_time, max_duration_sec, slo_percentage_target) - Pipeline SLA definitions.

You have access to a rich set of database and vector-store tools:
1. `search_codebase_and_docs`: Queries the vector database (ChromaDB) for pipeline documentation, design records, and dbt/Airflow files.
2. `get_table_schema`: Queries data schemas, descriptions, and PII markers for tables.
3. `get_table_lineage`: Traces upstream and downstream dependencies for tables.
4. `get_pipeline_history`: Displays execution state and recent run metrics.
5. `get_failed_run_diagnosis`: Analyzes error logs from failing pipelines and provides recommendations.
6. `trigger_data_quality_check`: Runs an active quality test suite on a specific table, logs results, and returns test statuses.
7. `nl2sql`: Translates a user's natural language query about tables, transactions, pipeline runs, or catalog metadata into a Snowflake SELECT query, executes it, and returns the query and results. Use this when the user asks for specific custom metrics, record checks, or metadata queries not covered by other tools.

Follow a clean tool-use loop:
- First, analyze what the user needs.
- If they ask for schemas, lineage, or logs, use the specific database tools. Do NOT hallucinate names or types.
- If they ask general architecture questions, use search_codebase_and_docs.
- If they request custom reports or custom queries, use nl2sql.
- IMPORTANT FOR NL2SQL: When calling 'nl2sql', you must ALWAYS pass the user's natural language question (e.g. 'how many tables are there') as the 'nl_query' parameter. Do NOT write or generate SQL yourself when calling 'nl2sql'; the tool will perform the translation and run it against Snowflake automatically.
- Provide professional, structured responses in clean Markdown. Present tables clearly.
- Ground your suggestions in the active outputs of the tools."""

            # Format/clean chat history for Bedrock
            if chat_history is None:
                messages = [{"role": "user", "content": [{"text": user_prompt}]}]
            else:
                # Find first user message to comply with Bedrock validation (must start with user)
                first_user_idx = -1
                for idx, msg in enumerate(chat_history):
                    if msg["role"] == "user":
                        first_user_idx = idx
                        break
                
                if first_user_idx != -1:
                    clean_history = chat_history[first_user_idx:]
                else:
                    clean_history = [{"role": "user", "content": user_prompt}]
                
                messages = []
                for msg in clean_history:
                    role = msg["role"]
                    content = msg["content"]
                    
                    if isinstance(content, str):
                        messages.append({
                            "role": role,
                            "content": [{"text": content}]
                        })
                    elif isinstance(content, list):
                        messages.append({
                            "role": role,
                            "content": content
                        })
                    else:
                        messages.append({
                            "role": role,
                            "content": [{"text": str(content)}]
                        })

            # Start Converse API call loop
            max_loops = 5
            for _ in range(max_loops):
                print(f"DEBUG: Sending messages to Bedrock: {json.dumps(messages, indent=2)}")
                
                gen_ctx = None
                if trace_ctx:
                    try:
                        gen_ctx = self.langfuse.start_as_current_observation(
                            as_type="generation",
                            name=self.model_id,
                            model=self.model_id,
                            input=messages,
                            model_parameters={"temperature": 0.1}
                        )
                        gen_ctx.__enter__()
                    except Exception as e:
                        print(f"Warning: Langfuse generation failed to start: {e}")
                        gen_ctx = None

                response = self.bedrock_client.converse(
                    modelId=self.model_id,
                    messages=messages,
                    system=[{"text": system_prompt}],
                    inferenceConfig={"temperature": 0.1, "maxTokens": 2048},
                    toolConfig={"tools": self.bedrock_tools_spec}
                )
                
                # Check for output content
                output_message = response["output"]["message"]
                messages.append(output_message)
                
                if gen_ctx:
                    try:
                        text_output = ""
                        for part in output_message.get("content", []):
                            if "text" in part:
                                text_output += part["text"]
                        self.langfuse.update_current_generation(output=text_output or str(output_message))
                        gen_ctx.__exit__(None, None, None)
                    except Exception as e:
                        print(f"Warning: Langfuse generation failed to end: {e}")

                # Extract reasoning text (thinking) if present
                full_text = ""
                for part in output_message.get("content", []):
                    if "text" in part:
                        full_text += part["text"]
                
                stop_reason = response.get("stopReason")
                thought_content, final_content = self._parse_thinking_and_content(full_text)
                if thought_content and callback:
                    callback("thought", thought_content)
                elif not thought_content and full_text and callback and stop_reason == "tool_use":
                    callback("thought", full_text)
                
                # Check if model requested a tool call
                stop_reason = response["stopReason"]
                if stop_reason == "tool_use":
                    tool_requests = response["output"]["message"]["content"]
                    tool_responses = []
                    
                    for item in tool_requests:
                        if "toolUse" in item:
                            tool_use = item["toolUse"]
                            tool_name = tool_use["name"]
                            tool_args = tool_use["input"]
                            tool_call_id = tool_use["toolUseId"]
                            
                            if callback:
                                callback("tool_start", {"name": tool_name, "args": tool_args})
                                
                            tool_span_ctx = None
                            if trace_ctx:
                                try:
                                    tool_span_ctx = self.langfuse.start_as_current_observation(
                                        as_type="tool",
                                        name=tool_name,
                                        input=tool_args
                                    )
                                    tool_span_ctx.__enter__()
                                except Exception as e:
                                    print(f"Warning: Langfuse span failed to start: {e}")
                                    tool_span_ctx = None
                            
                            # Invoke tool
                            if tool_name in self.tools_map:
                                try:
                                    tool_fn = self.tools_map[tool_name]
                                    # Execute tool
                                    tool_result = tool_fn(**tool_args)
                                except Exception as e:
                                    tool_result = f"Error executing tool {tool_name}: {e}"
                            else:
                                tool_result = f"Error: Tool '{tool_name}' not implemented."
                                
                            if callback:
                                callback("tool_end", {"name": tool_name, "result": tool_result})
                                
                            if tool_span_ctx:
                                try:
                                    self.langfuse.update_current_span(output=str(tool_result))
                                    tool_span_ctx.__exit__(None, None, None)
                                except Exception as e:
                                    print(f"Warning: Langfuse span failed to end: {e}")
                            
                            tool_responses.append({
                                "toolResult": {
                                    "toolUseId": tool_call_id,
                                    "content": [{"text": str(tool_result)}]
                                }
                            })
                            
                    # Append tool responses to chat history and loop back to model
                    messages.append({
                        "role": "user",
                        "content": tool_responses
                    })
                else:
                    # Model returned a final textual response!
                    full_text = ""
                    for part in output_message.get("content", []):
                        if "text" in part:
                            full_text += part["text"]
                    
                    thought_content, final_content = self._parse_thinking_and_content(full_text)
                    if final_content == "":
                        final_content = "Deco was unable to formulate a textual response."
                        
                    if trace_ctx:
                        try:
                            self.langfuse.update_current_span(output=final_content)
                        except:
                            pass
                            
                    return final_content
                    
            return "Loop limit reached before agent could formulate a final response."
            
        except Exception as e:
            print(f"AWS Bedrock error: {e}")
            raise e
        finally:
            if trace_ctx:
                try:
                    trace_ctx.__exit__(None, None, None)
                except:
                    pass

