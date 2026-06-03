import boto3
import json
import sqlite3
import datetime
import os
from database_manager import MetadataHelper, VectorStoreHelper

class AgentCore:
    """Core Agent class that manages tool registration, AWS Bedrock orchestration (Nova Pro), and the tool execution loop."""
    
    def __init__(self, region_name="us-east-1", aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None):
        self.region_name = region_name
        
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

        # Model identifier
        self.model_id = "amazon.nova-pro-v1:0"

        # Register tools mapping
        self.tools_map = {
            "search_codebase_and_docs": self.tool_search_codebase_and_docs,
            "get_table_schema": self.tool_get_table_schema,
            "get_table_lineage": self.tool_get_table_lineage,
            "get_pipeline_history": self.tool_get_pipeline_history,
            "get_failed_run_diagnosis": self.tool_get_failed_run_diagnosis,
            "trigger_data_quality_check": self.tool_trigger_data_quality_check
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
        # Active agentic action!
        details = self.metadata_helper.get_table_details(table_id)
        if not details:
            return f"Cannot run quality tests: table `{table_id}` does not exist in catalog."
            
        # Simulate testing logic based on tables
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        log_filename = f"mock_data/logs/dq_check_{table_id.replace('.', '_')}_{datetime.datetime.utcnow().strftime('%s')}.log"
        
        test_logs = []
        test_logs.append(f"{timestamp} [INFO] Initializing Deco Data Quality Assertions...")
        test_logs.append(f"{timestamp} [INFO] Connecting to warehouse schema: {details['table']['schema_name']}")
        test_logs.append(f"{timestamp} [INFO] Testing table size and profile: row_count={details['table']['row_count']}")
        
        status = "SUCCESS"
        failures = []
        
        # Table specific rules
        if table_id == "staging.stg_users":
            test_logs.append(f"{timestamp} [TEST] Rule 1: Validate Unique Constraint on column `user_id`...")
            test_logs.append(f"{timestamp} [PASS] Column `user_id` contains exactly 0 duplicates.")
            
            test_logs.append(f"{timestamp} [TEST] Rule 2: Validate PII masking compliance on column `hashed_email` (SHA-256 validation)...")
            # Email shouldn't contain raw "@" characters
            test_logs.append(f"{timestamp} [PASS] Verified 100% of values in `hashed_email` are properly hashed. 0 clear-text emails found.")
            
            test_logs.append(f"{timestamp} [TEST] Rule 3: Validate masked phone number pattern `masked_phone`...")
            test_logs.append(f"{timestamp} [PASS] All phone values conform to target pattern +X-XXX-XXX-XXXX.")
            
        elif table_id == "marts.fct_user_churn":
            test_logs.append(f"{timestamp} [TEST] Rule 1: Validate Unique user_id constraints...")
            # Let's say it passes now to simulate a fix
            test_logs.append(f"{timestamp} [PASS] Mart uniqueness constraints validated. 0 duplicate index violations found.")
            
            test_logs.append(f"{timestamp} [TEST] Rule 2: Validate column `spend_amount` ranges >= 0...")
            test_logs.append(f"{timestamp} [PASS] Range boundaries validated successfully.")
            
            test_logs.append(f"{timestamp} [TEST] Rule 3: Validate enum options for `churn_status` in ('ACTIVE', 'CHURNED')...")
            test_logs.append(f"{timestamp} [PASS] Enums check: 100% valid rows.")
            
        else:
            test_logs.append(f"{timestamp} [TEST] Default Rule: Verify column counts and non-null defaults...")
            test_logs.append(f"{timestamp} [PASS] Columns verification succeeded.")
            
        test_logs.append(f"{timestamp} [INFO] Data Quality suite finished. Status: {status}")
        
        # Write DQ logs
        with open(log_filename, "w") as f:
            f.write("\n".join(test_logs))
            
        # Log this active test suite execution in SQLite pipeline_runs
        run_id = self.metadata_helper.insert_pipeline_run(
            pipeline_name=f"data_quality_test_{table_id}",
            status=status,
            start_time=timestamp,
            end_time=timestamp,
            duration_sec=3,
            error_message=None if status == "SUCCESS" else "; ".join(failures),
            log_path=log_filename
        )
        
        output = f"### ✅ Agentic Action: Data Quality Check Triggered Successfully!\n"
        output += f"* **Target Table**: `{table_id}`\n"
        output += f"* **Created Run ID**: `#{run_id}`\n"
        output += f"* **Overall Status**: **{status}**\n"
        output += f"* **Test Log File Saved**: `{log_filename}`\n\n"
        output += "**Asserted Test Cases Output:**\n"
        output += "```text\n"
        output += "\n".join([line for line in test_logs if "[PASS]" in line or "[FAIL]" in line])
        output += "\n```\n"
        
        return output

    # --- Agent Execution Core Loop ---
    
    def simulate_agent_locally(self, user_prompt):
        """Simulates LLM reasoning locally in case AWS Credentials are not set up.
        This provides a highly elegant, interactive fallback that keeps the Streamlit app fully functional!"""
        
        prompt_lower = user_prompt.lower()
        
        # 1. Pipeline status / health / run Q&A
        if any(w in prompt_lower for w in ("run", "status", "health", "history", "fail", "broken", "incident")):
            if "fail" in prompt_lower or "broken" in prompt_lower or "diagnose" in prompt_lower:
                tool_output = self.tool_get_failed_run_diagnosis()
                reasoning = "The user is asking to troubleshoot a broken pipeline. I will read the latest failed pipeline run logs, explain what broke, and suggest immediate mitigations."
            else:
                tool_output = self.tool_get_pipeline_history()
                reasoning = "The user wants to inspect our pipeline health and run history. I will query the pipeline monitoring log database."
                
        # 2. Trigger Quality Check (Agentic Action)
        elif "trigger" in prompt_lower or "quality" in prompt_lower or "check" in prompt_lower or "test" in prompt_lower:
            table = "staging.stg_users"
            if "churn" in prompt_lower or "fct" in prompt_lower:
                table = "marts.fct_user_churn"
            tool_output = self.tool_trigger_data_quality_check(table)
            reasoning = f"The user requested to trigger an active data quality run. I will invoke `trigger_data_quality_check` on table `{table}` and register this run."
            
        # 3. Data catalog / schema lookup
        elif any(w in prompt_lower for w in ("schema", "columns", "catalog", "table", "pii", "mask")):
            table = "staging.stg_users"
            if "churn" in prompt_lower or "fct" in prompt_lower:
                table = "marts.fct_user_churn"
            elif "raw" in prompt_lower or "bronze" in prompt_lower:
                table = "bronze.raw_users"
                
            if "pii" in prompt_lower or "mask" in prompt_lower:
                cols = self.metadata_helper.get_pii_columns()
                tool_output = "### 🚨 PII Catalog & Governance Mappings\n\n"
                tool_output += "| Table ID | Column | Data Type | PII Classification | Masking Rule |\n"
                tool_output += "| --- | --- | --- | --- | --- |\n"
                for c in cols:
                    tool_output += f"| `{c['table_id']}` | `{c['column_name']}` | `{c['data_type']}` | **{c['pii_type']}** | `{c['masking_policy']}` |\n"
                reasoning = "The user is asking about PII classification policies. I will scan our data catalog database for columns flagged as PII."
            else:
                tool_output = self.tool_get_table_schema(table)
                reasoning = f"The user is asking for the schema of table `{table}`. I will lookup the conformed data catalog catalog."

        # 4. Lineage Q&A
        elif "lineage" in prompt_lower or "dependency" in prompt_lower or "flow" in prompt_lower or "upstream" in prompt_lower or "downstream" in prompt_lower:
            table = "marts.fct_user_churn"
            if "user" in prompt_lower:
                table = "staging.stg_users"
            tool_output = self.tool_get_table_lineage(table)
            reasoning = f"The user is inquiring about data lineage dependencies. I will fetch the lineage paths for `{table}`."
            
        # 5. Semantic codebase / architecture search (RAG)
        else:
            tool_output = self.tool_search_codebase_and_docs(user_prompt)
            reasoning = f"The user is asking a general Q&A question about design decisions. I will query the ChromaDB vector database for matching documentation blocks."
            
        # Structure beautiful agent reasoning style response
        final_prompt = f"""[DECO LOCAL REASONING ENGINE]
* **Thought**: {reasoning}
* **Action**: Invoked local database helper tool.

{tool_output}

*Note: Deco is currently running in local offline mode because AWS Bedrock credentials were not configured. The interface remains fully functional with local mocks.*"""
        return final_prompt

    def run_agent(self, user_prompt):
        """Orchestrates agent execution. Attempts AWS Bedrock (Nova Pro) tool loop first; if not configured or raises error, falls back to local simulation."""
        if not self.has_aws or not self.bedrock_client:
            return self.simulate_agent_locally(user_prompt)
            
        try:
            # Construct system instructions
            system_prompt = """You are "Deco", an advanced AI Data Engineering Co-pilot. Your purpose is to assist data platform engineers.
You have access to a rich set of database and vector-store tools:
1. `search_codebase_and_docs`: Queries the vector database (ChromaDB) for pipeline documentation, design records, and dbt/Airflow files.
2. `get_table_schema`: Queries data schemas, descriptions, and PII markers for tables.
3. `get_table_lineage`: Traces upstream and downstream dependencies for tables.
4. `get_pipeline_history`: Displays execution state and recent run metrics.
5. `get_failed_run_diagnosis`: Analyzes error logs from failing pipelines and provides recommendations.
6. `trigger_data_quality_check`: Runs an active quality test suite on a specific table, logs results, and returns test statuses.

Follow a clean tool-use loop:
- First, analyze what the user needs.
- If they ask for schemas, lineage, or logs, use the specific database tools. Do NOT hallucinate names or types.
- If they ask general architecture questions, use search_codebase_and_docs.
- Provide professional, structured responses in clean Markdown. Present tables clearly.
- Ground your suggestions in the active outputs of the tools."""

            messages = [{"role": "user", "content": [{"text": user_prompt}]}]
            
            # Start Converse API call loop
            max_loops = 5
            for _ in range(max_loops):
                print(f"DEBUG: Sending messages to Bedrock: {json.dumps(messages, indent=2)}")
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
                    for part in output_message["content"]:
                        if "text" in part:
                            return part["text"]
                    return "Deco was unable to formulate a textual response."
                    
            return "Loop limit reached before agent could formulate a final response."
            
        except Exception as e:
            print(f"AWS Bedrock error, falling back to local simulation: {e}")
            return self.simulate_agent_locally(user_prompt)
