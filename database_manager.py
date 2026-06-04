import os
import snowflake.connector
from dotenv import load_dotenv
import chromadb
from chromadb.utils import embedding_functions
import pandas as pd

# Load environment variables
load_dotenv()

class MetadataHelper:
    """Helper class to query the structured Snowflake database containing DE catalog, lineage, and run logs."""
    
    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.database = os.getenv("SNOWFLAKE_DATABASE", "Capstone_DB")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "SIGMA_WH")
        self.email_salt = os.getenv("EMAIL_HASH_SALT", "f8c3d9b1e5a26748c9d0e1f2b3a4c5d6")


    def _get_connection(self):
        if not self.account or not self.user or not self.password:
            raise ValueError("Missing Snowflake credentials. Please check your .env file.")
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            warehouse=self.warehouse
        )

    def _execute_query(self, query, params=()):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            if cursor.description:
                # SELECT queries: map uppercase Snowflake column names to lowercase keys for compatibility
                columns = [col[0].lower() for col in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results
            else:
                # INSERT/UPDATE/DELETE: commit and return rowcount
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            print(f"Error executing Snowflake query: {e}")
            return {"error": str(e)}
        finally:
            cursor.close()
            conn.close()

    def get_all_tables(self):
        """Retrieve all registered tables in the data catalog."""
        query = "SELECT table_id, schema_name, table_name, description, row_count, size_bytes FROM public.tables"
        return self._execute_query(query)

    def get_table_details(self, table_id):
        """Retrieve schema, descriptions, and PII tags for a single table."""
        # 1. Fetch table details
        table_info = self._execute_query("SELECT * FROM public.tables WHERE table_id = %s", (table_id,))
        if not table_info or "error" in table_info:
            return None
        
        # 2. Fetch column details
        columns_info = self._execute_query(
            "SELECT column_name, data_type, description, is_pii, pii_type, masking_policy FROM public.columns WHERE table_id = %s", 
            (table_id,)
        )
        
        return {
            "table": table_info[0],
            "columns": columns_info
        }

    def get_pii_columns(self):
        """Find all columns flagged as containing PII along with their masking policies."""
        query = """
            SELECT c.table_id, c.column_name, c.data_type, c.pii_type, c.masking_policy, t.description as table_desc 
            FROM public.columns c
            JOIN public.tables t ON c.table_id = t.table_id
            WHERE c.is_pii = TRUE
        """
        return self._execute_query(query)

    def get_lineage(self, table_id, direction="both"):
        """Trace data lineage upstream or downstream from a target table.
        
        Args:
            table_id: The target table path (e.g. staging.stg_users)
            direction: 'upstream', 'downstream', or 'both'
        """
        upstream = []
        downstream = []
        
        # Find upstream tables (where target_table is our table_id)
        if direction in ("upstream", "both"):
            upstream = self._execute_query(
                "SELECT source_table, lineage_type FROM public.lineage WHERE target_table = %s", (table_id,)
            )
            
        # Find downstream tables (where source_table is our table_id)
        if direction in ("downstream", "both"):
            downstream = self._execute_query(
                "SELECT target_table, lineage_type FROM public.lineage WHERE source_table = %s", (table_id,)
            )
            
        return {
            "table_id": table_id,
            "upstream": upstream,
            "downstream": downstream
        }

    def get_pipeline_runs(self, limit=10):
        """Retrieve recent pipeline run history."""
        query = """
            SELECT run_id, pipeline_name, status, start_time, end_time, duration_sec, error_message, log_path 
            FROM public.pipeline_runs 
            ORDER BY run_id DESC 
            LIMIT %s
        """
        return self._execute_query(query, (limit,))

    def get_latest_failed_run(self):
        """Fetch details of the latest failed pipeline run to help with troubleshooting."""
        query = """
            SELECT run_id, pipeline_name, status, start_time, end_time, duration_sec, error_message, log_path 
            FROM public.pipeline_runs 
            WHERE status = 'FAILED'
            ORDER BY run_id DESC 
            LIMIT 1
        """
        result = self._execute_query(query)
        return result[0] if result else None

    def get_pipeline_slo_compliance(self):
        """Evaluate SLO status based on execution history."""
        # 1. Fetch configured SLO targets
        slo_targets = self._execute_query("SELECT * FROM public.pipeline_slo")
        
        compliance_report = []
        if isinstance(slo_targets, dict) and "error" in slo_targets:
            return compliance_report

        for target in slo_targets:
            p_name = target["pipeline_name"]
            sla_time_str = target["sla_target_time"]
            max_duration = target["max_duration_sec"]
            
            # Fetch last 5 runs
            runs = self._execute_query(
                "SELECT status, start_time, duration_sec FROM public.pipeline_runs WHERE pipeline_name = %s ORDER BY run_id DESC LIMIT 5",
                (p_name,)
            )
            
            if not runs or isinstance(runs, dict) and "error" in runs:
                continue
                
            success_count = sum(1 for r in runs if r["status"] == "SUCCESS")
            total_runs = len(runs)
            success_rate = (success_count / total_runs) * 100
            
            # Check SLA timing failures (completion time exceeding target or status failed)
            duration_violations = sum(1 for r in runs if r["duration_sec"] > max_duration)
            
            compliance_report.append({
                "pipeline_name": p_name,
                "sla_target_completion_time": sla_time_str,
                "max_duration_allowed_sec": max_duration,
                "recent_runs_evaluated": total_runs,
                "success_rate_percent": success_rate,
                "duration_violations_count": duration_violations,
                "slo_adherence_status": "HIGHLY COMPLIANT" if success_rate >= target["slo_percentage_target"] else "BREACHED"
            })
            
        return compliance_report

    def get_dq_summary(self):
        """Run live data quality checks against Snowflake bronze tables and quarantine tables.
        Returns a structured dict with per-table DQ metrics."""
        results = {}

        # --- bronze.raw_users DQ checks ---
        raw_users_metrics = self._execute_query("""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
                SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
                SUM(CASE WHEN country_code IS NULL THEN 1 ELSE 0 END) AS null_country_code,
                SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_first_name,
                SUM(CASE WHEN phone_number IS NULL THEN 1 ELSE 0 END) AS null_phone
            FROM bronze.raw_users
        """)
        results["bronze.raw_users"] = raw_users_metrics[0] if isinstance(raw_users_metrics, list) and raw_users_metrics else {"error": "Query failed"}

        # --- bronze.raw_transactions DQ checks ---
        raw_tx_metrics = self._execute_query("""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_id,
                SUM(CASE WHEN amount_usd < 0 THEN 1 ELSE 0 END) AS negative_amounts,
                SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
                MIN(amount_usd) AS min_amount,
                MAX(amount_usd) AS max_amount
            FROM bronze.raw_transactions
        """)
        results["bronze.raw_transactions"] = raw_tx_metrics[0] if isinstance(raw_tx_metrics, list) and raw_tx_metrics else {"error": "Query failed"}

        # --- Quarantine table sizes ---
        quarantine_users = self._execute_query("SELECT COUNT(*) AS row_count FROM staging.stg_users_quarantine")
        results["staging.stg_users_quarantine"] = quarantine_users[0] if isinstance(quarantine_users, list) and quarantine_users else {"row_count": 0}

        quarantine_tx = self._execute_query("SELECT COUNT(*) AS row_count FROM staging.stg_transactions_quarantine")
        results["staging.stg_transactions_quarantine"] = quarantine_tx[0] if isinstance(quarantine_tx, list) and quarantine_tx else {"row_count": 0}

        # --- staging.stg_users post-clean checks ---
        stg_users_metrics = self._execute_query("""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN hashed_email IS NULL THEN 1 ELSE 0 END) AS null_hashed_email,
                SUM(CASE WHEN masked_phone IS NULL THEN 1 ELSE 0 END) AS null_masked_phone
            FROM staging.stg_users
        """)
        results["staging.stg_users"] = stg_users_metrics[0] if isinstance(stg_users_metrics, list) and stg_users_metrics else {"error": "Query failed"}

        return results

    def run_dq_etl_pipeline(self, table_id):
        """Generate a fresh batch of data with realistic DQ issues, run full ETL, and log the pipeline run.
        Simulates Airflow-style orchestration trigger for bronze DQ testing."""
        import random
        import datetime as dt

        conn = self._get_connection()
        cursor = conn.cursor()
        start_ts = dt.datetime.utcnow()
        status = "SUCCESS"
        error_message = None
        dq_findings = {}

        try:
            # Use the insert timestamp to filter only THIS batch in ETL
            batch_start = start_ts.strftime("%Y-%m-%d %H:%M:%S")

            if table_id == "bronze.raw_users":
                batch_size = 200
                first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "David", "Emily", "Frank", "Grace", "Henry"]
                last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
                countries = ["US", "CA", "GB", "DE", "FR", "IN", "JP"]

                users = []
                null_id_count = 0
                null_email_count = 0
                null_cc_count = 0
                now_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")

                for i in range(batch_size):
                    uid = f"USR_DQ_{random.randint(90000, 999999)}"
                    fn = random.choice(first_names)
                    ln = random.choice(last_names)
                    email = f"{fn.lower()}.{ln.lower()}_{i}@dqtest.com"
                    phone = "".join(str(random.randint(0, 9)) for _ in range(10))
                    country = random.choice(countries)
                    r = random.random()
                    if r < 0.025:         # ~2.5% critical null user_id
                        uid = None
                        null_id_count += 1
                    elif r < 0.055:       # ~3% null email (high severity)
                        email = None
                        null_email_count += 1
                    elif r < 0.085:       # ~3% null country (medium severity)
                        country = None
                        null_cc_count += 1
                    users.append((uid, fn, ln, email, phone, country, now_str, now_str))

                cursor.executemany("""
                    INSERT INTO bronze.raw_users
                        (id, first_name, last_name, email, phone_number, country_code, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, users)

                # ETL: clean records → staging.stg_users
                cursor.execute(f"""
                    INSERT INTO staging.stg_users
                        (user_id, hashed_email, masked_phone, country, created_timestamp, updated_timestamp)
                    SELECT
                        id,
                        CASE WHEN email IS NULL THEN NULL
                             ELSE SHA2(LOWER(email) || '{self.email_salt}', 256) END,
                        CASE WHEN phone_number IS NULL THEN NULL
                             ELSE CONCAT('+', COALESCE(country_code,'XX'), '-XXX-XXX-', RIGHT(phone_number,4)) END,
                        UPPER(country_code),
                        CAST(created_at AS TIMESTAMP),
                        CAST(updated_at AS TIMESTAMP)
                    FROM bronze.raw_users
                    WHERE id IS NOT NULL
                      AND CAST(created_at AS TIMESTAMP) >= '{batch_start}'
                """)

                # ETL: bad records → staging.stg_users_quarantine
                cursor.execute(f"""
                    INSERT INTO staging.stg_users_quarantine
                        (id, first_name, last_name, email, phone_number, country_code,
                         created_at, updated_at, quarantine_reason)
                    SELECT id, first_name, last_name, email, phone_number, country_code,
                           CAST(created_at AS TIMESTAMP), CAST(updated_at AS TIMESTAMP),
                           'Critical: Null user_id (id)'
                    FROM bronze.raw_users
                    WHERE id IS NULL
                      AND CAST(created_at AS TIMESTAMP) >= '{batch_start}'
                """)

                # MERGE new users into marts.fct_user_churn
                cursor.execute(f"""
                    MERGE INTO marts.fct_user_churn AS tgt
                    USING (
                        SELECT u.user_id, u.hashed_email, u.country,
                               COALESCE(t.lifetime_transaction_count, 0) AS tx_count,
                               COALESCE(t.lifetime_spend_usd, 0.0)       AS spend_amount,
                               t.last_active_timestamp,
                               CASE WHEN t.last_active_timestamp IS NULL THEN 'CHURNED'
                                    WHEN DATEDIFF('day', t.last_active_timestamp, CURRENT_DATE()) > 30 THEN 'CHURNED'
                                    ELSE 'ACTIVE' END AS churn_status
                        FROM staging.stg_users u
                        LEFT JOIN marts.fct_user_transactions t ON u.user_id = t.user_id
                        WHERE CAST(u.created_timestamp AS TIMESTAMP) >= '{batch_start}'
                    ) AS src
                    ON tgt.user_id = src.user_id
                    WHEN NOT MATCHED THEN
                        INSERT (user_id, hashed_email, country, tx_count, spend_amount, last_active_timestamp, churn_status)
                        VALUES (src.user_id, src.hashed_email, src.country, src.tx_count,
                                src.spend_amount, src.last_active_timestamp, src.churn_status)
                """)

                dq_findings = {
                    "batch_size": batch_size,
                    "null_user_id": null_id_count,
                    "null_email": null_email_count,
                    "null_country_code": null_cc_count,
                    "quarantined": null_id_count,
                    "passed_to_staging": batch_size - null_id_count,
                }

            elif table_id == "bronze.raw_transactions":
                batch_size = 500
                statuses_list = ["completed", "failed", "test_payment"]
                weights = [0.85, 0.10, 0.05]
                payments = ["credit card", "apple pay", "paypal", "bank transfer"]

                # Fetch some real user IDs to make FK data realistic
                cursor.execute("SELECT user_id FROM staging.stg_users LIMIT 200")
                rows = cursor.fetchall()
                user_ids = [r[0] for r in rows] if rows else [f"USR_{1000 + i}" for i in range(50)]

                transactions = []
                null_txid_count = 0
                neg_amount_count = 0
                now_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")

                for j in range(batch_size):
                    txn_id = f"TXN_DQ_{random.randint(9000000, 99999999)}"
                    uid = random.choice(user_ids)
                    amount = round(random.uniform(5.0, 450.0), 2)
                    txn_status = random.choices(statuses_list, weights=weights)[0]
                    payment = random.choice(payments)
                    r = random.random()
                    if r < 0.025:        # ~2.5% critical null transaction_id
                        txn_id = None
                        null_txid_count += 1
                    elif r < 0.05:       # ~2.5% negative amounts (high severity, auto-fixed)
                        amount = -amount
                        neg_amount_count += 1
                    transactions.append((txn_id, uid, amount, txn_status, payment, now_str))

                cursor.executemany("""
                    INSERT INTO bronze.raw_transactions
                        (transaction_id, user_id, amount_usd, status, payment_method, transaction_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, transactions)

                # ETL: clean → staging.stg_transactions (ABS fixes negative amounts)
                cursor.execute(f"""
                    INSERT INTO staging.stg_transactions
                        (transaction_id, user_id, transaction_amount_usd,
                         transaction_status, payment_method, transaction_timestamp)
                    SELECT transaction_id, user_id,
                           ABS(CAST(amount_usd AS DECIMAL(18,2))),
                           LOWER(status), payment_method,
                           CAST(transaction_time AS TIMESTAMP)
                    FROM bronze.raw_transactions
                    WHERE transaction_id IS NOT NULL
                      AND CAST(transaction_time AS TIMESTAMP) >= '{batch_start}'
                """)

                # ETL: bad → staging.stg_transactions_quarantine
                cursor.execute(f"""
                    INSERT INTO staging.stg_transactions_quarantine
                        (transaction_id, user_id, amount_usd, status,
                         payment_method, transaction_time, quarantine_reason)
                    SELECT transaction_id, user_id, amount_usd, status, payment_method,
                           CAST(transaction_time AS TIMESTAMP),
                           'Critical: Null transaction_id (transaction_id)'
                    FROM bronze.raw_transactions
                    WHERE transaction_id IS NULL
                      AND CAST(transaction_time AS TIMESTAMP) >= '{batch_start}'
                """)

                # MERGE updated transaction aggregates into marts.fct_user_transactions
                cursor.execute(f"""
                    MERGE INTO marts.fct_user_transactions AS tgt
                    USING (
                        SELECT user_id,
                               COUNT(*)                      AS cnt,
                               SUM(transaction_amount_usd)   AS total,
                               MAX(transaction_timestamp)    AS last_ts
                        FROM staging.stg_transactions
                        WHERE transaction_status = 'completed'
                          AND CAST(transaction_timestamp AS TIMESTAMP) >= '{batch_start}'
                        GROUP BY user_id
                    ) AS src
                    ON tgt.user_id = src.user_id
                    WHEN MATCHED THEN UPDATE SET
                        lifetime_transaction_count = tgt.lifetime_transaction_count + src.cnt,
                        lifetime_spend_usd         = tgt.lifetime_spend_usd + src.total,
                        last_active_timestamp      = GREATEST(tgt.last_active_timestamp, src.last_ts)
                    WHEN NOT MATCHED THEN
                        INSERT (user_id, lifetime_transaction_count, lifetime_spend_usd, last_active_timestamp)
                        VALUES (src.user_id, src.cnt, src.total, src.last_ts)
                """)

                dq_findings = {
                    "batch_size": batch_size,
                    "null_transaction_id": null_txid_count,
                    "negative_amounts": neg_amount_count,
                    "quarantined": null_txid_count,
                    "passed_to_staging": batch_size - null_txid_count,
                }

            conn.commit()

        except Exception as exc:
            status = "FAILED"
            error_message = str(exc)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            end_ts = dt.datetime.utcnow()
            duration = max(1, int((end_ts - start_ts).total_seconds()))
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass

        # Log the run to public.pipeline_runs
        run_id = self.insert_pipeline_run(
            pipeline_name=f"dq_etl_{table_id}",
            status=status,
            start_time=start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_time=end_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_sec=duration,
            error_message=error_message,
            log_path=f"logs/dq_etl_{table_id.replace('.', '_')}_{start_ts.strftime('%Y%m%d%H%M%S')}.log",
        )

        return {
            "status": status,
            "run_id": run_id,
            "table_id": table_id,
            "dq_findings": dq_findings,
            "duration_sec": duration,
            "error_message": error_message,
        }

    def get_dq_pipeline_history(self):
        """Get dynamic run history for both bronze DQ ETL pipelines."""
        pipelines = {
            "dq_etl_bronze.raw_users": "bronze.raw_users",
            "dq_etl_bronze.raw_transactions": "bronze.raw_transactions",
        }
        result = {}
        for pipeline_name, table_label in pipelines.items():
            runs = self._execute_query(
                """SELECT run_id, status, start_time, duration_sec, error_message
                   FROM public.pipeline_runs
                   WHERE pipeline_name = %s
                   ORDER BY run_id DESC LIMIT 10""",
                (pipeline_name,),
            )
            if isinstance(runs, list) and runs:
                total = len(runs)
                successes = sum(1 for r in runs if r["status"] == "SUCCESS")
                result[table_label] = {
                    "pipeline_name": pipeline_name,
                    "runs": runs,
                    "total_runs": total,
                    "success_count": successes,
                    "failure_count": total - successes,
                    "success_rate": round((successes / total) * 100, 1),
                    "last_run": runs[0],
                }
            else:
                result[table_label] = {
                    "pipeline_name": pipeline_name,
                    "runs": [],
                    "total_runs": 0,
                    "success_count": 0,
                    "failure_count": 0,
                    "success_rate": 0.0,
                    "last_run": None,
                }
        return result

    def insert_pipeline_run(self, pipeline_name, status, start_time, end_time, duration_sec, error_message, log_path):
        """Programmatically insert a new pipeline run. Perfect for showing agentic actions in real time."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO public.pipeline_runs (pipeline_name, status, start_time, end_time, duration_sec, error_message, log_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (pipeline_name, status, start_time, end_time, duration_sec, error_message, log_path)
            )
            # Retrieve generated IDENTITY/run_id value
            cursor.execute("SELECT LAST_QUERY_ID()")
            q_id = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT run_id FROM public.pipeline_runs WHERE run_id = (SELECT max(run_id) FROM public.pipeline_runs)")
            row = cursor.fetchone()
            run_id = row[0] if row else None
            
            conn.commit()
            return run_id
        except Exception as e:
            print(f"Error inserting pipeline run: {e}")
            return {"error": str(e)}
        finally:
            cursor.close()
            conn.close()


class VectorStoreHelper:
    """Helper class to load documents/codebase into ChromaDB and query the vector store."""
    
    def __init__(self, db_dir="mock_data/chromadb_store"):
        self.db_dir = db_dir
        self.client = chromadb.PersistentClient(path=db_dir)
        self.embedding_function = embedding_functions.DefaultEmbeddingFunction()
        
    def get_or_create_collection(self, name="deco_knowledge_base"):
        return self.client.get_or_create_collection(
            name=name,
            embedding_function=self.embedding_function
        )

    def load_markdown_docs(self, docs_dir="mock_data/docs"):
        """Recursively scan documentation directories and index markdown files."""
        collection = self.get_or_create_collection()
        documents = []
        metadatas = []
        ids = []
        
        for root, _, files in os.walk(docs_dir):
            for file in files:
                if file.endswith(".md"):
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, start=os.path.dirname(docs_dir))
                    
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        
                    chunks = content.split("\n## ")
                    for i, chunk in enumerate(chunks):
                        if not chunk.strip():
                            continue
                        
                        chunk_text = ("## " + chunk) if i > 0 else chunk
                        
                        documents.append(chunk_text)
                        metadatas.append({
                            "source_file": rel_path,
                            "file_type": "documentation",
                            "chunk_id": i
                        })
                        ids.append(f"doc_{rel_path.replace('/', '_').replace('.', '_')}_{i}")
                        
        if documents:
            collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
        return len(documents)

    def load_codebase(self, codebase_dir="mock_data/codebase"):
        """Recursively scan codebase directories and index SQL and Python files."""
        collection = self.get_or_create_collection()
        documents = []
        metadatas = []
        ids = []
        
        for root, _, files in os.walk(codebase_dir):
            for file in files:
                if file.endswith((".sql", ".py")):
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, start=os.path.dirname(codebase_dir))
                    
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        
                    documents.append(content)
                    metadatas.append({
                        "source_file": rel_path,
                        "file_type": "code_" + file.split(".")[-1],
                        "chunk_id": 0
                    })
                    ids.append(f"code_{rel_path.replace('/', '_').replace('.', '_')}_0")
                    
        if documents:
            collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
        return len(documents)

    def query(self, query_text, n_results=3):
        """Query the vector store for semantic matches."""
        collection = self.get_or_create_collection()
        results = collection.query(
            query_texts=[query_text],
            n_results=n_results
        )
        
        formatted_results = []
        if results and results["documents"]:
            docs = results["documents"][0]
            metas = results["metadatas"][0]
            distances = results["distances"][0] if "distances" in results else [0]*len(docs)
            
            for doc, meta, dist in zip(docs, metas, distances):
                formatted_results.append({
                    "content": doc,
                    "metadata": meta,
                    "distance": dist
                })
        return formatted_results
