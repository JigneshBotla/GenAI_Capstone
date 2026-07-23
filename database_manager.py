import os
import snowflake.connector
import chromadb
from chromadb.utils import embedding_functions
import pandas as pd
import math
import re
from collections import Counter

# Load environment variables
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")
class MetadataHelper:
    """Helper class to query the structured Snowflake database containing DE catalog, lineage, and run logs."""
    
    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")   # Optional fallback
        self.pat = os.getenv("SNOWFLAKE_PAT")
        self.database = os.getenv("SNOWFLAKE_DATABASE", "Capstone_DB")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "SIGMA_WH")
        self.role = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
        self.email_salt = os.getenv("EMAIL_HASH_SALT", "f8c3d9b1e5a26748c9d0e1f2b3a4c5d6")


    def _get_connection(self):
        """Create a Snowflake connection using PAT if available, otherwise password."""

        if not self.account or not self.user:
            raise ValueError("Missing Snowflake account or username in .env")

        conn_params = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "role": self.role,
        }

        # Prefer Programmatic Access Token
        if self.pat:
            conn_params["authenticator"] = "PROGRAMMATIC_ACCESS_TOKEN"
            conn_params["token"] = self.pat
        else:
            if not self.password:
                raise ValueError(
                    "Neither SNOWFLAKE_PAT nor SNOWFLAKE_PASSWORD is configured."
                )
            conn_params["password"] = self.password

        return snowflake.connector.connect(**conn_params)

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


class BM25:
    """Lightweight, self-contained implementation of BM25 for ranking documents."""
    def __init__(self, corpus, k1=1.5, b=0.75):
        self.k1 = k1
        self.b = b
        self.corpus_size = len(corpus)
        self.avg_doc_len = 0.0
        self.doc_lengths = []
        self.doc_term_freqs = []
        self.df = Counter()
        self.idf = {}
        
        total_len = 0
        for doc in corpus:
            # Tokenize and normalize words, removing punctuation
            tokens = [w.strip(".,!?\"'()[]{}") for w in doc.lower().split() if w.strip()]
            self.doc_lengths.append(len(tokens))
            total_len += len(tokens)
            
            freqs = Counter(tokens)
            self.doc_term_freqs.append(freqs)
            for token in freqs:
                self.df[token] += 1
                
        self.avg_doc_len = total_len / self.corpus_size if self.corpus_size > 0 else 0.0
        
        for token, freq in self.df.items():
            # Standard BM25 IDF formulation
            self.idf[token] = math.log((self.corpus_size - freq + 0.5) / (freq + 0.5) + 1.0)

    def get_score(self, doc_idx, query_tokens):
        score = 0.0
        doc_len = self.doc_lengths[doc_idx]
        freqs = self.doc_term_freqs[doc_idx]
        
        for token in query_tokens:
            if token in freqs:
                tf = freqs[token]
                idf = self.idf.get(token, 0.0)
                numerator = idf * tf * (self.k1 + 1.0)
                denominator = tf + self.k1 * (1.0 - self.b + self.b * doc_len / self.avg_doc_len)
                score += numerator / denominator
        return score


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

    def semantic_chunking(self, content):
        """Splits prose content into semantic parent chunks using sentence embeddings and similarity thresholds."""
        # Split content into sentences
        sentence_ends = re.compile(r'(?<=[.!?])\s+')
        sentences = [s.strip() for s in sentence_ends.split(content) if s.strip()]
        
        if len(sentences) < 4:
            return [content]
            
        try:
            # Generate embeddings for each sentence
            embeddings = self.embedding_function(sentences)
        except Exception as e:
            print(f"Warning: Semantic chunking embedding failed, falling back to basic split: {e}")
            return [content]
            
        # Cosine similarity helper
        def cosine_similarity(u, v):
            dot = sum(a * b for a, b in zip(u, v))
            norm_u = math.sqrt(sum(a * a for a in u))
            norm_v = math.sqrt(sum(b * b for b in v))
            if norm_u == 0 or norm_v == 0:
                return 0.0
            return dot / (norm_u * norm_v)
            
        # Calculate distance between consecutive sentences
        distances = []
        for i in range(len(sentences) - 1):
            dist = 1.0 - cosine_similarity(embeddings[i], embeddings[i+1])
            distances.append(dist)
            
        if not distances:
            return [content]
            
        # Set split threshold at mean + 0.8 * std
        mean_dist = sum(distances) / len(distances)
        variance = sum((x - mean_dist) ** 2 for x in distances) / len(distances)
        std_dist = math.sqrt(variance)
        threshold = mean_dist + 0.8 * std_dist
        
        # Group sentences into parent chunks
        parent_chunks = []
        current_chunk = [sentences[0]]
        
        for i in range(len(distances)):
            next_sentence = sentences[i+1]
            if distances[i] > threshold:
                parent_chunks.append(" ".join(current_chunk))
                current_chunk = [next_sentence]
            else:
                current_chunk.append(next_sentence)
                
        if current_chunk:
            parent_chunks.append(" ".join(current_chunk))
            
        return parent_chunks

    def generate_child_chunks(self, parent_text):
        """Generates sliding window child chunks from a parent chunk of text."""
        sentence_ends = re.compile(r'(?<=[.!?])\s+')
        sentences = [s.strip() for s in sentence_ends.split(parent_text) if s.strip()]
        
        if len(sentences) <= 2:
            return [parent_text]
            
        child_chunks = []
        window_size = 2
        step = 1
        for i in range(0, len(sentences) - window_size + 1, step):
            window = sentences[i:i + window_size]
            child_chunks.append(" ".join(window))
        return child_chunks

    def chunk_codebase_file(self, content):
        """Splits code files into parent and child chunks based on line groups."""
        lines = content.splitlines()
        if len(lines) <= 20:
            return [{"parent": content, "children": [content]}]
            
        parent_size = 30
        parent_overlap = 5
        child_size = 10
        child_overlap = 3
        
        parent_child_groups = []
        
        i = 0
        while i < len(lines):
            parent_lines = lines[i:i + parent_size]
            parent_text = "\n".join(parent_lines)
            
            children = []
            j = 0
            while j < len(parent_lines):
                child_lines = parent_lines[j:j + child_size]
                child_text = "\n".join(child_lines)
                children.append(child_text)
                j += (child_size - child_overlap)
                if j >= len(parent_lines):
                    break
                    
            parent_child_groups.append({
                "parent": parent_text,
                "children": children
            })
            
            i += (parent_size - parent_overlap)
            if i >= len(lines):
                break
                
        return parent_child_groups

    def load_markdown_docs(self, docs_dir="mock_data/docs"):
        """Recursively scan documentation directories and index markdown files using semantic & parent-child chunking."""
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
                        
                    # Semantic chunking into parents
                    parent_chunks = self.semantic_chunking(content)
                    
                    for p_idx, parent_text in enumerate(parent_chunks):
                        parent_id = f"parent_{rel_path.replace('/', '_').replace('.', '_')}_{p_idx}"
                        child_texts = self.generate_child_chunks(parent_text)
                        
                        for c_idx, child_text in enumerate(child_texts):
                            documents.append(child_text)
                            metadatas.append({
                                "source_file": rel_path,
                                "file_type": "documentation",
                                "parent_id": parent_id,
                                "parent_text": parent_text,
                                "chunk_type": "child"
                            })
                            ids.append(f"child_{rel_path.replace('/', '_').replace('.', '_')}_{p_idx}_{c_idx}")
                            
        if documents:
            collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
        return len(documents)

    def load_codebase(self, codebase_dir="mock_data/codebase"):
        """Recursively scan codebase directories and index SQL and Python files using parent-child chunking."""
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
                        
                    groups = self.chunk_codebase_file(content)
                    
                    for p_idx, group in enumerate(groups):
                        parent_text = group["parent"]
                        parent_id = f"parent_{rel_path.replace('/', '_').replace('.', '_')}_{p_idx}"
                        
                        for c_idx, child_text in enumerate(group["children"]):
                            documents.append(child_text)
                            metadatas.append({
                                "source_file": rel_path,
                                "file_type": "code_" + file.split(".")[-1],
                                "parent_id": parent_id,
                                "parent_text": parent_text,
                                "chunk_type": "child"
                            })
                            ids.append(f"child_{rel_path.replace('/', '_').replace('.', '_')}_{p_idx}_{c_idx}")
                            
        if documents:
            collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
        return len(documents)

    def query(self, query_text, n_results=3):
        """Query the vector store using a hybrid search (BM25 + Vector) and Reciprocal Rank Fusion (RRF),
        returning parent chunks (from parent-child relationships) to context ground the response."""
        collection = self.get_or_create_collection()
        
        # 1. Vector Search
        try:
            vector_results = collection.query(
                query_texts=[query_text],
                n_results=20
            )
        except Exception as e:
            print(f"Warning: ChromaDB vector search failed: {e}")
            vector_results = None
            
        # 2. Retrieve all documents from collection for BM25 search
        try:
            all_chunks = collection.get()
        except Exception as e:
            print(f"Warning: Failed to fetch collection documents for BM25: {e}")
            all_chunks = None
            
        # Map to track doc results and RRF scoring
        doc_map = {}
        
        # Rankings lists
        vector_ranking = []
        bm25_ranking = []
        
        # Process vector search results ranking
        if vector_results and vector_results["ids"] and vector_results["ids"][0]:
            ids = vector_results["ids"][0]
            docs = vector_results["documents"][0]
            metas = vector_results["metadatas"][0]
            
            for rank, (doc_id, doc_text, meta) in enumerate(zip(ids, docs, metas)):
                vector_ranking.append(doc_id)
                doc_map[doc_id] = (doc_text, meta)
                
        # Process BM25 ranking
        if all_chunks and all_chunks["ids"]:
            all_ids = all_chunks["ids"]
            all_docs = all_chunks["documents"]
            all_metas = all_chunks["metadatas"]
            
            # Initialize BM25 search
            bm25 = BM25(all_docs)
            query_tokens = [w.strip(".,!?\"'()[]{}") for w in query_text.lower().split() if w.strip()]
            
            scores = []
            for doc_idx, doc_id in enumerate(all_ids):
                score = bm25.get_score(doc_idx, query_tokens)
                scores.append((doc_id, score, all_docs[doc_idx], all_metas[doc_idx]))
                
            # Sort by score descending
            scores.sort(key=lambda x: x[1], reverse=True)
            
            # Take top 20 for ranking
            for doc_id, score, doc_text, meta in scores[:20]:
                if score > 0.0:
                    bm25_ranking.append(doc_id)
                    doc_map[doc_id] = (doc_text, meta)
                    
        # 3. Reciprocal Rank Fusion (RRF)
        rrf_scores = {}
        k = 60
        
        for doc_id in doc_map.keys():
            v_rank = vector_ranking.index(doc_id) if doc_id in vector_ranking else None
            b_rank = bm25_ranking.index(doc_id) if doc_id in bm25_ranking else None
            
            v_score = 1.0 / (k + v_rank) if v_rank is not None else 0.0
            b_score = 1.0 / (k + b_rank) if b_rank is not None else 0.0
            
            rrf_scores[doc_id] = v_score + b_score
            
        # Sort docs by RRF score descending
        sorted_docs = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
        
        # 4. Formulate output results (retrieving parents)
        formatted_results = []
        retrieved_parent_ids = set()
        
        for doc_id, score in sorted_docs:
            if len(formatted_results) >= n_results:
                break
                
            doc_text, meta = doc_map[doc_id]
            parent_id = meta.get("parent_id") if isinstance(meta, dict) else None
            parent_text = meta.get("parent_text") if isinstance(meta, dict) else None
            
            if parent_text:
                if parent_id in retrieved_parent_ids:
                    continue
                retrieved_parent_ids.add(parent_id)
                content = parent_text
            else:
                content = doc_text
                
            formatted_results.append({
                "content": content,
                "metadata": meta,
                "score": score
            })
            
        return formatted_results

