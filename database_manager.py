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
