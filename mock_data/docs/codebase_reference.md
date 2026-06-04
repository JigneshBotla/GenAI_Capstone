# codebase_reference.md

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
    CONCAT('+', COALESCE(country_code, 'XX'), '-XXX-XXX-', RIGHT(phone_number, 4))
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

