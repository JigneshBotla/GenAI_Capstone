import os
import random
import datetime
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_snowflake_connection():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    database = os.getenv("SNOWFLAKE_DATABASE", "Capstone_DB")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "SIGMA_WH")

    if not account or not user or not password:
        raise ValueError("Missing Snowflake credentials. Please check your .env file.")

    print(f"Connecting to Snowflake account: {account} as user: {user}...")
    
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse
    )
    return conn, database

def generate_mock_data():
    """Generates synthetic data for raw_users and raw_transactions, including bad rows."""
    print("Generating synthetic mock data in Python (including bad rows)...")
    random.seed(42)  # Set seed for reproducibility

    # 1. Generate users
    first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "David", "Emily", "Frank", "Grace", "Henry", 
                   "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter", "Ryan", "Sophia"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", 
                  "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
    countries = ["US", "CA", "GB", "DE", "FR", "IN", "JP"]

    users = []
    user_ids = []
    base_date = datetime.datetime(2025, 1, 1)

    for i in range(1000):
        user_id = f"USR_{1000 + i}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email = f"{first_name.lower()}.{last_name.lower()}_{i}@example.com"
        phone_number = "".join(str(random.randint(0, 9)) for _ in range(10))
        country_code = random.choice(countries)
        
        # Created in the past year
        created_days_offset = random.randint(0, 300)
        created_at = base_date + datetime.timedelta(days=created_days_offset, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        if random.random() < 0.7:
            updated_at = created_at
        else:
            updated_at = created_at + datetime.timedelta(days=random.randint(1, 30))

        # Introduce bad user rows (Data Quality rules)
        # Rule 1: Null user_id (severity: critical, fix: quarantine) -> ~1.5% chance
        if random.random() < 0.015:
            user_id = None

        # Rule 2: Null country_code (severity: medium, continue) -> ~2% chance
        elif random.random() < 0.02:
            country_code = None

        # Rule 3: Null email (severity: high, continue) -> ~2% chance
        elif random.random() < 0.02:
            email = None

        users.append((
            user_id,
            first_name,
            last_name,
            email,
            phone_number,
            country_code,
            created_at.strftime("%Y-%m-%d %H:%M:%S"),
            updated_at.strftime("%Y-%m-%d %H:%M:%S")
        ))
        
        # Only keep valid user_ids for generating transaction data
        if user_id is not None:
            user_ids.append((user_id, created_at))

    # 2. Generate transactions
    transactions = []
    statuses = ["completed", "failed", "test_payment"]
    status_weights = [0.85, 0.10, 0.05]
    payment_methods = ["credit card", "apple pay", "paypal", "bank transfer"]

    for j in range(5000):
        transaction_id = f"TXN_{10000 + j}"
        user_info = random.choice(user_ids)
        user_id = user_info[0]
        user_created_at = user_info[1]
        
        amount_usd = round(random.uniform(5.00, 450.00), 2)
        status = random.choices(statuses, weights=status_weights)[0]
        payment_method = random.choice(payment_methods)
        
        # Transaction must occur after user was created
        tx_days_offset = random.randint(0, 60)
        tx_time = user_created_at + datetime.timedelta(days=tx_days_offset, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        # Introduce bad transaction rows (Data Quality rules)
        # Rule 1: Negative amounts (severity: high, fix: make it positive) -> ~2% chance
        if random.random() < 0.02:
            amount_usd = -amount_usd
        
        # Rule 2: Null transaction_id (severity: critical, fix: quarantine) -> ~1.5% chance
        elif random.random() < 0.015:
            transaction_id = None

        transactions.append((
            transaction_id,
            user_id,
            amount_usd,
            status,
            payment_method,
            tx_time.strftime("%Y-%m-%d %H:%M:%S")
        ))

    return users, transactions

def setup_snowflake():
    conn, default_db = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Create database and drop schemas first to prevent duplication
        print(f"Creating database: {default_db} if not exists...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {default_db}")
        cursor.execute(f"USE DATABASE {default_db}")

        schemas = ["bronze", "staging", "marts", "public"]
        for schema in schemas:
            print(f"Dropping schema: {schema} CASCADE if exists...")
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            print(f"Creating schema: {schema}...")
            cursor.execute(f"CREATE SCHEMA {schema}")

        # ------------------ CREATE METADATA TABLES ------------------
        print("Creating metadata tables in PUBLIC schema...")

        cursor.execute("""
        CREATE OR REPLACE TABLE public.tables (
            table_id VARCHAR PRIMARY KEY,
            schema_name VARCHAR NOT NULL,
            table_name VARCHAR NOT NULL,
            description VARCHAR,
            row_count INTEGER,
            size_bytes INTEGER
        ) COMMENT = 'Metadata table storing registration info for data catalog tables'
        """)

        cursor.execute("""
        CREATE OR REPLACE TABLE public.columns (
            table_id VARCHAR NOT NULL,
            column_name VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            description VARCHAR,
            is_pii BOOLEAN NOT NULL,
            pii_type VARCHAR,
            masking_policy VARCHAR,
            PRIMARY KEY (table_id, column_name)
        ) COMMENT = 'Metadata table storing column-level details and compliance rules'
        """)

        cursor.execute("""
        CREATE OR REPLACE TABLE public.lineage (
            source_table VARCHAR NOT NULL,
            target_table VARCHAR NOT NULL,
            lineage_type VARCHAR NOT NULL,
            PRIMARY KEY (source_table, target_table)
        ) COMMENT = 'Metadata table tracking upstream and downstream data flows'
        """)

        cursor.execute("""
        CREATE OR REPLACE TABLE public.pipeline_runs (
            run_id INTEGER IDENTITY(1001,1) PRIMARY KEY,
            pipeline_name VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            start_time VARCHAR NOT NULL,
            end_time VARCHAR,
            duration_sec INTEGER,
            error_message VARCHAR,
            log_path VARCHAR
        ) COMMENT = 'Operational log table recording pipeline executions'
        """)

        cursor.execute("""
        CREATE OR REPLACE TABLE public.pipeline_slo (
            pipeline_name VARCHAR PRIMARY KEY,
            sla_target_time VARCHAR NOT NULL,
            max_duration_sec INTEGER NOT NULL,
            slo_percentage_target REAL NOT NULL
        ) COMMENT = 'Metadata table defining pipeline SLO targets'
        """)

        # ------------------ SEED STATIC METADATA ------------------
        print("Seeding metadata tables (static catalog values)...")

        # 1. tables description registry (row_count and size_bytes initialized to NULL)
        tables_data = [
            ("bronze.raw_users", "bronze", "raw_users", "Raw ingestion replica of postgres customer registrations database.", None, None),
            ("bronze.raw_transactions", "bronze", "raw_transactions", "Raw transactions dump from Stripe API payments integration.", None, None),
            ("staging.stg_users", "staging", "stg_users", "Cleaned user demographics and profiles. Hashes emails & masks phone.", None, None),
            ("staging.stg_users_quarantine", "staging", "stg_users_quarantine", "Quarantined user records containing critical errors (null user_id).", None, None),
            ("staging.stg_transactions", "staging", "stg_transactions", "Standardized and cleaned transaction records. Excludes test payments.", None, None),
            ("staging.stg_transactions_quarantine", "staging", "stg_transactions_quarantine", "Quarantined transaction records containing critical errors (null transaction_id).", None, None),
            ("marts.fct_user_transactions", "marts", "fct_user_transactions", "Fact table containing rolling aggregate metrics on user transaction activities.", None, None),
            ("marts.fct_user_churn", "marts", "fct_user_churn", "Analyzes user activity to flag inactive accounts exceeding 30 days as churned. Primary reporting table.", None, None)
        ]
        cursor.executemany("INSERT INTO public.tables (table_id, schema_name, table_name, description, row_count, size_bytes) VALUES (%s, %s, %s, %s, %s, %s)", tables_data)

        # 2. columns
        columns_data = [
            # bronze.raw_users
            ("bronze.raw_users", "id", "TEXT", "Primary identifier for users.", False, "NONE", None),
            ("bronze.raw_users", "first_name", "TEXT", "First name of customer.", True, "NAME", "Raw (Restricted Access)"),
            ("bronze.raw_users", "last_name", "TEXT", "Last name of customer.", True, "NAME", "Raw (Restricted Access)"),
            ("bronze.raw_users", "email", "TEXT", "Primary email address of user.", True, "EMAIL", "Raw (Restricted Access)"),
            ("bronze.raw_users", "phone_number", "TEXT", "User phone number.", True, "PHONE", "Raw (Restricted Access)"),
            ("bronze.raw_users", "country_code", "TEXT", "ISO country code.", False, "NONE", None),
            ("bronze.raw_users", "created_at", "TIMESTAMP", "Creation timestamp in postgres.", False, "NONE", None),
            ("bronze.raw_users", "updated_at", "TIMESTAMP", "Last update timestamp.", False, "NONE", None),
            
            # bronze.raw_transactions
            ("bronze.raw_transactions", "transaction_id", "TEXT", "Stripe payment identifier.", False, "NONE", None),
            ("bronze.raw_transactions", "user_id", "TEXT", "Foreign key to user account.", False, "NONE", None),
            ("bronze.raw_transactions", "amount_usd", "NUMERIC", "Transaction billing value in USD.", False, "NONE", None),
            ("bronze.raw_transactions", "status", "TEXT", "Payment processor response.", False, "NONE", None),
            ("bronze.raw_transactions", "payment_method", "TEXT", "Method used: credit card, apple pay, etc.", False, "NONE", None),
            ("bronze.raw_transactions", "transaction_time", "TIMESTAMP", "Time of charge.", False, "NONE", None),

            # staging.stg_users
            ("staging.stg_users", "user_id", "TEXT", "Conformed primary customer index.", False, "NONE", None),
            ("staging.stg_users", "hashed_email", "TEXT", "Hashed secure email identifier for downstream matches.", True, "EMAIL", "SHA-256 with Pepper Salt"),
            ("staging.stg_users", "masked_phone", "TEXT", "Obfuscated customer contact.", True, "PHONE", "Masked: keeping code + last 4 digits"),
            ("staging.stg_users", "country", "TEXT", "Standardized uppercase country string.", False, "NONE", None),
            ("staging.stg_users", "created_timestamp", "TIMESTAMP", "Standardized user creation timestamp.", False, "NONE", None),
            ("staging.stg_users", "updated_timestamp", "TIMESTAMP", "Standardized profile update timestamp.", False, "NONE", None),

            # staging.stg_users_quarantine
            ("staging.stg_users_quarantine", "id", "TEXT", "Raw primary customer index (failing validation).", False, "NONE", None),
            ("staging.stg_users_quarantine", "first_name", "TEXT", "First name of customer.", True, "NAME", "Raw (Restricted Access)"),
            ("staging.stg_users_quarantine", "last_name", "TEXT", "Last name of customer.", True, "NAME", "Raw (Restricted Access)"),
            ("staging.stg_users_quarantine", "email", "TEXT", "Primary email address of user.", True, "EMAIL", "Raw (Restricted Access)"),
            ("staging.stg_users_quarantine", "phone_number", "TEXT", "User phone number.", True, "PHONE", "Raw (Restricted Access)"),
            ("staging.stg_users_quarantine", "country_code", "TEXT", "ISO country code.", False, "NONE", None),
            ("staging.stg_users_quarantine", "created_at", "TIMESTAMP", "Creation timestamp in postgres.", False, "NONE", None),
            ("staging.stg_users_quarantine", "updated_at", "TIMESTAMP", "Last update timestamp.", False, "NONE", None),
            ("staging.stg_users_quarantine", "quarantine_reason", "TEXT", "Reason for record quarantine.", False, "NONE", None),
            ("staging.stg_users_quarantine", "quarantined_at", "TIMESTAMP", "Timestamp when record was quarantined.", False, "NONE", None),

            # staging.stg_transactions
            ("staging.stg_transactions", "transaction_id", "TEXT", "Standardized primary transaction ID.", False, "NONE", None),
            ("staging.stg_transactions", "user_id", "TEXT", "Cleaned foreign key reference to user.", False, "NONE", None),
            ("staging.stg_transactions", "transaction_amount_usd", "DECIMAL(18,2)", "Standardized amount in USD decimal format.", False, "NONE", None),
            ("staging.stg_transactions", "transaction_status", "TEXT", "Lowercase transaction response code.", False, "NONE", None),
            ("staging.stg_transactions", "payment_method", "TEXT", "Consolidated billing method.", False, "NONE", None),
            ("staging.stg_transactions", "transaction_timestamp", "TIMESTAMP", "Standardized charge timestamp.", False, "NONE", None),

            # staging.stg_transactions_quarantine
            ("staging.stg_transactions_quarantine", "transaction_id", "TEXT", "Stripe payment identifier (failing validation).", False, "NONE", None),
            ("staging.stg_transactions_quarantine", "user_id", "TEXT", "Foreign key to user account.", False, "NONE", None),
            ("staging.stg_transactions_quarantine", "amount_usd", "NUMERIC", "Transaction billing value in USD.", False, "NONE", None),
            ("staging.stg_transactions_quarantine", "status", "TEXT", "Payment processor response.", False, "NONE", None),
            ("staging.stg_transactions_quarantine", "payment_method", "TEXT", "Method used: credit card, apple pay, etc.", False, "NONE", None),
            ("staging.stg_transactions_quarantine", "transaction_time", "TIMESTAMP", "Time of charge.", False, "NONE", None),
            ("staging.stg_transactions_quarantine", "quarantine_reason", "TEXT", "Reason for record quarantine.", False, "NONE", None),
            ("staging.stg_transactions_quarantine", "quarantined_at", "TIMESTAMP", "Timestamp when record was quarantined.", False, "NONE", None),

            # marts.fct_user_transactions
            ("marts.fct_user_transactions", "user_id", "TEXT", "Primary index key (one per user).", False, "NONE", None),
            ("marts.fct_user_transactions", "lifetime_transaction_count", "INTEGER", "Aggregated completed purchases count.", False, "NONE", None),
            ("marts.fct_user_transactions", "lifetime_spend_usd", "DECIMAL(18,2)", "Aggregated gross user spend in USD.", False, "NONE", None),
            ("marts.fct_user_transactions", "last_active_timestamp", "TIMESTAMP", "Timestamp of the user's latest completed charge.", False, "NONE", None),

            # marts.fct_user_churn
            ("marts.fct_user_churn", "user_id", "TEXT", "Primary key for customer analysis.", False, "NONE", None),
            ("marts.fct_user_churn", "hashed_email", "TEXT", "Anonymized user email for marketing segment hooks.", True, "EMAIL", "Inherited SHA-256 Mask"),
            ("marts.fct_user_churn", "country", "TEXT", "User registration location.", False, "NONE", None),
            ("marts.fct_user_churn", "tx_count", "INTEGER", "Total transaction count.", False, "NONE", None),
            ("marts.fct_user_churn", "spend_amount", "DECIMAL(18,2)", "Total aggregated lifetime spending.", False, "NONE", None),
            ("marts.fct_user_churn", "last_active_timestamp", "TIMESTAMP", "Most recent activity recorded.", False, "NONE", None),
            ("marts.fct_user_churn", "churn_status", "TEXT", "Flag: 'ACTIVE' (activity <= 30d ago) or 'CHURNED' (activity > 30d ago).", False, "NONE", None)
        ]
        cursor.executemany("INSERT INTO public.columns (table_id, column_name, data_type, description, is_pii, pii_type, masking_policy) VALUES (%s, %s, %s, %s, %s, %s, %s)", columns_data)

        # 3. lineage
        lineage_data = [
            ("bronze.raw_users", "staging.stg_users", "Direct"),
            ("bronze.raw_users", "staging.stg_users_quarantine", "Quarantine"),
            ("bronze.raw_transactions", "staging.stg_transactions", "Direct"),
            ("bronze.raw_transactions", "staging.stg_transactions_quarantine", "Quarantine"),
            ("staging.stg_transactions", "marts.fct_user_transactions", "Aggregated"),
            ("staging.stg_users", "marts.fct_user_churn", "Direct"),
            ("marts.fct_user_transactions", "marts.fct_user_churn", "Direct")
        ]
        cursor.executemany("INSERT INTO public.lineage (source_table, target_table, lineage_type) VALUES (%s, %s, %s)", lineage_data)

        # 4. pipeline_slo
        slo_data = [
            ("user_analytics_pipeline", "08:00:00", 3600, 99.0)
        ]
        cursor.executemany("INSERT INTO public.pipeline_slo (pipeline_name, sla_target_time, max_duration_sec, slo_percentage_target) VALUES (%s, %s, %s, %s)", slo_data)

        # 5. pipeline_runs
        runs_data = [
            (1001, "user_analytics_pipeline", "SUCCESS", "2026-05-29T02:00:00Z", "2026-05-29T02:00:30Z", 30, None, "mock_data/logs/run_1001_success.log"),
            (1002, "user_analytics_pipeline", "FAILED", "2026-05-30T04:15:00Z", "2026-05-30T04:15:14Z", 14, "Marts unique key violation inside marts.fct_user_churn", "mock_data/logs/run_1002_failed.log"),
            (1003, "user_analytics_pipeline", "FAILED", "2026-05-31T02:00:00Z", "2026-05-31T02:01:02Z", 62, "Database extraction timed out after 3 retries.", "mock_data/logs/run_1003_failed.log")
        ]
        cursor.executemany("INSERT INTO public.pipeline_runs (run_id, pipeline_name, status, start_time, end_time, duration_sec, error_message, log_path) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", runs_data)


        # ------------------ CREATE ACTUAL DATA TABLES ------------------
        print("Creating Capstone Medallion Data Tables...")

        # 1. bronze.raw_users
        cursor.execute("""
        CREATE OR REPLACE TABLE bronze.raw_users (
            id TEXT COMMENT 'Primary identifier for users.',
            first_name TEXT COMMENT 'First name of customer.',
            last_name TEXT COMMENT 'Last name of customer.',
            email TEXT COMMENT 'Primary email address of user.',
            phone_number TEXT COMMENT 'User phone number.',
            country_code TEXT COMMENT 'ISO country code.',
            created_at TIMESTAMP COMMENT 'Creation timestamp in postgres.',
            updated_at TIMESTAMP COMMENT 'Last update timestamp.'
        ) COMMENT = 'Raw ingestion replica of postgres customer registrations database.'
        """)

        # 2. bronze.raw_transactions
        cursor.execute("""
        CREATE OR REPLACE TABLE bronze.raw_transactions (
            transaction_id TEXT COMMENT 'Stripe payment identifier.',
            user_id TEXT COMMENT 'Foreign key to user account.',
            amount_usd NUMERIC(18, 2) COMMENT 'Transaction billing value in USD.',
            status TEXT COMMENT 'Payment processor response.',
            payment_method TEXT COMMENT 'Method used: credit card, apple pay, etc.',
            transaction_time TIMESTAMP COMMENT 'Time of charge.'
        ) COMMENT = 'Raw transactions dump from Stripe API payments integration.'
        """)

        # 3. staging.stg_users
        cursor.execute("""
        CREATE OR REPLACE TABLE staging.stg_users (
            user_id TEXT COMMENT 'Conformed primary customer index.',
            hashed_email TEXT COMMENT 'Hashed secure email identifier for downstream matches.',
            masked_phone TEXT COMMENT 'Obfuscated customer contact.',
            country TEXT COMMENT 'Standardized uppercase country string.',
            created_timestamp TIMESTAMP COMMENT 'Standardized user creation timestamp.',
            updated_timestamp TIMESTAMP COMMENT 'Standardized profile update timestamp.'
        ) COMMENT = 'Cleaned user demographics and profiles. Hashes primary email fields.'
        """)

        # 4. staging.stg_users_quarantine [NEW]
        cursor.execute("""
        CREATE OR REPLACE TABLE staging.stg_users_quarantine (
            id TEXT COMMENT 'Raw primary customer index (failing validation).',
            first_name TEXT COMMENT 'First name of customer.',
            last_name TEXT COMMENT 'Last name of customer.',
            email TEXT COMMENT 'Primary email address of user.',
            phone_number TEXT COMMENT 'User phone number.',
            country_code TEXT COMMENT 'ISO country code.',
            created_at TIMESTAMP COMMENT 'Creation timestamp in postgres.',
            updated_at TIMESTAMP COMMENT 'Last update timestamp.',
            quarantine_reason TEXT COMMENT 'Reason for record quarantine.',
            quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when record was quarantined.'
        ) COMMENT = 'Quarantined user records containing critical errors (null user_id).'
        """)

        # 5. staging.stg_transactions
        cursor.execute("""
        CREATE OR REPLACE TABLE staging.stg_transactions (
            transaction_id TEXT COMMENT 'Standardized primary transaction ID.',
            user_id TEXT COMMENT 'Cleaned foreign key reference to user.',
            transaction_amount_usd DECIMAL(18,2) COMMENT 'Standardized amount in USD decimal format.',
            transaction_status TEXT COMMENT 'Lowercase transaction response code.',
            payment_method TEXT COMMENT 'Consolidated billing method.',
            transaction_timestamp TIMESTAMP COMMENT 'Standardized charge timestamp.'
        ) COMMENT = 'Standardized currency conversions and transaction attributes. Ignores payments marked as test.'
        """)

        # 6. staging.stg_transactions_quarantine [NEW]
        cursor.execute("""
        CREATE OR REPLACE TABLE staging.stg_transactions_quarantine (
            transaction_id TEXT COMMENT 'Stripe payment identifier (failing validation).',
            user_id TEXT COMMENT 'Foreign key to user account.',
            amount_usd NUMERIC(18,2) COMMENT 'Transaction billing value in USD.',
            status TEXT COMMENT 'Payment processor response.',
            payment_method TEXT COMMENT 'Method used: credit card, apple pay, etc.',
            transaction_time TIMESTAMP COMMENT 'Time of charge.',
            quarantine_reason TEXT COMMENT 'Reason for record quarantine.',
            quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when record was quarantined.'
        ) COMMENT = 'Quarantined transaction records containing critical errors (null transaction_id).'
        """)

        # 7. marts.fct_user_transactions
        cursor.execute("""
        CREATE OR REPLACE TABLE marts.fct_user_transactions (
            user_id TEXT COMMENT 'Primary index key (one per user).',
            lifetime_transaction_count INTEGER COMMENT 'Aggregated completed purchases count.',
            lifetime_spend_usd DECIMAL(18,2) COMMENT 'Aggregated gross user spend in USD.',
            last_active_timestamp TIMESTAMP COMMENT 'Timestamp of the user''s latest completed charge.'
        ) COMMENT = 'Fact table containing rolling aggregate metrics on user transaction activities.'
        """)

        # 8. marts.fct_user_churn
        cursor.execute("""
        CREATE OR REPLACE TABLE marts.fct_user_churn (
            user_id TEXT COMMENT 'Primary key for customer analysis.',
            hashed_email TEXT COMMENT 'Anonymized user email for marketing segment hooks.',
            country TEXT COMMENT 'User registration location.',
            tx_count INTEGER COMMENT 'Total transaction count.',
            spend_amount DECIMAL(18,2) COMMENT 'Total aggregated lifetime spending.',
            last_active_timestamp TIMESTAMP COMMENT 'Most recent activity recorded.',
            churn_status TEXT COMMENT 'Flag: ''ACTIVE'' (activity <= 30d ago) or ''CHURNED'' (activity > 30d ago).'
        ) COMMENT = 'Analyzes user activity to flag inactive accounts exceeding 30 days as churned. Primary reporting table.'
        """)

        # ------------------ GENERATE AND SEED BRONZE DATA ------------------
        users, transactions = generate_mock_data()
        
        print("Inserting generated user records into bronze.raw_users...")
        cursor.executemany("""
            INSERT INTO bronze.raw_users (id, first_name, last_name, email, phone_number, country_code, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, users)

        print("Inserting generated transaction records into bronze.raw_transactions...")
        cursor.executemany("""
            INSERT INTO bronze.raw_transactions (transaction_id, user_id, amount_usd, status, payment_method, transaction_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, transactions)

        # ------------------ RUN SQL-BASED ETL PIPELINES ------------------
        # Clean Users ETL (Route where id is not null)
        print("Running SQL ETL Pipeline: Populating staging.stg_users...")
        cursor.execute("""
        INSERT INTO staging.stg_users (user_id, hashed_email, masked_phone, country, created_timestamp, updated_timestamp)
        SELECT
            id AS user_id,
            CASE 
                WHEN email IS NULL THEN NULL 
                ELSE SHA2(LOWER(email) || 'SUPER_SECRET_SALT_123', 256) 
            END AS hashed_email,
            CASE 
                WHEN phone_number IS NULL THEN NULL 
                ELSE CONCAT('+', COALESCE(country_code, 'XX'), '-XXX-XXX-', RIGHT(phone_number, 4)) 
            END AS masked_phone,
            UPPER(country_code) AS country,
            CAST(created_at AS TIMESTAMP) AS created_timestamp,
            CAST(updated_at AS TIMESTAMP) AS updated_timestamp
        FROM bronze.raw_users
        WHERE id IS NOT NULL
        """)

        # Quarantine Users ETL (Route where id is null)
        print("Running SQL ETL Pipeline: Populating staging.stg_users_quarantine...")
        cursor.execute("""
        INSERT INTO staging.stg_users_quarantine (id, first_name, last_name, email, phone_number, country_code, created_at, updated_at, quarantine_reason)
        SELECT
            id,
            first_name,
            last_name,
            email,
            phone_number,
            country_code,
            CAST(created_at AS TIMESTAMP) AS created_at,
            CAST(updated_at AS TIMESTAMP) AS updated_at,
            'Critical: Null user_id (id)' AS quarantine_reason
        FROM bronze.raw_users
        WHERE id IS NULL
        """)

        # Clean Transactions ETL (Route where transaction_id is not null; ABS(amount_usd) for high severity negative check)
        print("Running SQL ETL Pipeline: Populating staging.stg_transactions...")
        cursor.execute("""
        INSERT INTO staging.stg_transactions (transaction_id, user_id, transaction_amount_usd, transaction_status, payment_method, transaction_timestamp)
        SELECT
            transaction_id,
            user_id,
            ABS(CAST(amount_usd AS DECIMAL(18, 2))) AS transaction_amount_usd,
            LOWER(status) AS transaction_status,
            payment_method,
            CAST(transaction_time AS TIMESTAMP) AS transaction_timestamp
        FROM bronze.raw_transactions
        WHERE transaction_id IS NOT NULL
        """)

        # Quarantine Transactions ETL (Route where transaction_id is null)
        print("Running SQL ETL Pipeline: Populating staging.stg_transactions_quarantine...")
        cursor.execute("""
        INSERT INTO staging.stg_transactions_quarantine (transaction_id, user_id, amount_usd, status, payment_method, transaction_time, quarantine_reason)
        SELECT
            transaction_id,
            user_id,
            amount_usd,
            status,
            payment_method,
            CAST(transaction_time AS TIMESTAMP) AS transaction_time,
            'Critical: Null transaction_id (transaction_id)' AS quarantine_reason
        FROM bronze.raw_transactions
        WHERE transaction_id IS NULL
        """)

        print("Running SQL ETL Pipeline: Populating marts.fct_user_transactions...")
        cursor.execute("""
        INSERT INTO marts.fct_user_transactions (user_id, lifetime_transaction_count, lifetime_spend_usd, last_active_timestamp)
        SELECT
            user_id,
            COUNT(transaction_amount_usd) AS lifetime_transaction_count,
            SUM(transaction_amount_usd) AS lifetime_spend_usd,
            MAX(transaction_timestamp) AS last_active_timestamp
        FROM staging.stg_transactions
        WHERE transaction_status = 'completed'
        GROUP BY user_id
        """)

        print("Running SQL ETL Pipeline: Populating marts.fct_user_churn...")
        cursor.execute("""
        INSERT INTO marts.fct_user_churn (user_id, hashed_email, country, tx_count, spend_amount, last_active_timestamp, churn_status)
        SELECT
            u.user_id,
            u.hashed_email,
            u.country,
            COALESCE(a.lifetime_transaction_count, 0) AS tx_count,
            COALESCE(a.lifetime_spend_usd, 0.0) AS spend_amount,
            a.last_active_timestamp,
            CASE 
                -- Flag as churned if no transaction in past 30 days
                WHEN a.last_active_timestamp IS NULL THEN 'CHURNED'
                WHEN DATEDIFF('day', a.last_active_timestamp, CURRENT_DATE()) > 30 THEN 'CHURNED'
                ELSE 'ACTIVE'
            END AS churn_status
        FROM staging.stg_users u
        LEFT JOIN marts.fct_user_transactions a ON u.user_id = a.user_id
        """)

        # ------------------ CALCULATE DYNAMIC METRICS FOR CATALOG ------------------
        print("Calculating dynamic table statistics (row_count and size_bytes) from Snowflake metadata...")
        
        cursor.execute(f"""
            SELECT 
                LOWER(TABLE_SCHEMA) || '.' || LOWER(TABLE_NAME) AS table_id,
                ROW_COUNT,
                BYTES
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = '{default_db.upper()}' 
              AND TABLE_SCHEMA IN ('BRONZE', 'STAGING', 'MARTS')
        """)
        
        metrics = cursor.fetchall()
        for table_id, row_count, bytes_val in metrics:
            print(f"  Table `{table_id}`: row_count={row_count}, bytes={bytes_val}")
            cursor.execute("""
                UPDATE public.tables 
                SET row_count = %s, size_bytes = %s
                WHERE table_id = %s
            """, (row_count, bytes_val, table_id))

        print("Tables created, data inserted, ETL executed, and dynamic metrics seeded successfully in Snowflake!")

    except Exception as e:
        print(f"Error occurred during Snowflake setup: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_snowflake()
