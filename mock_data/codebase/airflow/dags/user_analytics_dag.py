# user_analytics_dag.py
# Ingests user profiles and event data, then runs dbt transformation pipeline.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['de-alerts@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def extract_users_from_rds():
    # Extracts fresh accounts from transaction database RDS
    # Custom ETL code logic
    print("Starting ingestion: Connecting to db.production.local ...")
    # Simulate DB fetching
    print("Ingested 14,812 rows. Writing to S3 bronze bucket...")

def extract_transactions_from_stripe():
    # Ingests transaction events from Stripe external API
    print("Querying Stripe endpoints...")
    print("Ingested 4,921 Stripe rows. Copying into S3 bronze...")

with DAG(
    'user_analytics_pipeline',
    default_args=default_args,
    description='Nightly ingestion and dbt marts rebuild pipeline for Customer Churn models',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['churn', 'users', 'stripe']
) as dag:

    ingest_users = PythonOperator(
        task_id='extract_users_from_rds',
        python_callable=extract_users_from_rds
    )

    ingest_transactions = PythonOperator(
        task_id='extract_transactions_from_stripe',
        python_callable=extract_transactions_from_stripe
    )

    snowflake_load_raw = SnowflakeOperator(
        task_id='load_s3_to_snowflake_bronze',
        snowflake_conn_id='snowflake_prod',
        sql="COPY INTO bronze.raw_users FROM @s3_bronze_stage/users/;"
    )

    dbt_run = BashOperator(
        task_id='dbt_run_transformations',
        bash_command='dbt run --profiles-dir . --project-dir ./dbt'
    )

    dbt_test = BashOperator(
        task_id='dbt_test_validations',
        bash_command='dbt test --profiles-dir . --project-dir ./dbt'
    )

    # Dependency routing
    [ingest_users, ingest_transactions] >> snowflake_load_raw >> dbt_run >> dbt_test
