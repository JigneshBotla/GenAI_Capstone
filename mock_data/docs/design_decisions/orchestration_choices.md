# Architectural Decision Record (ADR): Airflow for Orchestration & dbt for ELT

* **Status**: Approved
* **Date**: July 22, 2025
* **Author**: Platform Team

## Context
We need a robust, scalable workflow orchestrator capable of managing external API ingestion, heavy DB extraction, and modeling workflows.

## Decisions
1. **Decoupled Orchestration vs. Transformation**:
   * **Apache Airflow** is chosen to orchestrate *ingestion* tasks (extracting from PostgreSQL RDS, pulling third-party API data, dumping to S3, loading to Snowflake Bronze).
   * **dbt** is chosen for all internal Snowflake *transformations* (Silver staging and Gold marts).
2. **Orchestration Pattern**:
   * Airflow triggers the dbt jobs using the `dbt-cloud` CLI / API interface or locally inside ECS tasks, ensuring that dbt models run only after their respective Bronze source ingestion pipelines complete successfully.
   * Airflow monitors execution status, handles alerting to Slack/PagerDuty, and manages job retries.
