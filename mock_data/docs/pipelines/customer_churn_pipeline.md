# Customer Analytics & Chern Pipeline Documentation

## Overview
The Customer Analytics and Churn Pipeline is a vital data model that compiles daily activity patterns, transaction frequencies, and demographic metadata to calculate customer retention metrics and predict churn probability.

## Pipeline Flow
1. **Ingestion (Airflow)**: Extract daily postgres updates for customer profiles and transactional events. Load them into Snowflake `raw_users` and `raw_transactions`.
2. **Staging & Cleaning (dbt)**:
   * `stg_users`: Cleans name strings, enforces uniform timestamps, hashes raw emails.
   * `stg_transactions`: Verifies payment processor keys, casts floats, filters failed tests.
3. **Marts Aggregations (dbt)**:
   * `fct_user_transactions`: Aggregates transactions by user and day.
   * `fct_user_churn`: Joins transaction activity with user profiles to flags users as "inactive" or "active" based on a rolling 30-day window.

## SLO & Data SLA Targets
* **SLA Time**: Data must be completely refreshed and validated in `marts.fct_user_churn` daily by **08:00 UTC**.
* **Failure Actions**: If the Airflow orchestrator fails or dbt validations fail, a Slack alert is triggered, and downstream BI reports are flagged with a "Stale Data" warning.
