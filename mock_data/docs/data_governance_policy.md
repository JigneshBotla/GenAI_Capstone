# data_governance_policy.md

Compliance and data governance represent core components of the Deco platform. This document defines our Data Quality (DQ) pipeline rules, email anonymization logic, phone number masking structures, PII metadata audits, and quarantine architectures.

## 1. Data Quality (DQ) Pipeline Rules

To guarantee conformed values enter downstream analytical datasets, specific validation actions are triggered during SQL-based ETL runs in Snowflake. The table below documents the exact rule constraints and violation routing:

| Source Table | Target Table | Validating Rule Constraint | Action Taken on Violation / Logic |
|---|---|---|---|
| `bronze.raw_users` | `staging.stg_users` | `id IS NOT NULL` | Conformed clean record is loaded. |
| `bronze.raw_users` | `staging.stg_users_quarantine` | `id IS NULL` | Record is quarantined with a reason tag of `'Critical: Null user_id (id)'` and a quarantined timestamp. |
| `bronze.raw_users` | `staging.stg_users` | `email` or `country_code` is NULL | Warning is logged. Email is hashed (if null, remains null), and country code is standard-mapped (defaults to `'XX'` during phone masking and `'NONE'` for empty fields). |
| `bronze.raw_transactions` | `staging.stg_transactions` | `amount_usd >= 0` | Standard load. Negative amount values are auto-corrected using absolute value: `ABS(amount_usd)`. |
| `bronze.raw_transactions` | `staging.stg_transactions_quarantine` | `transaction_id IS NULL` | Record is quarantined with reason tag `'Critical: Null transaction_id (transaction_id)'`. |
| `staging.stg_transactions` | `marts.fct_user_transactions` | `transaction_status = 'completed'` | Filters out failing payments and test transactions. Only completed payments are aggregated per user. |
| `staging.stg_users` & `marts.fct_user_transactions` | `marts.fct_user_churn` | `DATEDIFF('day', last_active_timestamp, CURRENT_DATE()) <= 30` | If the last active date is more than 30 days ago (or is null), `churn_status` is marked as `'CHURNED'`. Otherwise, it is flagged as `'ACTIVE'`. |

## 2. Email Anonymization (Hashing)

Personally Identifiable Information (PII) must be obscured to comply with CCPA and GDPR constraints before analysts can query the tables.
* **Exact Hashing Formula**: 
  ```sql
  SHA2(LOWER(email) || 'SUPER_SECRET_SALT_123', 256)
  ```
  *(Note: In the active python code base implementation, this static salt value is loaded dynamically from the `EMAIL_HASH_SALT` environment variable to prevent secret leakage).*
* **Why Salting is Used**: Plain SHA-256 hashes are vulnerable to rainbow table dictionary attacks where common email addresses can be brute-forced. Appending a secret "salt" (or Pepper) prior to hashing ensures that the resulting string cannot be mapped back to its original plain text without possession of the system salt.
* **Where This Happens**: This masking is applied inside the staging layer when transforming data from `bronze.raw_users` into `staging.stg_users`. The output is loaded into the `hashed_email` column.

## 3. Phone Number Masking

Phone numbers are masked to balance analytical utility with consumer privacy.
* **Exact Masking Formula**:
  ```sql
  CONCAT('+', COALESCE(country_code, 'XX'), '-XXX-XXX-', RIGHT(phone_number, 4))
  ```
* **Why This Logic is Applied**: It preserves the regional context (the country code) and the final 4 digits of the phone number. Preserving this metadata lets customer service trace payment receipts and geo-segment analytics without exposing the full phone digits.
* **Where This Happens**: This transformation is executed during staging ETL loads from `bronze.raw_users` to the `masked_phone` column in `staging.stg_users`.

## 4. PII Metadata Tagging and Catalog Surfacing

Governance audits require tracking where PII is stored. 
* **Metadata Table Registry**: The Snowflake metadata table `public.columns` maintains a registry of column classifications. For every column, it tracks:
  - `is_pii` (BOOLEAN): Flagged as `TRUE` if the field contains raw or masked PII.
  - `pii_type` (VARCHAR): Classified as `NAME`, `EMAIL`, `PHONE`, or `NONE`.
  - `masking_policy` (VARCHAR): Details the anonymization status (e.g. `'SHA-256 with Pepper Salt'`, `'Masked: keeping code + last 4 digits'`, or `'Raw (Restricted Access)'`).
* **UI Catalog Surface**: The Streamlit interface queries this table dynamically to display schema tables. When users click on a table in the Catalog tab, columns tagged as PII are highlighted with alert indicators alongside their corresponding masking policies.

## 5. Purpose of Quarantine Tables

Rather than dropping raw rows that fail validation checks (e.g., null identifiers), the platform routes them to dedicated quarantine tables: `staging.stg_users_quarantine` and `staging.stg_transactions_quarantine`.
* **Lineage & Audit trails**: Retaining failed rows ensures that the raw data volume in Bronze matches the combined volume of staging clean rows and quarantined rows.
* **Debugging**: Data engineers can query quarantine tables to identify upstream extraction failures (e.g., API parser bugs generating null IDs).
* **Reprocessing**: Once upstream ingestion pipelines are corrected, quarantined rows can be reprocessed without rebuilding the entire historical Bronze catalog.
