# Data Platform Architecture Overview

Welcome to the enterprise Data Platform documentation! This project follows a modern Lakehouse architecture based on the **Medallion design pattern** (Bronze -> Silver -> Gold) hosted on AWS and Snowflake.

## Architectural Layers

```
  [Raw Sources]
        |  (Airflow + custom ingestion python scripts)
        v
+-----------------+
|   Bronze Lake   |  Raw database replication, JSON API outputs, S3 logs.
| (raw_schema)    |  * Append-only ingestion, partitioned by day.
+--------+--------+
         |  (dbt staging transformations)
         v
+-----------------+
|  Silver Lake    |  Cleaned, conformed, hashed PII, schema validated.
| (staging_schema)|  * Filtered records, default conversions, data type standardizations.
+--------+--------+
         |  (dbt marts transformations)
         v
+-----------------+
|   Gold Lake     |  Business-level aggregates, star schema, denormalized dimensions.
|  (marts_schema) |  * Highly optimized for BI (Looker, Tableau) and ML modeling.
+-----------------+
```

## Primary Storage & Transformation Tech Stack
* **Storage & Compute**: Snowflake acts as the primary data warehouse (Lakehouse compute).
* **Ingestion**: Custom Python scripts orchestrated by Apache Airflow load data from PostgreSQL RDS, external APIs, and S3 buckets into Snowflake's `Bronze` schema.
* **Transformations**: dbt (data build tool) is utilized to manage dependencies and build view/table layers across `Silver` (Staging) and `Gold` (Marts).
* **Metadata & Lineage**: Handled via table tags in Snowflake and dbt documentation manifests.
