-- stg_users.sql
-- Cleans and hashes raw user profile information from bronze.raw_users.
-- Implements SHA-256 hashing for compliance with ADR PII standards.

WITH raw_source AS (
    SELECT
        id AS user_id,
        first_name,
        last_name,
        email,
        phone_number,
        country_code,
        created_at,
        updated_at
    FROM {{ source('bronze', 'raw_users') }}
)

SELECT
    user_id,
    -- Hashing PII for GDPR compliance (ADR-002)
    SHA2(LOWER(email) || 'f8c3d9b1e5a26748c9d0e1f2b3a4c5d6') AS hashed_email,
    -- Masking phone number (keeps country code and last 4 digits)
    CONCAT('+', country_code, '-XXX-XXX-', RIGHT(phone_number, 4)) AS masked_phone,
    UPPER(country_code) AS country,
    CAST(created_at AS TIMESTAMP) AS created_timestamp,
    CAST(updated_at AS TIMESTAMP) AS updated_timestamp
FROM raw_source
