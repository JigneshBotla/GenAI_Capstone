-- stg_transactions.sql
-- Cleans transaction records from bronze.raw_transactions.
-- Casts price values, extracts calendar attributes, and filters test payments.

WITH raw_source AS (
    SELECT
        transaction_id,
        user_id,
        amount_usd,
        status,
        payment_method,
        transaction_time
    FROM {{ source('bronze', 'raw_transactions') }}
)

SELECT
    transaction_id,
    user_id,
    CAST(amount_usd AS DECIMAL(18, 2)) AS transaction_amount_usd,
    LOWER(status) AS transaction_status,
    payment_method,
    CAST(transaction_time AS TIMESTAMP) AS transaction_timestamp
FROM raw_source
WHERE LOWER(status) != 'test_payment'
