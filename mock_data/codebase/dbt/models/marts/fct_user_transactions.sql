-- fct_user_transactions.sql
-- Aggregates daily transaction statistics per user.
-- Depends on stg_transactions.

WITH transactions AS (
    SELECT
        user_id,
        transaction_amount_usd,
        transaction_timestamp
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_status = 'completed'
)

SELECT
    user_id,
    COUNT(transaction_amount_usd) AS lifetime_transaction_count,
    SUM(transaction_amount_usd) AS lifetime_spend_usd,
    MAX(transaction_timestamp) AS last_active_timestamp
FROM transactions
GROUP BY user_id
