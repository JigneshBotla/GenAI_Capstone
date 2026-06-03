-- fct_user_churn.sql
-- Defines customer status: active vs. inactive (churned).
-- Joins cleaned user metadata with transaction histories.
-- Must enforce unique constraint on user_id.

WITH users AS (
    SELECT
        user_id,
        hashed_email,
        country
    FROM {{ ref('stg_users') }}
),

activity AS (
    SELECT
        user_id,
        lifetime_transaction_count,
        lifetime_spend_usd,
        last_active_timestamp
    FROM {{ ref('fct_user_transactions') }}
)

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
FROM users u
LEFT JOIN activity a ON u.user_id = a.user_id
