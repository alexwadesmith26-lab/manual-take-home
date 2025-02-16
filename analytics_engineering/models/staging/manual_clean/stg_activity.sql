WITH stg_activity AS (
    SELECT
        customer_id
        , subscription_id
        , from_date AS subscription_start_date
        , to_date   AS subscription_end_date
    FROM {{ source('manual_clean', 'activity') }}
)

SELECT * FROM stg_activity