WITH fact_subscription_activity AS (
    SELECT
        CAST(activity_key AS STRING)                    AS activity_key
        , CAST(subscription_id AS STRING)               AS subscription_id
        , CAST(customer_id AS STRING)                   AS customer_id
        , CAST(calendar_date AS DATE)                   AS calendar_date   
        , CAST(subscription_acquisition_cohort AS DATE) AS subscription_acquisition_cohort
        , CAST(months_of_active_subscription AS INT64)  AS months_of_active_subscription
        , CAST(customer_maturity_type AS STRING)        AS customer_maturity_type
        , CAST(is_active_subscription AS INT64)         AS is_active_subscription
        , CAST(is_churned_subscription AS INT64)        AS is_churned_subscription
        , CURRENT_TIMESTAMP                             AS inserted_date_time
    FROM {{ ref('int_subscription_activity') }}
)

SELECT * FROM fact_subscription_activity