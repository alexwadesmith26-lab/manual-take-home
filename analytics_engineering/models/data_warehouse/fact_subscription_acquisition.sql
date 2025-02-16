WITH fact_subscription_acquisition AS (
    SELECT
        CAST(subscription_id AS STRING)               AS subscription_id
        , CAST(customer_id AS STRING)                 AS customer_id
        , CAST(subscription_acquisition_date AS DATE) AS subscription_acquisition_date
        , CAST(length_of_subscription_days AS INT64)  AS length_of_subscription_days
        , CAST(is_acquisition_subscription AS INT64)  AS is_acquisition_subscription
        , CAST(is_bounced_subscription AS INT64)      AS is_bounced_subscription
        , CAST(is_churned_subscription AS INT64)      AS is_churned_subscription
        , CURRENT_TIMESTAMP                           AS inserted_date_time
    FROM {{ ref('int_subscription_acquisition') }}
)

SELECT * FROM fact_subscription_acquisition