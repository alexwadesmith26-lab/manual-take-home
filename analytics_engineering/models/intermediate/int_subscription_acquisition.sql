WITH int_subscription_acquisition AS (
    SELECT
        customer_id
        , subscription_id
        , subscription_start_date AS subscription_acquisition_date
        , DATE_DIFF(subscription_end_date, subscription_start_date, day) AS length_of_subscription_days
        , 1 AS is_acquisition_subscription
        , CASE
            WHEN subscription_start_date = subscription_end_date THEN 1
            ELSE 0
        END AS is_bounced_subscription
        , CASE
            WHEN subscription_end_date IS NOT NULL THEN 1
            ELSE 0
        END AS is_churned_subscription
    FROM {{ ref('stg_activity') }}
)

SELECT * FROM int_subscription_acquisition