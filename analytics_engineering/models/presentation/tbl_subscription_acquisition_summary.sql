WITH tbl_subscription_acquisition_summary AS (
    SELECT
        DATE_TRUNC(subscription_acquisition_date, month) AS subscription_acquisition_cohort
        , SUM(is_acquisition_subscription) AS total_acquisition_subscriptions
        , SUM(is_bounced_subscription) AS total_bounced_subscriptions
        , SUM(is_acquisition_subscription) - SUM(is_bounced_subscription) AS total_non_bounced_subscriptions
        , SUM(is_churned_subscription) AS total_churned_subscriptions
        , SUM(CASE WHEN is_bounced_subscription = 1 THEN NULL ELSE length_of_subscription_days END) / (SUM(is_acquisition_subscription) - SUM(is_bounced_subscription)) AS average_subscription_length_days
        , CURRENT_TIMESTAMP AS inserted_date_time
    FROM {{ ref('fact_subscription_acquisition') }}
    GROUP BY 1
)

SELECT * FROM tbl_subscription_acquisition_summary