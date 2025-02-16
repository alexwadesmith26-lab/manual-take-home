WITH subscription_activity AS (
    SELECT
        customer_id
        , subscription_id
        , DATE_TRUNC(subscription_start_date, month) AS subscription_acquisition_cohort
        , subscription_start_date
        , DATE_TRUNC(subscription_start_date, month) AS subscription_start_month
        , subscription_end_date
        , DATE_TRUNC(subscription_end_date, month) AS subscription_end_month
    FROM {{ ref('stg_activity') }}
    WHERE subscription_start_date != subscription_end_date -- drop "bounced" subscriptions from time-series analysis
)

, monthly_dim_date AS (
    SELECT
        date_day
    FROM {{ ref('dim_date') }}
    WHERE date_day = DATE_TRUNC(date_day, month) -- reporting to be on monthly grain
)

, int_subscription_activity AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['s.subscription_id', 'd.date_day']) }} AS activity_key
        , s.subscription_id
        , s.customer_id
        , d.date_day AS calendar_date
        , s.subscription_acquisition_cohort
        , DATE_DIFF(d.date_day, s.subscription_start_date, month) AS months_of_active_subscription
        , CASE
            WHEN DATE_DIFF(d.date_day, s.subscription_start_date, month) <= 6 THEN 'New Customer'
            WHEN DATE_DIFF(d.date_day, s.subscription_start_date, month) <= 12 THEN 'Developing Customer'
            ELSE 'Mature Customer'
        END AS customer_maturity_type
        , 1 AS is_active_subscription
        , CASE
            WHEN d.date_day = s.subscription_end_month THEN 1
            ELSE 0
        END AS is_churned_subscription
        , CURRENT_TIMESTAMP AS inserted_date_time
    FROM monthly_dim_date AS d
    INNER JOIN subscription_activity AS s
        ON 
            d.date_day >= s.subscription_start_month
            AND d.date_day <= s.subscription_end_month
)

SELECT * FROM int_subscription_activity