WITH tbl_subscription_activity AS (
    SELECT
        a.activity_key
        , a.subscription_id
        , a.customer_id
        , a.calendar_date
        , c.customer_country
        , c.customer_business_category_group
        , a.subscription_acquisition_cohort
        , a.months_of_active_subscription
        , a.customer_maturity_type
        , a.is_active_subscription
        , a.is_churned_subscription
        , CURRENT_TIMESTAMP AS inserted_date_time
    FROM {{ ref('fact_subscription_activity') }} AS a
    INNER JOIN {{ ref('dim_customer') }} AS c
        ON a.customer_id = c.customer_id
)

SELECT * FROM tbl_subscription_activity