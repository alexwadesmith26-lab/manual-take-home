WITH dim_customer AS (
    SELECT
        CAST(customer_id AS STRING)                        AS customer_id
        , CAST(customer_country AS STRING)                 AS customer_country
        , CAST(customer_business_category_group AS STRING) AS customer_business_category_group
        , CURRENT_TIMESTAMP                                AS inserted_date_time
    FROM {{ ref('int_customer') }}
)

SELECT * FROM dim_customer