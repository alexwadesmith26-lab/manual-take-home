WITH int_customer AS (
    SELECT
        c.customer_id
        , c.customer_country
        , COALESCE(o.taxonomy_business_category_group, 'missing') AS customer_business_category_group -- fill in value for missing customer_ids in orders
        , CURRENT_TIMESTAMP AS inserted_date_time
    FROM {{ ref('stg_customers') }} AS c
    LEFT JOIN {{ ref('stg_acq_orders') }} AS o
        ON c.customer_id = o.customer_id
)

SELECT * FROM int_customer