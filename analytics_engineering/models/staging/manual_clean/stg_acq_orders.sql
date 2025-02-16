WITH stg_acq_orders AS (
    SELECT
        customer_id
        , taxonomy_business_category_group
    FROM {{ source('manual_clean', 'acq_orders') }}
)

SELECT * FROM stg_acq_orders