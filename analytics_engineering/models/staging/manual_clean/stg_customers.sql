WITH stg_customers AS (
    SELECT
        customer_id
        , customer_country
    FROM {{ source('manual_clean', 'customers') }}
)

SELECT * FROM stg_customers