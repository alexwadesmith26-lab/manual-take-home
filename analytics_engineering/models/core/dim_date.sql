WITH dim_date AS (
    {{ dbt_date.get_date_dimension("2019-01-01", "2025-01-01") }}
)

SELECT * FROM dim_date