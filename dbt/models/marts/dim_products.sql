WITH source AS (
    SELECT product_id, product_name,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY CASE WHEN product_name IS NOT NULL THEN 0 ELSE 1 END, _loaded_at DESC
        ) AS rn
    FROM {{ ref('stg_customer_transactions') }}
    WHERE product_id IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,
    product_id,
    COALESCE(product_name, 'Unknown Product') AS product_name,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM source WHERE rn = 1
ORDER BY product_id
