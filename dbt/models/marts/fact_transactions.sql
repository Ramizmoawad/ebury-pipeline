WITH stg AS (SELECT * FROM {{ ref('stg_customer_transactions') }}),
dim_prod AS (SELECT product_id, product_sk FROM {{ ref('dim_products') }})
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg._row_id']) }} AS transaction_sk,
    stg.transaction_id, stg.customer_id,
    dim_prod.product_sk, stg.product_id,
    stg.transaction_date, stg.transaction_month,
    stg.transaction_year, stg.transaction_month_num,
    stg.quantity, stg.price, stg.tax,
    stg.line_amount, stg.gross_amount,
    stg.dq_flags,
    (stg.dq_flags = '{}'::jsonb) AS is_clean_record,
    stg._loaded_at, stg._source_file,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM stg
LEFT JOIN dim_prod ON stg.product_id = dim_prod.product_id
