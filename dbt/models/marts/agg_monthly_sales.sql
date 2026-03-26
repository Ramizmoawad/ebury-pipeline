WITH base AS (SELECT * FROM {{ ref('fact_transactions') }} WHERE transaction_date IS NOT NULL)
SELECT
    transaction_month AS month_start,
    transaction_year AS year,
    transaction_month_num AS month_num,
    TO_CHAR(transaction_month, 'YYYY-MM') AS year_month,
    product_id,
    COUNT(*) AS total_transactions,
    COUNT(*) FILTER (WHERE is_clean_record) AS clean_transactions,
    SUM(line_amount) AS total_line_amount,
    SUM(gross_amount) AS total_gross_amount,
    SUM(tax) AS total_tax,
    SUM(quantity) AS total_units_sold,
    SUM(line_amount)  FILTER (WHERE is_clean_record) AS clean_line_amount,
    SUM(gross_amount) FILTER (WHERE is_clean_record) AS clean_gross_amount,
    AVG(price) AS avg_unit_price,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM base
GROUP BY transaction_month, transaction_year, transaction_month_num, product_id
ORDER BY transaction_month, product_id
