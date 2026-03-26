WITH base AS (
    SELECT * FROM {{ ref('fact_transactions') }}
    WHERE customer_id IS NOT NULL AND transaction_date IS NOT NULL
),
ranked AS (
    SELECT customer_id, product_id, COUNT(*) AS cnt,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
    FROM base GROUP BY customer_id, product_id
)
SELECT
    b.customer_id,
    COUNT(DISTINCT b.transaction_id) AS total_transactions,
    MIN(b.transaction_date) AS first_purchase_date,
    MAX(b.transaction_date) AS last_purchase_date,
    SUM(b.line_amount) AS total_line_amount,
    SUM(b.gross_amount) AS total_gross_amount,
    SUM(b.quantity) AS total_units_purchased,
    AVG(b.price) AS avg_unit_price,
    COUNT(*) FILTER (WHERE NOT b.is_clean_record) AS flagged_record_count,
    r.product_id AS top_product_id,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM base b
LEFT JOIN ranked r ON b.customer_id = r.customer_id AND r.rn = 1
GROUP BY b.customer_id, r.product_id
ORDER BY total_gross_amount DESC NULLS LAST
