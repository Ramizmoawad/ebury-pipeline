WITH raw AS (
    SELECT
        _row_id, _loaded_at, _source_file,
        CASE
            WHEN transaction_id ~ '^[0-9]+$' THEN transaction_id::INTEGER
            WHEN transaction_id ~ '[0-9]+$'  THEN REGEXP_REPLACE(transaction_id, '[^0-9]', '', 'g')::INTEGER
            ELSE NULL
        END AS transaction_id,
        CASE
            WHEN customer_id IS NULL OR customer_id = '' THEN NULL
            WHEN customer_id ~ '^[0-9]+\.0+$' THEN SPLIT_PART(customer_id, '.', 1)::INTEGER
            WHEN customer_id ~ '^[0-9]+$'     THEN customer_id::INTEGER
            ELSE NULL
        END AS customer_id,
        CASE
            WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(transaction_date, 'YYYY-MM-DD')
            WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(transaction_date, 'DD-MM-YYYY')
            ELSE NULL
        END AS transaction_date,
        CASE
            WHEN product_id ~ '^[0-9]+$' THEN product_id::INTEGER
            WHEN product_id ~ '[0-9]+$'  THEN REGEXP_REPLACE(product_id, '[^0-9]', '', 'g')::INTEGER
            ELSE NULL
        END AS product_id,
        NULLIF(TRIM(product_name), '') AS product_name,
        CASE WHEN quantity ~ '^[0-9]+(\.[0-9]+)?$' AND quantity::NUMERIC > 0
             THEN ROUND(quantity::NUMERIC)::INTEGER ELSE NULL END AS quantity,
        CASE WHEN price ~ '^[0-9]+(\.[0-9]+)?$' AND price::NUMERIC > 0
             THEN price::NUMERIC(12,4) ELSE NULL END AS price,
        CASE WHEN tax ~ '^[0-9]+(\.[0-9]+)?$' AND tax::NUMERIC >= 0
             THEN tax::NUMERIC(12,4) ELSE NULL END AS tax,
        (
            CASE WHEN transaction_id IS NULL OR transaction_id = ''
                 THEN '{"transaction_id":"NULL_VALUE"}'::jsonb ELSE '{}'::jsonb END ||
            CASE WHEN customer_id IS NULL OR customer_id = ''
                 THEN '{"customer_id":"NULL_VALUE"}'::jsonb ELSE '{}'::jsonb END ||
            CASE WHEN transaction_date IS NULL OR transaction_date = ''
                 THEN '{"transaction_date":"NULL_VALUE"}'::jsonb
                 WHEN transaction_date NOT SIMILAR TO '\d{4}-\d{2}-\d{2}'
                  AND transaction_date NOT SIMILAR TO '\d{2}-\d{2}-\d{4}'
                 THEN '{"transaction_date":"INVALID_FORMAT"}'::jsonb ELSE '{}'::jsonb END ||
            CASE WHEN quantity IS NULL OR quantity = ''
                 THEN '{"quantity":"NULL_VALUE"}'::jsonb ELSE '{}'::jsonb END ||
            CASE WHEN price IS NOT NULL AND price != '' AND price !~ '^[0-9]+(\.[0-9]+)?$'
                 THEN '{"price":"NON_NUMERIC"}'::jsonb ELSE '{}'::jsonb END ||
            CASE WHEN tax IS NOT NULL AND tax != '' AND tax !~ '^[0-9]+(\.[0-9]+)?$'
                 THEN '{"tax":"NON_NUMERIC"}'::jsonb ELSE '{}'::jsonb END
        ) AS dq_flags
    FROM {{ source('raw', 'customer_transactions') }}
),
enriched AS (
    SELECT *,
        CASE WHEN price IS NOT NULL AND quantity IS NOT NULL
             THEN ROUND(price * quantity, 4) ELSE NULL END AS line_amount,
        CASE WHEN price IS NOT NULL AND quantity IS NOT NULL AND tax IS NOT NULL
             THEN ROUND((price * quantity) + tax, 4) ELSE NULL END AS gross_amount,
        DATE_TRUNC('month', transaction_date) AS transaction_month,
        EXTRACT(YEAR  FROM transaction_date)::INTEGER AS transaction_year,
        EXTRACT(MONTH FROM transaction_date)::INTEGER AS transaction_month_num
    FROM raw
)
SELECT * FROM enriched
