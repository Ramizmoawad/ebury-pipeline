
---

## Ingestion Strategy: Full Refresh vs Incremental

### Current strategy: Full Refresh (TRUNCATE + INSERT)

Appropriate because the CSV is a **complete export** of all historical transactions on every run. The load is idempotent: running it multiple times on the same file produces the same result.

### Upgrade path: Incremental (UPSERT)

For incremental sources or large datasets where full reload is too expensive, switch to:
```python
execute_values(
    cur,
    """
    INSERT INTO raw.customer_transactions
        (transaction_id, customer_id, transaction_date,
         product_id, product_name, quantity, price, tax, _source_file)
    VALUES %s
    ON CONFLICT (transaction_id)
    DO UPDATE SET
        price      = EXCLUDED.price,
        _loaded_at = NOW()
    WHERE raw.customer_transactions.price
        IS DISTINCT FROM EXCLUDED.price
    """,
    rows_to_insert,
)
```

Requires: `ALTER TABLE raw.customer_transactions ADD CONSTRAINT uq_transaction_id UNIQUE (transaction_id);`

Cost: O(total rows) full refresh → O(new rows) incremental — potentially **100–1000x cheaper at scale**.

In dbt, switch `fact_transactions` to incremental materialisation:
```sql
{{ config(materialized='incremental', unique_key='transaction_id') }}
{% if is_incremental() %}
WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
```

---

## Future Improvements

1. **Incremental UPSERT** — see section above.
2. **Real Slack/PagerDuty alerts** in `on_failure_callback` — replace the `logger.error` stub.
3. **CeleryExecutor + Redis** — replace LocalExecutor for parallel task execution.
4. **dbt incremental materialisation** for marts (`unique_key='transaction_id'`).
5. **Astronomer Cosmos provider** — each dbt model becomes an individual Airflow task.
6. **PostgreSQL COPY FROM STDIN** — replace pandas+execute_values for 3–5x better throughput on large files.
7. **Managed DW** (Redshift/BigQuery/Snowflake) — only `profiles.yml` changes, SQL stays the same.
8. **HashiCorp Vault / AWS Secrets Manager** — replace `.env` file for production secrets.
9. **DAG Factory** — generate DAGs from YAML config for multi-source ingestion.
10. **PII masking** — hash customer identifiers in staging for analytics use cases.
