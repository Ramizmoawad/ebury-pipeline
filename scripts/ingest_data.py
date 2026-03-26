#!/usr/bin/env python3
import os, io, logging, sys
from datetime import timezone, datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_PARAMS = {
    "host":     os.environ.get("DW_HOST",     "localhost"),
    "port":     int(os.environ.get("DW_PORT", "5432")),
    "dbname":   os.environ.get("DW_DB",       "ebury_dw"),
    "user":     os.environ.get("DW_USER",     "dw_user"),
    "password": os.environ.get("DW_PASSWORD", "dw_password"),
}

DATA_FILE       = os.environ.get("DATA_FILE", "/opt/airflow/datafiles/customer_transactions.csv")
CHUNK_SIZE      = int(os.environ.get("CHUNK_SIZE", "500"))
PIPELINE_RUN_ID = os.environ.get("PIPELINE_RUN_ID", datetime.now(timezone.utc).isoformat())
REQUIRED_FIELDS = {"transaction_id", "transaction_date"}

def detect_issues(row, row_num):
    issues = []
    def flag(field, issue_type, raw_value, resolution):
        issues.append({"source_table": "raw.customer_transactions",
                        "row_identifier": row.get("transaction_id", f"row_{row_num}"),
                        "field_name": field, "issue_type": issue_type,
                        "raw_value": str(raw_value)[:500], "resolution": resolution,
                        "pipeline_run_id": PIPELINE_RUN_ID})
    for f in REQUIRED_FIELDS:
        if not row.get(f):
            flag(f, "NULL_VALUE", "", "LOAD_AS_NULL")
    cid = row.get("customer_id", "")
    if cid:
        try: float(cid)
        except ValueError: flag("customer_id", "INVALID_FORMAT", cid, "SET_NULL_IN_STAGING")
    td = row.get("transaction_date", "")
    if td:
        valid = any(
            (lambda: datetime.strptime(td, fmt) or True)()
            for fmt in ["%Y-%m-%d", "%d-%m-%Y"]
            if not (lambda fmt=fmt: datetime.strptime(td, fmt))()
            if False
        ) if False else False
        for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
            try: datetime.strptime(td, fmt); valid = True; break
            except ValueError: pass
        if not valid:
            flag("transaction_date", "INVALID_FORMAT", td, "SET_NULL_IN_STAGING")
    for f in ("price", "tax"):
        v = row.get(f, "")
        if v:
            try: float(v)
            except ValueError: flag(f, "NON_NUMERIC", v, "SET_NULL_IN_STAGING")
    qty = row.get("quantity", "")
    if qty:
        try:
            if float(qty) <= 0: flag("quantity", "OUT_OF_RANGE", qty, "SET_NULL_IN_STAGING")
        except ValueError: flag("quantity", "NON_NUMERIC", qty, "SET_NULL_IN_STAGING")
    return issues

def main():
    logger.info("Ingestion start | file=%s | run_id=%s", DATA_FILE, PIPELINE_RUN_ID)
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE raw.customer_transactions RESTART IDENTITY CASCADE;")
        total_rows, audit_buffer = 0, []
        for chunk_num, chunk in enumerate(
            pd.read_csv(DATA_FILE, dtype=str, keep_default_na=False, chunksize=CHUNK_SIZE)
        ):
            chunk = chunk.fillna("")
            rows = []
            for offset, (_, row) in enumerate(chunk.iterrows()):
                d = row.to_dict()
                audit_buffer.extend(detect_issues(d, chunk_num * CHUNK_SIZE + offset + 2))
                rows.append((PIPELINE_RUN_ID, d.get("transaction_id"), d.get("customer_id"),
                              d.get("transaction_date"), d.get("product_id"), d.get("product_name"),
                              d.get("quantity"), d.get("price"), d.get("tax")))
            execute_values(cur, """
                INSERT INTO raw.customer_transactions
                    (_source_file, transaction_id, customer_id, transaction_date,
                     product_id, product_name, quantity, price, tax)
                VALUES %s""", rows)
            total_rows += len(rows)
        if audit_buffer:
            execute_values(cur, """
                INSERT INTO audit.data_quality_log
                    (source_table, row_identifier, field_name, issue_type,
                     raw_value, resolution, pipeline_run_id)
                VALUES %s""",
                [(r["source_table"], r["row_identifier"], r["field_name"],
                  r["issue_type"], r["raw_value"], r["resolution"], r["pipeline_run_id"])
                 for r in audit_buffer])
            logger.warning("DQ issues logged: %d", len(audit_buffer))
        conn.commit()
        logger.info("Done | rows=%d | issues=%d", total_rows, len(audit_buffer))
    except Exception:
        conn.rollback(); logger.exception("Ingestion failed"); sys.exit(1)
    finally:
        cur.close(); conn.close()

if __name__ == "__main__":
    main()
