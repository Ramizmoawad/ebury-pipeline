# Ebury Data Pipeline — Senior Data Engineer (Platform) Case Study

> **Stack:** PostgreSQL 16 · Apache Airflow 2.8 · dbt 1.7 · Docker Compose

---

## Quickstart — un solo comando
```bash
git clone https://github.com/Ramizmoawad/ebury-pipeline.git
cd ebury-pipeline
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
mkdir -p airflow/logs && chmod 777 airflow/logs
docker compose build
docker compose up -d
```

Airflow UI disponible en **http://localhost:8080** (admin / admin) tras ~60 segundos.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Quality Framework](#2-data-quality-framework)
3. [Ingestion Strategy](#3-ingestion-strategy-full-refresh-vs-incremental)
4. [Repository Structure](#4-repository-structure)
5. [Prerequisites](#5-prerequisites)
6. [Setup Detallado](#6-setup-detallado)
7. [Ejecutar el Pipeline](#7-ejecutar-el-pipeline)
8. [Data Model](#8-data-model)
9. [Design Decisions](#9-design-decisions)
10. [Observability & Governance](#10-observability--governance)
11. [Future Improvements](#11-future-improvements)

---

## 1. Architecture Overview

### ELT, not ETL

Este pipeline sigue el patrón **ELT** (Extract → Load → Transform):

| Patrón | Dónde transforma | Ventaja |
|--------|-----------------|---------|
| **ETL** | Antes de cargar (Python/Spark) | Simple para datos pequeños |
| **ELT** | Después de cargar (dbt dentro de la DB) | Raw data siempre preservado; SQL versionado en Git |

### Las 4 capas
```
CSV (fuente)
    │
    │  Python ingest_data.py — chunked, idempotente
    ▼
raw schema          ← Todo TEXT. Copia exacta del origen. NUNCA se modifica.
    │
    │  dbt run --select staging
    ▼
staging schema      ← Limpio, tipado, dq_flags JSONB por fila. VIEW.
    │
    │  dbt run --select marts
    ▼
marts schema        ← dim_products, fact_transactions, agg_*. TABLE.

(paralelo)
audit schema        ← data_quality_log. Registro permanente de issues.
```

### Airflow DAG — 5 tareas en cadena
```
ingest_raw_data → dbt_run_staging → dbt_test_staging → dbt_run_marts → dbt_test_marts
```

Si `dbt_test_staging` falla → `dbt_run_marts` nunca se ejecuta.

---

## 2. Data Quality Framework

### Filosofía: etiquetar, no descartar

Las filas con problemas **nunca se eliminan silenciosamente**. Se cargan, se etiquetan con `dq_flags` JSONB, y el negocio decide qué filtrar.

### 3 capas de DQ

**Capa 1 — Ingesta (Python):** detecta issues, escribe en `audit.data_quality_log`, nunca bloquea la carga.

**Capa 2 — Staging (dbt SQL):** corrige lo recuperable (prefijos, formatos de fecha), pone NULL lo irrecuperable ('Two Hundred'), añade `is_clean_record` boolean.

**Capa 3 — Tests (dbt_expectations):** contratos de datos que detienen el DAG antes de que datos incorrectos lleguen a marts.

### Issues encontrados en el CSV

| Campo | Issue | Valor crudo | Resolución |
|-------|-------|-------------|-----------|
| `transaction_id` | `INVALID_FORMAT` | `"T1010"` | Strip prefijo → `1010` |
| `customer_id` | `INVALID_FORMAT` | `"501.0"` | Strip decimal → `501` |
| `customer_id` | `NULL_VALUE` | `""` | Mantener NULL |
| `transaction_date` | `INVALID_FORMAT` | `"18-07-2023"` | Parse DD-MM-YYYY → DATE |
| `product_id` | `INVALID_FORMAT` | `"P100"` | Strip prefijo → `100` |
| `price` | `NON_NUMERIC` | `"Two Hundred"` | NULL |
| `tax` | `NON_NUMERIC` | `"Fifteen"` | NULL |
| `quantity` | `NULL_VALUE` | `""` | Mantener NULL |

**Resultado:** 100 filas cargadas, 61 limpias (`is_clean_record = TRUE`), 39 flaggeadas.

---

## 3. Ingestion Strategy: Full Refresh vs Incremental

### Estrategia actual: Full Refresh (TRUNCATE + INSERT)

El CSV es un export completo en cada ejecución. TRUNCATE antes de cargar garantiza idempotencia: re-ejecutar siempre produce el mismo resultado.

### Upgrade path: Incremental (UPSERT)

Para fuentes incrementales o datasets grandes donde el full reload es demasiado costoso:
```python
# En ingest_data.py — reemplazar TRUNCATE + INSERT por:
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

Requiere: `ALTER TABLE raw.customer_transactions ADD CONSTRAINT uq_transaction_id UNIQUE (transaction_id);`

Coste: O(total filas) full refresh → O(filas nuevas) incremental — hasta **1000x más eficiente a escala**.

En dbt, cambiar `fact_transactions` a materialización incremental:
```sql
{{ config(materialized='incremental', unique_key='transaction_id') }}
{% if is_incremental() %}
WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
```

---

## 4. Repository Structure
```
ebury-pipeline/
├── docker-compose.yml              # Orquesta los 5 servicios
├── Dockerfile.airflow              # Imagen Airflow con dbt preinstalado
├── .env.example                    # Template de variables de entorno
├── README.md
│
├── datafiles/
│   └── customer_transactions.csv   # Dataset fuente
│
├── postgres/
│   └── init.sql                    # Crea schemas, tablas, usuarios y permisos
│
├── scripts/
│   └── ingest_data.py              # CSV → raw schema (ELT extract + load)
│
├── airflow/
│   ├── dags/
│   │   └── customer_transactions_dag.py   # DAG de 5 tareas
│   └── logs/                              # Logs de tareas (gitignored)
│
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml                # Conexión DB (lee desde env vars)
    ├── packages.yml                # dbt_utils, dbt_expectations
    ├── macros/
    │   ├── data_quality.sql        # Macros DQ reutilizables
    │   └── generate_schema_name.sql # Override de schema concatenation
    └── models/
        ├── staging/
        │   ├── stg_customer_transactions.sql
        │   └── schema.yml
        └── marts/
            ├── dim_products.sql
            ├── fact_transactions.sql
            ├── agg_monthly_sales.sql
            ├── agg_customer_summary.sql
            └── schema.yml
```

---

## 5. Prerequisites

- Docker Engine ≥ 24.x y Docker Compose Plugin v2.x
- 4 GB RAM disponibles para Docker
- Puertos 8080 (Airflow UI) y 5432 (PostgreSQL) libres

**Instalar Docker en Ubuntu 24.04 / Linux Mint Zena:**
```bash
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    -o /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
    https://download.docker.com/linux/ubuntu noble stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER && newgrp docker
```

> **Linux Mint:** Mint Zena está basado en Ubuntu 24.04 (Noble). Usa `noble` en la URL del repo de Docker, no `zena`.

---

## 6. Setup Detallado
```bash
# 1. Clonar el repositorio
git clone https://github.com/Ramizmoawad/ebury-pipeline.git
cd ebury-pipeline

# 2. Configurar variables de entorno
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env

# 3. Crear directorio de logs de Airflow
mkdir -p airflow/logs && chmod 777 airflow/logs

# 4. Construir imagen personalizada de Airflow (incluye dbt-postgres)
docker compose build

# 5. Arrancar todos los servicios
#    - PostgreSQL: crea schemas, tablas y permisos automáticamente
#    - airflow-init: ejecuta db migrate y crea usuario admin
#    - dbt: instala paquetes (dbt_utils, dbt_expectations) automáticamente
#    - airflow-webserver y airflow-scheduler arrancan tras el init
docker compose up -d
```

Verificar que todo está healthy (~60 segundos):
```bash
docker compose ps
```

---

## 7. Ejecutar el Pipeline

### Opción A — Airflow UI (recomendado)

1. Abrir **http://localhost:8080** (admin / admin)
2. Activar el DAG `customer_transactions_pipeline`
3. Click en ▶ **Trigger DAG**
4. Observar las 5 tareas ponerse en verde

### Opción B — CLI
```bash
docker exec ebury_airflow_scheduler \
    airflow dags unpause customer_transactions_pipeline

docker exec ebury_airflow_scheduler \
    airflow dags trigger customer_transactions_pipeline
```

### Verificar resultados
```sql
-- Conectar a la DB
docker exec -it ebury_postgres psql -U dw_user -d ebury_dw

-- Conteo por capa
SELECT 'raw rows'            AS capa, COUNT(*)::TEXT FROM raw.customer_transactions
UNION ALL SELECT 'dq issues',         COUNT(*)::TEXT FROM audit.data_quality_log
UNION ALL SELECT 'dim products',      COUNT(*)::TEXT FROM marts.dim_products
UNION ALL SELECT 'fact transactions', COUNT(*)::TEXT FROM marts.fact_transactions
UNION ALL SELECT 'clean rows',        COUNT(*)::TEXT FROM marts.fact_transactions WHERE is_clean_record
UNION ALL SELECT 'monthly agg',       COUNT(*)::TEXT FROM marts.agg_monthly_sales
UNION ALL SELECT 'customer summary',  COUNT(*)::TEXT FROM marts.agg_customer_summary;

-- Revenue limpio julio 2023
SELECT year_month, SUM(ROUND(clean_gross_amount::numeric, 2)) AS revenue
FROM marts.agg_monthly_sales GROUP BY year_month;

-- Top clientes
SELECT customer_id, total_transactions,
       ROUND(total_gross_amount::numeric, 2) AS total_spent
FROM marts.agg_customer_summary
ORDER BY total_spent DESC LIMIT 5;
```

### Teardown
```bash
docker compose down        # para servicios, conserva datos
docker compose down -v     # para servicios y borra todos los datos
```

---

## 8. Data Model
```
raw.customer_transactions (100 filas, todo TEXT)
    └── staging.stg_customer_transactions (VIEW — limpio + tipado)
            ├── marts.dim_products          (5 filas — SCD Tipo 1)
            ├── marts.fact_transactions     (100 filas — grain: 1 por transacción)
            ├── marts.agg_monthly_sales     (5 filas  — grain: mes × producto)
            └── marts.agg_customer_summary  (10 filas — grain: cliente)
```

### Columnas clave de fact_transactions

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `transaction_sk` | TEXT | Surrogate key SHA-256 |
| `product_sk` | TEXT | FK → `dim_products.product_sk` |
| `line_amount` | NUMERIC(12,4) | `price × quantity` |
| `gross_amount` | NUMERIC(12,4) | `line_amount + tax` |
| `dq_flags` | JSONB | Issues por campo — `{}` = limpia |
| `is_clean_record` | BOOLEAN | `dq_flags = '{}'` |

---

## 9. Design Decisions

**Raw schema en TEXT** — preserva el dato original. Si hay un bug en la lógica de limpieza, siempre se puede volver al raw y re-derivar sin re-ingestar.

**NUMERIC(12,4) para dinero** — FLOAT acumula errores de redondeo. `0.1 + 0.2` en FLOAT = `0.30000000000000004`. En finanzas esto es inaceptable.

**Surrogate keys SHA-256** — los IDs de fuente pueden cambiar formato (T1010 → 1010) o reutilizarse. La surrogate key es estable e independiente del sistema origen.

**staging = VIEW, marts = TABLE** — VIEW: lógica de limpieza cambia frecuentemente, re-ejecutar es barato. TABLE: consumidores necesitan queries rápidas.

**No descartar filas sucias** — `is_clean_record` deja la decisión al negocio. En finanzas, una fila descartada puede ser dinero real.

**BashOperator para ingesta** — ejecutar el script en su propio proceso evita contaminar el worker de Airflow. El `run_id` se pasa via template Jinja `{{ run_id }}` nativo.

**Docker Compose, no Kubernetes** — el PDF lo pedía explícitamente. Para 5 servicios locales, K8s añadiría semanas de setup sin valor real.

---

## 10. Observability & Governance

| Aspecto | Implementación |
|---------|---------------|
| Logs de tareas | Airflow almacena logs por tarea, accesibles via UI |
| Audit trail DQ | `audit.data_quality_log` — cada anomalía con `pipeline_run_id` |
| Linaje de datos | `dbt docs generate` produce grafo de linaje interactivo |
| Alertas (stub) | `on_failure_callback` — reemplazar `logger.error` con webhook Slack/PagerDuty |
| Catálogo | `dbt docs serve` expone descripciones de columnas y resultados de tests |
```bash
# Generar y servir documentación dbt
docker exec ebury_dbt dbt docs generate \
    --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt
```

---

## 11. Future Improvements

1. **UPSERT incremental** — ver sección 3 para implementación exacta.
2. **Alertas reales** en `on_failure_callback` — Slack/PagerDuty webhook.
3. **CeleryExecutor + Redis** — tareas paralelas en workers separados.
4. **dbt incremental** para marts (`unique_key='transaction_id'`).
5. **Astronomer Cosmos** — cada modelo dbt como tarea individual de Airflow.
6. **PostgreSQL COPY FROM STDIN** — 3-5x más throughput que execute_values para archivos grandes.
7. **Managed DW** (Redshift/BigQuery/Snowflake) — solo cambia `profiles.yml`.
8. **HashiCorp Vault** — reemplazar `.env` para gestión de secretos en producción.
9. **DAG Factory** — generar DAGs desde YAML config para múltiples fuentes.
10. **PII masking** — hashear identificadores de clientes en staging.
