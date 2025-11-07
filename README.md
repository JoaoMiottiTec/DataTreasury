# DataTreasury — Financial Data Platform (MVP)

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Prefect](https://img.shields.io/badge/Prefect-3.x-000000?logo=prefect)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?logo=postgresql)
![License](https://img.shields.io/badge/License-MIT-green)

> **Goal:** to learn more about data engineering by building a complete data platform using free and public data sources (BCB/SGS).
> **Pillars:** Ingestion → Modeling (DW Postgres) → Transformations (dbt) → Orchestration (Prefect) → API (Node/TS).

---

## 1. Overview

DataTreasury is a modular financial data platform that consolidates macroeconomic and market information into structured layers (Bronze → Silver → Gold).
The MVP focuses on building the first ingestion pipeline, fully automated via Prefect 3.x.

---

## 2. Project Structure

```
pipelines/
 ├── ingestors/
 │    ├── ingest_bcb.py        # BCB Ingestor (SELIC)
 │    └── ...
 ├── flows/
 │    ├── ingest_bcb_flow.py   # Prefect Flow (orchestrates fetch → upsert)
 │    └── deploy_bcb.py        # Daily deployment/cron (Prefect 3.x)
 └── warehouse/
      └── ... (future dbt models)
```

---

## 3. How to Run

### 3.1 Requirements

* Python 3.12+
* PostgreSQL running (local or Docker)
* `.env` file with:

  ```env
  DATABASE_URL=postgresql://user:pass@localhost:5432/datatreasury
  ```

---

### 3.2 Install dependencies

```bash
pip install -r requirements.txt
```

---

### 3.3 Run manually (simple mode)

Run the BCB ingestor directly without Prefect:

```bash
python -m pipelines.ingestors.ingest_bcb
```

> Fetches up to 10 years of SELIC data (`bcdata.sgs.11`) into `bronze.raw_macro_daily`.

---

### 3.4 Run via Prefect Flow

Execute the orchestrated Prefect pipeline:

```bash
PYTHONPATH=. python -m pipelines.flows.ingest_bcb_flow
```

> Each stage (`fetch → upsert`) runs as a Prefect task with retries, logs, and state tracking.

---

### 3.5 Schedule with Prefect (local mode)

Create a deployment and schedule daily execution at 08:00 (BRT):

```bash
PYTHONPATH=. python -m pipelines.flows.deploy_bcb
```

> Keep this process open — it creates the "BCB Daily" deployment and runs it automatically according to the cron configuration.

---

### 3.6 Schedule with Prefect Server (enterprise mode)

Run using Prefect Server + Worker for production-like orchestration:

```bash
# Start Prefect server
prefect server start

# In another terminal
export PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect worker start -p default

# Trigger a manual deployment run (test)
prefect deployment run "BCB Ingestion Flow/BCB Daily"
```

> Prefect Dashboard: [http://127.0.0.1:4200](http://127.0.0.1:4200)

---

## 4. High-Level Roadmap

| Stage                      | Description                                 | Status |
| -------------------------- | ------------------------------------------- | ------ |
| 1. Basic Infrastructure    | PostgreSQL setup and initial migration      | ✅      |
| 2. BCB Ingestion           | SELIC (SGS 11) via Prefect                  | ✅      |
| 3. CoinGecko Ingestion     | Cryptocurrency data (BTC, ETH)              | ⏳      |
| 4. Transformations (dbt)   | Staging and marts (Silver/Gold)             | ✅      |
| 5. Financial API (Node/TS) | REST endpoints for querying series          | ⏳      |
| 6. Observability           | Prefect Server, logs, and metrics (Grafana) | ⏳      |
| 7. Portfolio/Demo          | Documentation and public dashboard          | ⏳      |

---

## 5. Next Steps

* Add Docker Compose with Prefect Server + Worker + Postgres
* Implement CoinGecko and Yahoo Finance
* Build Silver models using dbt
* Develop Node.js API for financial queries
* Deploy a demo and public dashboard

---

## 6. Key Concepts

* Data Lakehouse: layered architecture (Bronze/Silver/Gold).
* Orchestration: Prefect 3.x using `serve()` and `CronSchedule`.
* Persistence: PostgreSQL with `psycopg_pool`.
* Observability: built-in task logs and retries.
* Best Practices: modularization, idempotency, and version control.

---

## 7. License

MIT — educational open-source project focused on learning advanced data engineering concepts.
