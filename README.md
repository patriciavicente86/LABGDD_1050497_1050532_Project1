# NYC Taxi Data Pipeline — Lambda vs. Kappa (LABGDD Project)

A fully offline, reproducible PySpark pipeline for the NYC TLC trip records dataset that implements **both**:
- **Lambda (batch)**: Bronze ➜ Silver ➜ Gold (analytics-ready tables)
- **Kappa (streaming)**: file-based micro-batches with Structured Streaming

The project is container-friendly (VS Code Dev Containers / Docker) and script-driven (Make). Results, figures, and tables are generated from existing outputs via a **read-only** notebook.

> **Goal**: Compare Lambda vs. Kappa along **freshness**, **accuracy**, and **operational complexity** under identical constraints on a single machine, and provide an auditable, portable reference for teaching and prototyping.

---

## Table of Contents
- [Project Layout](#project-layout)
- [Environment](#environment)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Pipelines](#pipelines)
  - [Lambda (Batch)](#lambda-batch)
  - [Kappa (Streaming)](#kappa-streaming)
- [Validation & Reporting](#validation--reporting)
- [Outputs](#outputs)
- [Notebooks](#notebooks)
- [Troubleshooting](#troubleshooting)
- [AI Assistance Disclosure](#ai-assistance-disclosure)
- [License & Citation](#license--citation)
- [Acknowledgments](#acknowledgments)

---

## Project Layout

```
.
├─ env/
│  └─ config.yaml              # project configuration (paths, services, years, DQ thresholds)
├─ lake/
│  ├─ bronze/                  # raw-to-bronze outputs (partitioned)
│  ├─ silver/                  # cleaned data (partitioned by pickup_date)
│  └─ gold/                    # analytics-ready tables
├─ make/
│  ├─ pipeline.mk              # main targets (silver, gold, kappa_start, metrics, compare, figures)
│  ├─ ingest.mk                # optional: raw ➜ bronze
│  ├─ bench.mk                 # (optional) benchmarking helpers
│  └─ validate.mk              # (optional) validation helpers
├─ reports/                    # figures, tables, and snapshot markdown
├─ src/
│  ├─ bronze_ingest.py         # (if present) raw ➜ bronze
│  ├─ clean_to_silver.py       # bronze ➜ silver
│  ├─ features_to_gold.py      # silver ➜ gold
│  ├─ kappa_driver.py          # kappa: streaming gold (agg_zone_hour_streaming)
│  ├─ lambda_driver.py         # lambda: orchestrates silver+gold (optional)
│  ├─ compare_lambda_kappa.py  # correctness comparison metrics
│  ├─ metrics.py               # row counts, retention, top-10
│  └─ figures.py               # plots from existing gold outputs
├─ notebooks/
│  └─ NYC_Taxi_Report.ipynb    # read-only report notebook (no ETL)
├─ requirements.txt
└─ README.md  (you are here)
```

> **Note**: Some files/targets are optional—your repo may not include every file listed above.

---

## Environment

- **Python**: 3.12 (works with 3.10+ if using matching PySpark)
- **PySpark**: 3.5.1
- **Key Python deps**: `pyyaml`, `pandas`, `pyarrow`, `matplotlib`, `jupyterlab`
- **Dev**: VS Code + Dev Containers (Docker) recommended

Install locally:
```bash
pip install -r requirements.txt
```

Or open the repository in VS Code and **Reopen in Container**.

---

## Quickstart

1) **Configure**
   - Edit `env/config.yaml` (see [Configuration](#configuration)).

2) **(Optional) Ingest to Bronze**
   - If your repo has an ingest target:
     ```bash
     make ingest
     ```
   - Otherwise, drop pre-fetched TLC Parquet files under `lake/bronze/service=<yellow|green>/year=<YYYY>/month=*/*.parquet`.

3) **Build Silver & Gold (Lambda)**
   ```bash
   make silver
   make gold
   # or, if defined
   make lambda     # alias for silver + gold
   ```

4) **(Optional) Start Kappa streaming**
   ```bash
   make kappa_start      # run in a terminal (press Ctrl+C to stop)
   make kappa_seed       # seed first month from bronze into bronze_stream
   make kappa_seed_next  # seed additional month(s) when ready
   ```

5) **Metrics / Compare / Figures**
   ```bash
   make metrics          # row counts & retention
   make compare          # Lambda vs Kappa correctness (overlap, exact %, MAE)
   make figures          # figures from existing gold outputs
   ```

6) **Notebook (read-only)**
   - Open `notebooks/NYC_Taxi_Report.ipynb` to generate/export figures & summary **without** re-running ETL.

---

## Configuration

`env/config.yaml` (example excerpt):
```yaml
bronze_path: ./lake/bronze
silver_path: ./lake/silver
gold_path:   ./lake/gold
years: [2024]
services: ["yellow","green"]
dq:
  max_trip_hours: 6
  min_distance_km: 0.1
  min_total_amount: 0
```

- **Paths** are relative to the repository root.
- **Years/Services** define the scope (e.g., 2024 + yellow+green).
- **DQ thresholds** are applied in `clean_to_silver.py` and mirrored in the streaming driver.

---

## Pipelines

### Lambda (Batch)
1) **Silver** (`src/clean_to_silver.py`): standardizes timestamps, applies DQ filters, de-duplicates, writes partitioned by `pickup_date` under `lake/silver/<service>/`.
2) **Gold** (`src/features_to_gold.py`): computes
   - `lake/gold/trip_features/`
   - `lake/gold/agg_zone_hour/`  (key: `service, pulocationid, pickup_date, hour`)
   - `lake/gold/agg_od_hour/`    (key: `service, pulocationid, dolocationid, pickup_date, hour`)

### Kappa (Streaming)
- **Driver** (`src/kappa_driver.py`): reads `lake/bronze_stream/...` using **Structured Streaming**, replicates cleaning/keying, and writes
  - `lake/gold/agg_zone_hour_streaming/`
- **Seeding**: use `make kappa_seed` / `make kappa_seed_next` to copy month folders from `lake/bronze/...` into `lake/bronze_stream/...` and simulate new arrivals.
- **Write mode**: `foreachBatch` with dynamic partition overwrite (idempotent per micro-batch).

---

## Validation & Reporting

- **Metrics** (`make metrics`, `src/metrics.py`):
  - Counts for Bronze, Silver (per service), Gold tables
  - Bronze➜Silver retention
  - Top zones by trips (from `agg_zone_hour`)

- **Batch vs. Streaming correctness** (`make compare`, `src/compare_lambda_kappa.py`):
  - Join `agg_zone_hour` (batch) with `agg_zone_hour_streaming` (stream) on `(service, pulocationid, pickup_date, hour)`
  - Report: overlap keys, exact-match %, MAE (trips/key)

- **Figures** (`make figures`, `src/figures.py`):
  - `reports/fig_hourly_demand.png`
  - `reports/fig_speed_by_hour.png`
  - `reports/table_top10_pickup_zones.csv`
  - `reports/results_discussion_snapshot.md` (optional)

- **Notebook** (`notebooks/NYC_Taxi_Report.ipynb`): reads `lake/gold/*` and writes figures/tables to `reports/` (no ETL).

---

## Outputs

- **Silver**: `lake/silver/<service>/pickup_date=*`
- **Gold (batch)**:
  - `lake/gold/trip_features/`
  - `lake/gold/agg_zone_hour/`
  - `lake/gold/agg_od_hour/`
- **Gold (stream)**:
  - `lake/gold/agg_zone_hour_streaming/`
- **Reports**:
  - `reports/fig_hourly_demand.png`
  - `reports/fig_speed_by_hour.png`
  - `reports/table_top10_pickup_zones.csv`

---

## Notebooks

- `notebooks/NYC_Taxi_Report.ipynb`
  - Robust path resolution (works from `/notebooks`)
  - Safe to run multiple times; produces figures/tables only
  - Great for packaging artifacts for the paper

---

## Troubleshooting

- **Path not found**: Ensure `env/config.yaml` paths are relative to repo root. The notebook resolves them relative to `env/` automatically.
- **Streaming “Append mode not supported”**: Aggregations over streams require watermark or batch sinks; we use `foreachBatch` + dynamic partition overwrite.
- **Schema/date errors**: NYC TLC mixes `tpep_*` (yellow) and `lpep_*` (green); scripts normalize to `pickup_datetime` / `dropoff_datetime` and handle TIMESTAMP_NTZ differences.
- **No images inline in notebook**: Plots are saved to `reports/`. Display with `IPython.display.Image` or open the PNG files directly.
- **Re-running safely**: Batch writes partition-by `pickup_date`; streaming uses idempotent `foreachBatch`—you can re-run without duplicating rows.

---

## AI Assistance Disclosure

We used AI assistants—**ChatGPT**, **GitHub Copilot**, and **Microsoft Copilot**—to help check references, accelerate code debugging/refactoring, and suggest wording edits; all code, results, and claims were reviewed and verified by the authors.

---

## License & Citation

- **License**: _Add your license here (e.g., MIT, Apache-2.0)._  
- **How to cite**: If you publish results based on this repository, please include a reference to this project and the NYC TLC dataset.

---

## Acknowledgments

- NYC Taxi & Limousine Commission (TLC) for open trip record data.
- Apache Spark and PySpark communities.
- LABGDD course staff for project guidance.
