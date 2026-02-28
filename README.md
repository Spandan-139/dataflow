[![Live Demo](https://img.shields.io/badge/🤗%20Live%20Demo-DataFlow-blue)](https://huggingface.co/spaces/Spandan-139/dataflow)
[![CI](https://github.com/Spandan-139/dataflow/actions/workflows/ci.yml/badge.svg)](https://github.com/Spandan-139/dataflow/actions)

# 🔄 DataFlow

A production-style ELT Analytics Platform built on GitHub Archive data with medallion architecture, Prefect orchestration, and DuckDB warehouse.

---

## Architecture
```
GitHub Archive (real events — pushes, PRs, stars, forks)
          │
          ▼
┌─────────────────────────┐
│    Prefect ETL Flow     │  Orchestration + retries
└─────────────────────────┘
          │
    ┌─────┴──────┐
    ▼            ▼
🥉 Bronze     🥈 Silver
Parquet       Parquet
Raw typed     Cleaned +
events        enriched
    │            │
    └─────┬──────┘
          ▼
      🥇 Gold
      Parquet
      Aggregated
      analytics
          │
          ▼
┌─────────────────────────┐
│    DuckDB Warehouse     │  SQL-queryable consolidation
└─────────────────────────┘
          │
    ┌─────┴──────┐
    ▼            ▼
FastAPI API   Streamlit
Analytics     Dashboard
REST API      Visualizations
```

---

## Medallion Layers

**🥉 Bronze** — Raw GitHub Archive events ingested as-is. Schema enforced, typed, partitioned by date/hour. No business logic.

**🥈 Silver** — Cleaned and enriched. Null filtering, repo owner/name splitting, event categorization (code/review/issues/social), org event flagging, temporal features.

**🥇 Gold** — Aggregated analytics tables. Top repos, event distributions, contributor rankings, hourly activity patterns. Optimized for query performance.

**🏛️ Warehouse** — DuckDB consolidates all Gold Parquet files into a single SQL-queryable database. Sub-second query times on 150k+ events.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Prefect 3 |
| Ingestion | httpx + async |
| Processing | Polars |
| Storage | Parquet (Bronze/Silver/Gold) |
| Warehouse | DuckDB |
| API | FastAPI |
| Dashboard | Streamlit + Plotly |
| Dataset | GitHub Archive (gharchive.org) |

---

## Quick Start

### 1. Clone and install
```bash
git clone https://github.com/Spandan-139/dataflow.git
cd dataflow
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
pip install -e .
```

### 2. Run the ETL pipeline
```bash
python flows/etl_flow.py
```

### 3. Build the warehouse
```bash
make warehouse
```

### 4. Start the API
```bash
make api
# → http://localhost:8000/docs
```

### 5. Start the dashboard
```bash
make dashboard
# → http://localhost:8501
```

---

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /summary` | Total events, repos, contributors |
| `GET /repos?limit=10` | Top repos by activity |
| `GET /events` | Event type distribution |
| `GET /activity` | Hourly activity patterns |
| `GET /contributors?limit=10` | Top contributors |
| `POST /warehouse/rebuild` | Rebuild warehouse from Gold |

---

## Dataset

GitHub Archive records all public GitHub events. Each hourly file contains ~100k-200k events including pushes, pull requests, issues, stars, forks, and more.

- Source: [gharchive.org](https://www.gharchive.org)
- Format: newline-delimited JSON, gzipped
- Volume: ~150k-200k events per hour
- Coverage: All public GitHub activity since 2011

---

## Project Structure
```
dataflow/
├── flows/
│   └── etl_flow.py          # Prefect DAG
├── src/
│   ├── ingestion/
│   │   └── gharchive.py     # GH Archive downloader + parser
│   ├── pipeline/
│   │   ├── bronze.py        # Raw → Parquet
│   │   ├── silver.py        # Clean + enrich
│   │   └── gold.py          # Aggregate analytics
│   ├── warehouse/
│   │   └── db.py            # DuckDB warehouse
│   ├── api/
│   │   └── main.py          # FastAPI analytics API
│   └── dashboard/
│       └── app.py           # Streamlit dashboard
├── tests/
├── Makefile
└── requirements.txt
```

---

## Author

**Spandan** — B.Tech CSE @ SRMIST  
GitHub: [@Spandan-139](https://github.com/Spandan-139)