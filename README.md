# Quake SQL

Natural-language → ClickHouse SQL, powered by GPT-5 grammar-constrained generation.

Ask a plain-English question about earthquakes, get validated SQL, see results.

## Quick Start

```bash
cp .env.example .env          # add your OPENAI_API_KEY + ClickHouse creds
uv sync --dev --python 3.12
scripts/bootstrap_local.sh    # starts ClickHouse, loads USGS data
scripts/run_app.sh            # http://127.0.0.1:8000
```

## Tests

```bash
uv run pytest tests/ -v       # no API key or database needed
```

## Deploy

The app ships with a [Dockerfile](Dockerfile). Set environment variables on your host (see [.env.example](.env.example)) and point `CLICKHOUSE_*` at a ClickHouse Cloud instance.

## Evals

10 benchmark cases (top-10 subset) comparing CFG vs free-form generation, scored on accuracy, hallucination, latency, and cost. A full 48-case suite is also available. See [evals/cases.json](evals/cases.json) and [evals/cases_full48.json](evals/cases_full48.json).

```bash
uv run inspect eval src/quake_sql/evals.py@quake_sql_benchmark --model openai/gpt-5.4-mini
```
