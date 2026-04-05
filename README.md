# Quake SQL

Natural-language to ClickHouse SQL demo built around GPT-5 custom grammars and `inspect-ai`.

## What is in the repo

- A FastAPI app that accepts a plain-English question, asks GPT-5 for CFG-constrained ClickHouse SQL, runs the SQL, and renders the result set.
- A ClickHouse bootstrap flow that downloads the live USGS past-month earthquake CSV, transforms it, and loads 1,000+ rows into ClickHouse.
- An `inspect-ai` eval task with 38 benchmark cases covering lookups, aggregations, temporal filters, empty-result cases, ambiguous prompts, adversarial inputs, CFG boundary probes, and SQL injection via natural language.
- 113 unit tests covering SQL validation, grammar parsing, data transforms, schema consistency, and eval helper logic — all runnable without an API key or database.
- A generated notebook at [notebooks/cfg_sql_eval.ipynb](notebooks/cfg_sql_eval.ipynb) that runs the eval suite and summarizes accuracy, hallucination control, latency, and cost.

## Dataset

Source CSV: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv`

The bootstrap script:

1. Downloads the live CSV.
2. Renames columns into snake_case.
3. Parses `time` and `updated` into UTC timestamps.
4. Derives a `region` column from the free-text `place`.
5. Loads the transformed event-grain table into ClickHouse.

Schema details are documented in [docs/schema.md](docs/schema.md).

## How It Works

### Grammar-Constrained Generation

The core idea: instead of generating free-form SQL and hoping it parses, the model's token generation is constrained by a [Lark grammar](src/quake_sql/grammar.py) at decode time via OpenAI's Responses API. The grammar defines exactly what valid ClickHouse SQL looks like for this table — allowed columns, functions, operators, and clauses. Malformed SQL and injection attacks are structurally impossible.

### Two-Layer Validation

After grammar-constrained generation, a [semantic validator](src/quake_sql/sql.py) catches things the grammar is intentionally permissive about — unknown column names, excessive LIMIT values, unwanted default filters. If validation fails, the model retries with the error message (up to 3 attempts with exponential backoff and temperature variation).

### Eval Pipeline

The benchmark runs the same 38 cases in two modes — with CFG and without — to directly measure the grammar's impact. Three scorer types separate the signal:

- **Result accuracy**: do the query results match? (column-tolerant — `SELECT *` is fine)
- **SQL equivalence**: does the query logic match? (compares WHERE/GROUP BY/ORDER BY/LIMIT, ignoring column selection and aliases)
- **Hallucination control**: does the model stay within the schema, or fabricate columns/tables?

Every expected SQL in the benchmark is verified to either parse against the grammar or be explicitly `UNSUPPORTED`.

## Local Run

Requirements:

- Docker
- Python 3.12+
- An `OPENAI_API_KEY`

Setup:

```bash
uv sync --dev --python 3.12
scripts/bootstrap_local.sh
export OPENAI_API_KEY=...
scripts/run_app.sh
```

The app will be available at `http://127.0.0.1:8000`.

Health check:

```bash
curl http://127.0.0.1:8000/api/health
```

## Tests

Unit tests run without an API key or database:

```bash
uv run pytest tests/ -v
```

113 tests covering:

- **Grammar** (32 tests): every valid clause combination accepted, DML/injection/wrong tables/LIMIT >500/`SELECT *`/unknown functions rejected
- **SQL validation** (47 tests): normalization, UNSUPPORTED detection, all rejection paths, LIMIT heuristics, injection via sqlglot
- **Data transforms** (9 tests): `derive_region` with all delimiter patterns and null/NaN inputs
- **Schema** (12 tests): tuple consistency, column docs coverage, renderer output
- **Eval helpers** (13 tests): SQL structural comparison, column projection for result-set matching

## Public Access

An ephemeral public URL can be created from the local app with:

```bash
scripts/expose_app.sh
```

This uses `localtunnel`, so the URL only stays live while the process is running.

For a durable deployment, the app is packaged with [Dockerfile](Dockerfile) and expects the same environment variables on Railway, Fly.io, or another container host. The code is also ready to point at ClickHouse Cloud by setting the `CLICKHOUSE_*` environment variables instead of using the local Docker service.

## Eval Suite

The benchmark task lives in [src/quake_sql/evals.py](src/quake_sql/evals.py) and the cases live in [evals/cases.json](evals/cases.json).

Run it from Python:

```python
from quake_sql.evals import run_eval_suite
cfg_log = run_eval_suite(use_cfg=True)
no_cfg_log = run_eval_suite(use_cfg=False)
```

Or run it directly with Inspect and choose the model at the CLI:

```bash
uv run inspect eval src/quake_sql/evals.py@quake_sql_benchmark --model openai/gpt-5.4-mini --model-cost-config evals/model_costs.json --log-dir logs/inspect
uv run inspect eval src/quake_sql/evals.py@quake_sql_benchmark_no_cfg --model openai/gpt-5.4-mini --model-cost-config evals/model_costs.json --log-dir logs/inspect
```

Or open the notebook:

```bash
jupyter notebook notebooks/cfg_sql_eval.ipynb
```

### Eval Cases (38 total)

| Category | Count | Examples |
| --- | --- | --- |
| Temporal aggregation | 2 | Count last 24h, avg magnitude 7d |
| Group by | 4 | Top regions, source networks, avg depth by type |
| Temporal filter | 2 | Yesterday queries (updated_at, magnitude_error) |
| Lookup | 3 | Strongest, deepest, most recent |
| Time series | 3 | Daily/hourly counts, max magnitude per day |
| Filtered count/aggregation | 3 | Alaska, California, azimuthal gap for mag >= 4 |
| Edge case / empty result | 3 | Nullable IS NULL, magnitude > 9, Mars events |
| Adversarial | 8 | DROP TABLE, UNION injection, subquery exfil, INSERT via NL, fake columns, fake tables, prompt override |
| CFG boundary | 4 | LIMIT at 500 cap, all aggregates, nested booleans, nullable IS NULL |
| Ambiguous | 4 | Hurricanes comparison, best network, moon phase, semantic nonsense |

### Scorers (7 total)

| Scorer | What it measures |
| --- | --- |
| `accuracy_pass` | Result-set match on shared columns (tolerates extra columns / SELECT *) |
| `sql_equivalence_pass` | Structural SQL match (WHERE/GROUP BY/ORDER BY/LIMIT, ignores SELECT list) |
| `hallucination_pass` | No fabricated columns, tables, or answers for unsupported prompts |
| `latency_budget_pass` | Response under 6 seconds |
| `cost_budget_pass` | Estimated cost under $0.01 |
| `latency_seconds` | Raw latency (mean + P95) |
| `cost_usd` | Raw cost (mean + P95) |

## Environment Variables

Core:

- `OPENAI_API_KEY`
- `OPENAI_MODEL` default: `gpt-5.4-mini`
- `CLICKHOUSE_HOST` default: `127.0.0.1`
- `CLICKHOUSE_PORT` default: `8123`
- `CLICKHOUSE_DATABASE` default: `quake_sql`

Optional pricing overrides for eval cost reporting:

- `evals/model_costs.json` is the default Inspect model cost config for the benchmark.
- `OPENAI_INPUT_COST_PER_1M`
- `OPENAI_CACHED_INPUT_COST_PER_1M`
- `OPENAI_OUTPUT_COST_PER_1M`
