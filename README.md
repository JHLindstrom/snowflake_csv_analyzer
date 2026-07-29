# 🔱 TRISHULA
### Trident Engine for Large-Scale Session & Funnel Analytics

**TRISHULA** is a local Python application for converting, sanitizing, and
analyzing Snowflake session-path CSV exports with **DuckDB** and Parquet.
Conversion is out-of-core; result tables and web responses are bounded but
still consume memory. Use the included benchmark command to validate capacity
on the workstation that will run the analysis.

Named after the divine trident (**Trishula**), representing three unified pillars of analytical power:
1. **Out-of-Core Parquet Conversion**: Streaming disk conversion with a configurable DuckDB memory ceiling.
2. **Transition Matrix Heatmaps**: Visualizing flow intensity and event-to-event navigation pairs.
3. **Funnel Retention & Drop-off Intelligence**: Pinpointing friction bottlenecks, conversion drop-offs, and session penetration.

---

## ✨ Key Capabilities

- ⚡ **Out-of-Core Conversion**: Convert files larger than available memory, subject to local disk space and DuckDB configuration.
- 🗜️ **Parquet Storage**: Converts raw CSV exports into compressed ZSTD `.parquet` files. Compression and query performance depend on the dataset and hardware.
- 🧹 **Automatic Event Sanitization & Deduplication**:
  - `consecutive` (*Default*): Collapses repeated adjacent events (`A -> A -> A` $\rightarrow$ `A`) to eliminate noisy loop pings while preserving true state transitions.
  - `unique`: Deduplicates event paths to retain unique events per session.
  - `none`: Operates on raw un-sanitized event paths.
- 🔍 **Auto-Delimiter Detection & Trimming**: Automatically detects separators (`->`, `,`, `>`, `|`) and trims leading/trailing whitespace around event tokens.
- 💡 **Executive Insights & Session KPIs**:
  - Single-Event Bounce Rate & Total Sessions Analyzed.
  - Average, Median, 75th percentile, 90th percentile, and Max session lengths.
  - Top Session Entry (starting) and Exit (drop-off) points.
- 🔥 **Transition Matrix Heatmaps**:
  - Color-coded terminal heatmaps.
  - Exportable, self-contained HTML dashboards with print/PDF support.
- 📉 **Funnel Retention & Drop-off Intelligence**:
  - Auto-detected top N event reach & penetration analysis.
  - Ordered multi-step funnel matching, including repeated steps.
- ⚡ **Instant Interruption (`Ctrl+C`)**: Built-in signal handling for instant cancellation without lingering C++ background locks.

---

## 🛠️ Installation

```bash
# Clone the repository
git clone https://github.com/JHLindstrom/snowflake_csv_analyzer.git
cd snowflake_csv_analyzer

# Install dependencies (DuckDB, Rich, Pandas, PyArrow, FastAPI)
pip install -r requirements.txt
```

For stable command-line entry points, install the project itself:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install .
trishula --help
trishula-web --help
```

For development and tests:

```bash
pip install -e ".[test]"
pytest -q
```

---

## 🚀 Quick Start (1-Step Pipeline)

To run the entire pipeline—inspect CSV, convert to Parquet, sanitize events, calculate KPIs, generate terminal heatmaps, visual drop-off bar charts, and export a high-res HTML dashboard—in **a single command**:

```bash
python3 cli.py run-all /path/to/your_snowflake_data.csv
```

### With Custom Funnel & Deduplication Options:
```bash
python3 cli.py run-all /path/to/your_snowflake_data.csv \
  --funnel "Home,Product_View,Add_To_Cart,Checkout,Payment,Order_Confirmation" \
  --dedupe consecutive \
  --top 15
```

## 🌐 Local Web UI

```bash
trishula web
# Open http://127.0.0.1:8000
```

The supported web workflow is:

1. Upload a `.csv`, `.parquet`, or `.pq` dataset.
2. Confirm its managed size and available disk space.
3. Review KPIs, construct an ordered funnel, inspect the transition matrix, or
   search exact event tokens and contiguous subpaths.
4. Print or export the current dashboard.
5. Select **Unload Dataset** when finished. This deletes the browser-managed
   upload files for the current session.

The **Help & How-to** tab in the UI contains the required schema, dedupe and
funnel semantics, common commands, storage behavior, and troubleshooting steps.
The README remains the authoritative reference for installation, configuration,
security, and the complete CLI.

---

## 🛠️ CLI Command Reference

### 1. 🚀 `run-all` (Automated 1-Step Pipeline)
Runs the end-to-end analysis pipeline and exports a self-contained HTML dashboard.
```bash
python3 cli.py run-all data.csv [-o output.parquet] [-d delimiter] [-f funnel_steps] [-u dedupe_mode] [--html report.html]
```

### 2. 💡 `insights` (Executive KPIs & Entry/Exit Points)
Generates bounce rates, session length percentiles (p50, p75, p90, max), and top entry/exit event lists.
```bash
python3 cli.py insights output.parquet --delimiter "->"
```

### 3. 🔥 `heatmap` (Transition Matrix & HTML Dashboard)
Renders a 2D color-coded transition matrix in the terminal and exports a self-contained HTML report.
```bash
python3 cli.py heatmap output.parquet --top 8 --html dashboard.html
```

### 4. 📉 `drop-offs` (Visual Funnel Retention Report)
Displays step-by-step session counts, conversion rates, drop-off percentages, and visual retention bars.
```bash
python3 cli.py drop-offs output.parquet --funnel "Home,Product_View,Checkout"
```

### 5. 🔍 `inspect` (CSV / Parquet Schema & Metadata)
Displays row counts, file sizes, column data types, and sample rows.
```bash
python3 cli.py inspect data.csv --limit 5
```

### 6. 🗜️ `convert` (CSV to Parquet Conversion)
Converts CSV into compressed Parquet out-of-core.
```bash
python3 cli.py convert data.csv output.parquet --compression ZSTD
```

### 7. 📈 `analyze` (Event Frequencies & Path Patterns)
Ranks individual event frequencies, step-to-step transition pairs, and top navigation paths.
```bash
python3 cli.py analyze output.parquet --top 15 --dedupe consecutive
```

### 8. 🔎 `search` (Session Pattern Filtering)
Filters sessions matching specific events or subpath sequences.
```bash
python3 cli.py search output.parquet --subpath "Add_To_Cart->Checkout" --limit 20
```

### 9. 💻 `query` (Custom DuckDB SQL Execution)
Runs arbitrary DuckDB SQL queries against the dataset (`data` table).
```bash
python3 cli.py query output.parquet --sql "SELECT TOTAL_EVENTS, COUNT(*) as sessions FROM data GROUP BY 1 ORDER BY 1 LIMIT 10"
```

### 10. 🧪 `generate-mock` (Synthetic Data Benchmark Generator)
Generates synthetic Snowflake CSV test datasets for benchmarking without exposing real data.
```bash
python3 cli.py generate-mock mock_snowflake.csv --rows 25000
```

### 11. 📏 `benchmark` (Repeatable Local Capacity Test)

Generates synthetic sessions and reports conversion throughput, compression,
analysis time, peak process RSS, and disk consumption.

```bash
trishula benchmark --rows 1000000 --output-dir ./benchmark-output
```

Start with a small smoke run, then increase row counts until they represent the
largest expected local export. Benchmark on the same disk and memory
configuration used for real analysis.

---

## 🔒 Security & Data Privacy

Dataset processing is performed locally and the application does not include
telemetry. The live dashboard and generated HTML reports use self-contained
assets and do not require public font or charting CDNs.

The supported deployment is a single-user localhost process bound to
`127.0.0.1`. Custom SQL and process restart are disabled unless
`TRISHULA_TRUSTED_LOCAL_MODE=true` is set.
Enable that mode only on a trusted workstation; it deliberately exposes
powerful local capabilities.

Optional web configuration:

```bash
export TRISHULA_MAX_UPLOAD_BYTES=10737418240  # 10 GiB
export TRISHULA_DUCKDB_MEMORY_LIMIT=1GB
export TRISHULA_QUERY_TIMEOUT_SECONDS=30
export TRISHULA_MAX_QUERY_ROWS=10000
export TRISHULA_QUERY_JOB_TTL_SECONDS=3600
export TRISHULA_SESSION_TTL_SECONDS=86400
export TRISHULA_SESSION_DB=/absolute/path/to/sessions.sqlite3
export TRISHULA_TRUSTED_LOCAL_MODE=true       # only when explicitly needed
python3 cli.py web
```

Uploads are streamed to generated filenames under `uploads/`; client-provided
paths are never used as destination paths. Each browser receives an isolated
dataset session through an HttpOnly, same-site cookie. Expired sessions and
their managed upload files are removed automatically.

The dashboard reports free disk space and managed dataset size. “Unload
Dataset” clears the active session and deletes files created by browser upload.

Trusted-local custom SQL can be submitted through `/api/query/start`, monitored
through `/api/query/{job_id}`, and cancelled through
`/api/query/{job_id}/cancel`. Completed job metadata expires automatically.

Network binding is an advanced, unsupported deployment mode. It is disabled
unless `TRISHULA_ACCESS_TOKEN`,
`TRISHULA_ALLOW_NETWORK=true`, and `TRISHULA_COOKIE_SECURE=true` are all set.
Use a long random token and terminate TLS at a trusted reverse proxy.

```bash
export TRISHULA_ACCESS_TOKEN="$(openssl rand -hex 32)"
export TRISHULA_ALLOW_NETWORK=true
export TRISHULA_COOKIE_SECURE=true
python3 cli.py web --host 0.0.0.0
```

## 🧪 Tests

```bash
pip install pytest
pytest -q
python3 test_analyzer.py
```

---

## 📄 License

MIT License © [Jonas Lindström](https://github.com/JHLindstrom)
