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
  - Exportable, self-contained HTML dashboards and deterministic local PDF reports.
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

1. Select **Load Dataset** to show or hide the upload panel, then upload a
   `.csv`, `.parquet`, or `.pq` dataset.
2. Confirm its managed size and available disk space.
3. Review KPIs, construct an ordered funnel, inspect the transition matrix, or
   search exact event tokens and contiguous subpaths.
4. Select **Export Selected Tab to PDF** to download a deterministic local PDF
   containing only the visible analysis tab. Other tabs are not loaded or
   included automatically. Help & How-to exports keep documentation cards,
   tables, and command examples together across page boundaries.
5. Select **Unload Dataset** when finished. This deletes the browser-managed
   upload files for the current session.

Opening **Funnel Retention** loads the available event names but does not start
a funnel query. A quick preset selects its steps and calculates immediately.
When constructing a custom funnel, add or remove all desired steps first and
then select **Calculate Funnel**. Funnel paths are split and deduplicated once
per query, rather than rescanning the dataset for every step.

Session Explorer displays long journeys as compact previews. Consecutive
repeated events are grouped with a multiplier, matched events and subpaths are
highlighted, and each result can be expanded independently inside a bounded
scroll area. Use **Collapse expanded journeys** to restore the compact result
set.

Uploads and analytics display a shared staged loading indicator. File uploads
show byte progress before validation and CSV-to-Parquet preparation; analytics
show their current operation and elapsed time. The indicator waits briefly
before appearing to avoid flicker on fast requests, and reports that Trishula is
still active when an operation exceeds ten seconds.

The **Help & How-to** tab in the UI contains the required schema, dedupe and
funnel semantics, common commands, storage behavior, and troubleshooting steps.
The README remains the authoritative reference for installation, configuration,
security, and the complete CLI.

The transition matrix reports whether it is calculating, empty, or unable to
load. A matrix with events but no populated cells means the selected sessions
contain no transitions between the displayed events; for example, every path
contains only one event.

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

### 12. `profile` (CPU Bottleneck Profile)

Runs the synthetic pipeline under Python's deterministic profiler and writes
benchmark JSON, a reusable `.pstats` file, and a cumulative-time summary:

```bash
trishula profile --rows 100000 --output-dir ./profile-output
```

Repeat the command with a representative production-sized export when one is
available. Synthetic results identify code-level hotspots but are not a
substitute for the final production workload.

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
export TRISHULA_DUCKDB_THREADS=4
export TRISHULA_CSV_MAX_LINE_SIZE=33554432  # 32 MiB per CSV record
export TRISHULA_MAX_CONCURRENT_ANALYTICS=1
export TRISHULA_ANALYTICS_QUEUE_TIMEOUT_SECONDS=30
export TRISHULA_QUERY_TIMEOUT_SECONDS=30
export TRISHULA_MAX_QUERY_ROWS=10000
export TRISHULA_QUERY_JOB_TTL_SECONDS=3600
export TRISHULA_SESSION_TTL_SECONDS=86400
export TRISHULA_SESSION_DB=/absolute/path/to/sessions.sqlite3
export TRISHULA_UPLOAD_DIR=/absolute/path/to/managed-uploads
export TRISHULA_TRUSTED_LOCAL_MODE=true       # only when explicitly needed
python3 cli.py web
```

For settings that should persist between terminal sessions, create an ignored
`trishula.ini` in the directory where Trishula is started:

```ini
[duckdb]
memory_limit = 4GB
threads = 4
```

The file is optional and is intentionally excluded from Git because appropriate
limits depend on the workstation. `TRISHULA_DUCKDB_MEMORY_LIMIT` and
`TRISHULA_DUCKDB_THREADS` override values from the file when they are set.
Set `TRISHULA_CONFIG_FILE=/absolute/path/to/trishula.ini` to use a configuration
file outside the working directory. An explicitly configured missing or invalid
file causes startup or the first analysis to fail with a clear error.

Uploads are streamed to generated filenames under `uploads/`; client-provided
paths are never used as destination paths. Each browser receives an isolated
dataset session through an HttpOnly, same-site cookie. Expired sessions and
their managed upload files are removed automatically.

CSV headers are mapped by meaning rather than position. Canonical
`SESSION`, `EVENT_PATH`, and `TOTAL_EVENTS` headers continue to work, while
unambiguous semantic variants are normalized during conversion. For example,
`VEHICLESESSION`, `SESSION_PATH_CAPPED_AT_TWO_REPEATS`, and
`STEP_COUNT_AFTER_REPEAT_CAP` map to the canonical analytics columns.
Unrelated columns are preserved. Files with missing or ambiguous semantic
columns are rejected instead of being guessed.

The web implementation is split by responsibility: `server.py` owns FastAPI
routes and application state, while `trishula_web/templates/` and
`trishula_web/static/` contain the dashboard shell, stylesheet, and browser
logic. `trishula_web/pdf_reports.py` owns bounded server-side PDF generation.

The dashboard loads analytics on demand when a tab is first opened instead of
starting every query after upload. DuckDB-heavy API operations share a bounded
query gate; `TRISHULA_MAX_CONCURRENT_ANALYTICS=1` is the safe default because
the DuckDB memory limit applies per connection. Increase concurrency only after
benchmarking the largest expected export on the target workstation. Requests
that cannot enter the gate within `TRISHULA_ANALYTICS_QUEUE_TIMEOUT_SECONDS`
return HTTP 503 instead of waiting indefinitely.

The Sankey Flow tab reuses the bounded transition-matrix calculation and
renders the 24 strongest links among the 10 most frequent events. Source and
destination stages are displayed separately so repeated events and cyclic
journeys remain readable without adding another DuckDB query type.

Every DuckDB connection uses `TRISHULA_DUCKDB_THREADS=4` by default. Lower it
to reduce CPU pressure and keep the workstation responsive, or raise it only
after benchmarking representative exports. Valid values are integers from 1
through 64. `TRISHULA_DUCKDB_MEMORY_LIMIT` accepts sizes such as `512MB`, `1GB`,
or `4GB`; invalid resource settings fail with a clear configuration error.
CSV records may be up to 32 MiB by default. Set
`TRISHULA_CSV_MAX_LINE_SIZE` to an integer number of bytes between 2,000,000
and 268,435,456 when a legitimate export contains larger records. Do not raise
it merely to accept a file detected as single-line; first verify that export's
row delimiters and quoting.
The active limits are shown in the dataset banner.

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
python3 -m pip install -e ".[test]"
python3 -m pytest -q
python3 test_analyzer.py
```

Browser-level tests run separately because they install an isolated Chromium:

```bash
python3 -m pip install -e ".[browser-test]"
python3 -m playwright install chromium
python3 -m pytest -q tests/test_browser.py
```

---

## 📄 License

MIT License © [Jonas Lindström](https://github.com/JHLindstrom)
