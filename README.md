# 🔱 TRISHULA
### Trident Engine for Large-Scale Session & Funnel Analytics

**TRISHULA** is a high-performance Python engine designed to stream, convert, parse, sanitize, and analyze multi-gigabyte Snowflake CSV exports (7.2GB+) using **DuckDB** and **Polars** with zero memory overflow.

Named after the divine trident (**Trishula**), representing three unified pillars of analytical power:
1. **Out-of-Core Parquet Conversion**: Fast streaming disk conversion with zero RAM overflow.
2. **Transition Matrix Heatmaps**: Visualizing flow intensity and event-to-event navigation pairs.
3. **Funnel Retention & Drop-off Intelligence**: Pinpointing friction bottlenecks, conversion drop-offs, and session penetration.

---

## ✨ Key Capabilities

- ⚡ **Out-of-Core Streaming Architecture**: Process multi-gigabyte files (50M+ rows) without loading full datasets into RAM.
- 🗜️ **Fast Parquet Storage**: Converts raw CSV exports into compressed ZSTD `.parquet` files, achieving **80%+ space savings** and **100x query speedups**.
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
  - Exportable **high-resolution interactive Chart.js HTML Dashboards** with single-click print/PDF capabilities.
- 📉 **Funnel Retention & Drop-off Intelligence**:
  - Auto-detected top N event reach & penetration analysis.
  - Custom multi-step sequential conversion funnel matching using ultra-fast C++ `list_position` array indexing.
- ⚡ **Instant Interruption (`Ctrl+C`)**: Built-in signal handling for instant cancellation without lingering C++ background locks.

---

## 🛠️ Installation

```bash
# Clone the repository
git clone https://github.com/JHLindstrom/snowflake_csv_analyzer.git
cd snowflake_csv_analyzer

# Install dependencies (DuckDB, Polars, Rich, Pandas, PyArrow)
pip install -r requirements.txt
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

---

## 🛠️ CLI Command Reference

### 1. 🚀 `run-all` (Automated 1-Step Pipeline)
Runs the end-to-end analysis pipeline and exports an interactive HTML dashboard.
```bash
python3 cli.py run-all data.csv [-o output.parquet] [-d delimiter] [-f funnel_steps] [-u dedupe_mode] [--html report.html]
```

### 2. 💡 `insights` (Executive KPIs & Entry/Exit Points)
Generates bounce rates, session length percentiles (p50, p75, p90, max), and top entry/exit event lists.
```bash
python3 cli.py insights output.parquet --delimiter "->"
```

### 3. 🔥 `heatmap` (Transition Matrix & HTML Dashboard)
Renders a 2D color-coded transition matrix in the terminal and exports an interactive HTML report.
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

---

## 🔒 Security & Data Privacy

This tool operates **100% locally on your machine**. No data, metrics, or telemetry are ever transmitted externally. All testing and development are conducted using locally generated synthetic mock data.

---

## 📄 License

MIT License © [Jonas Lindström](https://github.com/JHLindstrom)
