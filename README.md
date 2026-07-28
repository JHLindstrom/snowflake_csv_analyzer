# 🔱 TRISHULA (त्रिशूल)
### Trident Engine for Large-Scale Session & Funnel Analytics

A high-performance Python engine designed to stream, convert, parse, and analyze multi-gigabyte Snowflake CSV exports (7.2GB+) using DuckDB and Polars to maintain zero memory overflow.

Named after the divine trident (**Trishula**), representing three unified pillars of analytical power:
1. **Out-of-Core Parquet Conversion** (Fast streaming from disk)
2. **Transition Matrix Heatmaps** (Visualizing flow intensity between events)
3. **Funnel Retention & Drop-off Intelligence** (Identifying friction bottlenecks)

---

## ✨ Features

- **Out-of-Core CSV to Parquet Conversion**: Converts multi-gigabyte CSV files into compressed `.parquet` format using DuckDB streaming without exceeding system RAM.
- **Event Path Analytics**: Unnests complex user event paths (default separator: `->`) to rank individual event frequencies and step transition pairs.
- **Funnel Conversion Engine**: Computes sequential step-by-step conversion rates and drop-off percentages.
- **Session Pattern Search**: Filters sessions matching specific subpath patterns or event sequences.
- **Interactive CLI & Rich Formatting**: Features clean table displays, progress indicators, and custom SQL query support.

---

## 🛠️ Installation

```bash
# Clone the repository
git clone https://github.com/<YOUR_USERNAME>/snowflake_csv_analyzer.git
cd snowflake_csv_analyzer

# Install dependencies
pip install -r requirements.txt
```

---

## 🚀 Quick Start (1-Step Pipeline)

To run the entire pipeline (inspect CSV, convert to Parquet, verify schema, and run event analytics/funnels) in **one single command**:

```bash
python3 cli.py run-all /path/to/your_snowflake_data.csv --funnel "Home,Product_View,Add_To_Cart,Checkout,Payment,Order_Confirmation"
```

---

## 🛠️ Individual Commands

### 1. Inspect CSV / Parquet File Schema
```bash
python3 cli.py inspect /path/to/snowflake_export.csv --limit 5
```

### 2. Convert 7.2GB CSV to Compressed Parquet
```bash
python3 cli.py convert /path/to/snowflake_export.csv output.parquet --compression ZSTD
```

### 3. Run Event Path & Funnel Analytics
```bash
python3 cli.py analyze output.parquet --delimiter "->" --top 15 --funnel "Home,Product_View,Add_To_Cart,Checkout,Payment,Order_Confirmation"
```

### 4. Search Sessions by Subpath Pattern
```bash
python3 cli.py search output.parquet --subpath "Add_To_Cart->Checkout" --limit 10
```

### 5. Execute Custom DuckDB SQL Query
```bash
python3 cli.py query output.parquet --sql "SELECT TOTAL_EVENTS, COUNT(*) as session_count FROM data GROUP BY 1 ORDER BY 1 LIMIT 10"
```

---

## 🔒 Privacy & Data Security

This tool operates **100% locally**. No data or telemetry is transmitted externally, ensuring full security when analyzing sensitive datasets.

---

## 📄 License

MIT License
