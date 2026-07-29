import sys
import os
import argparse
import subprocess
import duckdb
import pandas as pd
from typing import Optional, List

user_site = os.path.expanduser("~/Library/Python/3.9/lib/python/site-packages")
if os.path.exists(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from converter import convert_csv_to_parquet, inspect_file
from event_parser import (
    detect_delimiter,
    get_event_frequencies,
    get_top_paths,
    get_transition_pairs,
    calculate_funnel,
    search_sessions,
    run_custom_query
)
from insights import (
    get_executive_summary_metrics,
    get_entry_exit_analytics,
    get_transition_matrix
)

app = FastAPI(title="Trishula Web Analytics", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global active dataset state
ACTIVE_FILE = None
ACTIVE_DELIMITER = "->"
ACTIVE_PARQUET = None

UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

def init_active_file(file_path: str, delimiter: str = "->"):
    global ACTIVE_FILE, ACTIVE_DELIMITER, ACTIVE_PARQUET
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    ACTIVE_FILE = file_path
    base, ext = os.path.splitext(file_path)
    
    if ext.lower() == ".csv":
        ACTIVE_PARQUET = f"{base}.parquet"
        # Convert to parquet if missing or out of date
        if not os.path.exists(ACTIVE_PARQUET) or os.path.getmtime(file_path) > os.path.getmtime(ACTIVE_PARQUET):
            convert_csv_to_parquet(file_path, ACTIVE_PARQUET)
    else:
        ACTIVE_PARQUET = file_path
        
    ACTIVE_DELIMITER = detect_delimiter(ACTIVE_PARQUET) if delimiter == "->" else delimiter

@app.get("/api/browse-file")
def browse_file():
    """Opens native macOS Finder file open dialog window."""
    try:
        cmd = 'osascript -e "POSIX path of (choose file with prompt \\"Select Snowflake CSV or Parquet file:\\")"'
        output = subprocess.check_output(cmd, shell=True, timeout=120).decode('utf-8').strip()
        if output and os.path.exists(output):
            init_active_file(output)
            return {"success": True, "filepath": output, "state": get_state()}
    except Exception as e:
        return {"success": False, "error": str(e)}
    return {"success": False, "error": "No file selected"}

@app.post("/api/upload-file")
async def upload_file(file: UploadFile = File(...)):
    """Uploads a CSV or Parquet file via browser file selector."""
    try:
        dest_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(dest_path, "wb") as f:
            content = await file.read()
            f.write(content)
        init_active_file(dest_path)
        return {"success": True, "filepath": dest_path, "state": get_state()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/load-file")
def load_file(payload: dict):
    file_path = payload.get("filepath", "").strip()
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=400, detail=f"File not found on machine: {file_path}")
    try:
        init_active_file(file_path)
        return get_state()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/state")
def get_state():
    if not ACTIVE_PARQUET or not os.path.exists(ACTIVE_PARQUET):
        return {"loaded": False}
    return {
        "loaded": True,
        "raw_file": ACTIVE_FILE,
        "parquet_file": ACTIVE_PARQUET,
        "delimiter": ACTIVE_DELIMITER,
        "file_size_mb": round(os.path.getsize(ACTIVE_PARQUET) / (1024**2), 2)
    }

@app.get("/api/inspect")
def inspect():
    if not ACTIVE_PARQUET:
        raise HTTPException(status_code=400, detail="No active file loaded")
    return inspect_file(ACTIVE_PARQUET, limit=5)

@app.get("/api/insights")
def insights(delimiter: Optional[str] = None):
    if not ACTIVE_PARQUET:
        raise HTTPException(status_code=400, detail="No active file loaded")
    delim = delimiter or ACTIVE_DELIMITER
    summary = get_executive_summary_metrics(ACTIVE_PARQUET)
    entry_exit = get_entry_exit_analytics(ACTIVE_PARQUET, delimiter=delim, top_n=5)
    return {
        "summary": summary,
        "entry_points": entry_exit["entry_points"].to_dict(orient="records"),
        "exit_points": entry_exit["exit_points"].to_dict(orient="records")
    }

@app.get("/api/events")
def events(top: int = 20, dedupe: str = "consecutive"):
    if not ACTIVE_PARQUET:
        raise HTTPException(status_code=400, detail="No active file loaded")
    df = get_event_frequencies(ACTIVE_PARQUET, delimiter=ACTIVE_DELIMITER, top_n=top, dedupe_mode=dedupe)
    return df.to_dict(orient="records")

@app.get("/api/heatmap")
def heatmap(top: int = 8, dedupe: str = "consecutive"):
    if not ACTIVE_PARQUET:
        raise HTTPException(status_code=400, detail="No active file loaded")
    matrix = get_transition_matrix(ACTIVE_PARQUET, delimiter=ACTIVE_DELIMITER, top_n=top)
    return {
        "columns": matrix.columns.tolist(),
        "index": matrix.index.tolist(),
        "data": matrix.values.tolist()
    }

@app.get("/api/funnel")
def funnel(steps: str, dedupe: str = "consecutive", sequential: bool = True):
    if not ACTIVE_PARQUET:
        raise HTTPException(status_code=400, detail="No active file loaded")
    step_list = [s.strip() for s in steps.split(",") if s.strip()]
    if not step_list:
        raise HTTPException(status_code=400, detail="No steps provided")
    df = calculate_funnel(ACTIVE_PARQUET, step_list, delimiter=ACTIVE_DELIMITER, sequential=sequential, dedupe_mode=dedupe)
    return df.to_dict(orient="records")

@app.get("/api/search")
def search(event: Optional[str] = None, subpath: Optional[str] = None, min_events: int = 1, limit: int = 20):
    if not ACTIVE_PARQUET:
        raise HTTPException(status_code=400, detail="No active file loaded")
    df = search_sessions(ACTIVE_PARQUET, contains_event=event, exact_subpath=subpath, min_events=min_events, limit=limit)
    return df.to_dict(orient="records")

@app.post("/api/query")
def query(payload: dict):
    if not ACTIVE_PARQUET:
        raise HTTPException(status_code=400, detail="No active file loaded")
    sql = payload.get("sql")
    if not sql:
        raise HTTPException(status_code=400, detail="Missing SQL query")
    try:
        df = run_custom_query(ACTIVE_PARQUET, sql)
        return {
            "columns": df.columns.tolist(),
            "records": df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/", response_class=HTMLResponse)
def index():
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trishula Web - Session & Funnel Intelligence Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-dark: #090d16;
            --card-bg: rgba(21, 30, 48, 0.75);
            --card-border: rgba(255, 255, 255, 0.08);
            --accent-cyan: #38bdf8;
            --accent-purple: #818cf8;
            --accent-green: #34d399;
            --accent-rose: #fb7185;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
        }

        * { box-sizing: border-box; }
        body {
            margin: 0;
            padding: 0;
            background: var(--bg-dark);
            color: var(--text-main);
            font-family: 'Outfit', -apple-system, sans-serif;
            background-image: 
                radial-gradient(at 0% 0%, rgba(56, 189, 248, 0.08) 0px, transparent 50%),
                radial-gradient(at 100% 100%, rgba(129, 140, 248, 0.08) 0px, transparent 50%);
            background-attachment: fixed;
            min-height: 100vh;
        }

        .navbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 32px;
            background: rgba(15, 23, 42, 0.85);
            backdrop-filter: blur(16px);
            border-bottom: 1px solid var(--card-border);
            position: sticky;
            top: 0;
            z-index: 100;
        }
        .brand {
            display: flex;
            align-items: center;
            gap: 12px;
            font-size: 22px;
            font-weight: 800;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, #38bdf8, #818cf8);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .nav-items { display: flex; gap: 8px; }
        .nav-btn {
            background: transparent;
            border: 1px solid transparent;
            color: var(--text-muted);
            padding: 10px 18px;
            border-radius: 10px;
            font-weight: 600;
            font-size: 14px;
            cursor: pointer;
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .nav-btn:hover { color: var(--text-main); background: rgba(255,255,255,0.05); }
        .nav-btn.active {
            background: rgba(56, 189, 248, 0.12);
            color: var(--accent-cyan);
            border-color: rgba(56, 189, 248, 0.3);
        }

        .container { max-width: 1400px; margin: 0 auto; padding: 32px; }

        .glass-card {
            background: var(--card-bg);
            backdrop-filter: blur(12px);
            border: 1px solid var(--card-border);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }

        .info-box {
            background: rgba(56, 189, 248, 0.08);
            border-left: 4px solid var(--accent-cyan);
            padding: 16px 20px;
            border-radius: 0 12px 12px 0;
            margin-bottom: 24px;
            font-size: 14px;
            line-height: 1.6;
            color: #cbd5e1;
        }
        .info-box strong { color: var(--accent-cyan); }

        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin-bottom: 28px; }
        .kpi-card {
            background: rgba(30, 41, 59, 0.5);
            border: 1px solid var(--card-border);
            padding: 20px;
            border-radius: 14px;
        }
        .kpi-title { font-size: 12px; text-transform: uppercase; color: var(--text-muted); font-weight: 700; letter-spacing: 0.05em; }
        .kpi-val { font-size: 30px; font-weight: 800; color: var(--accent-cyan); margin-top: 8px; }

        .btn-action {
            background: linear-gradient(135deg, #38bdf8, #0284c7);
            color: #0f172a;
            border: none;
            padding: 10px 20px;
            border-radius: 10px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        .btn-action:hover { opacity: 0.9; transform: scale(1.02); }

        .btn-secondary {
            background: rgba(255, 255, 255, 0.08);
            color: var(--text-main);
            border: 1px solid var(--card-border);
            padding: 10px 20px;
            border-radius: 10px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        .btn-secondary:hover { background: rgba(255, 255, 255, 0.15); }

        .tag-pill {
            background: rgba(56, 189, 248, 0.1);
            color: var(--accent-cyan);
            border: 1px solid rgba(56, 189, 248, 0.3);
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }

        .chip-btn {
            background: rgba(129, 140, 248, 0.15);
            color: #a5b4fc;
            border: 1px solid rgba(129, 140, 248, 0.3);
            padding: 6px 12px;
            border-radius: 16px;
            font-size: 13px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }
        .chip-btn:hover { background: rgba(129, 140, 248, 0.3); color: #fff; }

        .breadcrumb-pill {
            background: rgba(15, 23, 42, 0.8);
            border: 1px solid var(--card-border);
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 12px;
            font-family: 'JetBrains Mono', monospace;
            color: #e2e8f0;
            display: inline-flex;
            align-items: center;
        }
        .breadcrumb-arrow { color: #38bdf8; margin: 0 6px; font-weight: bold; }

        table { width: 100%; border-collapse: collapse; margin-top: 16px; font-size: 14px; }
        th, td { padding: 14px 18px; text-align: left; border-bottom: 1px solid var(--card-border); }
        th { background: rgba(15, 23, 42, 0.6); color: var(--text-muted); font-size: 12px; text-transform: uppercase; }

        input, select {
            background: rgba(15, 23, 42, 0.8);
            border: 1px solid var(--card-border);
            color: var(--text-main);
            padding: 10px 16px;
            border-radius: 8px;
            font-family: inherit;
            font-size: 14px;
        }
        input:focus, select:focus { outline: none; border-color: var(--accent-cyan); }

        .bar-container {
            background: rgba(255,255,255,0.05);
            border-radius: 6px;
            height: 20px;
            width: 100%;
            overflow: hidden;
        }
        .bar-fill {
            background: linear-gradient(90deg, #38bdf8, #818cf8);
            height: 100%;
            border-radius: 6px;
            transition: width 0.4s ease;
        }
        .tab-panel { display: none; }
        .tab-panel.active { display: block; }
    </style>
</head>
<body>
    <!-- Top Navbar -->
    <div class="navbar">
        <div class="brand">🔱 TRISHULA WEB</div>
        <div class="nav-items">
            <button class="nav-btn active" onclick="switchTab('overview')">📊 Executive KPIs</button>
            <button class="nav-btn" onclick="switchTab('funnel')">🎛️ Funnel Retention</button>
            <button class="nav-btn" onclick="switchTab('heatmap')">🔥 Transition Matrix</button>
            <button class="nav-btn" onclick="switchTab('search')">🔎 Session Explorer</button>
        </div>
        <div style="display: flex; gap: 10px; align-items: center;">
            <button class="nav-btn" onclick="openFileModal()">📁 Load Dataset</button>
            <select id="dedupeSelect" onchange="onDedupeChange()">
                <option value="consecutive">Dedupe: Consecutive</option>
                <option value="unique">Dedupe: Unique</option>
                <option value="none">Dedupe: None (Raw)</option>
            </select>
            <button class="btn-action" onclick="window.print()">🖨️ Export PDF</button>
        </div>
    </div>

    <div class="container">
        <!-- Dataset Loader Card -->
        <div id="loaderCard" class="glass-card" style="border: 2px solid rgba(56, 189, 248, 0.4); background: rgba(15, 23, 42, 0.95); padding: 36px; text-align: center;">
            <h2 style="margin: 0 0 8px 0;">Select Dataset File</h2>
            <p style="color: #94a3b8; margin: 0 0 24px 0;">Open your macOS Finder file dialog or choose a CSV/Parquet export:</p>
            
            <div style="display: flex; gap: 12px; max-width: 700px; margin: 0 auto; justify-content: center; flex-wrap: wrap;">
                <button class="btn-action" onclick="triggerNativeFinder()" style="padding: 12px 24px; font-size: 15px;">
                    📂 Open Finder Window...
                </button>
                <button class="btn-secondary" onclick="document.getElementById('browserFileInput').click()" style="padding: 12px 24px; font-size: 15px;">
                    📤 Upload CSV/Parquet
                </button>
                <input id="browserFileInput" type="file" accept=".csv,.parquet" style="display: none;" onchange="handleBrowserFileUpload(this.files)" />
            </div>

            <div style="margin-top: 20px; color: #64748b; font-size: 13px;">— OR ENTER LOCAL FILE PATH MANUALLY —</div>

            <div style="display: flex; gap: 12px; max-width: 600px; margin: 16px auto 0 auto;">
                <input id="filePathInput" type="text" placeholder="/path/to/your_snowflake_export.csv" style="flex: 1;" />
                <button class="btn-secondary" onclick="submitFileLoad()">⚡ Load Path</button>
            </div>

            <div id="fileError" style="color: #fb7185; margin-top: 16px; font-weight: bold; display: none;"></div>
            
            <div style="margin-top: 24px; font-size: 13px; color: #64748b;">
                💡 Quick test sample: <strong>test_synthetic_snowflake.parquet</strong>
                <button style="background: none; border: none; color: #38bdf8; cursor: pointer; margin-left: 8px; text-decoration: underline;" onclick="submitFileLoad('test_synthetic_snowflake.parquet')">Load Synthetic Sample</button>
            </div>
        </div>

        <!-- Active Dataset Banner -->
        <div id="datasetBanner" class="glass-card" style="padding: 16px 24px; display: none; justify-content: space-between; align-items: center;">
            <div>
                <span style="color: #94a3b8; font-size: 13px;">ACTIVE DATASET:</span>
                <strong id="activeFileName" style="margin-left: 8px; color: #38bdf8;">-</strong>
                <span id="activeFileSize" style="margin-left: 12px; color: #94a3b8;">-</span>
            </div>
            <span class="tag-pill">⚡ DuckDB Out-of-Core Engine</span>
        </div>

        <!-- Tab 1: Executive KPIs -->
        <div id="panel-overview" class="tab-panel active">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-title">Total Sessions Analyzed</div>
                    <div id="kpi-sessions" class="kpi-val">-</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Bounce Rate</div>
                    <div id="kpi-bounce" class="kpi-val" style="color: #34d399;">-</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Avg Events / Session</div>
                    <div id="kpi-avg" class="kpi-val">-</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Median Length (p50)</div>
                    <div id="kpi-median" class="kpi-val">-</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">90th Percentile Length</div>
                    <div id="kpi-p90" class="kpi-val" style="color: #fb7185;">-</div>
                </div>
            </div>

            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 24px;">
                <div class="glass-card">
                    <h3 style="color: #34d399;">Top Session Entry Points</h3>
                    <table id="entryTable">
                        <thead><tr><th>Entry Event</th><th>Sessions</th><th>Share %</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>

                <div class="glass-card">
                    <h3 style="color: #fb7185;">Top Session Exit Points</h3>
                    <table id="exitTable">
                        <thead><tr><th>Exit Event</th><th>Sessions</th><th>Share %</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Tab 2: Funnel Retention Builder -->
        <div id="panel-funnel" class="tab-panel">
            <!-- Explanatory Box -->
            <div class="info-box">
                💡 <strong>How Funnel Retention Analysis Works:</strong><br/>
                A <strong>Funnel</strong> measures how many user sessions complete an ordered sequence of user steps (e.g. <code>Home → Product_View → Checkout</code>). 
                The analysis tracks session progression step-by-step and calculates drop-off rates between consecutive stages. 
                Use the 1-click presets or choose steps from your dataset below to build your funnel!
            </div>

            <div class="glass-card">
                <h3 style="color: #38bdf8; margin-top: 0;">🎛️ Funnel Retention & Conversion Flow</h3>
                
                <!-- Quick Preset Flow Buttons -->
                <div style="margin-bottom: 20px;">
                    <span style="font-size: 13px; color: #94a3b8; font-weight: bold; margin-right: 10px;">QUICK PRESETS:</span>
                    <button class="chip-btn" onclick="applyFunnelPreset('top')">⚡ Top 4 Frequent Events</button>
                    <button class="chip-btn" onclick="applyFunnelPreset('checkout')" style="margin-left: 6px;">🛒 E-Commerce Checkout Flow</button>
                    <button class="chip-btn" onclick="applyFunnelPreset('search')" style="margin-left: 6px;">🔍 Search Discovery Flow</button>
                    <button class="chip-btn" onclick="clearFunnel()" style="margin-left: 6px; background: rgba(251, 113, 133, 0.15); color: #fb7185; border-color: rgba(251, 113, 133, 0.3);">🗑️ Clear All</button>
                </div>

                <!-- Funnel Step Tiles -->
                <div id="funnelPills" style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 20px; min-height: 42px; padding: 12px; background: rgba(15, 23, 42, 0.5); border-radius: 12px; border: 1px dashed var(--card-border);"></div>

                <!-- Add Step Selector -->
                <div style="display: flex; gap: 12px; align-items: center;">
                    <select id="addEventSelect" onchange="addStepToFunnel(this.value)">
                        <option value="">+ Select Event Step to Add...</option>
                    </select>
                    <span style="font-size: 13px; color: #64748b;">(Steps are evaluated in chronological order)</span>
                </div>
            </div>

            <div class="glass-card">
                <h3>Step Conversion & Retention Breakdown</h3>
                <table id="funnelMetricsTable">
                    <thead>
                        <tr>
                            <th>Step #</th>
                            <th>Event Step Name</th>
                            <th>Qualifying Sessions</th>
                            <th>Step Conversion %</th>
                            <th>Step Drop-Off %</th>
                            <th style="width: 35%;">Retention Bar</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <!-- Tab 3: Heatmap Matrix -->
        <div id="panel-heatmap" class="tab-panel">
            <div class="glass-card">
                <h3 style="color: #fb7185;">Event Transition Heatmap Matrix</h3>
                <p style="color: #94a3b8; font-size: 14px;">Source x Target event-to-event flow intensity matrix:</p>
                
                <div style="overflow-x: auto;">
                    <table id="heatmapTable" style="margin-top: 20px;">
                        <thead></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Tab 4: Session Explorer -->
        <div id="panel-search" class="tab-panel">
            <!-- Explanatory Box -->
            <div class="info-box">
                🔍 <strong>How the Session Explorer Works:</strong><br/>
                The <strong>Session Explorer</strong> allows you to inspect actual user navigation journeys. You can search for sessions containing specific event actions or filter by exact subpath sequences (e.g. <code>Search → Home</code>). Click any pre-populated quick filter below to explore sessions instantly!
            </div>

            <div class="glass-card">
                <h3 style="color: #38bdf8; margin-top: 0;">🔎 Session Search & Journey Inspector</h3>
                
                <!-- Pre-populated Quick Filter Chips -->
                <div style="margin-bottom: 20px;">
                    <span style="font-size: 13px; color: #94a3b8; font-weight: bold; margin-right: 10px;">PRE-POPULATED QUICK FILTERS:</span>
                    <div id="quickSearchChips" style="display: inline-flex; flex-wrap: wrap; gap: 8px; margin-top: 8px;"></div>
                </div>

                <div style="display: flex; gap: 16px; margin: 20px 0;">
                    <input id="searchEventInput" placeholder="Filter by event name (e.g. VehicleView or Search)" style="flex: 1;" />
                    <input id="searchSubpathInput" placeholder="Filter by subpath sequence (e.g. Search->Home)" style="flex: 1;" />
                    <button class="btn-action" onclick="runSearch()">🔎 Search Sessions</button>
                </div>

                <table id="searchTable">
                    <thead>
                        <tr>
                            <th>Session ID</th>
                            <th>User Event Navigation Journey (Breadcrumbs)</th>
                            <th>Total Events</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        let stateData = null;
        let selectedFunnelSteps = [];
        let allTopEventsList = [];

        window.addEventListener('DOMContentLoaded', () => {
            fetchState();
        });

        async function fetchState() {
            try {
                const res = await fetch('/api/state');
                stateData = await res.json();
                if (stateData.loaded) {
                    document.getElementById('loaderCard').style.display = 'none';
                    document.getElementById('datasetBanner').style.display = 'flex';
                    document.getElementById('activeFileName').innerText = stateData.parquet_file;
                    document.getElementById('activeFileSize').innerText = `(${stateData.file_size_mb} MB)`;
                    loadAllData();
                } else {
                    document.getElementById('loaderCard').style.display = 'block';
                    document.getElementById('datasetBanner').style.display = 'none';
                }
            } catch (err) {
                console.error(err);
            }
        }

        async function triggerNativeFinder() {
            const errDiv = document.getElementById('fileError');
            errDiv.style.display = 'none';

            try {
                const res = await fetch('/api/browse-file');
                const data = await res.json();
                if (data.success && data.filepath) {
                    document.getElementById('filePathInput').value = data.filepath;
                    fetchState();
                } else if (data.error && data.error !== 'No file selected') {
                    throw new Error(data.error);
                }
            } catch (err) {
                errDiv.innerText = err.message;
                errDiv.style.display = 'block';
            }
        }

        async function handleBrowserFileUpload(files) {
            if (!files || files.length === 0) return;
            const file = files[0];
            const errDiv = document.getElementById('fileError');
            errDiv.style.display = 'none';

            const formData = new FormData();
            formData.append('file', file);

            try {
                const res = await fetch('/api/upload-file', {
                    method: 'POST',
                    body: formData
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.detail || 'Upload failed');
                fetchState();
            } catch (err) {
                errDiv.innerText = err.message;
                errDiv.style.display = 'block';
            }
        }

        async function submitFileLoad(explicitPath) {
            const path = explicitPath || document.getElementById('filePathInput').value.trim();
            if (!path) return;

            const errDiv = document.getElementById('fileError');
            errDiv.style.display = 'none';

            try {
                const res = await fetch('/api/load-file', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ filepath: path })
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.detail || 'Failed to load file');
                fetchState();
            } catch (err) {
                errDiv.innerText = err.message;
                errDiv.style.display = 'block';
            }
        }

        function openFileModal() {
            document.getElementById('loaderCard').style.display = 'block';
        }

        function switchTab(tabName) {
            document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));

            if (event && event.target) {
                event.target.classList.add('active');
            }
            document.getElementById(`panel-${tabName}`).classList.add('active');
        }

        function onDedupeChange() {
            if (stateData && stateData.loaded) {
                loadAllData();
            }
        }

        function loadAllData() {
            loadInsights();
            loadEvents();
            loadHeatmap();
            runSearch();
        }

        async function loadInsights() {
            const res = await fetch('/api/insights');
            const data = await res.json();

            document.getElementById('kpi-sessions').innerText = data.summary.total_sessions.toLocaleString();
            document.getElementById('kpi-bounce').innerText = `${data.summary.bounce_rate_pct}%`;
            document.getElementById('kpi-avg').innerText = data.summary.avg_events_per_session;
            document.getElementById('kpi-median').innerText = `${data.summary.median_events} events`;
            document.getElementById('kpi-p90').innerText = `${data.summary.p90_events} events`;

            // Entry Table
            const entryTbody = document.querySelector('#entryTable tbody');
            entryTbody.innerHTML = data.entry_points.map(e => `
                <tr>
                    <td><strong style="color: #f8fafc">${e.event_name}</strong></td>
                    <td>${e.entry_count.toLocaleString()}</td>
                    <td><span class="tag-pill">${e.entry_share_pct}%</span></td>
                </tr>
            `).join('');

            // Exit Table
            const exitTbody = document.querySelector('#exitTable tbody');
            exitTbody.innerHTML = data.exit_points.map(e => `
                <tr>
                    <td><strong style="color: #f8fafc">${e.event_name}</strong></td>
                    <td>${e.exit_count.toLocaleString()}</td>
                    <td><span class="tag-pill" style="color: #fb7185; border-color: rgba(251,113,133,0.3)">${e.exit_share_pct}%</span></td>
                </tr>
            `).join('');
        }

        async function loadEvents() {
            const dedupe = document.getElementById('dedupeSelect').value;
            const res = await fetch(`/api/events?dedupe=${dedupe}`);
            const data = await res.json();

            allTopEventsList = data;

            // Pre-populate Add Event Dropdown
            const select = document.getElementById('addEventSelect');
            select.innerHTML = '<option value="">+ Select Event Step to Add...</option>' + 
                data.map(e => `<option value="${e.event_name}">${e.event_name} (${e.occurrence_count.toLocaleString()} occurrences)</option>`).join('');

            // Auto-populate Funnel if empty
            if (selectedFunnelSteps.length === 0 && data.length > 0) {
                selectedFunnelSteps = data.slice(0, 4).map(e => e.event_name);
            }

            // Populate Session Explorer Quick Filters
            const chipsDiv = document.getElementById('quickSearchChips');
            chipsDiv.innerHTML = data.slice(0, 6).map(e => `
                <button class="chip-btn" onclick="applySearchFilter('${e.event_name}', '')">Event: ${e.event_name}</button>
            `).join('');

            renderFunnelPills();
            loadFunnel();
        }

        function renderFunnelPills() {
            const container = document.getElementById('funnelPills');
            if (selectedFunnelSteps.length === 0) {
                container.innerHTML = `<span style="color: #64748b; font-size: 13px;">No funnel steps selected. Click a preset above or add a step!</span>`;
                return;
            }
            container.innerHTML = selectedFunnelSteps.map((step, idx) => `
                <span class="tag-pill" style="padding: 8px 16px; font-size: 14px; background: rgba(56, 189, 248, 0.15);">
                    <strong>#${idx+1}</strong> ${step}
                    <span style="cursor: pointer; margin-left: 8px; font-weight: bold; color: #fb7185" onclick="removeStepFromFunnel(${idx})">✕</span>
                </span>
            `).join('');
        }

        function applyFunnelPreset(presetType) {
            if (presetType === 'top') {
                selectedFunnelSteps = allTopEventsList.slice(0, 4).map(e => e.event_name);
            } else if (presetType === 'checkout') {
                const checkoutCandidates = ['Home', 'Search', 'Product_View', 'Add_To_Cart', 'Checkout', 'Payment', 'Order_Confirmation'];
                selectedFunnelSteps = checkoutCandidates.filter(c => allTopEventsList.some(e => e.event_name === c));
                if (selectedFunnelSteps.length === 0) selectedFunnelSteps = allTopEventsList.slice(0, 4).map(e => e.event_name);
            } else if (presetType === 'search') {
                const searchCandidates = ['Search', 'Product_View', 'Category_Browse', 'Add_To_Cart'];
                selectedFunnelSteps = searchCandidates.filter(c => allTopEventsList.some(e => e.event_name === c));
                if (selectedFunnelSteps.length === 0) selectedFunnelSteps = allTopEventsList.slice(0, 3).map(e => e.event_name);
            }
            renderFunnelPills();
            loadFunnel();
        }

        function clearFunnel() {
            selectedFunnelSteps = [];
            renderFunnelPills();
            const tbody = document.querySelector('#funnelMetricsTable tbody');
            tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: #64748b;">Add at least one step to compute funnel metrics.</td></tr>`;
        }

        function addStepToFunnel(stepName) {
            if (stepName && !selectedFunnelSteps.includes(stepName)) {
                selectedFunnelSteps.push(stepName);
                renderFunnelPills();
                loadFunnel();
            }
            document.getElementById('addEventSelect').value = "";
        }

        function removeStepFromFunnel(idx) {
            selectedFunnelSteps.splice(idx, 1);
            renderFunnelPills();
            if (selectedFunnelSteps.length > 0) loadFunnel();
            else clearFunnel();
        }

        async function loadFunnel() {
            if (selectedFunnelSteps.length === 0) return;
            const dedupe = document.getElementById('dedupeSelect').value;
            const stepsParam = selectedFunnelSteps.join(',');
            const res = await fetch(`/api/funnel?steps=${encodeURIComponent(stepsParam)}&dedupe=${dedupe}`);
            const data = await res.json();

            const tbody = document.querySelector('#funnelMetricsTable tbody');
            tbody.innerHTML = data.map(r => `
                <tr>
                    <td><strong>#${r.step_number}</strong></td>
                    <td><strong style="color: #38bdf8">${r.step_name}</strong></td>
                    <td><strong>${r.session_count.toLocaleString()}</strong></td>
                    <td><span class="tag-pill" style="color: #34d399">${r.step_conversion_pct}%</span></td>
                    <td><span class="tag-pill" style="color: #fb7185; border-color: rgba(251,113,133,0.3)">${r.step_dropoff_pct}%</span></td>
                    <td>
                        <div class="bar-container">
                            <div class="bar-fill" style="width: ${r.step_conversion_pct}%"></div>
                        </div>
                    </td>
                </tr>
            `).join('');
        }

        async function loadHeatmap() {
            const dedupe = document.getElementById('dedupeSelect').value;
            const res = await fetch(`/api/heatmap?dedupe=${dedupe}`);
            const data = await res.json();

            const thead = document.querySelector('#heatmapTable thead');
            thead.innerHTML = `<tr><th>From / To</th>${data.columns.map(c => `<th>${c}</th>`).join('')}</tr>`;

            const maxVal = Math.max(...data.data.flat());
            const tbody = document.querySelector('#heatmapTable tbody');
            tbody.innerHTML = data.index.map((rowLabel, rIdx) => `
                <tr>
                    <td><strong style="color: #38bdf8">${rowLabel}</strong></td>
                    ${data.data[rIdx].map(val => {
                        const bg = val > 0 ? `rgba(56, 189, 248, ${Math.max(0.15, val / maxVal)})` : 'rgba(30, 41, 59, 0.4)';
                        return `<td style="background: ${bg}; text-align: center; font-weight: bold;">${val > 0 ? val.toLocaleString() : '-'}</td>`;
                    }).join('')}
                </tr>
            `).join('');
        }

        function applySearchFilter(eventVal, subpathVal) {
            document.getElementById('searchEventInput').value = eventVal;
            document.getElementById('searchSubpathInput').value = subpathVal;
            runSearch();
        }

        async function runSearch() {
            const ev = document.getElementById('searchEventInput').value.trim();
            const sub = document.getElementById('searchSubpathInput').value.trim();
            let url = '/api/search?limit=25';
            if (ev) url += `&event=${encodeURIComponent(ev)}`;
            if (sub) url += `&subpath=${encodeURIComponent(sub)}`;

            const res = await fetch(url);
            const data = await res.json();

            const tbody = document.querySelector('#searchTable tbody');
            if (!data || data.length === 0) {
                tbody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: #64748b;">No matching sessions found.</td></tr>`;
                return;
            }

            tbody.innerHTML = data.map(s => {
                const steps = s.EVENT_PATH.split('->');
                const breadcrumbs = steps.map((st, i) => `
                    <span class="breadcrumb-pill">${st}</span>
                `).join('<span class="breadcrumb-arrow">➔</span>');

                return `
                    <tr>
                        <td><strong style="color: #38bdf8; font-family: monospace; font-size: 13px;">${s.SESSION}</strong></td>
                        <td>${breadcrumbs}</td>
                        <td><span class="tag-pill">${s.TOTAL_EVENTS} events</span></td>
                    </tr>
                `;
            }).join('');
        }
    </script>
</body>
</html>
"""

def main():
    parser = argparse.ArgumentParser(description="Trishula Web Dashboard Server")
    parser.add_argument("--file", "-f", default=None, help="Optional CSV or Parquet dataset file path")
    parser.add_argument("--port", "-p", type=int, default=8000, help="Port to run web server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host binding")
    args = parser.parse_args()

    if args.file and os.path.exists(args.file):
        init_active_file(args.file)
        print(f"[*] Pre-loaded dataset: '{args.file}'")

    print(f"\n🚀 TRISHULA WEB Dashboard running at: http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
