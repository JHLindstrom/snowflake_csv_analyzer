import sys
import os
import argparse
import duckdb
import pandas as pd
from typing import Optional, List

user_site = os.path.expanduser("~/Library/Python/3.9/lib/python/site-packages")
if os.path.exists(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

from fastapi import FastAPI, HTTPException, Query
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
            <h2 style="margin: 0 0 8px 0;">Load Dataset File</h2>
            <p style="color: #94a3b8; margin: 0 0 24px 0;">Enter the absolute path to your Snowflake CSV or Parquet export on your Mac:</p>
            
            <div style="display: flex; gap: 12px; max-width: 600px; margin: 0 auto;">
                <input id="filePathInput" type="text" placeholder="/path/to/your_snowflake_export.csv" style="flex: 1;" />
                <button class="btn-action" onclick="submitFileLoad()">⚡ Load Dataset</button>
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
            <div class="glass-card">
                <h3 style="color: #38bdf8;">Funnel Retention & Conversion Builder</h3>
                <p style="color: #94a3b8; font-size: 14px;">Construct and evaluate step-by-step conversion flows:</p>
                
                <div id="funnelPills" style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 20px;"></div>

                <div style="display: flex; gap: 12px;">
                    <select id="addEventSelect" onchange="addStepToFunnel(this.value)">
                        <option value="">+ Add Event Step to Funnel</option>
                    </select>
                </div>
            </div>

            <div class="glass-card">
                <h3>Step Conversion Metrics</h3>
                <table id="funnelMetricsTable">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Step Name</th>
                            <th>Sessions</th>
                            <th>Step Conversion</th>
                            <th>Step Drop-Off</th>
                            <th style="width: 30%;">Retention Bar</th>
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
            <div class="glass-card">
                <h3 style="color: #38bdf8;">Session Search & Explorer</h3>
                <div style="display: flex; gap: 16px; margin: 20px 0;">
                    <input id="searchEventInput" placeholder="Filter by event name (e.g. VehicleView)" style="flex: 1;" />
                    <input id="searchSubpathInput" placeholder="Filter by exact subpath (e.g. Search->Home)" style="flex: 1;" />
                    <button class="btn-action" onclick="runSearch()">🔎 Search Sessions</button>
                </div>

                <table id="searchTable">
                    <thead>
                        <tr>
                            <th>Session ID</th>
                            <th>Event Navigation Path</th>
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

            const select = document.getElementById('addEventSelect');
            select.innerHTML = '<option value="">+ Add Event Step to Funnel</option>' + 
                data.map(e => `<option value="${e.event_name}">${e.event_name} (${e.occurrence_count.toLocaleString()} occurrences)</option>`).join('');

            if (selectedFunnelSteps.length === 0 && data.length > 0) {
                selectedFunnelSteps = data.slice(0, 5).map(e => e.event_name);
            }
            renderFunnelPills();
            loadFunnel();
        }

        function renderFunnelPills() {
            const container = document.getElementById('funnelPills');
            container.innerHTML = selectedFunnelSteps.map((step, idx) => `
                <span class="tag-pill" style="padding: 8px 16px; font-size: 14px;">
                    #${idx+1} ${step}
                    <span style="cursor: pointer; margin-left: 8px; font-weight: bold; color: #fb7185" onclick="removeStepFromFunnel(${idx})">✕</span>
                </span>
            `).join('');
        }

        function addStepToFunnel(stepName) {
            if (stepName && !selectedFunnelSteps.includes(stepName)) {
                selectedFunnelSteps.push(stepName);
                renderFunnelPills();
                loadFunnel();
            }
        }

        function removeStepFromFunnel(idx) {
            selectedFunnelSteps.splice(idx, 1);
            renderFunnelPills();
            loadFunnel();
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
                    <td><strong>${r.step_number}</strong></td>
                    <td><strong style="color: #38bdf8">${r.step_name}</strong></td>
                    <td>${r.session_count.toLocaleString()}</td>
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

        async function runSearch() {
            const ev = document.getElementById('searchEventInput').value.trim();
            const sub = document.getElementById('searchSubpathInput').value.trim();
            let url = '/api/search?limit=25';
            if (ev) url += `&event=${encodeURIComponent(ev)}`;
            if (sub) url += `&subpath=${encodeURIComponent(sub)}`;

            const res = await fetch(url);
            const data = await res.json();

            const tbody = document.querySelector('#searchTable tbody');
            tbody.innerHTML = data.map(s => `
                <tr>
                    <td><strong style="color: #38bdf8; font-family: monospace;">${s.SESSION}</strong></td>
                    <td style="font-family: monospace; font-size: 13px;">${s.EVENT_PATH}</td>
                    <td><span class="tag-pill">${s.TOTAL_EVENTS}</span></td>
                </tr>
            `).join('');
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
