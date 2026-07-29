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
    <!-- React 18 & Babel -->
    <script src="https://unpkg.com/react@18/umd/react.production.min.js" crossorigin></script>
    <script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js" crossorigin></script>
    <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
    <!-- Chart.js -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <!-- Google Fonts & FontAwesome -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    
    <style>
        :root {
            --bg-dark: #090d16;
            --card-bg: rgba(21, 30, 48, 0.7);
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
            background: rgba(15, 23, 42, 0.8);
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
            transition: transform 0.2s;
        }
        .kpi-card:hover { transform: translateY(-2px); }
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

        .heatmap-grid {
            display: grid;
            gap: 4px;
            overflow-x: auto;
            margin-top: 20px;
        }
        .cell {
            padding: 14px;
            text-align: center;
            border-radius: 6px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 13px;
            font-weight: 600;
            transition: all 0.15s;
        }
        .cell:hover { transform: scale(1.05); z-index: 10; cursor: pointer; box-shadow: 0 0 12px rgba(56,189,248,0.4); }

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
    </style>
</head>
<body>
    <div id="root"></div>

    <script type="text/babel">
        const { useState, useEffect, useRef } = React;

        function App() {
            const [activeTab, setActiveTab] = useState('overview');
            const [state, setState] = useState(null);
            const [insights, setInsights] = useState(null);
            const [funnelData, setFunnelData] = useState([]);
            const [heatmapData, setHeatmapData] = useState(null);
            const [searchResults, setSearchResults] = useState([]);
            const [topEvents, setTopEvents] = useState([]);
            
            const [selectedFunnel, setSelectedFunnel] = useState([]);
            const [dedupeMode, setDedupeMode] = useState('consecutive');
            const [searchEvent, setSearchEvent] = useState('');
            const [searchSubpath, setSearchSubpath] = useState('');
            const [inputFilePath, setInputFilePath] = useState('');
            const [loadingFile, setLoadingFile] = useState(false);
            const [fileError, setFileError] = useState('');
            const [showFileModal, setShowFileModal] = useState(false);
            
            const chartRef = useRef(null);
            const chartInstance = useRef(null);

            useEffect(() => {
                fetchState();
            }, []);

            useEffect(() => {
                if (state && state.loaded) {
                    loadInsights();
                    loadEvents();
                    loadHeatmap();
                }
            }, [state, dedupeMode]);

            useEffect(() => {
                if (selectedFunnel.length > 0) {
                    loadFunnel();
                }
            }, [selectedFunnel, dedupeMode]);

            const fetchState = async () => {
                const res = await fetch('/api/state');
                const data = await res.json();
                setState(data);
                if (!data.loaded) {
                    setShowFileModal(true);
                }
            };

            const handleLoadFile = async (pathToLoad) => {
                const target = pathToLoad || inputFilePath;
                if (!target) return;
                setLoadingFile(true);
                setFileError('');
                try {
                    const res = await fetch('/api/load-file', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ filepath: target })
                    });
                    const data = await res.json();
                    if (!res.ok) throw new Error(data.detail || 'Failed to load file');
                    setState(data);
                    setShowFileModal(false);
                } catch (err) {
                    setFileError(err.message);
                } finally {
                    setLoadingFile(false);
                }
            };

            const loadInsights = async () => {
                const res = await fetch('/api/insights');
                const data = await res.json();
                setInsights(data);
            };

            const loadEvents = async () => {
                const res = await fetch(`/api/events?dedupe=${dedupeMode}`);
                const data = await res.json();
                setTopEvents(data);
                if (selectedFunnel.length === 0 && data.length > 0) {
                    setSelectedFunnel(data.slice(0, 5).map(e => e.event_name));
                }
            };

            const loadHeatmap = async () => {
                const res = await fetch(`/api/heatmap?dedupe=${dedupeMode}`);
                const data = await res.json();
                setHeatmapData(data);
            };

            const loadFunnel = async () => {
                const steps = selectedFunnel.join(',');
                const res = await fetch(`/api/funnel?steps=${encodeURIComponent(steps)}&dedupe=${dedupeMode}`);
                const data = await res.json();
                setFunnelData(data);
                renderFunnelChart(data);
            };

            const handleSearch = async () => {
                let url = `/api/search?limit=25`;
                if (searchEvent) url += `&event=${encodeURIComponent(searchEvent)}`;
                if (searchSubpath) url += `&subpath=${encodeURIComponent(searchSubpath)}`;
                const res = await fetch(url);
                const data = await res.json();
                setSearchResults(data);
            };

            const renderFunnelChart = (data) => {
                if (!chartRef.current) return;
                if (chartInstance.current) chartInstance.current.destroy();

                const ctx = chartRef.current.getContext('2d');
                chartInstance.current = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: data.map(d => d.step_name),
                        datasets: [{
                            label: 'Sessions Reaching Step',
                            data: data.map(d => d.session_count),
                            backgroundColor: 'rgba(56, 189, 248, 0.7)',
                            borderColor: '#38bdf8',
                            borderWidth: 2,
                            borderRadius: 8
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false }
                        },
                        scales: {
                            x: { grid: { display: false }, ticks: { color: '#94a3b8' } },
                            y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#94a3b8' } }
                        }
                    }
                });
            };

            const getHeatmapBg = (val, max) => {
                if (!val || max === 0) return 'rgba(30, 41, 59, 0.4)';
                const ratio = val / max;
                return `rgba(56, 189, 248, ${Math.max(0.15, ratio)})`;
            };

            return (
                <div>
                    <!-- Top Navbar -->
                    <div className="navbar">
                        <div className="brand">
                            <i className="fa-solid fa-trident"></i> TRISHULA WEB
                        </div>
                        <div className="nav-items">
                            <button className={`nav-btn ${activeTab === 'overview' ? 'active' : ''}`} onClick={() => setActiveTab('overview')}>
                                <i className="fa-solid fa-chart-pie"></i> Executive KPIs
                            </button>
                            <button className={`nav-btn ${activeTab === 'funnel' ? 'active' : ''}`} onClick={() => setActiveTab('funnel')}>
                                <i className="fa-solid fa-filter"></i> Funnel Retention
                            </button>
                            <button className={`nav-btn ${activeTab === 'heatmap' ? 'active' : ''}`} onClick={() => setActiveTab('heatmap')}>
                                <i className="fa-solid fa-fire"></i> Transition Matrix
                            </button>
                            <button className={`nav-btn ${activeTab === 'search' ? 'active' : ''}`} onClick={() => setActiveTab('search')}>
                                <i className="fa-solid fa-magnifying-glass"></i> Session Explorer
                            </button>
                        </div>
                        <div style={{display: 'flex', gap: '10px', alignItems: 'center'}}>
                            <button className="nav-btn" onClick={() => setShowFileModal(true)}>
                                <i className="fa-solid fa-folder-open"></i> Load Dataset
                            </button>
                            <select value={dedupeMode} onChange={(e) => setDedupeMode(e.target.value)}>
                                <option value="consecutive">Dedupe: Consecutive</option>
                                <option value="unique">Dedupe: Unique</option>
                                <option value="none">Dedupe: None (Raw)</option>
                            </select>
                            <button className="btn-action" onClick={() => window.print()}>
                                <i className="fa-solid fa-print"></i> Export PDF
                            </button>
                        </div>
                    </div>

                    <div className="container">
                        <!-- File Picker Modal / Banner -->
                        {(!state || !state.loaded || showFileModal) && (
                            <div className="glass-card" style={{border: '2px solid rgba(56, 189, 248, 0.4)', background: 'rgba(15, 23, 42, 0.95)', padding: '36px', textAlign: 'center'}}>
                                <i className="fa-solid fa-file-csv" style={{fontSize: '48px', color: '#38bdf8', marginBottom: '16px'}}></i>
                                <h2 style={{margin: '0 0 8px 0'}}>Load Dataset File</h2>
                                <p style={{color: '#94a3b8', margin: '0 0 24px 0'}}>Enter the absolute path to your Snowflake CSV or Parquet export on your Mac:</p>
                                
                                <div style={{display: 'flex', gap: '12px', maxWidth: '600px', margin: '0 auto'}}>
                                    <input 
                                        type="text" 
                                        placeholder="/path/to/your_snowflake_export.csv" 
                                        value={inputFilePath}
                                        onChange={(e) => setInputFilePath(e.target.value)}
                                        style={{flex: 1, padding: '12px 16px', fontSize: '15px'}}
                                    />
                                    <button className="btn-action" onClick={() => handleLoadFile()} disabled={loadingFile}>
                                        {loadingFile ? <i className="fa-solid fa-spinner fa-spin"></i> : <i className="fa-solid fa-bolt"></i>} Load Dataset
                                    </button>
                                </div>

                                {fileError && <p style={{color: '#fb7185', marginTop: '16px', fontWeight: 'bold'}}>{fileError}</p>}
                                
                                <div style={{marginTop: '24px', fontSize: '13px', color: '#64748b'}}>
                                    💡 Quick test sample: <strong>test_synthetic_snowflake.csv</strong> or <strong>test_synthetic_snowflake.parquet</strong>
                                    <button style={{background: 'none', border: 'none', color: '#38bdf8', cursor: 'pointer', marginLeft: '8px', textDecoration: 'underline'}} onClick={() => handleLoadFile('test_synthetic_snowflake.parquet')}>Load Synthetic Sample</button>
                                </div>
                            </div>
                        )}
                        <!-- File Status -->
                        {state && state.loaded && (
                            <div className="glass-card" style={{padding: '16px 24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                                <div>
                                    <span style={{color: '#94a3b8', fontSize: '13px'}}>ACTIVE DATASET:</span>
                                    <strong style={{marginLeft: '8px', color: '#38bdf8'}}>{state.parquet_file}</strong>
                                    <span style={{marginLeft: '12px', color: '#94a3b8'}}>({state.file_size_mb} MB)</span>
                                </div>
                                <span className="tag-pill"><i className="fa-solid fa-bolt"></i> DuckDB Out-of-Core Engine</span>
                            </div>
                        )}

                        <!-- Tab 1: Executive KPIs -->
                        {activeTab === 'overview' && insights && (
                            <div>
                                <div className="kpi-grid">
                                    <div className="kpi-card">
                                        <div className="kpi-title">Total Sessions Analyzed</div>
                                        <div className="kpi-val">{insights.summary.total_sessions.toLocaleString()}</div>
                                    </div>
                                    <div className="kpi-card">
                                        <div className="kpi-title">Bounce Rate</div>
                                        <div className="kpi-val" style={{color: '#34d399'}}>{insights.summary.bounce_rate_pct}%</div>
                                    </div>
                                    <div className="kpi-card">
                                        <div className="kpi-title">Avg Events / Session</div>
                                        <div className="kpi-val">{insights.summary.avg_events_per_session}</div>
                                    </div>
                                    <div className="kpi-card">
                                        <div className="kpi-title">Median Length (p50)</div>
                                        <div className="kpi-val">{insights.summary.median_events} events</div>
                                    </div>
                                    <div className="kpi-card">
                                        <div className="kpi-title">90th Percentile Length</div>
                                        <div className="kpi-val" style={{color: '#fb7185'}}>{insights.summary.p90_events} events</div>
                                    </div>
                                </div>

                                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px'}}>
                                    <div className="glass-card">
                                        <h3><i className="fa-solid fa-right-to-bracket" style={{color: '#34d399'}}></i> Top Session Entry Points</h3>
                                        <table>
                                            <thead>
                                                <tr><th>Entry Event</th><th>Sessions</th><th>Share %</th></tr>
                                            </thead>
                                            <tbody>
                                                {insights.entry_points.map((e, idx) => (
                                                    <tr key={idx}>
                                                        <td><strong style={{color: '#f8fafc'}}>{e.event_name}</strong></td>
                                                        <td>{e.entry_count.toLocaleString()}</td>
                                                        <td><span className="tag-pill">{e.entry_share_pct}%</span></td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>

                                    <div className="glass-card">
                                        <h3><i className="fa-solid fa-right-from-bracket" style={{color: '#fb7185'}}></i> Top Session Exit Points</h3>
                                        <table>
                                            <thead>
                                                <tr><th>Exit Event</th><th>Sessions</th><th>Share %</th></tr>
                                            </thead>
                                            <tbody>
                                                {insights.exit_points.map((e, idx) => (
                                                    <tr key={idx}>
                                                        <td><strong style={{color: '#f8fafc'}}>{e.event_name}</strong></td>
                                                        <td>{e.exit_count.toLocaleString()}</td>
                                                        <td><span className="tag-pill" style={{borderColor: 'rgba(251,113,133,0.3)', color: '#fb7185'}}>{e.exit_share_pct}%</span></td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        )}

                        <!-- Tab 2: Funnel Retention Builder -->
                        {activeTab === 'funnel' && (
                            <div>
                                <div className="glass-card">
                                    <h3><i className="fa-solid fa-filter" style={{color: '#38bdf8'}}></i> Funnel Retention & Conversion Builder</h3>
                                    <p style={{color: '#94a3b8', fontSize: '14px'}}>Add or remove steps to construct custom retention flows:</p>
                                    
                                    <div style={{display: 'flex', flexWrap: 'wrap', gap: '8px', marginBottom: '20px'}}>
                                        {selectedFunnel.map((step, idx) => (
                                            <span key={idx} className="tag-pill" style={{padding: '8px 16px', fontSize: '14px'}}>
                                                #{idx+1} {step} 
                                                <i className="fa-solid fa-xmark" style={{cursor: 'pointer', marginLeft: '6px'}} onClick={() => setSelectedFunnel(selectedFunnel.filter((_, i) => i !== idx))}></i>
                                            </span>
                                        ))}
                                    </div>

                                    <div style={{display: 'flex', gap: '12px'}}>
                                        <select onChange={(e) => {
                                            if (e.target.value && !selectedFunnel.includes(e.target.value)) {
                                                setSelectedFunnel([...selectedFunnel, e.target.value]);
                                            }
                                        }}>
                                            <option value="">+ Add Event Step to Funnel</option>
                                            {topEvents.map((e, i) => (
                                                <option key={i} value={e.event_name}>{e.event_name} ({e.occurrence_count.toLocaleString()} occurrences)</option>
                                            ))}
                                        </select>
                                    </div>

                                    <div style={{height: '340px', marginTop: '30px'}}>
                                        <canvas ref={chartRef}></canvas>
                                    </div>
                                </div>

                                <div className="glass-card">
                                    <h3>Step Conversion Metrics</h3>
                                    <table>
                                        <thead>
                                            <tr>
                                                <th>#</th>
                                                <th>Step Name</th>
                                                <th>Sessions</th>
                                                <th>Step Conversion</th>
                                                <th>Step Drop-Off</th>
                                                <th>Overall Retention</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {funnelData.map((row) => (
                                                <tr key={row.step_number}>
                                                    <td><strong>{row.step_number}</strong></td>
                                                    <td><strong style={{color: '#38bdf8'}}>{row.step_name}</strong></td>
                                                    <td>{row.session_count.toLocaleString()}</td>
                                                    <td><span className="tag-pill" style={{color: '#34d399'}}>{row.step_conversion_pct}%</span></td>
                                                    <td><span className="tag-pill" style={{color: '#fb7185', borderColor: 'rgba(251,113,133,0.3)'}}>{row.step_dropoff_pct}%</span></td>
                                                    <td><strong>{row.overall_conversion_pct}%</strong></td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        )}

                        <!-- Tab 3: Heatmap Matrix -->
                        {activeTab === 'heatmap' && heatmapData && (
                            <div className="glass-card">
                                <h3><i className="fa-solid fa-fire" style={{color: '#fb7185'}}></i> Event Transition Heatmap Matrix</h3>
                                <p style={{color: '#94a3b8', fontSize: '14px'}}>Hover or click cells to view flow intensity between events:</p>
                                
                                <div style={{overflowX: 'auto'}}>
                                    <table style={{marginTop: '20px'}}>
                                        <thead>
                                            <tr>
                                                <th>From / To</th>
                                                {heatmapData.columns.map(col => <th key={col}>{col}</th>)}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {heatmapData.index.map((rowLabel, rIdx) => {
                                                const maxVal = Math.max(...heatmapData.data.flat());
                                                return (
                                                    <tr key={rowLabel}>
                                                        <td><strong style={{color: '#38bdf8'}}>{rowLabel}</strong></td>
                                                        {heatmapData.data[rIdx].map((val, cIdx) => (
                                                            <td key={cIdx} style={{background: getHeatmapBg(val, maxVal), textAlign: 'center', fontWeight: 'bold'}}>
                                                                {val > 0 ? val.toLocaleString() : '-'}
                                                            </td>
                                                        ))}
                                                    </tr>
                                                );
                                            })}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        )}

                        <!-- Tab 4: Session Explorer -->
                        {activeTab === 'search' && (
                            <div className="glass-card">
                                <h3><i className="fa-solid fa-magnifying-glass" style={{color: '#38bdf8'}}></i> Session Search & Explorer</h3>
                                <div style={{display: 'flex', gap: '16px', margin: '20px 0'}}>
                                    <input placeholder="Filter by event name (e.g. VehicleView)" value={searchEvent} onChange={e => setSearchEvent(e.target.value)} />
                                    <input placeholder="Filter by exact subpath (e.g. Search->Home)" value={searchSubpath} onChange={e => setSearchSubpath(e.target.value)} />
                                    <button className="btn-action" onClick={handleSearch}><i className="fa-solid fa-search"></i> Search Sessions</button>
                                </div>

                                <table>
                                    <thead>
                                        <tr>
                                            <th>Session ID</th>
                                            <th>Event Navigation Path</th>
                                            <th>Total Events</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {searchResults.map((s, idx) => (
                                            <tr key={idx}>
                                                <td><strong style={{color: '#38bdf8', fontFamily: 'monospace'}}>{s.SESSION}</strong></td>
                                                <td style={{fontFamily: 'monospace', fontSize: '13px'}}>{s.EVENT_PATH}</td>
                                                <td><span className="tag-pill">{s.TOTAL_EVENTS}</span></td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}
                    </div>
                </div>
            );
        }

        ReactDOM.createRoot(document.getElementById('root')).render(<App />);
    </script>
</body>
</html>
"""

def main():
    parser = argparse.ArgumentParser(description="Trishula Web Dashboard Server")
    parser.add_argument("--file", "-f", default="test_synthetic_snowflake.csv", help="CSV or Parquet dataset file path")
    parser.add_argument("--port", "-p", type=int, default=8000, help="Port to run web server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host binding")
    args = parser.parse_args()

    if os.path.exists(args.file):
        init_active_file(args.file)
        print(f"[*] Loaded active dataset: '{args.file}'")
    else:
        print(f"[!] Warning: File '{args.file}' not found. Please load a dataset.")

    print(f"\n🚀 TRISHULA WEB Dashboard running at: http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
