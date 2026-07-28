import sys
import os

user_site = os.path.expanduser("~/Library/Python/3.9/lib/python/site-packages")
if os.path.exists(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

import pandas as pd
import json
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

console = Console()

def render_terminal_heatmap(matrix: pd.DataFrame) -> Table:
    """
    Renders a 2D transition matrix as a color-coded heatmap table in the terminal with strict column widths.
    """
    max_val = matrix.values.max() if matrix.values.max() > 0 else 1
    
    table = Table(
        title="🔥 Event-to-Event Transition Heatmap Matrix", 
        show_header=True, 
        header_style="bold cyan", 
        border_style="dim",
        box=box.ROUNDED,
        expand=False
    )
    table.add_column("From \\ To", style="bold yellow", max_width=15, overflow="ellipsis")
    
    for col in matrix.columns:
        short_col = col if len(col) <= 10 else col[:8] + ".."
        table.add_column(short_col, justify="right", max_width=10)
        
    for src_event in matrix.index:
        short_src = src_event if len(src_event) <= 14 else src_event[:12] + ".."
        row_cells = [Text(short_src, style="bold cyan")]
        for tgt_event in matrix.columns:
            val = matrix.loc[src_event, tgt_event]
            ratio = val / max_val
            
            if val == 0:
                style_str = "dim black"
            elif ratio < 0.15:
                style_str = "blue"
            elif ratio < 0.40:
                style_str = "cyan"
            elif ratio < 0.70:
                style_str = "yellow"
            else:
                style_str = "bold red"
                
            row_cells.append(Text(f"{val:,}", style=style_str))
        table.add_row(*row_cells)
        
    return table

def render_visual_funnel(funnel_df: pd.DataFrame, max_bar_width: int = 25) -> Table:
    """
    Renders a visual horizontal bar chart for funnel retention & drop-offs with clean line bounds.
    """
    table = Table(
        title="📉 Visual Funnel Retention & Drop-Off Report", 
        show_header=True, 
        header_style="bold cyan",
        box=box.ROUNDED,
        expand=False
    )
    table.add_column("#", justify="right", style="dim", max_width=3)
    table.add_column("Event Name", style="bold yellow", max_width=18, overflow="ellipsis")
    table.add_column("Sessions", justify="right", style="green", max_width=10)
    table.add_column("Conv %", justify="right", max_width=8)
    table.add_column("Drop %", justify="right", max_width=8)
    table.add_column("Retention Bar", style="bold", max_width=max_bar_width + 2)
    
    first_count = funnel_df['session_count'].iloc[0] if len(funnel_df) > 0 and funnel_df['session_count'].iloc[0] > 0 else 1
    
    for row in funnel_df.itertuples():
        bar_len = int((row.session_count / first_count) * max_bar_width)
        bar_str = "█" * bar_len + "░" * (max_bar_width - bar_len)
        
        if row.step_number == 1:
            conv_style, drop_style, bar_color = "bold green", "dim", "green"
        elif row.step_dropoff_pct < 40.0:
            conv_style, drop_style, bar_color = "green", "dim green", "green"
        elif row.step_dropoff_pct < 70.0:
            conv_style, drop_style, bar_color = "yellow", "bold yellow", "yellow"
        else:
            conv_style, drop_style, bar_color = "bold red", "bold red", "red"
            
        bar_text = Text(bar_str, style=bar_color)
        
        table.add_row(
            str(row.step_number),
            str(row.step_name),
            f"{row.session_count:,}",
            Text(f"{row.step_conversion_pct}%", style=conv_style),
            Text(f"{row.step_dropoff_pct}%", style=drop_style),
            bar_text
        )
        
    return table

def export_html_report(
    output_path: str,
    metrics: dict,
    entry_df: pd.DataFrame,
    exit_df: pd.DataFrame,
    matrix: pd.DataFrame,
    funnel_df: pd.DataFrame = None,
    paths_df: pd.DataFrame = None
) -> str:
    """
    Generates a high-resolution, interactive HTML Dashboard report with Chart.js vector charts.
    """
    # Prepare JSON data for Chart.js
    funnel_labels = funnel_df['step_name'].tolist() if funnel_df is not None and not funnel_df.empty else []
    funnel_counts = funnel_df['session_count'].tolist() if funnel_df is not None and not funnel_df.empty else []
    funnel_dropoffs = funnel_df['step_dropoff_pct'].tolist() if funnel_df is not None and not funnel_df.empty else []

    entry_labels = entry_df['event_name'].tolist() if entry_df is not None else []
    entry_counts = entry_df['entry_count'].tolist() if entry_df is not None else []

    exit_labels = exit_df['event_name'].tolist() if exit_df is not None else []
    exit_counts = exit_df['exit_count'].tolist() if exit_df is not None else []

    # HTML Matrix Table Cells
    matrix_headers = "".join([f"<th>{col}</th>" for col in matrix.columns])
    matrix_rows = ""
    max_val = matrix.values.max() if matrix.values.max() > 0 else 1
    
    for src in matrix.index:
        cells = [f"<td class='row-header'>{src}</td>"]
        for tgt in matrix.columns:
            val = matrix.loc[src, tgt]
            intensity = round((val / max_val) * 0.85, 2)
            cells.append(f"<td style='background-color: rgba(56, 189, 248, {intensity}); color: {'#fff' if intensity > 0.4 else '#cbd5e1'}; text-align: center; font-weight: 600;'>{val:,}</td>")
        matrix_rows += f"<tr>{''.join(cells)}</tr>"

    # Funnel Table Rows
    funnel_rows = ""
    if funnel_df is not None and not funnel_df.empty:
        for r in funnel_df.itertuples():
            funnel_rows += f"""
            <tr>
                <td>{r.step_number}</td>
                <td><strong>{r.step_name}</strong></td>
                <td>{r.session_count:,}</td>
                <td style="color: {'#22c55e' if r.step_conversion_pct > 50 else '#ef4444'}; font-weight: bold;">{r.step_conversion_pct}%</td>
                <td style="color: {'#ef4444' if r.step_dropoff_pct > 50 else '#94a3b8'};">{r.step_dropoff_pct}%</td>
                <td>{r.overall_conversion_pct}%</td>
            </tr>
            """

    # Top Paths Rows
    path_rows = ""
    if paths_df is not None and not paths_df.empty:
        for idx, r in enumerate(paths_df.itertuples(), 1):
            path_rows += f"""
            <tr>
                <td>{idx}</td>
                <td><code style="color: #38bdf8; background: #0f172a; padding: 4px 8px; border-radius: 4px;">{r.full_path}</code></td>
                <td>{r.TOTAL_EVENTS}</td>
                <td>{r.session_count:,}</td>
                <td><strong>{r.share_percent}%</strong></td>
            </tr>
            """

    abs_path = os.path.abspath(output_path)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Snowflake Analytics Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{ --bg: #0f172a; --card-bg: #1e293b; --text: #f8fafc; --accent: #38bdf8; --border: #334155; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: var(--bg); color: var(--text); margin: 0; padding: 40px; line-height: 1.5; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid var(--border); padding-bottom: 20px; margin-bottom: 30px; }}
        .header h1 {{ margin: 0; font-size: 28px; color: var(--accent); }}
        .header p {{ margin: 5px 0 0 0; color: #94a3b8; font-size: 14px; }}
        .btn-print {{ background: var(--accent); color: #0f172a; border: none; padding: 10px 20px; border-radius: 6px; font-weight: bold; cursor: pointer; }}
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .metric-card {{ background: var(--card-bg); padding: 20px; border-radius: 12px; border: 1px solid var(--border); box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }}
        .metric-card h4 {{ margin: 0; color: #94a3b8; font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }}
        .metric-card p {{ margin: 8px 0 0 0; font-size: 32px; font-weight: 800; color: var(--accent); }}
        .section {{ background: var(--card-bg); padding: 30px; border-radius: 16px; margin-bottom: 35px; border: 1px solid var(--border); }}
        .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 30px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 14px; }}
        th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid var(--border); }}
        th {{ background-color: #0f172a; color: #94a3b8; font-size: 12px; text-transform: uppercase; }}
        .row-header {{ background-color: #0f172a; font-weight: bold; color: var(--accent); }}
        .chart-container {{ position: relative; height: 320px; width: 100%; margin-top: 15px; }}
        @media print {{ body {{ background: #fff; color: #000; padding: 0; }} .section, .metric-card {{ background: #fff; border: 1px solid #ccc; color: #000; }} .btn-print {{ display: none; }} }}
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1>❄️ Snowflake Session Analytics Dashboard</h1>
            <p>High-Resolution Interactive Product & Conversion Insights Report</p>
        </div>
        <button class="btn-print" onclick="window.print()">Print / Export PDF</button>
    </div>

    <!-- Executive Metrics -->
    <div class="metrics-grid">
        <div class="metric-card">
            <h4>Total Sessions Analyzed</h4>
            <p>{metrics['total_sessions']:,}</p>
        </div>
        <div class="metric-card">
            <h4>Single-Event Bounce Rate</h4>
            <p style="color: {'#ef4444' if metrics['bounce_rate_pct'] > 40 else '#22c55e'};">{metrics['bounce_rate_pct']}%</p>
        </div>
        <div class="metric-card">
            <h4>Avg Events / Session</h4>
            <p>{metrics['avg_events_per_session']}</p>
        </div>
        <div class="metric-card">
            <h4>Median Session Depth</h4>
            <p>{metrics['median_events']} events</p>
        </div>
        <div class="metric-card">
            <h4>90th Percentile Depth</h4>
            <p>{metrics['p90_events']} events</p>
        </div>
    </div>

    {f'''
    <!-- Interactive Funnel Chart Section -->
    <div class="section">
        <h2>📉 Interactive Funnel Retention & Drop-Off Chart</h2>
        <div class="grid-2">
            <div class="chart-container">
                <canvas id="funnelChart"></canvas>
            </div>
            <div>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Event Step</th>
                            <th>Sessions</th>
                            <th>Conversion</th>
                            <th>Drop-off %</th>
                            <th>Overall</th>
                        </tr>
                    </thead>
                    <tbody>
                        {funnel_rows}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    ''' if funnel_rows else ''}

    <!-- Entry & Exit Breakdown -->
    <div class="section">
        <h2>🎯 Session Entry & Exit Traffic Distribution</h2>
        <div class="grid-2">
            <div>
                <h3>Top Starting Events (Entry Points)</h3>
                <div class="chart-container">
                    <canvas id="entryChart"></canvas>
                </div>
            </div>
            <div>
                <h3>Top Drop-off Events (Exit Points)</h3>
                <div class="chart-container">
                    <canvas id="exitChart"></canvas>
                </div>
            </div>
        </div>
    </div>

    <!-- Transition Matrix Heatmap Table -->
    <div class="section">
        <h2>🔥 Event Transition Matrix Heatmap</h2>
        <p style="color: #94a3b8; font-size: 14px;">Cell intensity indicates navigation flow volume between event pairs (From Row &rarr; To Column).</p>
        <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>From \ To</th>
                        {matrix_headers}
                    </tr>
                </thead>
                <tbody>
                    {matrix_rows}
                </tbody>
            </table>
        </div>
    </div>

    {f'''
    <!-- Top Paths Table -->
    <div class="section">
        <h2>🏆 Top Full User Navigation Paths</h2>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Navigation Path Sequence</th>
                    <th>Path Length</th>
                    <th>Session Count</th>
                    <th>Share (%)</th>
                </tr>
            </thead>
            <tbody>
                {path_rows}
            </tbody>
        </table>
    </div>
    ''' if path_rows else ''}

    <script>
        // Funnel Chart Rendering
        const funnelLabels = {json.dumps(funnel_labels)};
        const funnelCounts = {json.dumps(funnel_counts)};
        
        if (funnelLabels.length > 0) {{
            new Chart(document.getElementById('funnelChart'), {{
                type: 'bar',
                data: {{
                    labels: funnelLabels,
                    datasets: [{{
                        label: 'Sessions Reaching Step',
                        data: funnelCounts,
                        backgroundColor: '#38bdf8',
                        borderRadius: 6
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{ legend: {{ display: false }} }},
                    scales: {{
                        x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }} }},
                        y: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }} }}
                    }}
                }}
            }});
        }}

        // Entry Chart
        new Chart(document.getElementById('entryChart'), {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(entry_labels)},
                datasets: [{{
                    data: {json.dumps(entry_counts)},
                    backgroundColor: ['#22c55e', '#38bdf8', '#a855f7', '#eab308', '#f97316']
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#f8fafc' }} }} }}
            }}
        }});

        // Exit Chart
        new Chart(document.getElementById('exitChart'), {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(exit_labels)},
                datasets: [{{
                    data: {json.dumps(exit_counts)},
                    backgroundColor: ['#ef4444', '#f97316', '#eab308', '#a855f7', '#64748b']
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#f8fafc' }} }} }}
            }}
        }});
    </script>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
        
    return abs_path
