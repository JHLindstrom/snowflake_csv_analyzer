import sys
import os

user_site = os.path.expanduser("~/Library/Python/3.9/lib/python/site-packages")
if os.path.exists(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

console = Console()

def render_terminal_heatmap(matrix: pd.DataFrame) -> Table:
    """
    Renders a 2D transition matrix as a color-coded heatmap table in the terminal.
    """
    max_val = matrix.values.max() if matrix.values.max() > 0 else 1
    
    table = Table(title="🔥 Event-to-Event Transition Heatmap Matrix", show_header=True, header_style="bold magenta", border_style="dim")
    table.add_column("From \\ To", style="bold yellow")
    
    for col in matrix.columns:
        # Abbreviate header column names if long
        short_col = col if len(col) <= 12 else col[:10] + ".."
        table.add_column(short_col, justify="right")
        
    for src_event in matrix.index:
        row_cells = [Text(src_event, style="bold cyan")]
        for tgt_event in matrix.columns:
            val = matrix.loc[src_event, tgt_event]
            ratio = val / max_val
            
            # Color intensity styling based on flow volume
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

def render_visual_funnel(funnel_df: pd.DataFrame, max_bar_width: int = 30) -> Table:
    """
    Renders a visual horizontal bar chart for funnel retention & drop-offs.
    """
    table = Table(title="📉 Visual Funnel Retention & Drop-Off Report", show_header=True, header_style="bold cyan")
    table.add_column("Step", justify="right", style="dim")
    table.add_column("Event Name", style="bold yellow")
    table.add_column("Sessions", justify="right", style="green")
    table.add_column("Step Conversion", justify="right")
    table.add_column("Drop-Off %", justify="right")
    table.add_column("Retention Visual", style="bold")
    
    first_count = funnel_df['session_count'].iloc[0] if len(funnel_df) > 0 and funnel_df['session_count'].iloc[0] > 0 else 1
    
    for row in funnel_df.itertuples():
        # Compute bar length relative to initial funnel step
        bar_len = int((row.session_count / first_count) * max_bar_width)
        bar_str = "█" * bar_len + "░" * (max_bar_width - bar_len)
        
        # Color status based on step dropoff
        if row.step_number == 1:
            conv_style = "bold green"
            drop_style = "dim"
            bar_color = "green"
        elif row.step_dropoff_pct < 40.0:
            conv_style = "green"
            drop_style = "dim green"
            bar_color = "green"
        elif row.step_dropoff_pct < 70.0:
            conv_style = "yellow"
            drop_style = "bold yellow"
            bar_color = "yellow"
        else:
            conv_style = "bold red"
            drop_style = "bold red"
            bar_color = "red"
            
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
    funnel_df: pd.DataFrame = None
) -> None:
    """
    Generates a standalone, beautiful HTML analytics report to share with PMs.
    """
    # Build HTML matrix table cells
    matrix_headers = "".join([f"<th>{col}</th>" for col in matrix.columns])
    matrix_rows = ""
    max_val = matrix.values.max() if matrix.values.max() > 0 else 1
    
    for src in matrix.index:
        cells = [f"<td class='row-header'>{src}</td>"]
        for tgt in matrix.columns:
            val = matrix.loc[src, tgt]
            intensity = round((val / max_val) * 0.85, 2)
            cells.append(f"<td style='background-color: rgba(239, 68, 68, {intensity}); color: {'#fff' if intensity > 0.4 else '#e2e8f0'};'>{val:,}</td>")
        matrix_rows += f"<tr>{''.join(cells)}</tr>"

    # Build Funnel rows
    funnel_rows = ""
    if funnel_df is not None and not funnel_df.empty:
        for r in funnel_df.itertuples():
            funnel_rows += f"""
            <tr>
                <td>{r.step_number}</td>
                <td><strong>{r.step_name}</strong></td>
                <td>{r.session_count:,}</td>
                <td style="color: {'#22c55e' if r.step_conversion_pct > 50 else '#ef4444'};">{r.step_conversion_pct}%</td>
                <td style="color: {'#ef4444' if r.step_dropoff_pct > 50 else '#94a3b8'};">{r.step_dropoff_pct}%</td>
                <td>{r.overall_conversion_pct}%</td>
            </tr>
            """

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Snowflake Session Analytics Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #0f172a; color: #f8fafc; margin: 0; padding: 30px; }}
        h1, h2, h3 {{ color: #38bdf8; }}
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .metric-card {{ background: #1e293b; padding: 20px; border-radius: 10px; border: 1px solid #334155; }}
        .metric-card h4 {{ margin: 0; color: #94a3b8; font-size: 14px; text-transform: uppercase; }}
        .metric-card p {{ margin: 10px 0 0 0; font-size: 28px; font-weight: bold; color: #38bdf8; }}
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 30px; background: #1e293b; border-radius: 8px; overflow: hidden; }}
        th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid #334155; }}
        th {{ background-color: #0f172a; color: #94a3b8; font-size: 13px; text-transform: uppercase; }}
        .row-header {{ background-color: #0f172a; font-weight: bold; color: #38bdf8; }}
        .section {{ background: #1e293b; padding: 25px; border-radius: 12px; margin-bottom: 30px; border: 1px solid #334155; }}
    </style>
</head>
<body>
    <h1>📊 Snowflake Session Analytics & Insights Report</h1>
    <p style="color: #94a3b8;">Automated Product Management Report & Drop-off Heatmap</p>
    
    <div class="metrics-grid">
        <div class="metric-card">
            <h4>Total Sessions</h4>
            <p>{metrics['total_sessions']:,}</p>
        </div>
        <div class="metric-card">
            <h4>Bounce Rate</h4>
            <p style="color: {'#ef4444' if metrics['bounce_rate_pct'] > 40 else '#22c55e'};">{metrics['bounce_rate_pct']}%</p>
        </div>
        <div class="metric-card">
            <h4>Avg Events / Session</h4>
            <p>{metrics['avg_events_per_session']}</p>
        </div>
        <div class="metric-card">
            <h4>Median Session Length</h4>
            <p>{metrics['median_events']} events</p>
        </div>
    </div>

    <div class="section">
        <h2>🔥 Event Transition Heatmap Matrix</h2>
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

    {f'''
    <div class="section">
        <h2>📉 Funnel Conversion & Drop-off Breakdown</h2>
        <table>
            <thead>
                <tr>
                    <th>Step #</th>
                    <th>Event Name</th>
                    <th>Sessions</th>
                    <th>Step Conversion</th>
                    <th>Step Drop-off</th>
                    <th>Overall Retention</th>
                </tr>
            </thead>
            <tbody>
                {funnel_rows}
            </tbody>
        </table>
    </div>
    ''' if funnel_rows else ''}

</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[*] HTML Report successfully exported to '{output_path}'")
