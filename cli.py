import sys
import os
import signal
import select

# Instant force-exit on Ctrl+C (SIGINT) to break out of C++ extensions like DuckDB
def handle_sigint(sig, frame):
    print("\n\n[!] Process cancelled by user (Ctrl+C). Exiting immediately...")
    os._exit(1)

signal.signal(signal.SIGINT, handle_sigint)

user_site = os.path.expanduser("~/Library/Python/3.9/lib/python/site-packages")
if os.path.exists(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

import argparse
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn

from generate_mock_data import generate_csv
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
    get_entry_exit_analytics,
    get_executive_summary_metrics,
    get_transition_matrix
)
from visualizer import (
    render_terminal_heatmap,
    render_visual_funnel,
    export_html_report
)

console = Console()

def display_banner():
    banner = """
    [bold cyan]🔱 TRISHULA[/bold cyan]
    [bold yellow]Trident Engine for Large-Scale Session & Funnel Analytics[/bold yellow]
    [dim]Out-of-core streaming analytics engine powered by DuckDB & Polars[/dim]
    """
    console.print(Panel(banner, border_style="cyan", expand=False))

def handle_generate_mock(args):
    console.print(f"[bold yellow][*] Generating mock data ({args.rows:,} rows)...[/bold yellow]")
    generate_csv(args.output, args.rows)
    console.print(f"[bold green]✓ Successfully generated synthetic data at '{args.output}'[/bold green]")

def handle_inspect(args):
    res = inspect_file(args.file_path, limit=args.limit)
    
    console.print(f"\n📊 File Overview: [bold cyan]{args.file_path}[/bold cyan]")
    overview_table = Table(show_header=True, header_style="bold magenta")
    overview_table.add_column("Property", style="bold")
    overview_table.add_column("Value", style="yellow")
    
    total_rows_str = f"{res['total_rows']:,}" if isinstance(res['total_rows'], int) else str(res['total_rows'])
    overview_table.add_row("Total Rows", total_rows_str)
    overview_table.add_row("File Size", f"{res['file_size_mb']} MB")
    overview_table.add_row("Columns Count", str(len(res['columns'])))
    console.print(overview_table)
    
    console.print("\n[bold cyan]📋 Schema Definition[/bold cyan]")
    schema_table = Table(show_header=True, header_style="bold blue")
    schema_table.add_column("Column Name", style="bold green")
    schema_table.add_column("Data Type", style="yellow")
    for col in res['columns']:
        schema_table.add_row(col['name'], col['type'])
    console.print(schema_table)

    console.print(f"\n[bold cyan]🔍 Sample Rows (Top {args.limit})[/bold cyan]")
    sample_df = res['sample_df']
    df_table = Table(show_header=True, header_style="bold green")
    for col in sample_df.columns:
        df_table.add_column(str(col))
    for row in sample_df.itertuples(index=False):
        df_table.add_row(*[str(val) for val in row])
    console.print(df_table)

def handle_convert(args):
    console.print(f"[bold yellow][*] Starting fast CSV -> Parquet conversion...[/bold yellow]")
    res = convert_csv_to_parquet(args.csv_file, args.output, compression=args.compression)
    
    console.print("\n[bold green]✓ Conversion Finished Successfully![/bold green]")
    res_table = Table(show_header=True, header_style="bold cyan")
    res_table.add_column("Metric", style="bold")
    res_table.add_column("Details", style="yellow")
    
    res_table.add_row("CSV Size", f"{res['csv_size_mb']} MB")
    res_table.add_row("Parquet Size", f"{res['parquet_size_mb']} MB")
    res_table.add_row("Space Saved", f"{res['compression_ratio_percent']}%")
    res_table.add_row("Total Rows Converted", f"{res['row_count']:,}")
    res_table.add_row("Time Elapsed", f"{res['elapsed_seconds']} s")
    res_table.add_row("Throughput", f"{res['rows_per_second']:,} rows/sec")
    console.print(res_table)

def handle_analyze(args):
    console.print(f"\n[bold cyan]📈 Analyzing Event Path Data: {args.file_path}[/bold cyan]")
    dedupe_mode = getattr(args, 'dedupe', 'consecutive')
    
    # 1. Event Frequencies
    freq_df = get_event_frequencies(args.file_path, delimiter=args.delimiter, top_n=args.top, dedupe_mode=dedupe_mode)
    freq_table = Table(title=f"Top {len(freq_df)} Individual Events", show_header=True, header_style="bold blue")
    freq_table.add_column("Rank", justify="right", style="dim")
    freq_table.add_column("Event Name", style="bold yellow")
    freq_table.add_column("Occurrences", justify="right", style="green")
    freq_table.add_column("Share (%)", justify="right", style="cyan")
    
    for idx, row in enumerate(freq_df.itertuples(), 1):
        freq_table.add_row(str(idx), str(row.event_name), f"{row.occurrence_count:,}", f"{row.percentage}%")
    console.print(freq_table)

    # 2. Transition Pairs
    trans_df = get_transition_pairs(args.file_path, delimiter=args.delimiter, top_n=args.top, dedupe_mode=dedupe_mode)
    trans_table = Table(title=f"Top Step Transitions (Event A -> Event B)", show_header=True, header_style="bold green")
    trans_table.add_column("Rank", justify="right", style="dim")
    trans_table.add_column("Transition Pair", style="bold yellow")
    trans_table.add_column("Transition Count", justify="right", style="magenta")
    
    for idx, row in enumerate(trans_df.itertuples(), 1):
        trans_table.add_row(str(idx), str(row.transition), f"{row.transition_count:,}")
    console.print(trans_table)

    # 3. Top Full Paths
    paths_df = get_top_paths(args.file_path, top_n=10, dedupe_mode=dedupe_mode)
    paths_table = Table(title="Top 10 Full Navigation Paths", show_header=True, header_style="bold magenta")
    paths_table.add_column("Rank", justify="right", style="dim")
    paths_table.add_column("Event Path", style="bold white")
    paths_table.add_column("Total Events", justify="right", style="yellow")
    paths_table.add_column("Sessions", justify="right", style="green")
    paths_table.add_column("Share (%)", justify="right", style="cyan")
    
    for idx, row in enumerate(paths_df.itertuples(), 1):
        paths_table.add_row(str(idx), str(row.full_path), str(row.TOTAL_EVENTS), f"{row.session_count:,}", f"{row.share_percent}%")
    console.print(paths_table)

    # 4. Optional Funnel
    if args.funnel:
        steps = [s.strip() for s in args.funnel.split(",")]
        console.print(f"\n[bold cyan]📉 Funnel Conversion Analysis for: {' -> '.join(steps)}[/bold cyan]")
        funnel_df = calculate_funnel(args.file_path, steps, delimiter=args.delimiter, dedupe_mode=dedupe_mode)
        
        funnel_table = Table(show_header=True, header_style="bold red")
        funnel_table.add_column("Step #", justify="right", style="dim")
        funnel_table.add_column("Step Name", style="bold yellow")
        funnel_table.add_column("Sessions Reaching Step", justify="right", style="green")
        funnel_table.add_column("Step Conversion", justify="right", style="cyan")
        funnel_table.add_column("Step Dropoff", justify="right", style="red")
        funnel_table.add_column("Overall Conversion", justify="right", style="magenta")
        
        for row in funnel_df.itertuples():
            funnel_table.add_row(
                str(row.step_number),
                str(row.step_name),
                f"{row.session_count:,}",
                f"{row.step_conversion_pct}%",
                f"{row.step_dropoff_pct}%",
                f"{row.overall_conversion_pct}%"
            )
        console.print(funnel_table)

def handle_search(args):
    console.print(f"[bold yellow][*] Searching sessions in '{args.file_path}'...[/bold yellow]")
    df = search_sessions(
        args.file_path,
        contains_event=args.event,
        exact_subpath=args.subpath,
        min_events=args.min_events,
        limit=args.limit
    )
    console.print(f"\n[bold green]Found {len(df)} matching sessions (Limit: {args.limit})[/bold green]")
    table = Table(show_header=True, header_style="bold green")
    table.add_column("SESSION", style="bold cyan")
    table.add_column("EVENT_PATH", style="bold white")
    table.add_column("TOTAL_EVENTS", justify="right", style="yellow")
    for row in df.itertuples():
        table.add_row(str(row.SESSION), str(row.EVENT_PATH), str(row.TOTAL_EVENTS))
    console.print(table)

def handle_query(args):
    console.print(f"[bold yellow][*] Executing DuckDB SQL Query...[/bold yellow]")
    console.print(f"[dim]SQL: {args.sql}[/dim]\n")
    df = run_custom_query(args.file_path, args.sql)
    
    table = Table(show_header=True, header_style="bold cyan")
    for col in df.columns:
        table.add_column(str(col))
    for row in df.itertuples(index=False):
        table.add_row(*[str(val) for val in row])
    console.print(table)

def handle_insights(args):
    console.print(f"\n[bold cyan]💡 Executive Session Insights: {args.file_path}[/bold cyan]")
    
    # 1. Summary Metrics
    metrics = get_executive_summary_metrics(args.file_path)
    table = Table(title="Executive Summary KPIs", show_header=True, header_style="bold magenta")
    table.add_column("KPI Metric", style="bold")
    table.add_column("Value", style="yellow")
    
    table.add_row("Total Sessions Analyzed", f"{metrics['total_sessions']:,}")
    table.add_row("Single-Event Bounce Rate", f"{metrics['bounce_rate_pct']}% ({metrics['single_event_bounces']:,} sessions)")
    table.add_row("Avg Events / Session", str(metrics['avg_events_per_session']))
    table.add_row("Median Events / Session", str(metrics['median_events']))
    table.add_row("75th Percentile Session Length", str(metrics['p75_events']))
    table.add_row("90th Percentile Session Length", str(metrics['p90_events']))
    table.add_row("Max Events in Session", str(metrics['max_events']))
    console.print(table)
    
    # 2. Entry & Exit Analytics
    entry_exit = get_entry_exit_analytics(args.file_path, delimiter=args.delimiter, top_n=5)
    
    entry_table = Table(title="Top Session Entry Points (Starting Events)", show_header=True, header_style="bold green")
    entry_table.add_column("Entry Event", style="bold yellow")
    entry_table.add_column("Sessions", justify="right", style="green")
    entry_table.add_column("Share (%)", justify="right", style="cyan")
    for r in entry_exit['entry_points'].itertuples():
        entry_table.add_row(str(r.event_name), f"{r.entry_count:,}", f"{r.entry_share_pct}%")
    console.print(entry_table)

    exit_table = Table(title="Top Session Exit Points (Ending / Drop-off Events)", show_header=True, header_style="bold red")
    exit_table.add_column("Exit Event", style="bold yellow")
    exit_table.add_column("Sessions", justify="right", style="red")
    exit_table.add_column("Share (%)", justify="right", style="magenta")
    for r in entry_exit['exit_points'].itertuples():
        exit_table.add_row(str(r.event_name), f"{r.exit_count:,}", f"{r.exit_share_pct}%")
    console.print(exit_table)

def handle_heatmap(args, funnel_df=None):
    console.print(f"\n[bold cyan]🔥 Generating Event Transition Heatmap...[/bold cyan]")
    matrix = get_transition_matrix(args.file_path, delimiter=args.delimiter, top_n=args.top)
    heatmap_table = render_terminal_heatmap(matrix)
    console.print(heatmap_table)
    
    if args.html:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold yellow]{task.description}[/bold yellow]"),
            TimeElapsedColumn(),
            console=console,
            transient=True
        ) as progress:
            progress.add_task("Building High-Resolution HTML Dashboard...", total=None)
            metrics = get_executive_summary_metrics(args.file_path)
            entry_exit = get_entry_exit_analytics(args.file_path, delimiter=args.delimiter)
            paths_df = get_top_paths(args.file_path, top_n=10)
            
            if funnel_df is None and args.funnel:
                steps = [s.strip() for s in args.funnel.split(",")]
                funnel_df = calculate_funnel(args.file_path, steps, delimiter=args.delimiter)
                
            abs_html_path = export_html_report(args.html, metrics, entry_exit['entry_points'], entry_exit['exit_points'], matrix, funnel_df, paths_df)
            
        console.print(f"\n[bold green]✨ High-Resolution Dashboard HTML Report Exported![/bold green]")
        console.print(f"[bold cyan]🔗 Click to open in browser: file://{abs_html_path}[/bold cyan]")

def handle_dropoffs(args, funnel_df=None):
    if funnel_df is None:
        if not args.funnel:
            console.print("[bold red]Error: --funnel parameter required for drop-off analysis[/bold red]")
            return
        steps = [s.strip() for s in args.funnel.split(",")]
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold yellow]{task.description}[/bold yellow]"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=True
        ) as progress:
            task_id = progress.add_task("Calculating Funnel...", total=len(steps))
            
            def cb(current, total, step_name):
                progress.update(task_id, completed=current, description=f"Analyzing Funnel Step {current}/{total}: '{step_name}'")
                
            funnel_df = calculate_funnel(args.file_path, steps, delimiter=args.delimiter, progress_callback=cb)
            
    visual_table = render_visual_funnel(funnel_df)
    console.print(visual_table)

def check_skip_keypress() -> bool:
    """
    Non-blocking check for 's' or 'S' keypress on stdin (macOS/Linux).
    """
    if not sys.stdin.isatty():
        return False
    try:
        dr, _, _ = select.select([sys.stdin], [], [], 0)
        if dr:
            ch = sys.stdin.read(1)
            if ch.lower() == 's':
                return True
    except Exception:
        pass
    return False

def handle_run_all(args):
    console.print(f"\n[bold green]🚀 Starting Full Automated Pipeline for: {args.csv_file}[/bold green]\n")
    
    force_convert = getattr(args, 'force', False)
    parquet_output = args.output
    if not parquet_output:
        base, _ = os.path.splitext(args.csv_file)
        parquet_output = f"{base}.parquet"

    # Step 1: Check Parquet Cache & Convert CSV -> Parquet
    is_cache_valid = False
    if os.path.exists(parquet_output) and not force_convert:
        if os.path.exists(args.csv_file):
            csv_mtime = os.path.getmtime(args.csv_file)
            pq_mtime = os.path.getmtime(parquet_output)
            if pq_mtime >= csv_mtime:
                is_cache_valid = True

    if is_cache_valid:
        pq_size_mb = round(os.path.getsize(parquet_output) / (1024**2), 2)
        console.print(f"[bold green]1/5. ⚡ Found existing up-to-date Parquet storage ('{parquet_output}' - {pq_size_mb} MB).[/bold green]")
        console.print("[dim]Reusing cached Parquet storage for instant analytics! (Use --force to re-convert)[/dim]")
    else:
        console.print(f"[bold yellow]1/5. Converting CSV -> Parquet ('{parquet_output}')...[/bold yellow]")
        with console.status("[bold yellow]⌛ Streaming CSV to Parquet out-of-core... (Please wait)[/bold yellow]"):
            handle_convert(argparse.Namespace(csv_file=args.csv_file, output=parquet_output, compression=args.compression))
    console.print("\n" + "─"*60 + "\n")

    # Step 2: Inspect Parquet File Metadata & Sample Rows
    console.print("[bold yellow]2/5. Inspecting Parquet Schema & Metadata...[/bold yellow]")
    handle_inspect(argparse.Namespace(file_path=parquet_output, limit=3))
    console.print("\n" + "─"*60 + "\n")

    # Auto-detect Delimiter
    detected_delim = detect_delimiter(parquet_output) if args.delimiter == "->" else args.delimiter

    # Dedupe mode (default 'consecutive' to collapse repeated adjacent events like A->A->A)
    dedupe_mode = getattr(args, 'dedupe', 'consecutive')

    # Auto-detect Top N Events if --funnel is omitted
    funnel_param = args.funnel
    is_sequential = True
    if not funnel_param:
        is_sequential = False  # Use event reach/penetration mode for auto-detected top events
        max_funnel_depth = min(args.top, 8)
        freq_df = get_event_frequencies(parquet_output, delimiter=detected_delim, top_n=max_funnel_depth, dedupe_mode=dedupe_mode)
        top_events = freq_df['event_name'].tolist() if not freq_df.empty else []
        funnel_param = ",".join(top_events)
        console.print(f"[bold cyan]🔍 Auto-detected Top {len(top_events)} Events (Delimiter: '{detected_delim}', Dedupe: '{dedupe_mode}'):[/bold cyan] {', '.join(top_events)}")
        console.print("[dim]💡 Tip: Press 'S' key anytime to skip remaining funnel steps.[/dim]\n")

    # Calculate Funnel ONCE with live progress bar and 'S' keypress skip listener
    steps = [s.strip() for s in funnel_param.split(",")]
    user_requested_skip = False
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold yellow]{task.description}[/bold yellow]"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=True
    ) as progress:
        task_id = progress.add_task("Calculating Funnel Steps...", total=len(steps))
        
        def cb(current, total, step_name):
            nonlocal user_requested_skip
            if check_skip_keypress():
                progress.stop()
                answer = console.input("\n[bold yellow]⚠️ 'S' key pressed! Do you want to skip remaining steps? (Y/N): [/bold yellow]").strip().lower()
                if answer == 'y':
                    user_requested_skip = True
                    return False
                progress.start()
            progress.update(task_id, completed=current, description=f"Analyzing Funnel Step {current}/{total}: '{step_name}' (Press 'S' to skip)")
            return True

        shared_funnel_df = calculate_funnel(parquet_output, steps, delimiter=detected_delim, sequential=is_sequential, dedupe_mode=dedupe_mode, progress_callback=cb)

    if user_requested_skip:
        console.print("[bold magenta]⏩ Skipped remaining funnel steps on user request.[/bold magenta]\n")

    # Step 3: Executive Insights & KPIs
    console.print("[bold yellow]3/5. Generating Executive Session Insights...[/bold yellow]")
    handle_insights(argparse.Namespace(file_path=parquet_output, delimiter=detected_delim))
    console.print("\n" + "─"*60 + "\n")

    # Step 4: Event Transition Heatmap & Interactive HTML Report
    console.print("[bold yellow]4/5. Generating High-Resolution Transition Heatmap & HTML Dashboard...[/bold yellow]")
    html_export = args.html if args.html else f"{os.path.splitext(args.csv_file)[0]}_dashboard.html"
    handle_heatmap(argparse.Namespace(file_path=parquet_output, delimiter=detected_delim, top=args.top, html=html_export, funnel=funnel_param), funnel_df=shared_funnel_df)
    console.print("\n" + "─"*60 + "\n")

    # Step 5: Visual Drop-Off Report
    console.print("[bold yellow]5/5. Generating Visual Drop-Off & Retention Report...[/bold yellow]")
    handle_dropoffs(argparse.Namespace(file_path=parquet_output, delimiter=detected_delim, funnel=funnel_param), funnel_df=shared_funnel_df)

    abs_report_url = os.path.abspath(html_export)
    console.print(f"\n[bold green]✅ Full Pipeline Execution Complete![/bold green]")
    console.print(f"[bold white]📁 Fast Parquet Storage: '{parquet_output}'[/bold white]")
    console.print(f"[bold yellow]🌐 Click link to view High-Res Report in Browser:[/bold yellow]")
    console.print(f"[bold cyan]👉 file://{abs_report_url}[/bold cyan]\n")

def main():
    display_banner()
    parser = argparse.ArgumentParser(description="Snowflake Session CSV & Parquet Data Parser and Analyzer")
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # Subcommand: generate-mock
    mock_p = subparsers.add_parser("generate-mock", help="Generate synthetic CSV test data")
    mock_p.add_argument("-o", "--output", default="sample_snowflake_data.csv", help="Output CSV filepath")
    mock_p.add_argument("-r", "--rows", type=int, default=50000, help="Number of synthetic rows to generate")

    # Subcommand: inspect
    inspect_p = subparsers.add_parser("inspect", help="Inspect schema, row count, and sample data")
    inspect_p.add_argument("file_path", help="CSV or Parquet filepath")
    inspect_p.add_argument("-l", "--limit", type=int, default=5, help="Number of sample rows to show")

    # Subcommand: convert
    convert_p = subparsers.add_parser("convert", help="Convert CSV to Parquet out-of-core")
    convert_p.add_argument("csv_file", help="Input CSV filepath")
    convert_p.add_argument("output", help="Output Parquet filepath")
    convert_p.add_argument("-c", "--compression", default="ZSTD", choices=["ZSTD", "SNAPPY", "GZIP", "UNCOMPRESSED"], help="Parquet compression algorithm")

    # Subcommand: analyze
    analyze_p = subparsers.add_parser("analyze", help="Perform event frequencies, transition, and funnel analytics")
    analyze_p.add_argument("file_path", help="CSV or Parquet filepath")
    analyze_p.add_argument("-d", "--delimiter", default="->", help="Event path separator token")
    analyze_p.add_argument("-t", "--top", type=int, default=15, help="Number of top events/transitions to display")
    analyze_p.add_argument("-f", "--funnel", help="Comma-separated funnel steps (e.g. 'Home,Product_View,Checkout')")
    analyze_p.add_argument("-u", "--dedupe", default="consecutive", choices=["none", "consecutive", "unique"], help="Event deduplication mode")

    # Subcommand: search
    search_p = subparsers.add_parser("search", help="Search sessions matching event filters")
    search_p.add_argument("file_path", help="CSV or Parquet filepath")
    search_p.add_argument("-e", "--event", help="Filter sessions containing this event name")
    search_p.add_argument("-s", "--subpath", help="Filter sessions containing this exact subpath (e.g. 'Product_View->Add_To_Cart')")
    search_p.add_argument("-m", "--min-events", type=int, default=1, help="Minimum total events in session")
    search_p.add_argument("-l", "--limit", type=int, default=20, help="Max results")

    # Subcommand: query
    query_p = subparsers.add_parser("query", help="Execute custom DuckDB SQL against file")
    query_p.add_argument("file_path", help="CSV or Parquet filepath")
    query_p.add_argument("--sql", required=True, help="SQL query string (use 'data' to refer to file table)")

    # Subcommand: insights
    insights_p = subparsers.add_parser("insights", help="Executive summary metrics: Bounce rate, Entry/Exit points, and session lengths")
    insights_p.add_argument("file_path", help="CSV or Parquet filepath")
    insights_p.add_argument("-d", "--delimiter", default="->", help="Event path separator token")

    # Subcommand: heatmap
    heatmap_p = subparsers.add_parser("heatmap", help="Event transition matrix heatmap and HTML report export")
    heatmap_p.add_argument("file_path", help="CSV or Parquet filepath")
    heatmap_p.add_argument("-d", "--delimiter", default="->", help="Event path separator token")
    heatmap_p.add_argument("-t", "--top", type=int, default=8, help="Top N events to display in matrix")
    heatmap_p.add_argument("--html", help="Optional HTML report output file path (e.g. 'pm_report.html')")
    heatmap_p.add_argument("-f", "--funnel", help="Optional comma-separated funnel steps for HTML report")
    heatmap_p.add_argument("-u", "--dedupe", default="consecutive", choices=["none", "consecutive", "unique"], help="Event deduplication mode")

    # Subcommand: drop-offs
    drop_p = subparsers.add_parser("drop-offs", help="Visual funnel bar chart with retention & drop-off alerts")
    drop_p.add_argument("file_path", help="CSV or Parquet filepath")
    drop_p.add_argument("-f", "--funnel", required=True, help="Comma-separated funnel steps (e.g. 'Home,Product_View,Checkout')")
    drop_p.add_argument("-d", "--delimiter", default="->", help="Event path separator token")
    drop_p.add_argument("-u", "--dedupe", default="consecutive", choices=["none", "consecutive", "unique"], help="Event deduplication mode")

    # Subcommand: run-all
    run_all_p = subparsers.add_parser("run-all", help="Run full pipeline: inspect CSV -> convert to Parquet -> insights -> heatmap -> visual drop-offs -> HTML report")
    run_all_p.add_argument("csv_file", help="Input CSV filepath")
    run_all_p.add_argument("-o", "--output", help="Optional output Parquet filepath")
    run_all_p.add_argument("-d", "--delimiter", default="->", help="Event path separator token")
    run_all_p.add_argument("-t", "--top", type=int, default=15, help="Number of top events to analyze")
    run_all_p.add_argument("-f", "--funnel", help="Comma-separated funnel steps (e.g. 'Home,Product_View,Checkout')")
    run_all_p.add_argument("-u", "--dedupe", default="consecutive", choices=["none", "consecutive", "unique"], help="Event deduplication mode: 'consecutive' (default), 'unique', 'none'")
    run_all_p.add_argument("--force", action="store_true", help="Force re-converting CSV to Parquet even if cached Parquet file exists")
    run_all_p.add_argument("--html", help="Optional output HTML report filepath")
    run_all_p.add_argument("-c", "--compression", default="ZSTD", choices=["ZSTD", "SNAPPY", "GZIP", "UNCOMPRESSED"], help="Parquet compression algorithm")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    
    if args.command == "run-all":
        handle_run_all(args)
    elif args.command == "insights":
        handle_insights(args)
    elif args.command == "heatmap":
        handle_heatmap(args)
    elif args.command == "drop-offs":
        handle_dropoffs(args)
    elif args.command == "generate-mock":
        handle_generate_mock(args)
    elif args.command == "inspect":
        handle_inspect(args)
    elif args.command == "convert":
        handle_convert(args)
    elif args.command == "analyze":
        handle_analyze(args)
    elif args.command == "search":
        handle_search(args)
    elif args.command == "query":
        handle_query(args)

if __name__ == "__main__":
    main()
