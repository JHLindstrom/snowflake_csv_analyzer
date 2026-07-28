import sys
import os

user_site = os.path.expanduser("~/Library/Python/3.9/lib/python/site-packages")
if os.path.exists(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

import duckdb
import pandas as pd
from typing import Dict, Any

def _get_read_sql(file_path: str) -> str:
    clean_path = file_path.replace("'", "''")
    if file_path.endswith(".parquet") or file_path.endswith(".pq"):
        return f"read_parquet('{clean_path}')"
    return f"read_csv_auto('{clean_path}', header=True)"

def get_entry_exit_analytics(file_path: str, delimiter: str = "->", top_n: int = 10) -> Dict[str, pd.DataFrame]:
    """
    Analyzes entry events (first event) and exit events (last event) in user sessions directly in DuckDB SQL.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    clean_delim = delimiter.replace("'", "''")
    
    # 1. Entry Events (Top N)
    entry_sql = f"""
    WITH split_events AS (
        SELECT string_split(EVENT_PATH, '{clean_delim}') AS events
        FROM {read_sql}
        WHERE EVENT_PATH IS NOT NULL AND TOTAL_EVENTS > 0
    )
    SELECT 
        trim(events[1]) AS event_name,
        COUNT(*) AS entry_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM split_events), 2) AS entry_share_pct
    FROM split_events
    WHERE trim(events[1]) != ''
    GROUP BY 1
    ORDER BY entry_count DESC
    LIMIT {top_n};
    """
    entry_df = con.execute(entry_sql).fetchdf()

    # 2. Exit Events (Top N)
    exit_sql = f"""
    WITH split_events AS (
        SELECT string_split(EVENT_PATH, '{clean_delim}') AS events
        FROM {read_sql}
        WHERE EVENT_PATH IS NOT NULL AND TOTAL_EVENTS > 0
    )
    SELECT 
        trim(events[len(events)]) AS event_name,
        COUNT(*) AS exit_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM split_events), 2) AS exit_share_pct
    FROM split_events
    WHERE trim(events[len(events)]) != ''
    GROUP BY 1
    ORDER BY exit_count DESC
    LIMIT {top_n};
    """
    exit_df = con.execute(exit_sql).fetchdf()
    
    con.close()
    return {
        "entry_points": entry_df,
        "exit_points": exit_df
    }

def get_executive_summary_metrics(file_path: str) -> Dict[str, Any]:
    """
    Computes key executive metrics: total sessions, bounce rate, average events per session, and percentiles.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    
    query = f"""
    SELECT 
        COUNT(*) AS total_sessions,
        COUNT(CASE WHEN TOTAL_EVENTS = 1 THEN 1 END) AS single_event_bounces,
        ROUND(AVG(TOTAL_EVENTS), 2) AS avg_events_per_session,
        APPROX_QUANTILE(TOTAL_EVENTS, 0.50) AS median_events,
        APPROX_QUANTILE(TOTAL_EVENTS, 0.75) AS p75_events,
        APPROX_QUANTILE(TOTAL_EVENTS, 0.90) AS p90_events,
        MAX(TOTAL_EVENTS) AS max_events
    FROM {read_sql};
    """
    row = con.execute(query).fetchone()
    con.close()
    
    total_sessions = row[0]
    bounces = row[1]
    bounce_rate = round((bounces / total_sessions * 100), 2) if total_sessions > 0 else 0.0
    
    return {
        "total_sessions": total_sessions,
        "single_event_bounces": bounces,
        "bounce_rate_pct": bounce_rate,
        "avg_events_per_session": row[2],
        "median_events": row[3],
        "p75_events": row[4],
        "p90_events": row[5],
        "max_events": row[6]
    }

def get_transition_matrix(file_path: str, delimiter: str = "->", top_n: int = 8) -> pd.DataFrame:
    """
    Generates a 2D Transition Matrix (Source Event x Target Event) for heatmaps.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    clean_delim = delimiter.replace("'", "''")
    
    # 1. Find top N events
    top_events_sql = f"""
    WITH unnested AS (
        SELECT unnest(string_split(EVENT_PATH, '{clean_delim}')) AS event_name
        FROM {read_sql}
    )
    SELECT trim(event_name) FROM unnested WHERE trim(event_name) != ''
    GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT {top_n};
    """
    top_events = [r[0] for r in con.execute(top_events_sql).fetchall()]
    
    # 2. Get transition counts between top events
    pairs_sql = f"""
    WITH split_events AS (
        SELECT string_split(EVENT_PATH, '{clean_delim}') AS events
        FROM {read_sql}
        WHERE EVENT_PATH IS NOT NULL
    ),
    pairs AS (
        SELECT 
            trim(events[i]) AS source_event,
            trim(events[i+1]) AS target_event
        FROM split_events, generate_series(1, len(events) - 1) AS t(i)
    )
    SELECT source_event, target_event, COUNT(*) AS count
    FROM pairs
    GROUP BY 1, 2;
    """
    pairs_df = con.execute(pairs_sql).fetchdf()
    con.close()
    
    # Pivot into 2D matrix
    matrix = pd.DataFrame(0, index=top_events, columns=top_events)
    for row in pairs_df.itertuples():
        if row.source_event in matrix.index and row.target_event in matrix.columns:
            matrix.loc[row.source_event, row.target_event] = row.count
            
    return matrix
