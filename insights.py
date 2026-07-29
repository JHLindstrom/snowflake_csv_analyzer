import os

import duckdb
import pandas as pd
from typing import Dict, Any


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    memory_limit = os.getenv("TRISHULA_DUCKDB_MEMORY_LIMIT", "1GB").replace("'", "''")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    return con


def _get_read_sql(file_path: str) -> str:
    clean_path = file_path.replace("'", "''")
    if file_path.endswith(".parquet") or file_path.endswith(".pq"):
        return f"read_parquet('{clean_path}')"
    return f"read_csv_auto('{clean_path}', header=True)"

def get_entry_exit_analytics(file_path: str, delimiter: str = "->", top_n: int = 10) -> Dict[str, pd.DataFrame]:
    """
    Analyzes entry events (first event) and exit events (last event) in user sessions directly in DuckDB SQL.
    """
    con = _connect()
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
    con = _connect()
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

def get_transition_matrix(
    file_path: str,
    delimiter: str = "->",
    top_n: int = 8,
    dedupe_mode: str = "none",
) -> pd.DataFrame:
    """
    Generates a 2D Transition Matrix (Source Event x Target Event) for heatmaps.
    """
    if dedupe_mode not in {"none", "consecutive", "unique"}:
        raise ValueError(f"Unsupported deduplication mode: {dedupe_mode}")

    con = _connect()
    read_sql = _get_read_sql(file_path)
    clean_delim = delimiter.replace("'", "''")
    filtered_events_expr = {
        "none": "events",
        "consecutive": """
            list_filter(
                list_transform(
                    generate_series(1, len(events)),
                    event_index -> CASE
                        WHEN event_index = 1
                          OR events[event_index] IS DISTINCT FROM events[event_index - 1]
                        THEN events[event_index]
                        ELSE NULL
                    END
                ),
                event_name -> event_name IS NOT NULL
            )
        """,
        "unique": """
            list_transform(
                list_filter(
                    generate_series(1, len(events)),
                    event_index -> list_position(events, events[event_index]) = event_index
                ),
                event_index -> events[event_index]
            )
        """,
    }[dedupe_mode]

    # Materialize each split path once, deduplicate within its list, and derive
    # adjacent pairs by index. This avoids both the previous near-quadratic
    # repeated splitting and memory-heavy window sorts over every event.
    query = f"""
    WITH paths AS MATERIALIZED (
        SELECT
            row_number() OVER () AS path_id,
            list_filter(
                list_transform(
                    string_split(EVENT_PATH, '{clean_delim}'),
                    event_name -> trim(event_name)
                ),
                event_name -> event_name != ''
            ) AS events
        FROM {read_sql}
        WHERE EVENT_PATH IS NOT NULL
    ),
    filtered_paths AS MATERIALIZED (
        SELECT path_id, {filtered_events_expr} AS events
        FROM paths
    ),
    event_counts AS (
        SELECT event_name, count(*) AS event_count
        FROM filtered_paths,
             unnest(events) AS event(event_name)
        GROUP BY event_name
    ),
    top_events AS (
        SELECT
            event_name,
            row_number() OVER (
                ORDER BY event_count DESC, event_name
            ) AS event_rank
        FROM event_counts
        ORDER BY event_count DESC, event_name
        LIMIT {top_n}
    ),
    pairs AS (
        SELECT
            events[event_index] AS source_event,
            events[event_index + 1] AS target_event
        FROM filtered_paths,
             generate_series(1, len(events) - 1) AS position(event_index)
    ),
    transition_counts AS (
        SELECT source_event, target_event, count(*) AS transition_count
        FROM pairs
        GROUP BY source_event, target_event
    )
    SELECT
        source.event_name AS source_event,
        target.event_name AS target_event,
        coalesce(transitions.transition_count, 0) AS transition_count,
        source.event_rank AS source_rank,
        target.event_rank AS target_rank
    FROM top_events AS source
    CROSS JOIN top_events AS target
    LEFT JOIN transition_counts AS transitions
        ON transitions.source_event = source.event_name
       AND transitions.target_event = target.event_name
    ORDER BY source.event_rank, target.event_rank;
    """
    try:
        matrix_rows = con.execute(query).fetchdf()
    finally:
        con.close()

    if matrix_rows.empty:
        return pd.DataFrame(dtype="int64")

    top_events = (
        matrix_rows[["source_event", "source_rank"]]
        .drop_duplicates()
        .sort_values("source_rank")["source_event"]
        .tolist()
    )
    matrix = (
        matrix_rows.pivot(
            index="source_event",
            columns="target_event",
            values="transition_count",
        )
        .reindex(index=top_events, columns=top_events, fill_value=0)
        .astype("int64")
    )
    matrix.index.name = None
    matrix.columns.name = None
    return matrix
