import sys
import os

user_site = os.path.expanduser("~/Library/Python/3.9/lib/python/site-packages")
if os.path.exists(user_site) and user_site not in sys.path:
    sys.path.insert(0, user_site)

import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional

def _get_read_sql(file_path: str) -> str:
    clean_path = file_path.replace("'", "''")
    if file_path.endswith(".parquet") or file_path.endswith(".pq"):
        return f"read_parquet('{clean_path}')"
    return f"read_csv_auto('{clean_path}', header=True)"

def get_event_frequencies(file_path: str, delimiter: str = "->", top_n: int = 20) -> pd.DataFrame:
    """
    Unnests EVENT_PATH by delimiter and calculates individual event frequencies.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    clean_delim = delimiter.replace("'", "''")
    
    query = f"""
    WITH unnested AS (
        SELECT 
            unnest(string_split(EVENT_PATH, '{clean_delim}')) AS event_name
        FROM {read_sql}
        WHERE EVENT_PATH IS NOT NULL
    )
    SELECT 
        trim(event_name) AS event_name,
        COUNT(*) AS occurrence_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM unnested), 2) AS percentage
    FROM unnested
    WHERE trim(event_name) != ''
    GROUP BY 1
    ORDER BY occurrence_count DESC
    LIMIT {top_n};
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df

def get_top_paths(file_path: str, top_n: int = 15, min_events: int = 1) -> pd.DataFrame:
    """
    Ranks the most frequent full user navigation paths in high-speed DuckDB SQL.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    
    query = f"""
    SELECT 
        EVENT_PATH AS full_path,
        TOTAL_EVENTS,
        COUNT(*) AS session_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {read_sql}), 2) AS share_percent
    FROM {read_sql}
    WHERE EVENT_PATH IS NOT NULL AND TOTAL_EVENTS >= {min_events}
    GROUP BY 1, 2
    ORDER BY session_count DESC
    LIMIT {top_n};
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df

def get_transition_pairs(file_path: str, delimiter: str = "->", top_n: int = 20) -> pd.DataFrame:
    """
    Calculates step-to-step event transition pairs (e.g. Step A -> Step B).
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    clean_delim = delimiter.replace("'", "''")
    
    query = f"""
    WITH split_events AS (
        SELECT 
            SESSION,
            string_split(EVENT_PATH, '{clean_delim}') AS events
        FROM {read_sql}
        WHERE EVENT_PATH IS NOT NULL
    ),
    pairs AS (
        SELECT 
            trim(events[i]) AS source_event,
            trim(events[i+1]) AS target_event
        FROM split_events,
             generate_series(1, len(events) - 1) AS t(i)
    )
    SELECT 
        source_event || ' -> ' || target_event AS transition,
        source_event,
        target_event,
        COUNT(*) AS transition_count
    FROM pairs
    WHERE trim(source_event) != '' AND trim(target_event) != ''
    GROUP BY 1, 2, 3
    ORDER BY transition_count DESC
    LIMIT {top_n};
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df

def calculate_funnel(file_path: str, steps: List[str], delimiter: str = "->") -> pd.DataFrame:
    """
    Calculates sequential funnel drop-off metrics in a SINGLE-PASS high-speed DuckDB query.
    """
    if not steps:
        raise ValueError("Funnel requires at least one step.")

    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    clean_steps = [s.strip() for s in steps]
    
    # Build a single-pass CASE WHEN query for all steps to scan the file ONCE
    select_expressions = []
    patterns = []
    
    for idx, step in enumerate(clean_steps):
        sub_sequence = clean_steps[:idx+1]
        pattern = "%".join([s.replace("%", "\\%") for s in sub_sequence])
        pattern = f"%{pattern}%"
        clean_pat = pattern.replace("'", "''")
        patterns.append(clean_pat)
        select_expressions.append(f"COUNT(CASE WHEN EVENT_PATH LIKE '{clean_pat}' THEN 1 END) AS step_{idx+1}")
        
    single_pass_sql = f"""
    SELECT 
        {', '.join(select_expressions)}
    FROM {read_sql};
    """
    
    row = con.execute(single_pass_sql).fetchone()
    con.close()
    
    funnel_data = []
    first_count = row[0] if len(row) > 0 and row[0] is not None else 0
    prev_count = first_count
    
    for idx, count in enumerate(row):
        cnt = count if count is not None else 0
        step_name = clean_steps[idx]
        
        if idx == 0:
            conversion_pct = 100.0 if cnt > 0 else 0.0
            dropoff_pct = 0.0
        else:
            conversion_pct = round((cnt / prev_count) * 100, 2) if prev_count > 0 else 0.0
            dropoff_pct = round(100.0 - conversion_pct, 2)
            
        overall_pct = round((cnt / first_count) * 100, 2) if first_count > 0 else 0.0
        prev_count = cnt

        funnel_data.append({
            "step_number": idx + 1,
            "step_name": step_name,
            "session_count": cnt,
            "step_conversion_pct": conversion_pct,
            "step_dropoff_pct": dropoff_pct,
            "overall_conversion_pct": overall_pct
        })
        
    return pd.DataFrame(funnel_data)

def search_sessions(
    file_path: str, 
    contains_event: Optional[str] = None,
    exact_subpath: Optional[str] = None,
    min_events: int = 1,
    limit: int = 50
) -> pd.DataFrame:
    """
    Searches sessions containing specific events or subpaths.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    
    where_clauses = [f"TOTAL_EVENTS >= {min_events}"]
    if contains_event:
        clean_ev = contains_event.replace("'", "''")
        where_clauses.append(f"EVENT_PATH LIKE '%{clean_ev}%'")
    if exact_subpath:
        clean_sub = exact_subpath.replace("'", "''")
        where_clauses.append(f"EVENT_PATH LIKE '%{clean_sub}%'")
        
    where_str = " AND ".join(where_clauses)
    
    query = f"""
    SELECT SESSION, EVENT_PATH, TOTAL_EVENTS
    FROM {read_sql}
    WHERE {where_str}
    LIMIT {limit};
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df

def run_custom_query(file_path: str, sql_query: str) -> pd.DataFrame:
    """
    Executes user-provided DuckDB SQL query. Replace 'data' table keyword with the read expression.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    
    formatted_sql = sql_query.replace("data", read_sql)
    df = con.execute(formatted_sql).fetchdf()
    con.close()
    return df
