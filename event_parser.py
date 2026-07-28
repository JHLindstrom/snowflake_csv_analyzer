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

def _get_split_sql(file_path: str, delimiter: str, dedupe_mode: Optional[str] = None) -> str:
    clean_delim = delimiter.replace("'", "''")
    path_expr = "EVENT_PATH"
    if dedupe_mode:
        path_expr = sanitize_event_path_sql("EVENT_PATH", delimiter=delimiter, mode=dedupe_mode)
    return f"list_transform(string_split({path_expr}, '{clean_delim}'), x -> trim(x))"

def detect_delimiter(file_path: str) -> str:
    """
    Auto-detects the event path delimiter (->, ,, >, |) from sample rows.
    """
    try:
        con = duckdb.connect(database=":memory:")
        read_sql = _get_read_sql(file_path)
        sample = con.execute(f"SELECT EVENT_PATH FROM {read_sql} WHERE EVENT_PATH IS NOT NULL AND trim(EVENT_PATH) != '' LIMIT 10").fetchall()
        con.close()
        
        sample_text = " ".join([r[0] for r in sample if r[0]])
        if "->" in sample_text:
            return "->"
        elif "," in sample_text:
            return ","
        elif ">" in sample_text:
            return ">"
        elif "|" in sample_text:
            return "|"
    except Exception:
        pass
    return "->"

def sanitize_event_path_sql(column_name: str = "EVENT_PATH", delimiter: str = "->", mode: str = "consecutive") -> str:
    """
    Returns SQL expression to sanitize EVENT_PATH strings.
    - 'consecutive': Collapses repeated adjacent events (A->A->A -> A)
    - 'unique': Keeps only unique events per session
    """
    clean_delim = delimiter.replace("'", "''")
    split_list = f"list_transform(string_split({column_name}, '{clean_delim}'), x -> trim(x))"
    
    if mode == "consecutive":
        # Pure DuckDB list operation: filter out elements where event[i] == event[i-1]
        dedupe_expr = f"""
        list_filter(
            list_transform(
                generate_series(1, len({split_list})),
                i -> CASE WHEN i = 1 OR {split_list}[i] != {split_list}[i-1] THEN {split_list}[i] ELSE NULL END
            ),
            x -> x IS NOT NULL
        )
        """
        return f"array_to_string({dedupe_expr}, '{clean_delim}')"
    elif mode == "unique":
        return f"array_to_string(list_distinct({split_list}), '{clean_delim}')"
    return column_name

def get_event_frequencies(file_path: str, delimiter: str = "->", top_n: int = 20, dedupe_mode: Optional[str] = None) -> pd.DataFrame:
    """
    Unnests EVENT_PATH by delimiter and calculates individual event frequencies.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    clean_delim = delimiter.replace("'", "''")
    path_expr = "EVENT_PATH"
    if dedupe_mode and dedupe_mode != "none":
        path_expr = sanitize_event_path_sql("EVENT_PATH", delimiter=delimiter, mode=dedupe_mode)
    
    query = f"""
    WITH unnested AS (
        SELECT 
            unnest(string_split({path_expr}, '{clean_delim}')) AS event_name
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

def get_top_paths(file_path: str, top_n: int = 15, min_events: int = 1, dedupe_mode: Optional[str] = None) -> pd.DataFrame:
    """
    Ranks the most frequent full user navigation paths in high-speed DuckDB SQL.
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    path_expr = "EVENT_PATH"
    if dedupe_mode and dedupe_mode != "none":
        path_expr = sanitize_event_path_sql("EVENT_PATH", mode=dedupe_mode)
    
    query = f"""
    SELECT 
        {path_expr} AS full_path,
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

def get_transition_pairs(file_path: str, delimiter: str = "->", top_n: int = 20, dedupe_mode: Optional[str] = None) -> pd.DataFrame:
    """
    Calculates step-to-step event transition pairs (e.g. Step A -> Step B).
    """
    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    clean_delim = delimiter.replace("'", "''")
    path_expr = "EVENT_PATH"
    if dedupe_mode and dedupe_mode != "none":
        path_expr = sanitize_event_path_sql("EVENT_PATH", delimiter=delimiter, mode=dedupe_mode)
    
    query = f"""
    WITH split_events AS (
        SELECT 
            SESSION,
            string_split({path_expr}, '{clean_delim}') AS events
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

def calculate_funnel(
    file_path: str, 
    steps: List[str], 
    delimiter: str = "->",
    sequential: bool = True,
    dedupe_mode: Optional[str] = None,
    progress_callback = None,
    skip_check_callback = None
) -> pd.DataFrame:
    """
    Calculates session reach or sequential funnel conversion in ultra-fast DuckDB SQL.
    Uses C++ list position indexing without array transformation overhead for maximum throughput.
    """
    if not steps:
        raise ValueError("Funnel requires at least one step.")

    con = duckdb.connect(database=":memory:")
    read_sql = _get_read_sql(file_path)
    # Always use fast direct array splitting (list_position gives identical first-occurrence ordering)
    split_sql = _get_split_sql(file_path, delimiter, dedupe_mode=None)
    clean_steps = [s.strip() for s in steps]
    total_steps = len(clean_steps)
    
    # Get total sessions count for baseline percent calculation
    total_sessions = con.execute(f"SELECT COUNT(*) FROM {read_sql}").fetchone()[0]
    total_sessions = total_sessions if total_sessions > 0 else 1
    
    funnel_data = []
    first_count = None
    prev_count = None
    
    for idx, step in enumerate(clean_steps):
        # Check if user requested to skip remaining steps
        if skip_check_callback and skip_check_callback():
            break

        if progress_callback:
            should_continue = progress_callback(idx + 1, total_steps, step)
            if should_continue is False:
                break
            
        sub_sequence = clean_steps[:idx+1]
        
        if sequential and len(sub_sequence) > 1:
            # Sequential mode: Step N must occur after Step N-1
            conds = []
            for i in range(len(sub_sequence)):
                curr_step = sub_sequence[i].replace("'", "''")
                conds.append(f"list_position(events, '{curr_step}') > 0")
                if i > 0:
                    prev_step = sub_sequence[i-1].replace("'", "''")
                    conds.append(f"list_position(events, '{curr_step}') > list_position(events, '{prev_step}')")
            where_expr = " AND ".join(conds)
        else:
            # Independent reach / Step 1 mode: Session contains current step
            curr_step = step.replace("'", "''")
            where_expr = f"list_position(events, '{curr_step}') > 0"
        
        sql = f"""
        WITH split_events AS (
            SELECT {split_sql} AS events
            FROM {read_sql}
            WHERE EVENT_PATH IS NOT NULL
        )
        SELECT COUNT(*) 
        FROM split_events
        WHERE {where_expr};
        """
        count = con.execute(sql).fetchone()[0]
        cnt = count if count is not None else 0
        
        if idx == 0:
            first_count = cnt
            prev_count = cnt
            conversion_pct = round((cnt / total_sessions) * 100, 2)
            dropoff_pct = round(100.0 - conversion_pct, 2)
        else:
            conversion_pct = round((cnt / prev_count) * 100, 2) if prev_count > 0 else 0.0
            dropoff_pct = round(100.0 - conversion_pct, 2)
            prev_count = cnt
            
        overall_pct = round((cnt / first_count) * 100, 2) if first_count > 0 else 0.0

        funnel_data.append({
            "step_number": idx + 1,
            "step_name": step,
            "session_count": cnt,
            "step_conversion_pct": conversion_pct,
            "step_dropoff_pct": dropoff_pct,
            "overall_conversion_pct": overall_pct
        })
        
    con.close()
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
