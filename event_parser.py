import os

import duckdb
import pandas as pd
import threading
from typing import List, Dict, Any, Optional


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
        con = _connect()
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
        # list_distinct does not preserve source order in DuckDB. Retain only
        # the first indexed occurrence of each event instead.
        unique_indexes = (
            f"list_filter(generate_series(1, len({split_list})), "
            f"i -> list_position({split_list}, {split_list}[i]) = i)"
        )
        unique_events = f"list_transform({unique_indexes}, i -> {split_list}[i])"
        return f"array_to_string({unique_events}, '{clean_delim}')"
    return column_name

def get_event_frequencies(file_path: str, delimiter: str = "->", top_n: int = 20, dedupe_mode: Optional[str] = None) -> pd.DataFrame:
    """
    Unnests EVENT_PATH by delimiter and calculates individual event frequencies.
    """
    con = _connect()
    read_sql = _get_read_sql(file_path)
    clean_delim = delimiter.replace("'", "''")
    mode = dedupe_mode or "none"
    index_filter = ""
    if mode == "consecutive":
        index_filter = (
            "WHERE event_index = 1 "
            "OR events[event_index] != events[event_index - 1]"
        )
    elif mode == "unique":
        index_filter = (
            "WHERE list_position(events, events[event_index]) = event_index"
        )

    query = f"""
    WITH paths AS (
        SELECT
            list_transform(
                string_split(EVENT_PATH, '{clean_delim}'),
                event -> trim(event)
            ) AS events
        FROM {read_sql}
        WHERE EVENT_PATH IS NOT NULL
    ),
    indexed AS (
        SELECT
            events,
            generate_subscripts(events, 1) AS event_index
        FROM paths
    ),
    unnested AS (
        SELECT events[event_index] AS event_name
        FROM indexed
        {index_filter}
    ),
    counts AS (
        SELECT
            event_name,
            COUNT(*) AS occurrence_count
        FROM unnested
        WHERE event_name != ''
        GROUP BY event_name
    )
    SELECT
        event_name,
        occurrence_count,
        ROUND(
            occurrence_count * 100.0
            / NULLIF(SUM(occurrence_count) OVER (), 0),
            2
        ) AS percentage
    FROM counts
    ORDER BY occurrence_count DESC
    LIMIT {top_n};
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df

def get_top_paths(
    file_path: str,
    top_n: int = 15,
    min_events: int = 1,
    dedupe_mode: Optional[str] = None,
    delimiter: str = "->",
) -> pd.DataFrame:
    """
    Ranks the most frequent full user navigation paths in high-speed DuckDB SQL.
    """
    con = _connect()
    read_sql = _get_read_sql(file_path)
    path_expr = "EVENT_PATH"
    if dedupe_mode and dedupe_mode != "none":
        path_expr = sanitize_event_path_sql("EVENT_PATH", delimiter=delimiter, mode=dedupe_mode)
    
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
    con = _connect()
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

    con = _connect()
    read_sql = _get_read_sql(file_path)
    split_sql = _get_split_sql(file_path, delimiter, dedupe_mode=dedupe_mode)
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
        
        if sequential:
            # Carry the matched position forward so repeated steps such as
            # A -> B -> A are matched correctly.
            position_ctes = []
            previous_cte = "split_events"
            previous_position = None
            for position_idx, funnel_step in enumerate(sub_sequence, 1):
                clean_step = funnel_step.replace("'", "''")
                cte_name = f"matched_{position_idx}"
                if previous_position is None:
                    position_expr = f"list_position(events, '{clean_step}')"
                else:
                    relative_position = (
                        f"list_position(list_slice(events, {previous_position} + 1, len(events)), "
                        f"'{clean_step}')"
                    )
                    position_expr = (
                        f"CASE WHEN {previous_position} > 0 AND {relative_position} > 0 "
                        f"THEN {previous_position} + {relative_position} ELSE 0 END"
                    )
                position_ctes.append(
                    f"{cte_name} AS (SELECT *, {position_expr} AS position_{position_idx} "
                    f"FROM {previous_cte})"
                )
                previous_cte = cte_name
                previous_position = f"position_{position_idx}"
            matching_sql = ",\n".join(position_ctes)
            sql = f"""
            WITH split_events AS (
                SELECT {split_sql} AS events
                FROM {read_sql}
                WHERE EVENT_PATH IS NOT NULL
            ),
            {matching_sql}
            SELECT COUNT(*)
            FROM {previous_cte}
            WHERE {previous_position} > 0;
            """
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
    limit: int = 50,
    delimiter: str = "->",
) -> pd.DataFrame:
    """
    Searches sessions containing specific events or subpaths.
    """
    con = _connect()
    read_sql = _get_read_sql(file_path)
    
    where_clauses = [f"TOTAL_EVENTS >= {min_events}"]
    clean_delimiter = delimiter.replace("'", "''")
    if contains_event:
        clean_ev = contains_event.replace("'", "''")
        events_expr = (
            f"list_transform(string_split(EVENT_PATH, '{clean_delimiter}'), "
            "event -> trim(event))"
        )
        where_clauses.append(f"list_contains({events_expr}, '{clean_ev}')")
    if exact_subpath:
        clean_sub = exact_subpath.replace("'", "''")
        where_clauses.append(
            f"strpos('{clean_delimiter}' || EVENT_PATH || '{clean_delimiter}', "
            f"'{clean_delimiter}' || '{clean_sub}' || '{clean_delimiter}') > 0"
        )
        
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

def run_custom_query(
    file_path: str,
    sql_query: str,
    max_rows: Optional[int] = None,
    timeout_seconds: Optional[float] = None,
    connection_callback=None,
) -> pd.DataFrame:
    """
    Executes user-provided DuckDB SQL query. Replace 'data' table keyword with the read expression.
    """
    con = _connect()
    if connection_callback:
        connection_callback(con)
    read_sql = _get_read_sql(file_path)
    
    con.execute(f"CREATE VIEW data AS SELECT * FROM {read_sql}")
    timer = None
    if timeout_seconds:
        timer = threading.Timer(timeout_seconds, con.interrupt)
        timer.daemon = True
        timer.start()
    try:
        cursor = con.execute(sql_query)
        if max_rows is None:
            return cursor.fetchdf()
        rows = cursor.fetchmany(max_rows + 1)
        if len(rows) > max_rows:
            raise ValueError(f"Query result exceeds the {max_rows:,}-row limit")
        columns = [column[0] for column in cursor.description]
        return pd.DataFrame.from_records(rows, columns=columns)
    finally:
        if timer:
            timer.cancel()
        con.close()
