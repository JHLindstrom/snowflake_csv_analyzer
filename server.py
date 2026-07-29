import sys
import os
import argparse
import hashlib
import hmac
import json
import secrets
import shutil
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
import duckdb
import pandas as pd
from typing import Any, Optional, List

from fastapi import FastAPI, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn

from converter import convert_csv_to_parquet, inspect_file, validate_dataset_schema
from duckdb_config import get_duckdb_settings
from errors import DatasetValidationError
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
from trishula_web.pdf_reports import build_selected_tab_pdf

app = FastAPI(title="Trishula Web Analytics", version="0.3.0")
WEB_ROOT = Path(__file__).with_name("trishula_web")
app.mount("/static", StaticFiles(directory=WEB_ROOT / "static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@dataclass
class DatasetState:
    raw_file: Optional[str] = None
    parquet_file: Optional[str] = None
    delimiter: str = "->"
    managed_files: tuple = ()
    last_access: float = 0.0


@dataclass
class QueryJob:
    job_id: str
    session_id: Optional[str]
    status: str = "queued"
    created_at: float = 0.0
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    connection: Any = None
    result: Optional[dict] = None
    error: Optional[str] = None


DEFAULT_DATASET_STATE = DatasetState()
DATASET_SESSIONS = {}
DATASET_SESSIONS_LOCK = RLock()
SESSION_CONTEXT: ContextVar[Optional[str]] = ContextVar("dataset_session", default=None)
SESSION_COOKIE = "trishula_session"
AUTH_COOKIE = "trishula_access"
QUERY_JOBS = {}
QUERY_JOBS_LOCK = RLock()

UPLOAD_DIR = os.getenv(
    "TRISHULA_UPLOAD_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads"),
)
os.makedirs(UPLOAD_DIR, exist_ok=True)
TRUSTED_LOCAL_MODE = os.getenv("TRISHULA_TRUSTED_LOCAL_MODE", "").lower() in {"1", "true", "yes"}
MAX_UPLOAD_BYTES = int(os.getenv("TRISHULA_MAX_UPLOAD_BYTES", str(10 * 1024**3)))
UPLOAD_CHUNK_BYTES = 1024 * 1024
ALLOWED_EXTENSIONS = {".csv", ".parquet", ".pq"}
VALID_DEDUPE_MODES = {"none", "consecutive", "unique"}
MAX_FUNNEL_STEPS = int(os.getenv("TRISHULA_MAX_FUNNEL_STEPS", "50"))
MAX_QUERY_ROWS = int(os.getenv("TRISHULA_MAX_QUERY_ROWS", "10000"))
QUERY_TIMEOUT_SECONDS = float(os.getenv("TRISHULA_QUERY_TIMEOUT_SECONDS", "30"))
SESSION_TTL_SECONDS = int(os.getenv("TRISHULA_SESSION_TTL_SECONDS", "86400"))
MAX_SQL_CHARS = int(os.getenv("TRISHULA_MAX_SQL_CHARS", "100000"))
QUERY_JOB_TTL_SECONDS = int(os.getenv("TRISHULA_QUERY_JOB_TTL_SECONDS", "3600"))
MAX_CONCURRENT_ANALYTICS = max(
    1, int(os.getenv("TRISHULA_MAX_CONCURRENT_ANALYTICS", "1"))
)
ANALYTICS_QUEUE_TIMEOUT_SECONDS = max(
    0.1, float(os.getenv("TRISHULA_ANALYTICS_QUEUE_TIMEOUT_SECONDS", "30"))
)
ANALYTICS_QUERY_SLOTS = threading.BoundedSemaphore(MAX_CONCURRENT_ANALYTICS)
ACCESS_TOKEN = os.getenv("TRISHULA_ACCESS_TOKEN", "")
COOKIE_SECURE = os.getenv("TRISHULA_COOKIE_SECURE", "").lower() in {"1", "true", "yes"}
ALLOW_NETWORK = os.getenv("TRISHULA_ALLOW_NETWORK", "").lower() in {"1", "true", "yes"}
SESSION_DB_PATH = Path(
    os.getenv("TRISHULA_SESSION_DB", str(Path(UPLOAD_DIR) / "sessions.sqlite3"))
).expanduser().resolve()

LOGIN_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Trishula Login</title><style>
body{font-family:system-ui,sans-serif;background:#090d16;color:#f8fafc;display:grid;place-items:center;min-height:100vh;margin:0}
form{width:min(380px,calc(100% - 48px));background:#151e30;padding:28px;border-radius:16px;border:1px solid #334155}
input,button{width:100%;box-sizing:border-box;padding:12px;margin-top:12px;border-radius:8px}
button{background:#38bdf8;border:0;font-weight:700;cursor:pointer}.error{color:#fb7185;min-height:1.5em}
</style></head><body><form id="login"><h1>Trishula</h1><p>Enter the configured access token.</p>
<input id="token" type="password" autocomplete="current-password" required>
<button type="submit">Sign in</button><p class="error" id="error"></p></form>
<script>document.getElementById('login').addEventListener('submit',async(e)=>{e.preventDefault();
const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify({token:document.getElementById('token').value})});
if(r.ok){location.href='/'}else{document.getElementById('error').textContent='Invalid access token'}}</script>
</body></html>"""


def _initialize_session_database() -> None:
    SESSION_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(SESSION_DB_PATH) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_sessions (
                session_id TEXT PRIMARY KEY,
                raw_file TEXT,
                parquet_file TEXT,
                delimiter TEXT NOT NULL,
                managed_files TEXT NOT NULL,
                last_access REAL NOT NULL
            )
            """
        )
    os.chmod(SESSION_DB_PATH, 0o600)


def _persist_dataset_state(session_id: str, state: DatasetState) -> None:
    with sqlite3.connect(SESSION_DB_PATH) as con:
        con.execute(
            """
            INSERT INTO dataset_sessions
                (session_id, raw_file, parquet_file, delimiter, managed_files, last_access)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                raw_file=excluded.raw_file,
                parquet_file=excluded.parquet_file,
                delimiter=excluded.delimiter,
                managed_files=excluded.managed_files,
                last_access=excluded.last_access
            """,
            (
                session_id,
                state.raw_file,
                state.parquet_file,
                state.delimiter,
                json.dumps(state.managed_files),
                state.last_access,
            ),
        )


def _load_dataset_state(session_id: str) -> Optional[DatasetState]:
    with sqlite3.connect(SESSION_DB_PATH) as con:
        row = con.execute(
            """
            SELECT raw_file, parquet_file, delimiter, managed_files, last_access
            FROM dataset_sessions WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
    if not row:
        return None
    return DatasetState(
        raw_file=row[0],
        parquet_file=row[1],
        delimiter=row[2],
        managed_files=tuple(json.loads(row[3])),
        last_access=row[4],
    )


def _delete_persisted_session(session_id: str) -> None:
    with sqlite3.connect(SESSION_DB_PATH) as con:
        con.execute("DELETE FROM dataset_sessions WHERE session_id = ?", (session_id,))


def _prune_persisted_sessions(expired_before: float) -> None:
    with sqlite3.connect(SESSION_DB_PATH) as con:
        rows = con.execute(
            """
            SELECT session_id, raw_file, parquet_file, delimiter, managed_files, last_access
            FROM dataset_sessions WHERE last_access < ?
            """,
            (expired_before,),
        ).fetchall()
        for row in rows:
            _cleanup_dataset_state(
                DatasetState(
                    raw_file=row[1],
                    parquet_file=row[2],
                    delimiter=row[3],
                    managed_files=tuple(json.loads(row[4])),
                    last_access=row[5],
                )
            )
        con.execute(
            "DELETE FROM dataset_sessions WHERE last_access < ?", (expired_before,)
        )


_initialize_session_database()


def _apply_security_headers(response, nonce: str):
    response.headers["Content-Security-Policy"] = (
        f"default-src 'self'; script-src 'self' 'nonce-{nonce}'; "
        "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
        "font-src 'self'; connect-src 'self'; object-src 'none'; "
        "base-uri 'none'; frame-ancestors 'none'"
    )
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _expected_auth_cookie() -> str:
    return hashlib.sha256(ACCESS_TOKEN.encode("utf-8")).hexdigest()


def _cleanup_dataset_state(state: DatasetState) -> None:
    upload_root = Path(UPLOAD_DIR).resolve()
    for file_path in state.managed_files:
        candidate = Path(file_path).resolve()
        try:
            candidate.relative_to(upload_root)
        except ValueError:
            continue
        candidate.unlink(missing_ok=True)


@app.middleware("http")
async def dataset_session_middleware(request: Request, call_next):
    nonce = secrets.token_urlsafe(24)
    request.state.csp_nonce = nonce
    if ACCESS_TOKEN and request.url.path != "/api/login":
        supplied_token = request.cookies.get(AUTH_COOKIE, "")
        if not hmac.compare_digest(supplied_token, _expected_auth_cookie()):
            if request.method == "GET" and request.url.path == "/":
                login_html = LOGIN_HTML.replace("<script>", f'<script nonce="{nonce}">')
                return _apply_security_headers(
                    HTMLResponse(login_html, status_code=401), nonce
                )
            return _apply_security_headers(
                JSONResponse({"detail": "Authentication required"}, status_code=401),
                nonce,
            )

    session_id = request.cookies.get(SESSION_COOKIE)
    now = time.time()
    with DATASET_SESSIONS_LOCK:
        _prune_persisted_sessions(now - SESSION_TTL_SECONDS)
        expired_ids = [
            key
            for key, value in DATASET_SESSIONS.items()
            if value.last_access and now - value.last_access > SESSION_TTL_SECONDS
        ]
        for expired_id in expired_ids:
            _cleanup_dataset_state(DATASET_SESSIONS.pop(expired_id))
            _delete_persisted_session(expired_id)
        if session_id and session_id not in DATASET_SESSIONS:
            persisted_state = _load_dataset_state(session_id)
            if persisted_state:
                DATASET_SESSIONS[session_id] = persisted_state
        if not session_id or session_id not in DATASET_SESSIONS:
            session_id = uuid.uuid4().hex
            DATASET_SESSIONS[session_id] = DatasetState(
                raw_file=DEFAULT_DATASET_STATE.raw_file,
                parquet_file=DEFAULT_DATASET_STATE.parquet_file,
                delimiter=DEFAULT_DATASET_STATE.delimiter,
                last_access=now,
            )
        else:
            DATASET_SESSIONS[session_id].last_access = now
    context_token = SESSION_CONTEXT.set(session_id)
    try:
        response = await call_next(request)
    finally:
        with DATASET_SESSIONS_LOCK:
            current_state = DATASET_SESSIONS.get(session_id)
            if current_state:
                _persist_dataset_state(session_id, current_state)
        SESSION_CONTEXT.reset(context_token)
    response.set_cookie(
        SESSION_COOKIE,
        session_id,
        httponly=True,
        samesite="strict",
        secure=COOKIE_SECURE,
    )
    return _apply_security_headers(response, nonce)


@app.post("/api/login")
def login(payload: dict):
    if not ACCESS_TOKEN:
        raise HTTPException(status_code=404, detail="Authentication is not configured")
    supplied_token = payload.get("token", "")
    if not isinstance(supplied_token, str) or not hmac.compare_digest(
        supplied_token, ACCESS_TOKEN
    ):
        raise HTTPException(status_code=401, detail="Invalid access token")
    response = JSONResponse({"success": True})
    response.set_cookie(
        AUTH_COOKIE,
        _expected_auth_cookie(),
        httponly=True,
        samesite="strict",
        secure=COOKIE_SECURE,
    )
    return response


@app.post("/api/logout")
def logout():
    session_id = SESSION_CONTEXT.get()
    if session_id:
        with DATASET_SESSIONS_LOCK:
            state = DATASET_SESSIONS.pop(session_id, None)
            if state:
                _cleanup_dataset_state(state)
            _delete_persisted_session(session_id)
    response = JSONResponse({"success": True})
    response.delete_cookie(AUTH_COOKIE)
    response.delete_cookie(SESSION_COOKIE)
    return response


def _current_dataset_state() -> DatasetState:
    session_id = SESSION_CONTEXT.get()
    if session_id is None:
        return DEFAULT_DATASET_STATE
    with DATASET_SESSIONS_LOCK:
        return DATASET_SESSIONS[session_id]


def _require_dataset() -> DatasetState:
    state = _current_dataset_state()
    if not state.parquet_file or not os.path.exists(state.parquet_file):
        raise HTTPException(status_code=400, detail="No active file loaded")
    return state


def _validate_dedupe_mode(mode: str) -> str:
    if mode not in VALID_DEDUPE_MODES:
        raise HTTPException(status_code=422, detail="Invalid deduplication mode")
    return mode


def _require_trusted_local_mode(feature: str) -> None:
    if not TRUSTED_LOCAL_MODE:
        raise HTTPException(
            status_code=403,
            detail=f"{feature} is disabled. Set TRISHULA_TRUSTED_LOCAL_MODE=true only on a trusted local machine.",
        )


@contextmanager
def _analytics_query_slot():
    """Bound concurrent DuckDB workloads so per-connection limits remain useful."""
    acquired = ANALYTICS_QUERY_SLOTS.acquire(
        timeout=ANALYTICS_QUEUE_TIMEOUT_SECONDS
    )
    if not acquired:
        raise HTTPException(
            status_code=503,
            detail=(
                "Analytics capacity is busy. Wait for the active calculation "
                "to finish and retry."
            ),
        )
    try:
        yield
    finally:
        ANALYTICS_QUERY_SLOTS.release()


def init_active_file(
    file_path: str,
    delimiter: str = "->",
    state: Optional[DatasetState] = None,
    managed: bool = False,
):
    state = state or _current_dataset_state()
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    base, ext = os.path.splitext(file_path)

    if ext.lower() == ".csv":
        parquet_file = f"{base}.parquet"
        # Convert to parquet if missing or out of date
        if not os.path.exists(parquet_file) or os.path.getmtime(file_path) > os.path.getmtime(parquet_file):
            convert_csv_to_parquet(file_path, parquet_file)
    else:
        parquet_file = file_path

    validate_dataset_schema(parquet_file)
    detected_delimiter = detect_delimiter(parquet_file) if delimiter == "->" else delimiter
    previous_managed_files = state.managed_files
    state.raw_file = file_path
    state.parquet_file = parquet_file
    state.delimiter = detected_delimiter
    state.managed_files = (file_path, parquet_file) if managed else ()
    if previous_managed_files != state.managed_files:
        _cleanup_dataset_state(DatasetState(managed_files=previous_managed_files))
    return state

@app.post("/api/restart")
def restart_server():
    """Triggers an in-place process restart of the Python web server."""
    _require_trusted_local_mode("Server restart")
    def _restart():
        time.sleep(0.5)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    threading.Thread(target=_restart, daemon=True).start()
    return {"success": True, "message": "Server process restarting..."}

@app.post("/api/upload-file")
async def upload_file(file: UploadFile = File(...)):
    """Uploads a CSV or Parquet file via browser file selector."""
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only CSV and Parquet datasets are supported")
    dest_path = Path(UPLOAD_DIR).resolve() / f"{uuid.uuid4().hex}{suffix}"
    bytes_written = 0
    try:
        with dest_path.open("xb") as destination:
            while chunk := await file.read(UPLOAD_CHUNK_BYTES):
                bytes_written += len(chunk)
                if bytes_written > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Upload exceeds configured size limit")
                destination.write(chunk)
        init_active_file(str(dest_path), managed=True)
        return {"success": True, "filename": file.filename, "state": get_state()}
    except HTTPException:
        dest_path.unlink(missing_ok=True)
        dest_path.with_suffix(".parquet").unlink(missing_ok=True)
        raise
    except DatasetValidationError as exc:
        dest_path.unlink(missing_ok=True)
        dest_path.with_suffix(".parquet").unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as e:
        dest_path.unlink(missing_ok=True)
        dest_path.with_suffix(".parquet").unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        await file.close()

@app.get("/api/state")
def get_state():
    state = _current_dataset_state()
    if not state.parquet_file or not os.path.exists(state.parquet_file):
        return {"loaded": False}
    duckdb_settings = get_duckdb_settings()
    return {
        "loaded": True,
        "raw_file": os.path.basename(state.raw_file),
        "parquet_file": os.path.basename(state.parquet_file),
        "delimiter": state.delimiter,
        "file_size_mb": round(os.path.getsize(state.parquet_file) / (1024**2), 2),
        "duckdb_memory_limit": duckdb_settings.memory_limit,
        "duckdb_threads": duckdb_settings.threads,
        "csv_max_line_size": duckdb_settings.csv_max_line_size,
    }


@app.get("/api/storage")
def storage_status():
    state = _current_dataset_state()
    disk = shutil.disk_usage(UPLOAD_DIR)
    managed_bytes = sum(
        Path(file_path).stat().st_size
        for file_path in state.managed_files
        if Path(file_path).is_file()
    )
    return {
        "disk_free_gb": round(disk.free / 1024**3, 2),
        "disk_total_gb": round(disk.total / 1024**3, 2),
        "managed_dataset_mb": round(managed_bytes / 1024**2, 2),
        "managed_upload": bool(state.managed_files),
    }


@app.post("/api/unload")
def unload_dataset():
    state = _current_dataset_state()
    removed_managed_files = len(
        [file_path for file_path in state.managed_files if Path(file_path).is_file()]
    )
    _cleanup_dataset_state(state)
    state.raw_file = None
    state.parquet_file = None
    state.delimiter = "->"
    state.managed_files = ()
    return {"success": True, "removed_managed_files": removed_managed_files}


@app.get("/api/inspect")
def inspect():
    state = _require_dataset()
    with _analytics_query_slot():
        return inspect_file(state.parquet_file, limit=5)

@app.get("/api/insights")
def insights(delimiter: Optional[str] = None):
    state = _require_dataset()
    delim = delimiter or state.delimiter
    with _analytics_query_slot():
        summary = get_executive_summary_metrics(state.parquet_file)
        entry_exit = get_entry_exit_analytics(
            state.parquet_file, delimiter=delim, top_n=5
        )
    return {
        "summary": summary,
        "entry_points": entry_exit["entry_points"].to_dict(orient="records"),
        "exit_points": entry_exit["exit_points"].to_dict(orient="records")
    }

@app.get("/api/events")
def events(top: int = Query(20, ge=1, le=200), dedupe: str = "consecutive"):
    state = _require_dataset()
    with _analytics_query_slot():
        df = get_event_frequencies(
            state.parquet_file,
            delimiter=state.delimiter,
            top_n=top,
            dedupe_mode=_validate_dedupe_mode(dedupe),
        )
    return df.to_dict(orient="records")

@app.get("/api/heatmap")
def heatmap(top: int = Query(8, ge=1, le=50), dedupe: str = "consecutive"):
    state = _require_dataset()
    try:
        with _analytics_query_slot():
            matrix = get_transition_matrix(
                state.parquet_file,
                delimiter=state.delimiter,
                top_n=top,
                dedupe_mode=_validate_dedupe_mode(dedupe),
            )
    except duckdb.OutOfMemoryException as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                "Transition calculation exceeded the configured DuckDB memory "
                "limit. Try a smaller dataset or increase "
                "TRISHULA_DUCKDB_MEMORY_LIMIT."
            ),
        ) from exc
    return {
        "columns": matrix.columns.tolist(),
        "index": matrix.index.tolist(),
        "data": matrix.values.tolist()
    }

@app.get("/api/funnel")
def funnel(steps: str, dedupe: str = "consecutive", sequential: bool = True):
    state = _require_dataset()
    step_list = [s.strip() for s in steps.split(",") if s.strip()]
    if not step_list:
        raise HTTPException(status_code=400, detail="No steps provided")
    if len(step_list) > MAX_FUNNEL_STEPS:
        raise HTTPException(status_code=422, detail="Too many funnel steps")
    with _analytics_query_slot():
        df = calculate_funnel(
            state.parquet_file,
            step_list,
            delimiter=state.delimiter,
            sequential=sequential,
            dedupe_mode=_validate_dedupe_mode(dedupe),
        )
    return df.to_dict(orient="records")

@app.get("/api/search")
def search(
    event: Optional[str] = Query(None, max_length=500),
    subpath: Optional[str] = Query(None, max_length=2000),
    min_events: int = Query(1, ge=0),
    limit: int = Query(20, ge=1, le=500),
):
    state = _require_dataset()
    with _analytics_query_slot():
        df = search_sessions(
            state.parquet_file,
            contains_event=event,
            exact_subpath=subpath,
            min_events=min_events,
            limit=limit,
            delimiter=state.delimiter,
        )
    return df.to_dict(orient="records")

@app.post("/api/query")
def query(payload: dict):
    _require_trusted_local_mode("Custom SQL")
    state = _require_dataset()
    sql = payload.get("sql")
    if not sql:
        raise HTTPException(status_code=400, detail="Missing SQL query")
    if not isinstance(sql, str) or len(sql) > MAX_SQL_CHARS:
        raise HTTPException(status_code=422, detail="SQL query is too large")
    try:
        with _analytics_query_slot():
            df = run_custom_query(
                state.parquet_file,
                sql,
                max_rows=MAX_QUERY_ROWS,
                timeout_seconds=QUERY_TIMEOUT_SECONDS,
            )
        return {
            "columns": df.columns.tolist(),
            "records": df.to_dict(orient="records")
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _prune_query_jobs() -> None:
    cutoff = time.time() - QUERY_JOB_TTL_SECONDS
    with QUERY_JOBS_LOCK:
        expired = [
            job_id
            for job_id, job in QUERY_JOBS.items()
            if job.finished_at and job.finished_at < cutoff
        ]
        for job_id in expired:
            QUERY_JOBS.pop(job_id, None)


def _run_query_job(job: QueryJob, parquet_file: str, sql: str) -> None:
    with QUERY_JOBS_LOCK:
        if job.status == "cancelling":
            job.status = "cancelled"
            job.finished_at = time.time()
            return
        job.status = "running"
        job.started_at = time.time()

    def register_connection(connection):
        with QUERY_JOBS_LOCK:
            job.connection = connection
            if job.status == "cancelling":
                connection.interrupt()

    try:
        with _analytics_query_slot():
            with QUERY_JOBS_LOCK:
                if job.status == "cancelling":
                    job.status = "cancelled"
                    return
            df = run_custom_query(
                parquet_file,
                sql,
                max_rows=MAX_QUERY_ROWS,
                timeout_seconds=QUERY_TIMEOUT_SECONDS,
                connection_callback=register_connection,
            )
        with QUERY_JOBS_LOCK:
            if job.status == "cancelling":
                job.status = "cancelled"
            else:
                job.status = "completed"
                job.result = {
                    "columns": df.columns.tolist(),
                    "records": df.to_dict(orient="records"),
                }
    except Exception as exc:
        with QUERY_JOBS_LOCK:
            if job.status == "cancelling":
                job.status = "cancelled"
            else:
                job.status = "failed"
                job.error = str(exc)
    finally:
        with QUERY_JOBS_LOCK:
            job.connection = None
            job.finished_at = time.time()


def _get_owned_query_job(job_id: str) -> QueryJob:
    with QUERY_JOBS_LOCK:
        job = QUERY_JOBS.get(job_id)
        if not job or job.session_id != SESSION_CONTEXT.get():
            raise HTTPException(status_code=404, detail="Query job not found")
        return job


@app.post("/api/query/start")
def start_query(payload: dict):
    _require_trusted_local_mode("Custom SQL")
    state = _require_dataset()
    sql = payload.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        raise HTTPException(status_code=400, detail="Missing SQL query")
    if len(sql) > MAX_SQL_CHARS:
        raise HTTPException(status_code=422, detail="SQL query is too large")
    _prune_query_jobs()
    job = QueryJob(
        job_id=uuid.uuid4().hex,
        session_id=SESSION_CONTEXT.get(),
        created_at=time.time(),
    )
    with QUERY_JOBS_LOCK:
        QUERY_JOBS[job.job_id] = job
    threading.Thread(
        target=_run_query_job,
        args=(job, state.parquet_file, sql),
        daemon=True,
    ).start()
    return {"job_id": job.job_id, "status": job.status}


@app.get("/api/query/{job_id}")
def query_status(job_id: str):
    job = _get_owned_query_job(job_id)
    progress = None
    with QUERY_JOBS_LOCK:
        if job.connection is not None:
            try:
                raw_progress = job.connection.query_progress()
                progress = round(float(raw_progress), 2) if raw_progress >= 0 else None
            except Exception:
                progress = None
        return {
            "job_id": job.job_id,
            "status": job.status,
            "progress_pct": progress,
            "elapsed_seconds": round(
                (job.finished_at or time.time()) - (job.started_at or job.created_at),
                2,
            ),
            "result": job.result if job.status == "completed" else None,
            "error": job.error,
        }


@app.post("/api/query/{job_id}/cancel")
def cancel_query(job_id: str):
    job = _get_owned_query_job(job_id)
    with QUERY_JOBS_LOCK:
        if job.status not in {"queued", "running"}:
            return {"job_id": job.job_id, "status": job.status}
        job.status = "cancelling"
        if job.connection is not None:
            job.connection.interrupt()
    return {"job_id": job.job_id, "status": "cancelling"}


@app.post("/api/export-pdf")
def export_pdf(payload: dict):
    try:
        pdf = build_selected_tab_pdf(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    tab = str(payload.get("tab", "analysis"))
    safe_tab = "".join(character for character in tab if character.isalnum() or character == "-")
    return Response(
        pdf,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="trishula-{safe_tab or "analysis"}.pdf"'
        },
    )


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    template_path = WEB_ROOT / "templates" / "dashboard.html"
    return template_path.read_text(encoding="utf-8").replace(
        "__CSP_NONCE__", request.state.csp_nonce
    )

def main():
    parser = argparse.ArgumentParser(description="Trishula Web Dashboard Server")
    parser.add_argument("--file", "-f", default=None, help="Optional CSV or Parquet dataset file path")
    parser.add_argument("--port", "-p", type=int, default=8000, help="Port to run web server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host binding")
    args = parser.parse_args()
    try:
        duckdb_settings = get_duckdb_settings()
    except ValueError as exc:
        parser.error(f"Invalid Trishula configuration: {exc}")
    if args.host not in {"127.0.0.1", "localhost", "::1"} and not (
        ACCESS_TOKEN and ALLOW_NETWORK and COOKIE_SECURE
    ):
        parser.error(
            "Non-loopback binding requires TRISHULA_ACCESS_TOKEN, "
            "TRISHULA_ALLOW_NETWORK=true, and TRISHULA_COOKIE_SECURE=true."
        )

    if args.file and os.path.exists(args.file):
        init_active_file(args.file)
        print(f"[*] Pre-loaded dataset: '{args.file}'")

    print(
        f"[*] DuckDB limits: {duckdb_settings.threads} threads, "
        f"{duckdb_settings.memory_limit}"
    )
    print(f"\n🚀 TRISHULA WEB Dashboard running at: http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
