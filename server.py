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
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from converter import convert_csv_to_parquet, inspect_file, validate_dataset_schema
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

app = FastAPI(title="Trishula Web Analytics", version="0.3.0")

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

UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
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
    return {
        "loaded": True,
        "raw_file": os.path.basename(state.raw_file),
        "parquet_file": os.path.basename(state.parquet_file),
        "delimiter": state.delimiter,
        "file_size_mb": round(os.path.getsize(state.parquet_file) / (1024**2), 2)
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


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trishula Web - Session & Funnel Intelligence Dashboard</title>
    <style>
        :root {
            --bg-dark: #090d16;
            --card-bg: rgba(21, 30, 48, 0.75);
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
            background: rgba(15, 23, 42, 0.85);
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

        .info-box {
            background: rgba(56, 189, 248, 0.08);
            border-left: 4px solid var(--accent-cyan);
            padding: 16px 20px;
            border-radius: 0 12px 12px 0;
            margin-bottom: 24px;
            font-size: 14px;
            line-height: 1.6;
            color: #cbd5e1;
        }
        .info-box strong { color: var(--accent-cyan); }

        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin-bottom: 28px; }
        .kpi-card {
            background: rgba(30, 41, 59, 0.5);
            border: 1px solid var(--card-border);
            padding: 20px;
            border-radius: 14px;
        }
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

        .btn-secondary {
            background: rgba(255, 255, 255, 0.08);
            color: var(--text-main);
            border: 1px solid var(--card-border);
            padding: 10px 20px;
            border-radius: 10px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        .btn-secondary:hover { background: rgba(255, 255, 255, 0.15); }

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

        .chip-btn {
            background: rgba(129, 140, 248, 0.15);
            color: #a5b4fc;
            border: 1px solid rgba(129, 140, 248, 0.3);
            padding: 6px 12px;
            border-radius: 16px;
            font-size: 13px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }
        .chip-btn:hover { background: rgba(129, 140, 248, 0.3); color: #fff; }

        .breadcrumb-pill {
            background: rgba(15, 23, 42, 0.8);
            border: 1px solid var(--card-border);
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 12px;
            font-family: 'JetBrains Mono', monospace;
            color: #e2e8f0;
            display: inline-flex;
            align-items: center;
        }
        .breadcrumb-arrow { color: #38bdf8; margin: 0 6px; font-weight: bold; }

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

        .bar-container {
            background: rgba(255,255,255,0.05);
            border-radius: 6px;
            height: 20px;
            width: 100%;
            overflow: hidden;
        }
        .bar-fill {
            background: linear-gradient(90deg, #38bdf8, #818cf8);
            height: 100%;
            border-radius: 6px;
            transition: width 0.4s ease;
        }
        .tab-panel { display: none; }
        .tab-panel.active { display: block; }
    </style>
</head>
<body>
    <!-- Top Navbar -->
    <div class="navbar">
        <div class="brand">🔱 TRISHULA WEB</div>
        <div class="nav-items">
            <button class="nav-btn active" data-tab="overview">📊 Executive KPIs</button>
            <button class="nav-btn" data-tab="funnel">🎛️ Funnel Retention</button>
            <button class="nav-btn" data-tab="heatmap">🔥 Transition Matrix</button>
            <button class="nav-btn" data-tab="search">🔎 Session Explorer</button>
            <button class="nav-btn" data-tab="help">❓ Help & How-to</button>
        </div>
        <div style="display: flex; gap: 10px; align-items: center;">
            <button class="nav-btn" id="openFileButton">📁 Load Dataset</button>
            <select id="dedupeSelect">
                <option value="consecutive">Dedupe: Consecutive</option>
                <option value="unique">Dedupe: Unique</option>
                <option value="none">Dedupe: None (Raw)</option>
            </select>
            <button class="nav-btn" id="restartButton" style="color: #fb7185; border-color: rgba(251, 113, 133, 0.3);">
                🔄 Restart Server
            </button>
            <button class="btn-action" id="printButton">🖨️ Export PDF</button>
        </div>
    </div>

    <div class="container">
        <!-- Dataset Loader Card -->
        <div id="loaderCard" class="glass-card" style="border: 2px solid rgba(56, 189, 248, 0.4); background: rgba(15, 23, 42, 0.95); padding: 36px; text-align: center;">
            <h2 style="margin: 0 0 8px 0;">Select Dataset File</h2>
            <p style="color: #94a3b8; margin: 0 0 24px 0;">Choose a CSV or Parquet export to upload:</p>
            
            <div style="display: flex; gap: 12px; max-width: 700px; margin: 0 auto; justify-content: center; flex-wrap: wrap;">
                <button class="btn-action" id="browserFileButton" style="padding: 12px 24px; font-size: 15px;">
                    📤 Upload CSV/Parquet
                </button>
                <input id="browserFileInput" type="file" accept=".csv,.parquet" style="display: none;" />
            </div>

            <div id="fileError" style="color: #fb7185; margin-top: 16px; font-weight: bold; display: none;"></div>
        </div>

        <!-- Active Dataset Banner -->
        <div id="datasetBanner" class="glass-card" style="padding: 16px 24px; display: none; justify-content: space-between; align-items: center;">
            <div>
                <span style="color: #94a3b8; font-size: 13px;">ACTIVE DATASET:</span>
                <strong id="activeFileName" style="margin-left: 8px; color: #38bdf8;">-</strong>
                <span id="activeFileSize" style="margin-left: 12px; color: #94a3b8;">-</span>
            </div>
            <div style="display:flex;gap:10px;align-items:center;">
                <span id="storageStatus" class="tag-pill">Storage: checking…</span>
                <span class="tag-pill">⚡ DuckDB Out-of-Core Engine</span>
                <button id="unloadButton" class="btn-secondary">Unload Dataset</button>
            </div>
        </div>

        <!-- Tab 1: Executive KPIs -->
        <div id="panel-overview" class="tab-panel active">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-title">Total Sessions Analyzed</div>
                    <div id="kpi-sessions" class="kpi-val">-</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Bounce Rate</div>
                    <div id="kpi-bounce" class="kpi-val" style="color: #34d399;">-</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Avg Events / Session</div>
                    <div id="kpi-avg" class="kpi-val">-</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Median Length (p50)</div>
                    <div id="kpi-median" class="kpi-val">-</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">90th Percentile Length</div>
                    <div id="kpi-p90" class="kpi-val" style="color: #fb7185;">-</div>
                </div>
            </div>

            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 24px;">
                <div class="glass-card">
                    <h3 style="color: #34d399;">Top Session Entry Points</h3>
                    <table id="entryTable">
                        <thead><tr><th>Entry Event</th><th>Sessions</th><th>Share %</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>

                <div class="glass-card">
                    <h3 style="color: #fb7185;">Top Session Exit Points</h3>
                    <table id="exitTable">
                        <thead><tr><th>Exit Event</th><th>Sessions</th><th>Share %</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Tab 2: Funnel Retention Builder -->
        <div id="panel-funnel" class="tab-panel">
            <!-- Explanatory Box -->
            <div class="info-box">
                💡 <strong>How Funnel Retention Analysis Works:</strong><br/>
                A <strong>Funnel</strong> measures how many user sessions complete an ordered sequence of user steps (e.g. <code>Home → Product_View → Checkout</code>). 
                The analysis tracks session progression step-by-step and calculates drop-off rates between consecutive stages. 
                Use the 1-click presets or choose steps from your dataset below to build your funnel!
            </div>

            <div class="glass-card">
                <h3 style="color: #38bdf8; margin-top: 0;">🎛️ Funnel Retention & Conversion Flow</h3>
                
                <!-- Quick Preset Flow Buttons -->
                <div style="margin-bottom: 20px;">
                    <span style="font-size: 13px; color: #94a3b8; font-weight: bold; margin-right: 10px;">QUICK PRESETS:</span>
                    <button class="chip-btn funnel-preset" data-preset="top" disabled>⚡ Top 4 Frequent Events</button>
                    <button class="chip-btn funnel-preset" data-preset="checkout" style="margin-left: 6px;" disabled>🛒 E-Commerce Checkout Flow</button>
                    <button class="chip-btn funnel-preset" data-preset="search" style="margin-left: 6px;" disabled>🔍 Search Discovery Flow</button>
                    <button class="chip-btn" id="clearFunnelButton" style="margin-left: 6px; background: rgba(251, 113, 133, 0.15); color: #fb7185; border-color: rgba(251, 113, 133, 0.3);">🗑️ Clear All</button>
                </div>
                <div id="funnelEventStatus" role="status" aria-live="polite" style="color: #94a3b8; margin-bottom: 12px;">
                    Open this tab to load available events.
                </div>

                <!-- Funnel Step Tiles -->
                <div id="funnelPills" style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 20px; min-height: 42px; padding: 12px; background: rgba(15, 23, 42, 0.5); border-radius: 12px; border: 1px dashed var(--card-border);"></div>

                <!-- Add Step Selector -->
                <div style="display: flex; gap: 12px; align-items: center;">
                    <select id="addEventSelect">
                        <option value="">+ Select Event Step to Add...</option>
                    </select>
                    <span style="font-size: 13px; color: #64748b;">(Steps are evaluated in chronological order)</span>
                </div>
            </div>

            <div class="glass-card">
                <h3>Step Conversion & Retention Breakdown</h3>
                <table id="funnelMetricsTable">
                    <thead>
                        <tr>
                            <th>Step #</th>
                            <th>Event Step Name</th>
                            <th>Qualifying Sessions</th>
                            <th>Step Conversion %</th>
                            <th>Step Drop-Off %</th>
                            <th style="width: 35%;">Retention Bar</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <!-- Tab 3: Heatmap Matrix -->
        <div id="panel-heatmap" class="tab-panel">
            <div class="glass-card">
                <h3 style="color: #fb7185;">Event Transition Heatmap Matrix</h3>
                <p style="color: #94a3b8; font-size: 14px;">Source x Target event-to-event flow intensity matrix:</p>
                <div id="heatmapStatus" role="status" aria-live="polite" style="color: #94a3b8; margin-top: 16px;">
                    Load a dataset to calculate transitions.
                </div>
                
                <div style="overflow-x: auto;">
                    <table id="heatmapTable" style="margin-top: 20px; display: none;">
                        <thead></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Tab 4: Session Explorer -->
        <div id="panel-search" class="tab-panel">
            <!-- Explanatory Box -->
            <div class="info-box">
                🔍 <strong>How the Session Explorer Works:</strong><br/>
                The <strong>Session Explorer</strong> allows you to inspect actual user navigation journeys. You can search for sessions containing specific event actions or filter by exact subpath sequences (e.g. <code>Search → Home</code>). Click any pre-populated quick filter below to explore sessions instantly!
            </div>

            <div class="glass-card">
                <h3 style="color: #38bdf8; margin-top: 0;">🔎 Session Search & Journey Inspector</h3>
                
                <!-- Pre-populated Quick Filter Chips -->
                <div style="margin-bottom: 20px;">
                    <span style="font-size: 13px; color: #94a3b8; font-weight: bold; margin-right: 10px;">PRE-POPULATED QUICK FILTERS:</span>
                    <div id="quickSearchChips" style="display: inline-flex; flex-wrap: wrap; gap: 8px; margin-top: 8px;"></div>
                </div>

                <div style="display: flex; gap: 16px; margin: 20px 0;">
                    <input id="searchEventInput" placeholder="Filter by event name (e.g. VehicleView or Search)" style="flex: 1;" />
                    <input id="searchSubpathInput" placeholder="Filter by subpath sequence (e.g. Search->Home)" style="flex: 1;" />
                    <button class="btn-action" id="searchButton">🔎 Search Sessions</button>
                </div>
                <div id="searchStatus" role="status" aria-live="polite" style="color: #94a3b8; margin-bottom: 12px;"></div>

                <table id="searchTable">
                    <thead>
                        <tr>
                            <th>Session ID</th>
                            <th>User Event Navigation Journey (Breadcrumbs)</th>
                            <th>Total Events</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <!-- Tab 5: Help and How-to -->
        <div id="panel-help" class="tab-panel">
            <div class="info-box">
                <strong>Localhost support model:</strong> Trishula is designed
                for one trusted user on the same workstation. Keep the server
                bound to <code>127.0.0.1</code>.
            </div>

            <div class="glass-card">
                <h2>Quick start</h2>
                <ol>
                    <li>Select a CSV or Parquet file with <strong>Upload CSV/Parquet</strong>.</li>
                    <li>Confirm the active filename, managed size, and free disk space.</li>
                    <li>Review Executive KPIs, then build an ordered funnel or inspect transitions.</li>
                    <li>Use Session Explorer for exact event-token and contiguous-subpath searches.</li>
                    <li>Export with <strong>Export PDF</strong>, or unload the dataset when finished.</li>
                </ol>
            </div>

            <div class="glass-card">
                <h2>Required dataset schema</h2>
                <table>
                    <thead><tr><th>Column</th><th>Purpose</th><th>Example</th></tr></thead>
                    <tbody>
                        <tr><td><code>SESSION</code></td><td>Session identifier</td><td><code>SESS_123</code></td></tr>
                        <tr><td><code>EVENT_PATH</code></td><td>Ordered event sequence</td><td><code>Home-&gt;Search-&gt;Checkout</code></td></tr>
                        <tr><td><code>TOTAL_EVENTS</code></td><td>Numeric session depth</td><td><code>3</code></td></tr>
                    </tbody>
                </table>
                <p>Supported path delimiters are <code>-&gt;</code>, comma,
                <code>&gt;</code>, and pipe. Detection samples the first
                non-empty paths; provide the delimiter explicitly in the CLI
                if detection is ambiguous.</p>
            </div>

            <div class="glass-card">
                <h2>Dedupe and funnel semantics</h2>
                <ul>
                    <li><strong>Consecutive:</strong> <code>A→A→B</code> becomes <code>A→B</code>.</li>
                    <li><strong>Unique:</strong> retains the first occurrence of each event in source order.</li>
                    <li><strong>None:</strong> analyzes the raw path.</li>
                </ul>
                <p>Sequential funnels require each step to occur after the
                previous matched step. Intervening events are allowed, and
                repeated funnel steps such as <code>A→B→A</code> are supported.</p>
            </div>

            <div class="glass-card">
                <h2>Storage and safety</h2>
                <ul>
                    <li>Browser uploads are streamed into generated filenames under <code>uploads/</code>.</li>
                    <li><strong>Unload Dataset</strong> deletes the managed browser-upload files for the current session.</li>
                    <li>Custom SQL and restart require trusted-local mode.</li>
                    <li>Custom SQL is bounded by configured time and result-row limits and can be cancelled through the job API.</li>
                </ul>
            </div>

            <div class="glass-card">
                <h2>Useful commands</h2>
                <pre><code>trishula run-all data.csv --funnel "Home,Search,Checkout"
trishula inspect data.csv --limit 5
trishula benchmark --rows 1000000 --output-dir ./benchmark-output
trishula-web --host 127.0.0.1 --port 8000</code></pre>
                <p>The repository README contains the full command reference,
                environment-variable reference, test instructions, and
                advanced security notes.</p>
            </div>

            <div class="glass-card">
                <h2>Troubleshooting</h2>
                <ul>
                    <li><strong>Missing columns:</strong> export or rename fields to the required uppercase schema.</li>
                    <li><strong>Incorrect funnel counts:</strong> verify delimiter detection and selected dedupe mode.</li>
                    <li><strong>Empty transition matrix:</strong> wait for calculation to finish, then check the displayed status. Sessions containing only one event legitimately have no transitions.</li>
                    <li><strong>Out of memory:</strong> lower <code>TRISHULA_DUCKDB_MEMORY_LIMIT</code> and confirm sufficient temporary disk space.</li>
                    <li><strong>Slow analytics:</strong> tabs load on demand and heavy queries are serialized by default. Benchmark before increasing <code>TRISHULA_MAX_CONCURRENT_ANALYTICS</code>.</li>
                    <li><strong>Upload rejected:</strong> check file extension, configured upload limit, and CSV/Parquet validity.</li>
                    <li><strong>Trusted feature returns 403:</strong> restart with <code>TRISHULA_TRUSTED_LOCAL_MODE=true</code> only on a trusted workstation.</li>
                </ul>
            </div>
        </div>
    </div>

    <script nonce="__CSP_NONCE__">
        let stateData = null;
        let selectedFunnelSteps = [];
        let allTopEventsList = [];
        let activeTab = 'overview';
        const loadedTabs = new Set();
        const tabLoadPromises = new Map();

        function escapeHtml(value) {
            const node = document.createElement('span');
            node.textContent = String(value ?? '');
            return node.innerHTML;
        }

        window.addEventListener('DOMContentLoaded', () => {
            fetchState();
            document.querySelectorAll('[data-tab]').forEach(button => {
                button.addEventListener('click', () => switchTab(button.dataset.tab));
            });
            document.getElementById('openFileButton').addEventListener('click', openFileModal);
            document.getElementById('dedupeSelect').addEventListener('change', onDedupeChange);
            document.getElementById('restartButton').addEventListener('click', triggerServerRestart);
            document.getElementById('printButton').addEventListener('click', () => window.print());
            document.getElementById('browserFileButton').addEventListener(
                'click', () => document.getElementById('browserFileInput').click()
            );
            document.getElementById('browserFileInput').addEventListener(
                'change', event => handleBrowserFileUpload(event.target.files)
            );
            document.querySelectorAll('.funnel-preset').forEach(button => {
                button.addEventListener('click', () => applyFunnelPreset(button.dataset.preset));
            });
            document.getElementById('clearFunnelButton').addEventListener('click', clearFunnel);
            document.getElementById('addEventSelect').addEventListener(
                'change', event => addStepToFunnel(event.target.value)
            );
            document.getElementById('searchButton').addEventListener('click', runSearch);
            document.getElementById('unloadButton').addEventListener('click', unloadDataset);
        });

        async function fetchState() {
            try {
                const res = await fetch('/api/state');
                stateData = await res.json();
                if (stateData.loaded) {
                    document.getElementById('loaderCard').style.display = 'none';
                    document.getElementById('datasetBanner').style.display = 'flex';
                    document.getElementById('activeFileName').innerText = stateData.parquet_file;
                    document.getElementById('activeFileSize').innerText = `(${stateData.file_size_mb} MB)`;
                    loadStorageStatus();
                    resetTabLoads();
                    loadTabData(activeTab);
                } else {
                    document.getElementById('loaderCard').style.display = 'block';
                    document.getElementById('datasetBanner').style.display = 'none';
                    resetTabLoads();
                }
            } catch (err) {
                console.error(err);
            }
        }

        async function loadStorageStatus() {
            const response = await fetch('/api/storage');
            if (!response.ok) return;
            const storage = await response.json();
            document.getElementById('storageStatus').textContent =
                `${storage.disk_free_gb} GB free · ${storage.managed_dataset_mb} MB managed`;
        }

        async function unloadDataset() {
            if (!confirm('Unload this dataset? Uploaded managed files will be deleted.')) return;
            const response = await fetch('/api/unload', {method: 'POST'});
            if (response.ok) {
                stateData = {loaded: false};
                selectedFunnelSteps = [];
                resetTabLoads();
                await fetchState();
            }
        }

        async function triggerServerRestart() {
            if (!confirm("Are you sure you want to restart the Trishula Web Server process?")) return;
            try {
                await fetch('/api/restart', { method: 'POST' });
            } catch(e) {}
            alert("🔄 Trishula Web Server is restarting... Reloading page!");
            setTimeout(() => {
                window.location.reload();
            }, 1500);
        }

        async function handleBrowserFileUpload(files) {
            if (!files || files.length === 0) return;
            const file = files[0];
            const errDiv = document.getElementById('fileError');
            errDiv.style.display = 'none';

            const formData = new FormData();
            formData.append('file', file);

            try {
                const res = await fetch('/api/upload-file', {
                    method: 'POST',
                    body: formData
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.detail || 'Upload failed');
                fetchState();
            } catch (err) {
                errDiv.innerText = err.message;
                errDiv.style.display = 'block';
            }
        }

        function openFileModal() {
            document.getElementById('loaderCard').style.display = 'block';
        }

        function switchTab(tabName) {
            document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));

            const tabButton = document.querySelector(`[data-tab="${tabName}"]`);
            if (tabButton) tabButton.classList.add('active');
            document.getElementById(`panel-${tabName}`).classList.add('active');
            activeTab = tabName;
            loadTabData(tabName);
        }

        function onDedupeChange() {
            if (stateData && stateData.loaded) {
                loadedTabs.delete('funnel');
                loadedTabs.delete('heatmap');
                if (activeTab === 'funnel' || activeTab === 'heatmap') {
                    loadTabData(activeTab, true);
                }
            }
        }

        function resetTabLoads() {
            loadedTabs.clear();
            tabLoadPromises.clear();
        }

        function loadTabData(tabName, force = false) {
            if (!stateData || !stateData.loaded || tabName === 'help') return Promise.resolve();
            if (!force && loadedTabs.has(tabName)) return Promise.resolve();
            if (tabLoadPromises.has(tabName)) {
                const currentRequest = tabLoadPromises.get(tabName);
                return force
                    ? currentRequest.then(() => loadTabData(tabName, true))
                    : currentRequest;
            }

            const loaders = {
                overview: loadInsights,
                funnel: loadEvents,
                heatmap: loadHeatmap,
                search: runSearch
            };
            const loader = loaders[tabName];
            if (!loader) return Promise.resolve();

            const request = Promise.resolve()
                .then(loader)
                .then(() => loadedTabs.add(tabName))
                .catch(err => console.error(`Unable to load ${tabName} tab`, err))
                .finally(() => tabLoadPromises.delete(tabName));
            tabLoadPromises.set(tabName, request);
            return request;
        }

        async function loadInsights() {
            const res = await fetch('/api/insights');
            const data = await res.json();

            document.getElementById('kpi-sessions').innerText = data.summary.total_sessions.toLocaleString();
            document.getElementById('kpi-bounce').innerText = `${data.summary.bounce_rate_pct}%`;
            document.getElementById('kpi-avg').innerText = data.summary.avg_events_per_session;
            document.getElementById('kpi-median').innerText = `${data.summary.median_events} events`;
            document.getElementById('kpi-p90').innerText = `${data.summary.p90_events} events`;

            // Entry Table
            const entryTbody = document.querySelector('#entryTable tbody');
            entryTbody.innerHTML = data.entry_points.map(e => `
                <tr>
                    <td><strong style="color: #f8fafc">${escapeHtml(e.event_name)}</strong></td>
                    <td>${e.entry_count.toLocaleString()}</td>
                    <td><span class="tag-pill">${e.entry_share_pct}%</span></td>
                </tr>
            `).join('');

            // Exit Table
            const exitTbody = document.querySelector('#exitTable tbody');
            exitTbody.innerHTML = data.exit_points.map(e => `
                <tr>
                    <td><strong style="color: #f8fafc">${escapeHtml(e.event_name)}</strong></td>
                    <td>${e.exit_count.toLocaleString()}</td>
                    <td><span class="tag-pill" style="color: #fb7185; border-color: rgba(251,113,133,0.3)">${e.exit_share_pct}%</span></td>
                </tr>
            `).join('');
        }

        async function loadEvents() {
            const dedupe = document.getElementById('dedupeSelect').value;
            const select = document.getElementById('addEventSelect');
            const status = document.getElementById('funnelEventStatus');
            const presetButtons = document.querySelectorAll('.funnel-preset');
            status.style.color = '#94a3b8';
            status.textContent = 'Loading frequent events…';
            presetButtons.forEach(button => button.disabled = true);
            select.disabled = true;
            select.replaceChildren();
            const placeholderOption = document.createElement('option');
            placeholderOption.value = '';
            placeholderOption.textContent = 'Loading events…';
            select.appendChild(placeholderOption);
            try {
                const res = await fetch(`/api/events?dedupe=${dedupe}`);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Event discovery failed');
                if (!Array.isArray(data)) throw new Error('Invalid event response');

                allTopEventsList = data;
                placeholderOption.textContent = data.length
                    ? '+ Select Event Step to Add...'
                    : 'No events available';
                data.forEach(e => {
                    const option = document.createElement('option');
                    option.value = e.event_name;
                    option.textContent = `${e.event_name} (${e.occurrence_count.toLocaleString()} occurrences)`;
                    select.appendChild(option);
                });

                const chipsDiv = document.getElementById('quickSearchChips');
                chipsDiv.replaceChildren();
                data.slice(0, 6).forEach(e => {
                    const button = document.createElement('button');
                    button.className = 'chip-btn';
                    button.textContent = `Event: ${e.event_name}`;
                    button.addEventListener('click', () => applySearchFilter(e.event_name, ''));
                    chipsDiv.appendChild(button);
                });

                if (!data.length) {
                    status.textContent = 'No usable events were found in the active dataset.';
                    selectedFunnelSteps = [];
                    renderFunnelPills();
                    return;
                }

                status.style.color = '#34d399';
                status.textContent = `Loaded ${data.length} frequent events.`;
                presetButtons.forEach(button => button.disabled = false);
                select.disabled = false;
                if (selectedFunnelSteps.length === 0) {
                    selectedFunnelSteps = data.slice(0, 4).map(e => e.event_name);
                }
                renderFunnelPills();
                await loadFunnel();
            } catch (err) {
                allTopEventsList = [];
                selectedFunnelSteps = [];
                status.style.color = '#fb7185';
                status.textContent = `Unable to load events: ${err.message}`;
                placeholderOption.textContent = 'Events unavailable';
                renderFunnelPills();
                throw err;
            }
        }

        function renderFunnelPills() {
            const container = document.getElementById('funnelPills');
            if (selectedFunnelSteps.length === 0) {
                container.innerHTML = `<span style="color: #64748b; font-size: 13px;">No funnel steps selected. Click a preset above or add a step!</span>`;
                return;
            }
            container.innerHTML = selectedFunnelSteps.map((step, idx) => `
                <span class="tag-pill" style="padding: 8px 16px; font-size: 14px; background: rgba(56, 189, 248, 0.15);">
                    <strong>#${idx+1}</strong> ${escapeHtml(step)}
                    <button type="button" class="remove-funnel-step" data-index="${idx}" style="cursor: pointer; margin-left: 8px; font-weight: bold; color: #fb7185; background: none; border: 0;">✕</button>
                </span>
            `).join('');
            container.querySelectorAll('.remove-funnel-step').forEach(button => {
                button.addEventListener('click', () => removeStepFromFunnel(Number(button.dataset.index)));
            });
        }

        function applyFunnelPreset(presetType) {
            if (allTopEventsList.length === 0) {
                const status = document.getElementById('funnelEventStatus');
                status.style.color = '#fb7185';
                status.textContent = 'Events are not available yet. Wait for loading to finish or review the error above.';
                return;
            }
            if (presetType === 'top') {
                selectedFunnelSteps = allTopEventsList.slice(0, 4).map(e => e.event_name);
            } else if (presetType === 'checkout') {
                const checkoutCandidates = ['Home', 'Search', 'Product_View', 'Add_To_Cart', 'Checkout', 'Payment', 'Order_Confirmation'];
                selectedFunnelSteps = checkoutCandidates.filter(c => allTopEventsList.some(e => e.event_name === c));
                if (selectedFunnelSteps.length === 0) selectedFunnelSteps = allTopEventsList.slice(0, 4).map(e => e.event_name);
            } else if (presetType === 'search') {
                const searchCandidates = ['Search', 'Product_View', 'Category_Browse', 'Add_To_Cart'];
                selectedFunnelSteps = searchCandidates.filter(c => allTopEventsList.some(e => e.event_name === c));
                if (selectedFunnelSteps.length === 0) selectedFunnelSteps = allTopEventsList.slice(0, 3).map(e => e.event_name);
            }
            renderFunnelPills();
            loadFunnel();
        }

        function clearFunnel() {
            selectedFunnelSteps = [];
            renderFunnelPills();
            const tbody = document.querySelector('#funnelMetricsTable tbody');
            tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: #64748b;">Add at least one step to compute funnel metrics.</td></tr>`;
        }

        function addStepToFunnel(stepName) {
            if (stepName) {
                selectedFunnelSteps.push(stepName);
                renderFunnelPills();
                loadFunnel();
            }
            document.getElementById('addEventSelect').value = "";
        }

        function removeStepFromFunnel(idx) {
            selectedFunnelSteps.splice(idx, 1);
            renderFunnelPills();
            if (selectedFunnelSteps.length > 0) loadFunnel();
            else clearFunnel();
        }

        async function loadFunnel() {
            if (selectedFunnelSteps.length === 0) return;
            const dedupe = document.getElementById('dedupeSelect').value;
            const stepsParam = selectedFunnelSteps.join(',');
            const status = document.getElementById('funnelEventStatus');
            const tbody = document.querySelector('#funnelMetricsTable tbody');
            try {
                status.style.color = '#94a3b8';
                status.textContent = 'Calculating funnel retention…';
                const res = await fetch(`/api/funnel?steps=${encodeURIComponent(stepsParam)}&dedupe=${dedupe}`);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Funnel calculation failed');
                if (!Array.isArray(data)) throw new Error('Invalid funnel response');

                tbody.innerHTML = data.map(r => `
                    <tr>
                        <td><strong>#${r.step_number}</strong></td>
                        <td><strong style="color: #38bdf8">${escapeHtml(r.step_name)}</strong></td>
                        <td><strong>${r.session_count.toLocaleString()}</strong></td>
                        <td><span class="tag-pill" style="color: #34d399">${r.step_conversion_pct}%</span></td>
                        <td><span class="tag-pill" style="color: #fb7185; border-color: rgba(251,113,133,0.3)">${r.step_dropoff_pct}%</span></td>
                        <td>
                            <div class="bar-container">
                                <div class="bar-fill" style="width: ${r.step_conversion_pct}%"></div>
                            </div>
                        </td>
                    </tr>
                `).join('');
                status.style.color = '#34d399';
                status.textContent = `Funnel calculated for ${data.length} steps.`;
            } catch (err) {
                status.style.color = '#fb7185';
                status.textContent = `Unable to calculate funnel: ${err.message}`;
                tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: #fb7185;">Funnel calculation failed.</td></tr>`;
            }
        }

        async function loadHeatmap() {
            const dedupe = document.getElementById('dedupeSelect').value;
            const table = document.getElementById('heatmapTable');
            const status = document.getElementById('heatmapStatus');
            const thead = document.querySelector('#heatmapTable thead');
            const tbody = document.querySelector('#heatmapTable tbody');
            status.style.color = '#94a3b8';
            status.textContent = 'Calculating transition matrix…';
            status.style.display = 'block';
            table.style.display = 'none';
            thead.innerHTML = '';
            tbody.innerHTML = '';

            try {
                const res = await fetch(`/api/heatmap?dedupe=${dedupe}`);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Transition calculation failed');
                if (!data.columns?.length || !data.index?.length || !data.data?.length) {
                    status.textContent = 'No events are available for a transition matrix.';
                    return;
                }

                const values = data.data.flat();
                const maxVal = Math.max(0, ...values);
                thead.innerHTML = `<tr><th>From / To</th>${data.columns.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr>`;
                tbody.innerHTML = data.index.map((rowLabel, rIdx) => `
                    <tr>
                        <td><strong style="color: #38bdf8">${escapeHtml(rowLabel)}</strong></td>
                        ${data.data[rIdx].map(val => {
                            const intensity = maxVal > 0 ? val / maxVal : 0;
                            const bg = val > 0 ? `rgba(56, 189, 248, ${Math.max(0.15, intensity)})` : 'rgba(30, 41, 59, 0.4)';
                            return `<td style="background: ${bg}; text-align: center; font-weight: bold;">${val > 0 ? val.toLocaleString() : '-'}</td>`;
                        }).join('')}
                    </tr>
                `).join('');
                table.style.display = 'table';
                status.textContent = maxVal > 0
                    ? `Showing transitions among the ${data.columns.length} most frequent events.`
                    : 'Events were found, but there are no transitions between them.';
            } catch (err) {
                status.style.color = '#fb7185';
                status.textContent = `Unable to load transition matrix: ${err.message}`;
            }
        }

        function applySearchFilter(eventVal, subpathVal) {
            document.getElementById('searchEventInput').value = eventVal;
            document.getElementById('searchSubpathInput').value = subpathVal;
            runSearch();
        }

        async function runSearch() {
            const ev = document.getElementById('searchEventInput').value.trim();
            const sub = document.getElementById('searchSubpathInput').value.trim();
            const status = document.getElementById('searchStatus');
            const tbody = document.querySelector('#searchTable tbody');
            let url = '/api/search?limit=25';
            if (ev) url += `&event=${encodeURIComponent(ev)}`;
            if (sub) url += `&subpath=${encodeURIComponent(sub)}`;
            try {
                status.style.color = '#94a3b8';
                status.textContent = 'Searching sessions…';
                const res = await fetch(url);
                const contentType = res.headers.get('content-type') || '';
                const data = contentType.includes('application/json')
                    ? await res.json()
                    : {detail: await res.text()};
                if (!res.ok) throw new Error(data.detail || 'Session search failed');
                if (!Array.isArray(data)) throw new Error('Invalid search response');

                if (data.length === 0) {
                    status.textContent = 'No matching sessions found.';
                    tbody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: #64748b;">No matching sessions found.</td></tr>`;
                    return;
                }

                tbody.innerHTML = data.map(s => {
                    const steps = s.EVENT_PATH.split('->');
                    const breadcrumbs = steps.map(st => `
                        <span class="breadcrumb-pill">${escapeHtml(st)}</span>
                    `).join('<span class="breadcrumb-arrow">➔</span>');

                    return `
                        <tr>
                            <td><strong style="color: #38bdf8; font-family: monospace; font-size: 13px;">${escapeHtml(s.SESSION)}</strong></td>
                            <td>${breadcrumbs}</td>
                            <td><span class="tag-pill">${s.TOTAL_EVENTS} events</span></td>
                        </tr>
                    `;
                }).join('');
                status.style.color = '#34d399';
                status.textContent = `Showing ${data.length} matching sessions.`;
            } catch (err) {
                status.style.color = '#fb7185';
                status.textContent = `Unable to search sessions: ${err.message}`;
                tbody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: #fb7185;">Session search failed.</td></tr>`;
            }
        }
    </script>
</body>
</html>
"""
    return html.replace("__CSP_NONCE__", request.state.csp_nonce)

def main():
    parser = argparse.ArgumentParser(description="Trishula Web Dashboard Server")
    parser.add_argument("--file", "-f", default=None, help="Optional CSV or Parquet dataset file path")
    parser.add_argument("--port", "-p", type=int, default=8000, help="Port to run web server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host binding")
    args = parser.parse_args()
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

    print(f"\n🚀 TRISHULA WEB Dashboard running at: http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
