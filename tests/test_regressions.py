import asyncio
import csv
import io
import re
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from fastapi import HTTPException, UploadFile
from starlette.requests import Request

import server
from converter import convert_csv_to_parquet, validate_dataset_schema
from event_parser import (
    calculate_funnel,
    get_transition_pairs,
    run_custom_query,
    sanitize_event_path_sql,
    search_sessions,
)
from visualizer import export_html_report
from benchmark import run_benchmark
from insights import get_transition_matrix


@pytest.fixture()
def event_dataset(tmp_path):
    csv_path = tmp_path / "events.csv"
    parquet_path = tmp_path / "events.parquet"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["SESSION", "EVENT_PATH", "TOTAL_EVENTS"])
        writer.writerows(
            [
                ["one", "A->B->A", 3],
                ["two", "A->B->C", 3],
                ["three", "A->A->B", 3],
                ["four", "B->A", 2],
            ]
        )
    convert_csv_to_parquet(str(csv_path), str(parquet_path))
    return parquet_path


def test_sequential_funnel_supports_repeated_steps(event_dataset):
    result = calculate_funnel(str(event_dataset), ["A", "B", "A"])
    assert result["session_count"].tolist() == [4, 3, 1]


def test_funnel_honors_consecutive_deduplication(event_dataset):
    raw = calculate_funnel(
        str(event_dataset), ["A", "A", "B"], dedupe_mode="none"
    )
    deduped = calculate_funnel(
        str(event_dataset), ["A", "A", "B"], dedupe_mode="consecutive"
    )
    assert raw.iloc[-1]["session_count"] == 1
    assert deduped.iloc[-1]["session_count"] == 0


def test_transition_counts_are_exact_and_dedupe_sensitive(event_dataset):
    raw = get_transition_pairs(str(event_dataset), top_n=20, dedupe_mode="none")
    deduped = get_transition_pairs(
        str(event_dataset), top_n=20, dedupe_mode="consecutive"
    )
    raw_counts = dict(zip(raw["transition"], raw["transition_count"]))
    deduped_counts = dict(zip(deduped["transition"], deduped["transition_count"]))
    assert raw_counts["A -> A"] == 1
    assert "A -> A" not in deduped_counts
    assert deduped_counts["A -> B"] == 3


def test_transition_matrix_is_exact_and_dedupe_sensitive(event_dataset):
    raw = get_transition_matrix(
        str(event_dataset), top_n=3, dedupe_mode="none"
    )
    deduped = get_transition_matrix(
        str(event_dataset), top_n=3, dedupe_mode="consecutive"
    )

    assert raw.loc["A", "A"] == 1
    assert raw.loc["A", "B"] == 3
    assert raw.loc["B", "A"] == 2
    assert deduped.loc["A", "A"] == 0
    assert deduped.loc["A", "B"] == 3
    assert deduped.loc["B", "A"] == 2


def test_event_search_matches_tokens_not_substrings(event_dataset):
    exact = search_sessions(str(event_dataset), contains_event="A")
    substring = search_sessions(str(event_dataset), contains_event="A->B")
    assert len(exact) == 4
    assert substring.empty


def test_subpath_search_respects_event_boundaries(event_dataset):
    matches = search_sessions(str(event_dataset), exact_subpath="A->B")
    assert set(matches["SESSION"]) == {"one", "two", "three"}


def test_unique_deduplication_preserves_first_occurrence_order():
    expression = sanitize_event_path_sql(
        "'A->B->A->C'", delimiter="->", mode="unique"
    )
    assert duckdb.sql(f"SELECT {expression}").fetchone()[0] == "A->B->C"


def test_custom_query_uses_a_view_instead_of_replacing_data_substrings(event_dataset):
    result = run_custom_query(
        str(event_dataset),
        "SELECT 'metadata' AS label, COUNT(*) AS count FROM data",
    )
    assert result.iloc[0].to_dict() == {"label": "metadata", "count": 4}


def test_custom_query_rejects_excessive_results(event_dataset):
    with pytest.raises(ValueError, match="row limit"):
        run_custom_query(str(event_dataset), "SELECT * FROM data", max_rows=2)


def test_background_query_job_completes_and_returns_bounded_result(event_dataset):
    job = server.QueryJob(
        job_id="job-one",
        session_id=None,
        created_at=1.0,
    )
    server._run_query_job(job, str(event_dataset), "SELECT COUNT(*) AS count FROM data")
    assert job.status == "completed"
    assert job.result["records"] == [{"count": 4}]


def test_queued_background_query_can_be_cancelled_before_execution(event_dataset):
    job = server.QueryJob(
        job_id="job-two",
        session_id=None,
        status="cancelling",
        created_at=1.0,
    )
    server._run_query_job(job, str(event_dataset), "SELECT * FROM data")
    assert job.status == "cancelled"
    assert job.result is None


def test_schema_validation_rejects_missing_required_columns(tmp_path):
    dataset = tmp_path / "invalid.csv"
    dataset.write_text("SESSION,EVENT_PATH\none,A\n", encoding="utf-8")
    with pytest.raises(ValueError, match="TOTAL_EVENTS"):
        validate_dataset_schema(str(dataset))


def test_dataset_state_is_isolated_between_browser_sessions(event_dataset):
    first_id = "first"
    second_id = "second"
    with server.DATASET_SESSIONS_LOCK:
        server.DATASET_SESSIONS[first_id] = server.DatasetState()
        server.DATASET_SESSIONS[second_id] = server.DatasetState()

    first_token = server.SESSION_CONTEXT.set(first_id)
    try:
        server.init_active_file(str(event_dataset))
        first_state = server.get_state()
    finally:
        server.SESSION_CONTEXT.reset(first_token)

    second_token = server.SESSION_CONTEXT.set(second_id)
    try:
        second_state = server.get_state()
    finally:
        server.SESSION_CONTEXT.reset(second_token)
        with server.DATASET_SESSIONS_LOCK:
            server.DATASET_SESSIONS.pop(first_id, None)
            server.DATASET_SESSIONS.pop(second_id, None)

    assert first_state["loaded"] is True
    assert second_state == {"loaded": False}


def test_dangerous_http_features_are_disabled_by_default(monkeypatch):
    monkeypatch.setattr(server, "TRUSTED_LOCAL_MODE", False)
    with pytest.raises(HTTPException) as exc:
        server._require_trusted_local_mode("Custom SQL")
    assert exc.value.status_code == 403


def test_browser_upload_is_the_only_dataset_loading_workflow():
    routes = {(route.path, method) for route in server.app.routes for method in route.methods}
    assert ("/api/upload-file", "POST") in routes
    assert not any(path in {"/api/browse-file", "/api/load-file"} for path, _ in routes)

    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    html = server.index(request)
    assert "Upload CSV/Parquet" in html
    assert "heatmapStatus" in html
    assert "Calculating transition matrix" in html
    assert "Open Finder Window" not in html
    assert "ENTER LOCAL FILE PATH" not in html
    assert "Load Synthetic Sample" not in html


def test_login_rejects_wrong_token_and_sets_httponly_cookie(monkeypatch):
    monkeypatch.setattr(server, "ACCESS_TOKEN", "expected-token")
    with pytest.raises(HTTPException) as exc:
        server.login({"token": "wrong-token"})
    assert exc.value.status_code == 401

    response = server.login({"token": "expected-token"})
    cookie = response.headers["set-cookie"]
    assert server.AUTH_COOKIE in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=strict" in cookie


def test_dataset_session_round_trips_through_sqlite(tmp_path, monkeypatch):
    database = tmp_path / "sessions.sqlite3"
    monkeypatch.setattr(server, "SESSION_DB_PATH", database)
    server._initialize_session_database()
    expected = server.DatasetState(
        raw_file="/data/input.csv",
        parquet_file="/data/input.parquet",
        delimiter="|",
        managed_files=("/data/input.csv", "/data/input.parquet"),
        last_access=123.0,
    )
    server._persist_dataset_state("session-one", expected)

    actual = server._load_dataset_state("session-one")

    assert actual == expected


def test_dashboard_uses_nonce_and_has_no_inline_event_handlers():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"

    dashboard = server.index(request)

    assert '<script nonce="test-nonce">' in dashboard
    assert "onclick=" not in dashboard
    assert "onchange=" not in dashboard


def test_dashboard_includes_task_focused_help():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    dashboard = server.index(request)
    assert "Help &amp; How-to" not in dashboard
    assert "Help & How-to" in dashboard
    assert "Required dataset schema" in dashboard
    assert "Dedupe and funnel semantics" in dashboard
    assert "Unload Dataset" in dashboard
    assert "Troubleshooting" in dashboard


def test_readme_has_no_removed_architecture_or_unverified_performance_claims():
    readme = Path("README.md").read_text(encoding="utf-8")
    stale_claims = ["Chart.js", "React", "zero memory overflow", "100x query"]
    assert all(claim not in readme for claim in stale_claims)
    assert "Help & How-to" in readme
    assert "single-user localhost" in readme


def test_application_and_package_versions_match():
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
    version = re.search(r'^version = "([^"]+)"', pyproject, re.MULTILINE).group(1)
    assert server.app.version == version


def test_standalone_report_is_self_contained_and_escapes_labels(tmp_path):
    output = tmp_path / "report.html"
    metrics = {
        "total_sessions": 1,
        "bounce_rate_pct": 0,
        "avg_events_per_session": 1,
        "median_events": 1,
        "p90_events": 1,
    }
    hostile = "<img src=x onerror=alert(1)>"
    entries = pd.DataFrame(
        [{"event_name": hostile, "entry_count": 1, "entry_share_pct": 100}]
    )
    exits = pd.DataFrame(
        [{"event_name": hostile, "exit_count": 1, "exit_share_pct": 100}]
    )
    matrix = pd.DataFrame([[0]], index=[hostile], columns=[hostile])

    export_html_report(str(output), metrics, entries, exits, matrix)
    report = output.read_text(encoding="utf-8")

    assert "https://" not in report
    assert hostile not in report
    assert "&lt;img src=x onerror=alert(1)&gt;" in report


def test_upload_ignores_traversal_filename_and_streams_to_upload_dir(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(server, "UPLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(server, "MAX_UPLOAD_BYTES", 1024)
    loaded_paths = []
    monkeypatch.setattr(
        server,
        "init_active_file",
        lambda path, **kwargs: loaded_paths.append(path),
    )
    monkeypatch.setattr(server, "get_state", lambda: {"loaded": True})
    upload = UploadFile(
        filename="../../server.py.csv",
        file=io.BytesIO(b"SESSION,EVENT_PATH,TOTAL_EVENTS\none,A,1\n"),
    )

    result = asyncio.run(server.upload_file(upload))

    uploaded_path = Path(loaded_paths[0]).resolve()
    assert uploaded_path.parent == tmp_path.resolve()
    assert uploaded_path.name != "server.py.csv"
    assert result["state"] == {"loaded": True}


def test_oversized_upload_is_deleted(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "UPLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(server, "MAX_UPLOAD_BYTES", 4)
    upload = UploadFile(filename="events.csv", file=io.BytesIO(b"too large"))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(server.upload_file(upload))

    assert exc.value.status_code == 413
    assert list(tmp_path.iterdir()) == []


def test_malformed_upload_and_conversion_artifact_are_deleted(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "UPLOAD_DIR", str(tmp_path))
    upload = UploadFile(
        filename="events.csv",
        file=io.BytesIO(b"SESSION,EVENT_PATH\none,A\n"),
    )

    with pytest.raises(HTTPException, match="TOTAL_EVENTS"):
        asyncio.run(server.upload_file(upload))

    assert list(tmp_path.iterdir()) == []


def test_unload_removes_only_managed_dataset_files(tmp_path):
    raw = tmp_path / "managed.csv"
    parquet = tmp_path / "managed.parquet"
    raw.write_text("data", encoding="utf-8")
    parquet.write_text("data", encoding="utf-8")
    session_id = "unload-session"
    state = server.DatasetState(
        raw_file=str(raw),
        parquet_file=str(parquet),
        managed_files=(str(raw), str(parquet)),
    )
    with server.DATASET_SESSIONS_LOCK:
        server.DATASET_SESSIONS[session_id] = state
    token = server.SESSION_CONTEXT.set(session_id)
    original_upload_dir = server.UPLOAD_DIR
    server.UPLOAD_DIR = str(tmp_path)
    try:
        result = server.unload_dataset()
    finally:
        server.UPLOAD_DIR = original_upload_dir
        server.SESSION_CONTEXT.reset(token)
        with server.DATASET_SESSIONS_LOCK:
            server.DATASET_SESSIONS.pop(session_id, None)

    assert result["removed_managed_files"] == 2
    assert not raw.exists()
    assert not parquet.exists()
    assert state.parquet_file is None


def test_benchmark_smoke_reports_resource_metrics():
    result = run_benchmark(25)
    assert result["rows"] == 25
    assert result["rows_per_second"] > 0
    assert result["peak_process_rss_mb"] > 0
    assert result["output_directory"] is None
