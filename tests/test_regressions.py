import asyncio
import csv
import io
import re
import threading
import time
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from fastapi import HTTPException, UploadFile
from starlette.requests import Request

import server
import event_parser
from converter import convert_csv_to_parquet, validate_dataset_schema
from duckdb_config import create_duckdb_connection, get_duckdb_settings
from errors import DatasetValidationError
from event_parser import (
    calculate_funnel,
    get_event_frequencies,
    get_transition_pairs,
    run_custom_query,
    sanitize_event_path_sql,
    search_sessions,
)
from visualizer import export_html_report
from benchmark import run_benchmark
from insights import get_transition_matrix
from trishula_web.pdf_reports import build_selected_tab_pdf
from performance_profile import run_performance_profile


def _dashboard_source(request):
    """Return the rendered shell and extracted assets for source-level assertions."""
    web_root = Path(server.WEB_ROOT)
    return "\n".join(
        [
            server.index(request),
            (web_root / "static" / "dashboard.css").read_text(encoding="utf-8"),
            (web_root / "static" / "dashboard.js").read_text(encoding="utf-8"),
        ]
    )


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


def test_funnel_executes_one_duckdb_query(event_dataset, monkeypatch):
    connection = create_duckdb_connection()
    executions = []

    class CountingConnection:
        def execute(self, query, *args):
            executions.append(query)
            return connection.execute(query, *args)

        def close(self):
            connection.close()

    monkeypatch.setattr(event_parser, "_connect", lambda: CountingConnection())

    result = event_parser.calculate_funnel(
        str(event_dataset), ["A", "B", "A"], dedupe_mode="consecutive"
    )

    assert result["session_count"].tolist() == [4, 3, 1]
    assert len(executions) == 1


def test_funnel_rejects_empty_steps(event_dataset):
    with pytest.raises(ValueError, match="cannot be empty"):
        calculate_funnel(str(event_dataset), ["A", " "])


def test_event_frequencies_honor_consecutive_and_unique_deduplication(event_dataset):
    raw = get_event_frequencies(str(event_dataset), top_n=10, dedupe_mode="none")
    consecutive = get_event_frequencies(
        str(event_dataset), top_n=10, dedupe_mode="consecutive"
    )
    unique = get_event_frequencies(str(event_dataset), top_n=10, dedupe_mode="unique")

    raw_counts = dict(zip(raw["event_name"], raw["occurrence_count"]))
    consecutive_counts = dict(
        zip(consecutive["event_name"], consecutive["occurrence_count"])
    )
    unique_counts = dict(zip(unique["event_name"], unique["occurrence_count"]))

    assert raw_counts == {"A": 6, "B": 4, "C": 1}
    assert consecutive_counts == {"A": 5, "B": 4, "C": 1}
    assert unique_counts == {"A": 4, "B": 4, "C": 1}


def test_duckdb_resource_settings_apply_to_connections(monkeypatch):
    monkeypatch.setenv("TRISHULA_DUCKDB_MEMORY_LIMIT", "512MB")
    monkeypatch.setenv("TRISHULA_DUCKDB_THREADS", "2")
    monkeypatch.setenv("TRISHULA_CSV_MAX_LINE_SIZE", "33554432")

    settings = get_duckdb_settings()
    connection = create_duckdb_connection()
    try:
        assert settings.memory_limit == "512MB"
        assert settings.threads == 2
        assert settings.csv_max_line_size == 33554432
        assert (
            connection.execute(
                "SELECT current_setting('memory_limit')"
            ).fetchone()[0]
            == "488.2 MiB"
        )
        assert (
            connection.execute("SELECT current_setting('threads')").fetchone()[0]
            == 2
        )
    finally:
        connection.close()


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "many", "65"])
def test_duckdb_thread_setting_rejects_invalid_values(monkeypatch, value):
    monkeypatch.setenv("TRISHULA_DUCKDB_THREADS", value)
    with pytest.raises(ValueError, match="TRISHULA_DUCKDB_THREADS"):
        get_duckdb_settings()


@pytest.mark.parametrize(
    "value", ["", "0GB", "lots", "1GB'; DROP TABLE data; --"]
)
def test_duckdb_memory_setting_rejects_invalid_values(monkeypatch, value):
    monkeypatch.setenv("TRISHULA_DUCKDB_MEMORY_LIMIT", value)
    with pytest.raises(ValueError, match="TRISHULA_DUCKDB_MEMORY_LIMIT"):
        get_duckdb_settings()


@pytest.mark.parametrize(
    "value", ["", "1999999", "268435457", "64MB", "1; DROP TABLE data"]
)
def test_csv_max_line_size_rejects_invalid_values(monkeypatch, value):
    monkeypatch.setenv("TRISHULA_CSV_MAX_LINE_SIZE", value)
    with pytest.raises(ValueError, match="TRISHULA_CSV_MAX_LINE_SIZE"):
        get_duckdb_settings()


def test_csv_conversion_accepts_record_larger_than_duckdb_default(tmp_path):
    csv_path = tmp_path / "large-record.csv"
    parquet_path = tmp_path / "large-record.parquet"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["SESSION", "EVENT_PATH", "TOTAL_EVENTS"])
        writer.writerow(["large", "A" * 2_100_000, 1])

    result = convert_csv_to_parquet(str(csv_path), str(parquet_path))

    assert result["row_count"] == 1
    assert parquet_path.exists()


def test_csv_conversion_explains_configured_line_limit(tmp_path, monkeypatch):
    csv_path = tmp_path / "oversized-record.csv"
    parquet_path = tmp_path / "oversized-record.parquet"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["SESSION", "EVENT_PATH", "TOTAL_EVENTS"])
        writer.writerow(["large", "A" * 2_100_000, 1])
    monkeypatch.setenv("TRISHULA_CSV_MAX_LINE_SIZE", "2000000")

    with pytest.raises(
        DatasetValidationError, match="TRISHULA_CSV_MAX_LINE_SIZE=2000000"
    ):
        convert_csv_to_parquet(str(csv_path), str(parquet_path))

    assert not parquet_path.exists()


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


def test_heatmap_reports_duckdb_memory_limit_as_json_error(event_dataset, monkeypatch):
    monkeypatch.setattr(
        server,
        "_require_dataset",
        lambda: server.DatasetState(parquet_file=str(event_dataset)),
    )

    def exhaust_memory(*args, **kwargs):
        raise duckdb.OutOfMemoryException("test memory limit")

    monkeypatch.setattr(server, "get_transition_matrix", exhaust_memory)

    with pytest.raises(HTTPException) as exc:
        server.heatmap()

    assert exc.value.status_code == 503
    assert "TRISHULA_DUCKDB_MEMORY_LIMIT" in exc.value.detail


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


def test_background_query_cancelled_while_waiting_does_not_execute(
    event_dataset, monkeypatch
):
    semaphore = threading.BoundedSemaphore(1)
    semaphore.acquire()
    monkeypatch.setattr(server, "ANALYTICS_QUERY_SLOTS", semaphore)
    monkeypatch.setattr(server, "ANALYTICS_QUEUE_TIMEOUT_SECONDS", 1)
    executed = threading.Event()
    monkeypatch.setattr(
        server,
        "run_custom_query",
        lambda *args, **kwargs: executed.set(),
    )
    job = server.QueryJob(
        job_id="job-waiting",
        session_id=None,
        created_at=time.time(),
    )
    worker = threading.Thread(
        target=server._run_query_job,
        args=(job, str(event_dataset), "SELECT * FROM data"),
    )
    worker.start()

    deadline = time.time() + 1
    while job.status != "running" and time.time() < deadline:
        time.sleep(0.01)
    job.status = "cancelling"
    semaphore.release()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert job.status == "cancelled"
    assert not executed.is_set()


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
    routes = {
        (route.path, method)
        for route in server.app.routes
        for method in getattr(route, "methods", ())
    }
    assert ("/api/upload-file", "POST") in routes
    assert not any(path in {"/api/browse-file", "/api/load-file"} for path, _ in routes)

    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    html = _dashboard_source(request)
    assert "Upload CSV/Parquet" in html
    assert "heatmapStatus" in html
    assert "Calculating transition matrix" in html
    assert 'id="sankeyStatus"' in html
    assert 'id="sankeyChart"' in html
    assert "Building Sankey Flow" in html
    assert "links.slice(0, 24)" in html
    assert "document.createElementNS" in html
    assert "fetch(`/api/heatmap?top=10&dedupe=${dedupe}`)" in html
    assert "Open Finder Window" not in html
    assert "ENTER LOCAL FILE PATH" not in html
    assert "Load Synthetic Sample" not in html


def test_dashboard_loads_only_the_active_tab_on_startup():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"

    dashboard = _dashboard_source(request)

    assert "loadAllData" not in dashboard
    assert "loadTabData(activeTab)" in dashboard
    assert "if (tabLoadPromises.has(tabName))" in dashboard
    assert "loadedTabs.delete('heatmap')" in dashboard
    assert "loadedTabs.delete('sankey')" in dashboard
    assert "sankey: loadSankey" in dashboard


def test_analytics_query_slot_serializes_concurrent_work(monkeypatch):
    monkeypatch.setattr(server, "ANALYTICS_QUERY_SLOTS", threading.BoundedSemaphore(1))
    monkeypatch.setattr(server, "ANALYTICS_QUEUE_TIMEOUT_SECONDS", 1)
    active = 0
    peak_active = 0
    state_lock = threading.Lock()
    first_entered = threading.Event()

    def worker():
        nonlocal active, peak_active
        with server._analytics_query_slot():
            with state_lock:
                active += 1
                peak_active = max(peak_active, active)
                first_entered.set()
            time.sleep(0.03)
            with state_lock:
                active -= 1

    first = threading.Thread(target=worker)
    second = threading.Thread(target=worker)
    first.start()
    assert first_entered.wait(timeout=1)
    second.start()
    first.join(timeout=1)
    second.join(timeout=1)

    assert not first.is_alive()
    assert not second.is_alive()
    assert peak_active == 1


def test_analytics_query_slot_times_out_when_capacity_is_busy(monkeypatch):
    semaphore = threading.BoundedSemaphore(1)
    semaphore.acquire()
    monkeypatch.setattr(server, "ANALYTICS_QUERY_SLOTS", semaphore)
    monkeypatch.setattr(server, "ANALYTICS_QUEUE_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(HTTPException) as exc:
        with server._analytics_query_slot():
            pass

    semaphore.release()
    assert exc.value.status_code == 503
    assert "capacity is busy" in exc.value.detail


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

    dashboard = _dashboard_source(request)

    assert '<script nonce="test-nonce" src="/static/dashboard.js"></script>' in dashboard
    assert "onclick=" not in dashboard
    assert "onchange=" not in dashboard


def test_dashboard_includes_task_focused_help():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    dashboard = _dashboard_source(request)
    assert "Help &amp; How-to" not in dashboard
    assert "Help & How-to" in dashboard
    assert "Required dataset schema" in dashboard
    assert "Dedupe and funnel semantics" in dashboard
    assert "Unload Dataset" in dashboard
    assert "Troubleshooting" in dashboard
    assert 'id="duckdbStatus"' in dashboard


def test_dashboard_exposes_funnel_and_search_loading_errors():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    dashboard = _dashboard_source(request)

    assert 'id="funnelEventStatus"' in dashboard
    assert 'id="searchStatus"' in dashboard
    assert "Loading frequent events…" in dashboard
    assert "Unable to load events:" in dashboard
    assert "Unable to calculate funnel:" in dashboard
    assert "Unable to search sessions:" in dashboard
    assert "stepName && !selectedFunnelSteps.includes(stepName)" not in dashboard
    assert 'id="calculateFunnelButton"' in dashboard
    load_events_source = dashboard.split("async function loadEvents()", 1)[1].split(
        "function renderFunnelPills()", 1
    )[0]
    assert "await loadFunnel()" not in load_events_source


def test_dashboard_compacts_and_expands_session_journeys():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    dashboard = _dashboard_source(request)

    assert 'id="collapseAllSessionsButton"' in dashboard
    assert 'id="expandAllSessionsButton"' not in dashboard
    assert 'class="journey-toggle"' in dashboard
    assert "compressJourney(steps, matches)" in dashboard
    assert "stateData?.delimiter || '->'" in dashboard
    assert "breadcrumb-pill${matchClass}" in dashboard
    assert "Consecutive repeated events are grouped." in dashboard


def test_dashboard_exports_only_the_selected_tab():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    dashboard = _dashboard_source(request)

    assert "Export Selected Tab to PDF" in dashboard
    assert "exportSelectedTabToPdf" in dashboard
    assert "await loadTabData(activeTab)" in dashboard
    assert "Preparing selected tab…" in dashboard
    assert "collectPdfBlocks(panel)" in dashboard
    assert "fetch('/api/export-pdf'" in dashboard
    assert "response.blob()" in dashboard
    assert "window.print()" not in dashboard


def test_selected_tab_pdf_is_generated_locally_and_bounded():
    payload = {
        "tab": "help",
        "title": "Trishula - Help & How-to",
        "dataset": "events.parquet",
        "dedupe": "Dedupe: Consecutive",
        "generated_at": "2026-07-29 20:00",
        "blocks": [
            {"type": "heading", "text": "Quick start"},
            {"type": "paragraph", "text": "Load a dataset and inspect the results."},
            {
                "type": "table",
                "rows": [["Column", "Purpose"], ["SESSION", "Session identifier"]],
            },
            {"type": "code", "text": "trishula web"},
        ],
    }
    pdf = build_selected_tab_pdf(payload)
    assert pdf.startswith(b"%PDF-")
    assert len(pdf) > 1_000

    oversized = dict(payload, blocks=[{"type": "paragraph", "text": "x"}] * 601)
    with pytest.raises(ValueError, match="too many"):
        build_selected_tab_pdf(oversized)


def test_dashboard_has_shared_interactive_loading_states():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    dashboard = _dashboard_source(request)

    assert 'id="loadingOverlay"' in dashboard
    assert 'id="loadingProgressBar"' in dashboard
    assert 'id="loadingElapsed"' in dashboard
    assert "function startLoading(" in dashboard
    assert "showDelayMs = 250" in dashboard
    assert "loading-overlay.pending" in dashboard
    assert "elapsedSeconds >= 10" in dashboard
    assert "prefers-reduced-motion: reduce" in dashboard
    assert "new XMLHttpRequest()" in dashboard
    assert "request.upload.addEventListener('progress'" in dashboard
    assert "Validating schema and preparing Parquet" in dashboard
    assert "Scanning sessions and calculating summary metrics" in dashboard
    assert "Counting event-to-event transitions" in dashboard


def test_dashboard_has_compact_branding_and_right_aligned_help():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    dashboard = _dashboard_source(request)

    assert 'class="brand-icon"' in dashboard
    assert '<span class="brand-copy"><span>TRISHULA</span><span>WEB</span></span>' in dashboard
    assert "color: #fbbf24;" in dashboard
    assert "@media (max-width: 1500px)" in dashboard
    assert "flex-basis: 100%;" in dashboard
    nav_actions = dashboard.split('<div class="nav-actions">', 1)[1].split("</div>", 1)[0]
    assert nav_actions.index('id="printButton"') < nav_actions.index('data-tab="help"')


def test_load_dataset_button_toggles_upload_panel_without_restart_control():
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.csp_nonce = "test-nonce"
    dashboard = _dashboard_source(request)

    assert 'id="openFileButton" aria-controls="loaderCard" aria-expanded="false"' in dashboard
    assert "function toggleFileLoader()" in dashboard
    assert "loaderCard.style.display = shouldShow ? 'block' : 'none';" in dashboard
    assert "loadButton.setAttribute('aria-expanded', String(shouldShow));" in dashboard
    assert "restartButton" not in dashboard
    assert "triggerServerRestart" not in dashboard


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


def test_pdf_dependency_is_declared_in_all_installation_paths():
    requirements = Path("requirements.txt").read_text(encoding="utf-8")
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
    setup = Path("setup.py").read_text(encoding="utf-8")
    assert "reportlab>=4.0" in requirements
    assert '"reportlab>=4.0"' in pyproject
    assert '"reportlab>=4.0"' in setup


def test_performance_profile_writes_reusable_artifacts(tmp_path):
    result = run_performance_profile(100, tmp_path, top_functions=5)
    assert Path(result["profile"]).is_file()
    assert Path(result["summary"]).is_file()
    assert "function calls" in Path(result["summary"]).read_text(encoding="utf-8")
    assert result["benchmark"]["rows"] == 100
    assert (tmp_path / "benchmark-results.json").is_file()


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
    assert result["heatmap_seconds"] >= 0
    assert result["heatmap_nonzero_cells"] > 0
    assert result["output_directory"] is None
