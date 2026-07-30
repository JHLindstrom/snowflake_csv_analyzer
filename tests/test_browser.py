import csv
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

playwright = pytest.importorskip("playwright.sync_api")
from playwright.sync_api import expect, sync_playwright


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def live_server(tmp_path_factory):
    root = tmp_path_factory.mktemp("browser")
    upload_dir = root / "uploads"
    upload_dir.mkdir()
    port = _free_port()
    env = os.environ.copy()
    env.update(
        {
            "TRISHULA_UPLOAD_DIR": str(upload_dir),
            "TRISHULA_SESSION_DB": str(upload_dir / "sessions.sqlite3"),
            "TRISHULA_DUCKDB_THREADS": "1",
            "TRISHULA_MAX_CONCURRENT_ANALYTICS": "1",
        }
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "server:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            import urllib.request

            with urllib.request.urlopen(url, timeout=0.5):
                break
        except Exception:
            time.sleep(0.1)
    else:
        process.terminate()
        output = process.communicate(timeout=5)[0]
        pytest.fail(f"Browser test server did not start:\n{output}")
    yield url, root
    process.terminate()
    process.wait(timeout=10)


def _write_dataset(path: Path):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["SESSION", "EVENT_PATH", "TOTAL_EVENTS"])
        writer.writerows(
            [
                ["one", "Home->Search->Checkout", 3],
                ["two", "Home->Search", 2],
                ["three", "Home->Checkout", 2],
                ["four", "Search->Checkout", 2],
            ]
        )


def test_primary_browser_journey(live_server):
    url, root = live_server
    dataset = root / "events.csv"
    _write_dataset(dataset)

    with sync_playwright() as runtime:
        browser = runtime.chromium.launch()
        page = browser.new_page()
        page.goto(url)

        expect(page.get_by_label("Trishula Web")).to_be_visible()
        expect(page.get_by_role("button", name="🔄 Restart Server")).to_have_count(0)

        load_button = page.get_by_role("button", name="📁 Load Dataset")
        expect(load_button).to_have_attribute("aria-expanded", "true")
        load_button.click()
        expect(load_button).to_have_attribute("aria-expanded", "false")
        load_button.click()
        expect(load_button).to_have_attribute("aria-expanded", "true")

        page.locator("#browserFileInput").set_input_files(dataset)
        expect(page.locator("#datasetBanner")).to_be_visible(timeout=20_000)
        expect(page.locator("#activeFileName")).to_contain_text(".parquet")

        page.get_by_role("button", name="🎛️ Funnel Retention").click()
        page.get_by_role("button", name="⚡ Top 4 Frequent Events").click()
        expect(page.locator("#funnelMetricsTable tbody tr")).not_to_have_count(
            0, timeout=20_000
        )

        page.get_by_role("button", name="❓ Help & How-to").click()
        expect(page.get_by_role("heading", name="Dataset semantics")).to_be_visible()
        expect(page.get_by_text("VEHICLESESSION", exact=True)).to_be_visible()
        expect(page.locator("#panel-help")).to_have_class(re.compile(r"\bactive\b"))
        with page.expect_download(timeout=20_000) as download_info:
            page.get_by_role(
                "button", name="🖨️ Export Selected Tab to PDF"
            ).click()
        download_path = download_info.value.path()
        assert download_path.read_bytes().startswith(b"%PDF-")
        browser.close()
