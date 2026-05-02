from __future__ import annotations

import datetime
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from typing import Optional

import cli_services as svc
import telegram_integration as tg

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_STATE_FILE = os.path.join(svc.CLI_STATE_DIR, "worker.json")
DEFAULT_POLL_INTERVAL = 3
SCAN_SUMMARY_INTERVAL_SECONDS = 30


def _ensure_state_dir():
    os.makedirs(svc.CLI_STATE_DIR, exist_ok=True)


def _is_process_running(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _load_worker_state() -> dict:
    if not os.path.exists(WORKER_STATE_FILE):
        return {}
    with open(WORKER_STATE_FILE, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    return data if isinstance(data, dict) else {}


def _write_worker_state(payload: dict):
    _ensure_state_dir()
    with open(WORKER_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def _clear_worker_state():
    if os.path.exists(WORKER_STATE_FILE):
        os.remove(WORKER_STATE_FILE)


def worker_status() -> dict:
    state = _load_worker_state()
    pid = state.get("pid")
    running = _is_process_running(pid)
    if state and not running:
        _clear_worker_state()
    return {
        "running": running,
        "pid": pid if running else None,
        "started_at": state.get("started_at") if running else None,
        "poll_interval": state.get("poll_interval") if running else None,
    }


def _status_counts(jobs: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job in jobs:
        status = str(job.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


class BrainWorker:
    def __init__(self, credentials_path: str = svc.CREDS_PATH, poll_interval: int = DEFAULT_POLL_INTERVAL):
        self.credentials_path = credentials_path
        self.poll_interval = poll_interval
        self._stop_requested = False
        self._telegram_thread: Optional[threading.Thread] = None
        self._last_scan_summary_at = 0.0
        self._last_scan_signature: Optional[tuple] = None

    def request_stop(self, *_args):
        self._stop_requested = True

    def _start_telegram_thread(self):
        try:
            runner = tg.TelegramBotRunner(
                credentials_path=self.credentials_path,
                poll_timeout=tg.DEFAULT_POLL_TIMEOUT,
            )
        except tg.TelegramConfigError as exc:
            logging.info("Telegram monitoring disabled: %s", exc)
            return

        logging.info(
            "Telegram monitoring configured: chat_id_configured=yes poll_timeout=%ss",
            runner.poll_timeout,
        )
        self._telegram_thread = threading.Thread(
            target=runner.run,
            name="brain-telegram-worker",
            daemon=True,
        )
        self._telegram_thread.start()
        logging.info("Telegram monitoring thread started.")

    def _next_pending_simulation_job(self, jobs: list[dict]) -> Optional[dict]:
        if any(job.get("status") == "running" for job in jobs):
            return None
        pending_jobs = [job for job in jobs if job.get("status") == "pending"]
        pending_jobs.sort(key=lambda job: job.get("created_at", ""))
        return pending_jobs[0] if pending_jobs else None

    def _log_scan_summary(self, jobs: list[dict], *, force: bool = False):
        counts = _status_counts(jobs)
        pending_jobs = [job for job in jobs if job.get("status") == "pending"]
        pending_jobs.sort(key=lambda job: job.get("created_at", ""))
        pending_ids = tuple(job.get("id") for job in pending_jobs)
        running_ids = tuple(job.get("id") for job in jobs if job.get("status") == "running")
        signature = (tuple(sorted(counts.items())), pending_ids[:3], running_ids[:3])
        now = time.monotonic()
        if (
            not force
            and signature == self._last_scan_signature
            and now - self._last_scan_summary_at < SCAN_SUMMARY_INTERVAL_SECONDS
        ):
            return

        self._last_scan_signature = signature
        self._last_scan_summary_at = now
        next_pending = pending_ids[0] if pending_ids else None
        logging.info(
            "Worker scan: total=%s pending=%s running=%s done=%s failed=%s stopped=%s next_pending=%s",
            len(jobs),
            counts.get("pending", 0),
            counts.get("running", 0),
            counts.get("done", 0),
            counts.get("failed", 0),
            counts.get("stopped", 0),
            next_pending or "-",
        )

    def _run_pending_jobs_once(self):
        jobs = svc.simulate_list()
        self._log_scan_summary(jobs)
        job = self._next_pending_simulation_job(jobs)
        if job is None:
            return

        job_id = job["id"]
        logging.info("Worker picked pending simulation job %s", job_id)
        svc.simulate_run(
            job_id,
            progress_cb=lambda msg: logging.info("[simulate %s] %s", job_id, msg),
        )

    def run_forever(self):
        _write_worker_state({
            "pid": os.getpid(),
            "started_at": datetime.datetime.now().isoformat(),
            "poll_interval": self.poll_interval,
        })
        logging.info(
            "Brain worker state written: pid=%s poll_interval=%ss state_file=%s",
            os.getpid(),
            self.poll_interval,
            WORKER_STATE_FILE,
        )
        self._start_telegram_thread()

        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)

        logging.info("Brain worker started (pid=%s).", os.getpid())
        self._log_scan_summary(svc.simulate_list(), force=True)
        try:
            while not self._stop_requested:
                self._run_pending_jobs_once()
                time.sleep(self.poll_interval)
        finally:
            logging.info("Brain worker stopping.")
            _clear_worker_state()


def ensure_background_worker_running(credentials_path: str = svc.CREDS_PATH,
                                     poll_interval: int = DEFAULT_POLL_INTERVAL) -> dict:
    status = worker_status()
    if status["running"]:
        return status

    command = [
        sys.executable,
        os.path.join(SCRIPT_DIR, "brain_cli.py"),
        "--credentials",
        credentials_path,
        "worker",
        "run",
        "--poll-interval",
        str(poll_interval),
    ]
    subprocess.Popen(
        command,
        cwd=SCRIPT_DIR,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    time.sleep(0.5)
    return worker_status()
