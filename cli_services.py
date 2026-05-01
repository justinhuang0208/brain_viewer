#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cli_services.py — Pure-Python service layer for brain_cli.py.

Zero Qt / GUI dependencies.  All public functions return plain dicts, lists,
or DataFrames so the CLI can easily serialise to JSON or plain text.
"""

from __future__ import annotations

import csv
import datetime
import itertools
import json
import logging
import os
import re
import sys
import time
import uuid as _uuid_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from alpha_registry import get_registry
from wq_session import (
    BRAIN_API_BASE,
    authenticate_with_brain,
    build_session_from_credentials,
    clear_login_state,
    extract_persona_url,
    get_session_for_request,
    load_login_cookies,
    load_pending_persona_session,
    load_persisted_session,
    save_login_cookies,
)

# ---------------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------------

SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
DATASETS_DIR    = os.path.join(SCRIPT_DIR, "datasets")
OPERATORS_DIR   = os.path.join(SCRIPT_DIR, "operators")
TEMPLATES_DIR   = os.path.join(SCRIPT_DIR, "templates")
ALPHAS_DIR      = os.path.join(SCRIPT_DIR, "alphas")
DATA_DIR        = os.path.join(SCRIPT_DIR, "data")
CREDS_PATH      = os.path.join(SCRIPT_DIR, "credentials.json")
CLI_STATE_DIR   = os.path.join(SCRIPT_DIR, ".brain_cli")
JOBS_DIR        = os.path.join(CLI_STATE_DIR, "jobs")
STOP_DIR        = os.path.join(CLI_STATE_DIR, "stop")
DATASETS_API    = f"{BRAIN_API_BASE}/data-sets"
DATAFIELDS_API  = f"{BRAIN_API_BASE}/data-fields"
OPERATORS_API   = f"{BRAIN_API_BASE}/operators"
OPERATORS_FILE  = os.path.join(OPERATORS_DIR, "operators.json")
OPERATOR_DOCS_DIR = os.path.join(OPERATORS_DIR, "docs")
DEFAULT_DATA_FIELD_OPTION = {
    "instrumentType": "EQUITY",
    "region": "USA",
    "universe": "TOP3000",
    "delay": 1,
}

# Simulation CSV header (matches WQSession output in simulation.py)
SIM_CSV_HEADER = [
    "passed", "delay", "region", "neutralization", "decay", "truncation",
    "sharpe", "fitness", "turnover", "weight", "subsharpe", "correlation",
    "universe", "link", "code",
]

PARAM_COLUMNS = ["code", "decay", "delay", "neutralization", "region", "truncation", "universe"]
SIMULATION_RATE_LIMIT_HEADERS = {
    "limit": "x-ratelimit-limit",
    "remaining": "x-ratelimit-remaining",
    "reset_seconds": "x-ratelimit-reset",
}
SIMULATION_ERROR_STATUSES = {"ERROR", "TIMEOUT", "FAIL", "CANCELLED"}
SIMULATION_DONE_STATUSES = {"COMPLETE", "WARNING"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_dirs():
    for d in [
        CLI_STATE_DIR,
        JOBS_DIR,
        STOP_DIR,
        DATA_DIR,
        TEMPLATES_DIR,
        ALPHAS_DIR,
        DATASETS_DIR,
        OPERATORS_DIR,
        OPERATOR_DOCS_DIR,
    ]:
        os.makedirs(d, exist_ok=True)


def _int_header(headers, name: str) -> Optional[int]:
    value = headers.get(name)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _simulation_rate_limit_from_headers(headers) -> Optional[dict]:
    values = {
        key: _int_header(headers, header_name)
        for key, header_name in SIMULATION_RATE_LIMIT_HEADERS.items()
    }
    if all(value is None for value in values.values()):
        return None

    now = datetime.datetime.now()
    reset_seconds = values.get("reset_seconds")
    reset_at = None
    if reset_seconds is not None:
        reset_at = (now + datetime.timedelta(seconds=max(reset_seconds, 0))).isoformat()

    return {
        "limit": values.get("limit"),
        "remaining": values.get("remaining"),
        "reset_seconds": reset_seconds,
        "reset_at": reset_at,
        "observed_at": now.isoformat(),
    }


def _seconds_until_iso(iso_value: Optional[str]) -> Optional[int]:
    if not iso_value:
        return None
    try:
        reset_at = datetime.datetime.fromisoformat(iso_value)
    except (TypeError, ValueError):
        return None
    return max(int((reset_at - datetime.datetime.now()).total_seconds()), 0)


def _retry_after_seconds(headers, default: float = 10.0) -> float:
    value = headers.get("Retry-After")
    if value is None:
        return default
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return default


def _request_with_rate_limit_retry(session: requests.Session, method: str, url: str,
                                   *, max_retries: int = 5, progress_cb=None,
                                   retry_context: str = "request", **kwargs) -> requests.Response:
    """Retry transient WQ API throttling/server failures without hiding auth errors."""
    for attempt in range(max_retries + 1):
        response = getattr(session, method)(url, **kwargs)
        if response.status_code == 429:
            if attempt >= max_retries:
                return response
            retry_after = int(response.headers.get("Retry-After", 300))
            if progress_cb:
                progress_cb(f"Rate limited during {retry_context}; retrying in {retry_after}s…")
            time.sleep(retry_after)
            continue
        if response.status_code in (502, 503, 504):
            if attempt >= max_retries:
                return response
            if progress_cb:
                progress_cb(f"WQ server returned {response.status_code} during {retry_context}; retrying in 10s…")
            time.sleep(10)
            continue
        return response
    return response


def _data_field_row(field: dict) -> dict:
    cov_raw = field.get("coverage", 0)
    try:
        cov = f"{int(round(float(cov_raw) * 100))}%"
    except Exception:
        cov = str(cov_raw)
    return {
        "Field":       field.get("id", ""),
        "Description": field.get("description", ""),
        "Type":        field.get("type", ""),
        "Coverage":    cov,
        "Users":       field.get("userCount", 0),
        "Alphas":      field.get("alphaCount", 0),
    }


def _operator_metadata_row(operator: dict) -> dict:
    return {
        "name": operator.get("name", ""),
        "category": operator.get("category", ""),
        "scope": operator.get("scope") or [],
        "definition": operator.get("definition", ""),
        "description": operator.get("description", ""),
        "documentation": operator.get("documentation"),
        "level": operator.get("level"),
    }


_ensure_dirs()


def _sanitize_filename(name: str) -> str:
    for ch in '<>:"/\\|?*':
        name = name.replace(ch, "_")
    return name.strip() or "template"


def _row_passed_bool(value: Any) -> bool:
    text = str(value).strip().upper()
    if text == "PASS":
        return True
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _notify_login_issue(reason: str, detail: str = "", cooldown_key: str = "auth-issue"):
    try:
        from telegram_integration import send_login_issue_notification

        send_login_issue_notification(reason, detail=detail, cooldown_key=cooldown_key)
    except Exception as exc:
        logging.warning("Unable to send Telegram auth notification: %s", exc)


# ---------------------------------------------------------------------------
# Job state store (file-backed)
# ---------------------------------------------------------------------------

class JobStore:
    """Simple file-backed job state manager under .brain_cli/jobs/."""

    @staticmethod
    def create(job_type: str, params: dict) -> str:
        job_id = _uuid_mod.uuid4().hex[:12]
        now    = datetime.datetime.now().isoformat()
        job    = {
            "id":         job_id,
            "type":       job_type,
            "status":     "pending",
            "created_at": now,
            "updated_at": now,
            "params":     params,
            "result_file": None,
            "error":      None,
            "pid":        None,
        }
        JobStore._write(job_id, job)
        return job_id

    @staticmethod
    def get(job_id: str) -> Optional[dict]:
        p = os.path.join(JOBS_DIR, f"{job_id}.json")
        if not os.path.exists(p):
            return None
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def update(job_id: str, **kwargs):
        job = JobStore.get(job_id)
        if job is None:
            return
        job.update(kwargs)
        job["updated_at"] = datetime.datetime.now().isoformat()
        JobStore._write(job_id, job)

    @staticmethod
    def mutate(job_id: str, mutator):
        job = JobStore.get(job_id)
        if job is None:
            return None
        mutator(job)
        job["updated_at"] = datetime.datetime.now().isoformat()
        JobStore._write(job_id, job)
        return job

    @staticmethod
    def list_jobs(job_type: Optional[str] = None) -> List[dict]:
        jobs = []
        for fn in os.listdir(JOBS_DIR):
            if fn.endswith(".json"):
                p = os.path.join(JOBS_DIR, fn)
                try:
                    with open(p, "r", encoding="utf-8") as fh:
                        j = json.load(fh)
                    if job_type is None or j.get("type") == job_type:
                        jobs.append(j)
                except Exception:
                    pass
        jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return jobs

    @staticmethod
    def _write(job_id: str, job: dict):
        p = os.path.join(JOBS_DIR, f"{job_id}.json")
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(job, fh, indent=2)

    @staticmethod
    def request_stop(job_id: str):
        Path(STOP_DIR, f"{job_id}.stop").touch()

    @staticmethod
    def is_stop_requested(job_id: str) -> bool:
        return os.path.exists(os.path.join(STOP_DIR, f"{job_id}.stop"))

    @staticmethod
    def clear_stop(job_id: str):
        p = os.path.join(STOP_DIR, f"{job_id}.stop")
        if os.path.exists(p):
            os.remove(p)


# ---------------------------------------------------------------------------
# Auth service
# ---------------------------------------------------------------------------

def wq_authenticate(credentials_path: str = CREDS_PATH) -> Tuple[Optional[requests.Session], Optional[str], Optional[str]]:
    """
    Attempt WQ Brain authentication.

    Returns
    -------
    (session, None, None)          – on success
    (session, "persona", url)      – on Persona challenge
    (None, "error", message)       – on failure
    """
    try:
        session = load_persisted_session(credentials_path) or build_session_from_credentials(credentials_path)
    except FileNotFoundError:
        return None, "error", f"Credentials file not found: {credentials_path}"
    except Exception as exc:
        return None, "error", f"Error reading credentials: {exc}"

    try:
        return authenticate_with_brain(session)
    except requests.exceptions.Timeout:
        return None, "error", "Connection timed out."
    except requests.exceptions.RequestException as exc:
        return None, "error", f"Network error: {exc}"


def auth_login_status(credentials_path: str = CREDS_PATH) -> dict:
    """Return login status without starting a new Persona inquiry."""
    pending_session, pending_url = load_pending_persona_session(credentials_path)
    if pending_session is not None and pending_url:
        return {
            "status": "persona_pending",
            "persona_url": pending_url,
            "message": "Persona verification is already pending. Complete this URL or run auth persona-complete.",
        }

    session = load_persisted_session(credentials_path)
    if session is None:
        return {
            "status": "not_logged_in",
            "message": "No saved WQ session. Run auth login to start a Persona flow.",
        }

    _, login_time = load_login_cookies(session)
    login_age = None
    if login_time is not None:
        login_age = str(datetime.datetime.now() - login_time).split(".")[0]

    try:
        response = session.options(f"{BRAIN_API_BASE}/simulations", timeout=10)
    except requests.exceptions.Timeout:
        return {"status": "failed", "message": "Connection timed out.", "login_age": login_age}
    except requests.exceptions.RequestException as exc:
        return {"status": "failed", "message": f"Network error: {exc}", "login_age": login_age}

    if response.status_code == 200:
        return {"status": "logged_in", "message": "Saved session is valid.", "login_age": login_age}
    if response.status_code == 401:
        return {
            "status": "expired",
            "message": "Saved session is expired. Run auth login to start a Persona flow.",
            "login_age": login_age,
        }
    return {
        "status": "unknown",
        "message": f"Session check returned HTTP {response.status_code}.",
        "login_age": login_age,
    }


def auth_persona_complete(session: requests.Session, persona_url: str,
                          poll_interval: int = 5, max_attempts: int = 60,
                          progress_cb=None) -> Tuple[bool, str]:
    """
    Poll persona completion.  Returns (success, message).
    Caller should open *persona_url* in a browser before calling this.
    """
    for attempt in range(max_attempts):
        if progress_cb:
            progress_cb(f"Polling persona verification ({attempt + 1}/{max_attempts})…")
        try:
            r = session.post(persona_url, timeout=15)
            if r.ok:
                save_login_cookies(session)
                return True, "Persona verification complete."
            time.sleep(poll_interval)
        except requests.exceptions.RequestException:
            time.sleep(poll_interval)
    return False, "Persona verification timed out."


def auth_complete_from_credentials(credentials_path: str = CREDS_PATH,
                                   poll_interval: int = 5,
                                   progress_cb=None) -> dict:
    """
    Full login flow including persona polling.  Suitable for CLI --wait mode.
    Returns dict with keys: status, session (or None), message.
    """
    pending_session, pending_url = load_pending_persona_session(credentials_path)
    if pending_session is not None and pending_url:
        if progress_cb:
            progress_cb(f"Reusing pending Persona URL:\n  {pending_url}\nThen wait…")
        ok, msg = auth_persona_complete(pending_session, pending_url, poll_interval=poll_interval,
                                        progress_cb=progress_cb)
        if ok:
            return {"status": "logged_in", "session": pending_session, "message": msg}
        return {"status": "failed", "session": None, "message": msg}

    session, kind, detail = wq_authenticate(credentials_path)
    if kind is None:
        return {"status": "logged_in", "session": session, "message": "Authentication successful."}
    if kind == "persona":
        if progress_cb:
            progress_cb(f"Persona required. Open this URL in a browser:\n  {detail}\nThen wait…")
        ok, msg = auth_persona_complete(session, detail, poll_interval=poll_interval,
                                        progress_cb=progress_cb)
        if ok:
            return {"status": "logged_in", "session": session, "message": msg}
        return {"status": "failed", "session": None, "message": msg}
    return {"status": "failed", "session": None, "message": detail}


# ---------------------------------------------------------------------------
# Dataset service
# ---------------------------------------------------------------------------

def datasets_list(datasets_dir: str = DATASETS_DIR) -> List[dict]:
    """List available local dataset CSVs."""
    out = []
    if not os.path.isdir(datasets_dir):
        return out
    for fn in sorted(os.listdir(datasets_dir)):
        if fn.endswith("_fields_formatted.csv"):
            ds_id = fn.replace("_fields_formatted.csv", "")
            fp    = os.path.join(datasets_dir, fn)
            size  = os.path.getsize(fp)
            try:
                df = pd.read_csv(fp, nrows=0)
                rows = sum(1 for _ in open(fp, encoding="utf-8")) - 1
            except Exception:
                rows = -1
            out.append({"dataset_id": ds_id, "file": fn, "rows": rows, "size_bytes": size})
    return out


def datasets_refresh(datasets_dir: str = DATASETS_DIR,
                     credentials_path: str = CREDS_PATH,
                     progress_cb=None) -> dict:
    """Fetch all dataset field metadata from WQ Brain API and save locally."""
    try:
        session, kind, detail = get_session_for_request(credentials_path)
    except FileNotFoundError:
        return {"status": "error", "message": f"Credentials file not found: {credentials_path}"}
    except Exception as exc:
        return {"status": "error", "message": f"Error preparing session: {exc}"}
    if kind == "persona":
        _notify_login_issue(
            "Dataset refresh requires Persona verification.",
            detail,
            cooldown_key="datasets-persona-required",
        )
        return {"status": "error", "message": f"Persona verification required: {detail}"}
    if kind == "error":
        _notify_login_issue(
            "Dataset refresh failed because login is invalid.",
            detail,
            cooldown_key="datasets-login-invalid",
        )
        return {"status": "error", "message": detail}

    os.makedirs(datasets_dir, exist_ok=True)
    refreshed = []
    errors    = []

    opt = dict(DEFAULT_DATA_FIELD_OPTION)
    dataset_limit = 50
    field_limit = 50
    if progress_cb:
        progress_cb(
            "Fetching dataset list from WQ data-sets "
            f"({opt['region']}/{opt['universe']}/delay={opt['delay']})."
        )

    dataset_ids: List[str] = []
    offset = 0
    while True:
        params = {
            **opt,
            "limit": dataset_limit,
            "offset": offset,
        }
        try:
            r = _request_with_rate_limit_retry(
                session,
                "get",
                DATASETS_API,
                params=params,
                timeout=20,
                progress_cb=progress_cb,
                retry_context="dataset list refresh",
            )
            if r.status_code == 401:
                clear_login_state()
                persona_url = extract_persona_url(r)
                if persona_url:
                    _notify_login_issue(
                        "Saved session expired during dataset list refresh.",
                        persona_url,
                        cooldown_key="datasets-list-persona",
                    )
                    return {"status": "error", "message": f"Persona verification required: {persona_url}"}
                _notify_login_issue(
                    "Saved session expired during dataset list refresh.",
                    "Unauthorized while fetching dataset list.",
                    cooldown_key="datasets-list-unauthorized",
                )
                return {"status": "error", "message": "Unauthorized while fetching dataset list."}
            r.raise_for_status()
            body = r.json()
        except Exception as exc:
            return {"status": "error", "message": f"Error fetching dataset list: {exc}"}

        results = body.get("results", [])
        dataset_ids.extend(ds["id"] for ds in results if ds.get("id"))
        if offset + dataset_limit >= body.get("count", 0) or not results:
            break
        offset += dataset_limit
        time.sleep(1)

    dataset_ids = sorted(set(dataset_ids))
    if progress_cb:
        progress_cb(f"Found {len(dataset_ids)} datasets. Fetching data fields.")

    for i, ds_id in enumerate(dataset_ids):
        if progress_cb:
            progress_cb(f"Fetching {ds_id} ({i+1}/{len(dataset_ids)})…")
        try:
            rows = []
            offset = 0
            while True:
                params = {
                    **opt,
                    "dataset.id": ds_id,
                    "limit": field_limit,
                    "offset": offset,
                }
                r = _request_with_rate_limit_retry(
                    session,
                    "get",
                    DATAFIELDS_API,
                    params=params,
                    timeout=20,
                    progress_cb=progress_cb,
                    retry_context=f"data fields refresh for {ds_id}",
                )
                if r.status_code == 401:
                    clear_login_state()
                    persona_url = extract_persona_url(r)
                    if persona_url:
                        _notify_login_issue(
                            f"Saved session expired while refreshing data fields for {ds_id}.",
                            persona_url,
                            cooldown_key=f"data-fields-persona-{ds_id}",
                        )
                        return {"status": "error", "message": f"Persona verification required: {persona_url}"}
                    _notify_login_issue(
                        f"Saved session expired while refreshing data fields for {ds_id}.",
                        f"Unauthorized while fetching fields for {ds_id}.",
                        cooldown_key=f"data-fields-unauthorized-{ds_id}",
                    )
                    return {"status": "error", "message": f"Unauthorized while fetching fields for {ds_id}."}
                r.raise_for_status()
                body = r.json()
                results = body.get("results", [])
                for field in results:
                    rows.append(_data_field_row(field))
                if offset + field_limit >= body.get("count", 0) or not results:
                    break
                offset += field_limit
                time.sleep(1)
            if rows:
                df  = pd.DataFrame(rows, columns=["Field", "Description", "Type", "Coverage", "Users", "Alphas"])
                out = os.path.join(datasets_dir, f"{ds_id}_fields_formatted.csv")
                df.to_csv(out, index=False)
                refreshed.append(ds_id)
        except Exception as exc:
            errors.append(f"{ds_id}: {exc}")

    return {"status": "ok", "refreshed": refreshed, "errors": errors, "total": len(dataset_ids)}


def datasets_show(dataset_id: str, datasets_dir: str = DATASETS_DIR) -> Optional[pd.DataFrame]:
    """Return DataFrame of fields for *dataset_id*, or None if not found."""
    fp = os.path.join(datasets_dir, f"{dataset_id}_fields_formatted.csv")
    if not os.path.exists(fp):
        return None
    return pd.read_csv(fp)


def datasets_search(query: str, datasets_dir: str = DATASETS_DIR,
                    dataset_id: Optional[str] = None) -> List[dict]:
    """Simple text search across field names and descriptions."""
    results = []
    if dataset_id:
        targets = [dataset_id]
    else:
        targets = [e["dataset_id"] for e in datasets_list(datasets_dir)]

    q = query.lower()
    for ds_id in targets:
        df = datasets_show(ds_id, datasets_dir)
        if df is None:
            continue
        for _, row in df.iterrows():
            field = str(row.get("Field", "")).lower()
            desc  = str(row.get("Description", "")).lower()
            if q in field or q in desc:
                results.append({
                    "dataset_id":  ds_id,
                    "field":       row.get("Field", ""),
                    "description": row.get("Description", ""),
                    "type":        row.get("Type", ""),
                    "coverage":    row.get("Coverage", ""),
                })
    return results


def datasets_export_fields(dataset_id: str, output_path: str,
                           datasets_dir: str = DATASETS_DIR) -> dict:
    """Export dataset fields CSV to *output_path*."""
    df = datasets_show(dataset_id, datasets_dir)
    if df is None:
        return {"status": "error", "message": f"Dataset '{dataset_id}' not found locally."}
    df.to_csv(output_path, index=False)
    return {"status": "ok", "rows": len(df), "output": output_path}


# ---------------------------------------------------------------------------
# Operator service
# ---------------------------------------------------------------------------

def operators_list(operators_dir: str = OPERATORS_DIR) -> List[dict]:
    """List locally cached WQ Brain operator metadata."""
    fp = os.path.join(operators_dir, "operators.json")
    if not os.path.exists(fp):
        return []
    try:
        with open(fp, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return []
    if isinstance(data, dict):
        data = data.get("operators", [])
    if not isinstance(data, list):
        return []
    return [_operator_metadata_row(op) for op in data if isinstance(op, dict)]


def operators_refresh(operators_dir: str = OPERATORS_DIR,
                      credentials_path: str = CREDS_PATH,
                      include_docs: bool = True,
                      progress_cb=None) -> dict:
    """Fetch WQ Brain operator metadata and optional detailed docs."""
    try:
        session, kind, detail = get_session_for_request(credentials_path)
    except FileNotFoundError:
        return {"status": "error", "message": f"Credentials file not found: {credentials_path}"}
    except Exception as exc:
        return {"status": "error", "message": f"Error preparing session: {exc}"}
    if kind == "persona":
        _notify_login_issue(
            "Operator refresh requires Persona verification.",
            detail,
            cooldown_key="operators-persona-required",
        )
        return {"status": "error", "message": f"Persona verification required: {detail}"}
    if kind == "error":
        _notify_login_issue(
            "Operator refresh failed because login is invalid.",
            detail,
            cooldown_key="operators-login-invalid",
        )
        return {"status": "error", "message": detail}

    os.makedirs(operators_dir, exist_ok=True)
    docs_dir = os.path.join(operators_dir, "docs")
    os.makedirs(docs_dir, exist_ok=True)

    if progress_cb:
        progress_cb("Fetching operator list from WQ operators.")
    try:
        response = _request_with_rate_limit_retry(
            session,
            "get",
            OPERATORS_API,
            timeout=20,
            progress_cb=progress_cb,
            retry_context="operator list refresh",
        )
        if response.status_code == 401:
            clear_login_state()
            persona_url = extract_persona_url(response)
            if persona_url:
                _notify_login_issue(
                    "Saved session expired during operator refresh.",
                    persona_url,
                    cooldown_key="operators-persona",
                )
                return {"status": "error", "message": f"Persona verification required: {persona_url}"}
            _notify_login_issue(
                "Saved session expired during operator refresh.",
                "Unauthorized while fetching operators.",
                cooldown_key="operators-unauthorized",
            )
            return {"status": "error", "message": "Unauthorized while fetching operators."}
        response.raise_for_status()
        body = response.json()
    except Exception as exc:
        return {"status": "error", "message": f"Error fetching operators: {exc}"}

    if not isinstance(body, list):
        return {"status": "error", "message": "Unexpected operators response shape."}

    operators = sorted(
        (_operator_metadata_row(op) for op in body if isinstance(op, dict)),
        key=lambda item: item.get("name", ""),
    )
    list_path = os.path.join(operators_dir, "operators.json")
    with open(list_path, "w", encoding="utf-8") as fh:
        json.dump(operators, fh, ensure_ascii=False, indent=2)

    docs_refreshed = []
    errors = []
    if include_docs:
        for i, op in enumerate(operators):
            name = op.get("name", "")
            doc_path = op.get("documentation")
            if not name or not doc_path:
                continue
            if progress_cb:
                progress_cb(f"Fetching operator doc {name} ({i+1}/{len(operators)})…")
            try:
                doc_response = _request_with_rate_limit_retry(
                    session,
                    "get",
                    urljoin(BRAIN_API_BASE, doc_path),
                    timeout=20,
                    progress_cb=progress_cb,
                    retry_context=f"operator doc refresh for {name}",
                )
                if doc_response.status_code == 401:
                    clear_login_state()
                    return {"status": "error", "message": "Unauthorized while fetching operator docs."}
                doc_response.raise_for_status()
                doc_body = doc_response.json()
                with open(os.path.join(docs_dir, f"{_sanitize_filename(name)}.json"), "w", encoding="utf-8") as fh:
                    json.dump(doc_body, fh, ensure_ascii=False, indent=2)
                docs_refreshed.append(name)
            except Exception as exc:
                errors.append(f"{name}: {exc}")

    return {
        "status": "ok",
        "total": len(operators),
        "output": list_path,
        "docs_refreshed": docs_refreshed,
        "docs_total": len(docs_refreshed),
        "errors": errors,
    }


def operators_show(name: str, operators_dir: str = OPERATORS_DIR,
                   include_doc: bool = True) -> Optional[dict]:
    """Return one cached operator by name."""
    for op in operators_list(operators_dir):
        if op.get("name") == name:
            result = dict(op)
            if include_doc:
                doc_file = os.path.join(operators_dir, "docs", f"{_sanitize_filename(name)}.json")
                if os.path.exists(doc_file):
                    try:
                        with open(doc_file, "r", encoding="utf-8") as fh:
                            result["doc"] = json.load(fh)
                    except Exception:
                        result["doc"] = None
            return result
    return None


def operators_search(query: str, operators_dir: str = OPERATORS_DIR,
                     category: Optional[str] = None) -> List[dict]:
    """Search cached operator names, definitions, descriptions, and metadata."""
    q = query.lower()
    results = []
    for op in operators_list(operators_dir):
        if category and str(op.get("category", "")).lower() != category.lower():
            continue
        haystack = " ".join([
            str(op.get("name", "")),
            str(op.get("category", "")),
            str(op.get("definition", "")),
            str(op.get("description", "")),
            str(op.get("level", "")),
            " ".join(str(s) for s in op.get("scope", [])),
        ]).lower()
        if q in haystack:
            results.append(op)
    return results


# ---------------------------------------------------------------------------
# Template service
# ---------------------------------------------------------------------------

DEFAULT_TEMPLATES = {
    "[Default] Basic ts_rank": {
        "code": "ts_rank({field}, 126)",
        "description": "Time-series rank over 126 days",
    },
    "[Default] Basic rank": {
        "code": "rank({field})",
        "description": "Cross-sectional rank",
    },
    "[Default] Basic zscore": {
        "code": "zscore({field})",
        "description": "Standardized score",
    },
    "[Default] Industry comparison": {
        "code": "group_rank({field}, industry)",
        "description": "Rank within industry",
    },
    "[Default] Complex momentum": {
        "code": "ts_rank(ts_delta({field}, 5) / ts_delay({field}, 5), 21)",
        "description": "21-day rank of 5-day rate of change",
    },
}


def templates_list(templates_dir: str = TEMPLATES_DIR) -> List[dict]:
    """List all templates (defaults + saved)."""
    out = []
    for name, data in DEFAULT_TEMPLATES.items():
        out.append({"name": name, "description": data["description"],
                    "source": "default", "file": None})
    if os.path.isdir(templates_dir):
        for fn in sorted(os.listdir(templates_dir)):
            if fn.endswith(".json"):
                fp = os.path.join(templates_dir, fn)
                try:
                    with open(fp, "r", encoding="utf-8") as fh:
                        t = json.load(fh)
                    if "name" in t and "code" in t:
                        out.append({"name": t["name"],
                                    "description": t.get("description", ""),
                                    "source": "custom", "file": fn})
                except Exception:
                    pass
    return out


def templates_show(name: str, templates_dir: str = TEMPLATES_DIR) -> Optional[dict]:
    """Return template data dict for *name* (default or custom)."""
    if name in DEFAULT_TEMPLATES:
        d = dict(DEFAULT_TEMPLATES[name])
        d["name"] = name
        d["source"] = "default"
        return d
    if os.path.isdir(templates_dir):
        for fn in os.listdir(templates_dir):
            if fn.endswith(".json"):
                fp = os.path.join(templates_dir, fn)
                try:
                    with open(fp, "r", encoding="utf-8") as fh:
                        t = json.load(fh)
                    if t.get("name") == name:
                        t["source"] = "custom"
                        t["file"]   = fn
                        return t
                except Exception:
                    pass
    return None


def templates_save(name: str, code: str, description: str = "",
                   templates_dir: str = TEMPLATES_DIR) -> dict:
    """Save or update a custom template. Returns dict with path info."""
    os.makedirs(templates_dir, exist_ok=True)
    # Check if it already exists (update)
    if os.path.isdir(templates_dir):
        for fn in os.listdir(templates_dir):
            if fn.endswith(".json"):
                fp = os.path.join(templates_dir, fn)
                try:
                    with open(fp, "r", encoding="utf-8") as fh:
                        t = json.load(fh)
                    if t.get("name") == name:
                        t["code"]        = code
                        t["description"] = description
                        t["updated_at"]  = datetime.datetime.now().isoformat()
                        with open(fp, "w", encoding="utf-8") as fh:
                            json.dump(t, fh, ensure_ascii=False, indent=2)
                        return {"status": "updated", "name": name, "file": fn}
                except Exception:
                    pass
    # New template
    ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    fn   = f"{_sanitize_filename(name)}_{ts}.json"
    fp   = os.path.join(templates_dir, fn)
    data = {
        "name":        name,
        "code":        code,
        "description": description,
        "created_at":  datetime.datetime.now().isoformat(),
    }
    with open(fp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    return {"status": "created", "name": name, "file": fn}


def templates_delete(name: str, templates_dir: str = TEMPLATES_DIR) -> dict:
    """Delete a custom template by name."""
    if name in DEFAULT_TEMPLATES:
        return {"status": "error", "message": "Cannot delete a default template."}
    if os.path.isdir(templates_dir):
        for fn in os.listdir(templates_dir):
            if fn.endswith(".json"):
                fp = os.path.join(templates_dir, fn)
                try:
                    with open(fp, "r", encoding="utf-8") as fh:
                        t = json.load(fh)
                    if t.get("name") == name:
                        os.remove(fp)
                        return {"status": "deleted", "name": name, "file": fn}
                except Exception:
                    pass
    return {"status": "error", "message": f"Template '{name}' not found."}


def templates_placeholders(name: str, templates_dir: str = TEMPLATES_DIR) -> dict:
    """List {placeholder} names found in a template."""
    t = templates_show(name, templates_dir)
    if t is None:
        return {"status": "error", "message": f"Template '{name}' not found."}
    placeholders = list(dict.fromkeys(re.findall(r'\{(\w+)\}', t["code"])))
    return {"name": name, "placeholders": placeholders}


# ---------------------------------------------------------------------------
# Generate service
# ---------------------------------------------------------------------------

def _eliminate_dead_code(code: str) -> str:
    """Remove demonstrably unreachable variable assignments from alpha code."""
    _assign_pat = re.compile(r'^\s*([A-Za-z_]\w*)\s*=\s*(.+)$')
    _ident_pat  = re.compile(r'\b([A-Za-z_]\w*)\b')

    lines      = code.split('\n')
    stripped   = [(i, ln.rstrip()) for i, ln in enumerate(lines)]
    non_empty  = [(i, ln) for i, ln in stripped if ln.strip()]

    if not non_empty:
        return code

    assignments: dict = {}
    for orig_idx, ln in non_empty:
        m = _assign_pat.match(ln)
        if m:
            assignments[m.group(1)] = (orig_idx, ln)

    if not assignments:
        return code

    def refs(text: str) -> set:
        return set(_ident_pat.findall(text))

    used_vars: set = set()
    has_output_expr = False
    for _, ln in non_empty:
        if not _assign_pat.match(ln):
            used_vars |= refs(ln)
            has_output_expr = True

    if not has_output_expr and non_empty:
        last_m = _assign_pat.match(non_empty[-1][1])
        if last_m:
            used_vars.add(last_m.group(1))

    changed = True
    while changed:
        changed = False
        for var, (_, ln) in assignments.items():
            if var in used_vars:
                m = _assign_pat.match(ln)
                if m:
                    new = refs(m.group(2)) - {var}
                    before = len(used_vars)
                    used_vars |= new
                    if len(used_vars) > before:
                        changed = True

    dead_indices = {
        orig_idx for var, (orig_idx, _) in assignments.items() if var not in used_vars
    }
    if not dead_indices:
        return code
    return '\n'.join(ln for i, ln in enumerate(lines) if i not in dead_indices)


def generate_preview(template: str, pools: Dict[str, List[str]],
                     limit: int = 20, eliminate_dead: bool = True) -> List[dict]:
    """
    Generate strategy previews from a template + placeholder value pools.

    Returns list of dicts with keys: code, candidate (placeholder map), index.
    """
    placeholders = list(dict.fromkeys(re.findall(r'\{(\w+)\}', template)))
    active       = [p for p in placeholders if p in pools]
    if not active:
        # Template has no active placeholders — render as-is
        code = _eliminate_dead_code(template) if eliminate_dead else template
        return [{"index": 0, "code": code, "candidate": {}}]

    all_combos = list(itertools.product(*[pools[p] for p in active]))
    if len(all_combos) > limit:
        import random
        all_combos = random.sample(all_combos, limit)

    results = []
    for i, combo in enumerate(all_combos):
        candidate = dict(zip(active, combo))
        code      = template
        for k, v in candidate.items():
            code = code.replace(f'{{{k}}}', str(v))
        if eliminate_dead:
            code = _eliminate_dead_code(code)
        results.append({"index": i, "code": code, "candidate": candidate})
    return results


def generate_file(strategies: List[dict], output_path: str,
                  sim_params: Optional[dict] = None) -> dict:
    """
    Write a list of strategy dicts to a Python file in parameters.py format.

    Each strategy dict should have at least a ``code`` key.  Additional keys
    become simulation parameters (decay, delay, etc.).
    """
    defaults = {
        "decay": 4, "delay": 1, "neutralization": "SUBINDUSTRY",
        "region": "USA", "truncation": 0.08, "universe": "TOP3000",
    }
    if sim_params:
        defaults.update(sim_params)

    lines = ["# Auto-generated by brain_cli generate file\n",
             "DATA = [\n"]
    for s in strategies:
        entry = {k: v for k, v in defaults.items()}
        entry["code"] = s.get("code", "")
        for k in PARAM_COLUMNS:
            if k in s and k != "code":
                entry[k] = s[k]
        lines.append(f"    {json.dumps(entry, ensure_ascii=False)},\n")
    lines.append("]\n")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.writelines(lines)
    return {"status": "ok", "strategies": len(strategies), "output": output_path}


# ---------------------------------------------------------------------------
# Backtest service
# ---------------------------------------------------------------------------

def backtest_list(data_dir: str = DATA_DIR) -> List[dict]:
    """List backtest CSV files in *data_dir*."""
    out = []
    if not os.path.isdir(data_dir):
        return out
    for fn in sorted(os.listdir(data_dir)):
        if fn.endswith(".csv") and fn not in ("_header1.csv", "_header2.csv"):
            fp = os.path.join(data_dir, fn)
            sz = os.path.getsize(fp)
            try:
                rows = sum(1 for _ in open(fp, encoding="utf-8-sig")) - 1
            except Exception:
                rows = -1
            out.append({"file": fn, "path": fp, "rows": rows, "size_bytes": sz})
    return out


def backtest_load(filename: str, data_dir: str = DATA_DIR) -> Optional[pd.DataFrame]:
    """Load a backtest CSV and return a DataFrame."""
    fp = filename if os.path.isabs(filename) else os.path.join(data_dir, filename)
    if not os.path.exists(fp):
        return None
    try:
        df = pd.read_csv(fp, encoding="utf-8-sig")
        return df
    except Exception:
        return None


def backtest_show(filename: str, limit: int = 50,
                  data_dir: str = DATA_DIR) -> Optional[dict]:
    """Return summary + first *limit* rows of a backtest file."""
    df = backtest_load(filename, data_dir)
    if df is None:
        return None
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    summary = {}
    for col in numeric_cols:
        summary[col] = {
            "mean":  round(float(df[col].mean()), 4),
            "min":   round(float(df[col].min()),  4),
            "max":   round(float(df[col].max()),  4),
        }
    if "passed" in df.columns:
        summary["pass_rate"] = round(df["passed"].apply(_row_passed_bool).mean(), 4)

    return {
        "file":    filename,
        "rows":    len(df),
        "columns": df.columns.tolist(),
        "summary": summary,
        "head":    df.head(limit).to_dict(orient="records"),
    }


def backtest_filter(filename: str,
                    sharpe_min:   Optional[float] = None,
                    fitness_min:  Optional[float] = None,
                    passed_only:  bool = False,
                    universe:     Optional[str] = None,
                    neutralization: Optional[str] = None,
                    data_dir:     str = DATA_DIR) -> Optional[pd.DataFrame]:
    """Filter a backtest DataFrame by common criteria."""
    df = backtest_load(filename, data_dir)
    if df is None:
        return None
    if sharpe_min is not None and "sharpe" in df.columns:
        df = df[pd.to_numeric(df["sharpe"], errors="coerce") >= sharpe_min]
    if fitness_min is not None and "fitness" in df.columns:
        df = df[pd.to_numeric(df["fitness"], errors="coerce") >= fitness_min]
    if passed_only and "passed" in df.columns:
        df = df[df["passed"].apply(_row_passed_bool)]
    if universe and "universe" in df.columns:
        df = df[df["universe"].astype(str).str.upper() == universe.upper()]
    if neutralization and "neutralization" in df.columns:
        df = df[df["neutralization"].astype(str).str.upper() == neutralization.upper()]
    return df.reset_index(drop=True)


def _compute_composite_score(row: dict,
                              sharpe_weight:    float = 0.40,
                              fitness_weight:   float = 0.30,
                              subsharpe_weight: float = 0.20,
                              turnover_weight:  float = 0.10) -> float:
    """Compute scalar composite score for one backtest row (mirrors evolution.py)."""
    def _f(key: str, default: float = 0.0) -> float:
        try:
            return float(row.get(key, default) or default)
        except (ValueError, TypeError):
            return float(default)

    sharpe    = _f("sharpe")
    fitness   = _f("fitness")
    subsharpe = _f("subsharpe")
    turnover  = _f("turnover", 50.0)
    passed    = _row_passed_bool(row.get("passed", ""))

    sharpe_score    = max(0.0, sharpe)    / 2.0
    fitness_score   = max(0.0, fitness)
    subsharpe_score = max(0.0, subsharpe) / 1.5
    turnover_score  = max(0.0, 1.0 - abs(turnover - 10.0) / 30.0)

    score = (sharpe_weight    * sharpe_score
           + fitness_weight   * fitness_score
           + subsharpe_weight * subsharpe_score
           + turnover_weight  * turnover_score)
    if passed:
        score *= 1.10
    return round(score, 6)


def backtest_score(filename: str, top_n: int = 0,
                   data_dir: str = DATA_DIR) -> Optional[pd.DataFrame]:
    """Add composite_score column to a backtest DataFrame, sorted descending."""
    df = backtest_load(filename, data_dir)
    if df is None:
        return None
    df["composite_score"] = df.apply(lambda r: _compute_composite_score(r.to_dict()), axis=1)
    df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)
    if top_n > 0:
        df = df.head(top_n)
    return df


def backtest_diversity_filter(filename: str, top_n: int = 20,
                               min_hamming: float = 0.5,
                               data_dir: str = DATA_DIR) -> Optional[pd.DataFrame]:
    """
    Return up to *top_n* rows that are diverse in code content.

    Uses character-level jaccard similarity on code tokens to filter near-duplicates.
    Rows are first sorted by composite_score so the best are kept.
    """
    df = backtest_score(filename, data_dir=data_dir)
    if df is None or "code" not in df.columns:
        return df

    def _tokenset(code: str) -> set:
        return set(re.findall(r'\b\w+\b', str(code).lower()))

    def _jaccard(s1: set, s2: set) -> float:
        if not s1 and not s2:
            return 1.0
        return len(s1 & s2) / len(s1 | s2)

    selected = []
    tok_sets: List[set] = []
    for _, row in df.iterrows():
        ts = _tokenset(row.get("code", ""))
        if not selected:
            selected.append(row)
            tok_sets.append(ts)
            continue
        # Accept row if it is sufficiently different from all already-selected rows
        max_sim = max(_jaccard(ts, s) for s in tok_sets)
        if max_sim < (1.0 - min_hamming):
            selected.append(row)
            tok_sets.append(ts)
        if len(selected) >= top_n:
            break

    return pd.DataFrame(selected).reset_index(drop=True)


def backtest_export(filename: str, output_path: str,
                    format: str = "csv",
                    data_dir: str = DATA_DIR) -> dict:
    """Export backtest data to a file."""
    df = backtest_load(filename, data_dir)
    if df is None:
        return {"status": "error", "message": f"File '{filename}' not found."}
    if format == "json":
        df.to_json(output_path, orient="records", force_ascii=False, indent=2)
    else:
        df.to_csv(output_path, index=False)
    return {"status": "ok", "rows": len(df), "output": output_path}


# ---------------------------------------------------------------------------
# CLI WQ Session (no Qt dependencies)
# ---------------------------------------------------------------------------

class _StopFlag:
    def __init__(self, job_id: Optional[str] = None):
        self._job_id = job_id
        self.stop_requested = False

    def check(self):
        if self._job_id and JobStore.is_stop_requested(self._job_id):
            self.stop_requested = True
        return self.stop_requested


class CLISimulationSession(requests.Session):
    """
    Headless version of WQSession from simulation.py.
    Writes results to CSV and updates job state. No Qt signals.
    """

    def __init__(self, credentials_path: str = CREDS_PATH,
                 existing_session: Optional[requests.Session] = None,
                 job_id: Optional[str] = None,
                 output_csv: Optional[str] = None,
                 progress_cb=None):
        super().__init__()
        self._job_id     = job_id
        self._stop_flag  = _StopFlag(job_id)
        self._progress_cb = progress_cb
        self._csv_lock   = Lock()
        self._quota_lock = Lock()
        self._submit_lock = Lock()
        self._simulation_quota: Optional[dict] = None
        self._csv_file   = output_csv or os.path.join(
            DATA_DIR, f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        self._log_file   = self._csv_file.replace(".csv", ".log")
        self.login_expired = False
        self._load_latest_simulation_quota()

        if existing_session is not None:
            self.__dict__.update({k: v for k, v in existing_session.__dict__.items()
                                   if not k.startswith("_") or k in ("_cookies",)})
            self.cookies = requests.cookies.cookiejar_from_dict(
                requests.utils.dict_from_cookiejar(existing_session.cookies))
            self.headers = existing_session.headers.copy()
            self.auth    = getattr(existing_session, "auth", None)
        else:
            self._login(credentials_path)

    def _login(self, credentials_path: str):
        try:
            session, kind, detail = get_session_for_request(credentials_path)
            if kind is None and session is not None:
                self.__dict__.update({k: v for k, v in session.__dict__.items()
                                       if not k.startswith("_") or k in ("_cookies",)})
                self.cookies = requests.cookies.cookiejar_from_dict(
                    requests.utils.dict_from_cookiejar(session.cookies))
                self.headers = session.headers.copy()
                self.auth = getattr(session, "auth", None)
                return
            if kind == "persona":
                _notify_login_issue(
                    "CLI simulation requires Persona verification.",
                    detail,
                    cooldown_key="cli-sim-persona-required",
                )
                self._emit(f"Persona verification required: {detail}")
            else:
                _notify_login_issue(
                    "CLI simulation login failed.",
                    detail or "Login failed.",
                    cooldown_key="cli-sim-login-failed",
                )
                self._emit(f"Login failed: {detail}")
            self.login_expired = True
        except Exception as exc:
            _notify_login_issue(
                "CLI simulation login errored.",
                str(exc),
                cooldown_key="cli-sim-login-error",
            )
            self._emit(f"Login error: {exc}")
            self.login_expired = True

    def _emit(self, msg: str):
        if self._progress_cb:
            self._progress_cb(msg)
        else:
            print(f"[simulate] {msg}", file=sys.stderr)

    def _load_latest_simulation_quota(self):
        try:
            jobs = JobStore.list_jobs("simulate")
        except Exception:
            return
        jobs.sort(key=lambda job: job.get("updated_at", ""), reverse=True)
        for job in jobs:
            quota = job.get("simulation_quota")
            if isinstance(quota, dict):
                with self._quota_lock:
                    self._simulation_quota = quota
                return

    def _record_simulation_quota(self, response: requests.Response) -> Optional[dict]:
        quota = _simulation_rate_limit_from_headers(response.headers)
        if quota is None:
            return None
        with self._quota_lock:
            self._simulation_quota = quota
        if self._job_id:
            JobStore.update(self._job_id, simulation_quota=quota)
        return quota

    def _quota_wait_seconds(self) -> int:
        with self._quota_lock:
            quota = dict(self._simulation_quota or {})
        if quota.get("remaining") != 0:
            return 0
        reset_at_seconds = _seconds_until_iso(quota.get("reset_at"))
        if reset_at_seconds is not None:
            return reset_at_seconds
        reset_seconds = quota.get("reset_seconds")
        if isinstance(reset_seconds, int):
            return max(reset_seconds, 0)
        return 0

    def _wait_for_simulation_quota(self) -> bool:
        wait_seconds = self._quota_wait_seconds()
        if wait_seconds <= 0:
            return True

        message = f"Simulation daily limit reached; waiting {wait_seconds}s for reset."
        self._emit(message)
        if self._job_id:
            JobStore.update(self._job_id, progress_message=message)

        deadline = time.monotonic() + wait_seconds
        while True:
            if self._stop_flag.check():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            time.sleep(min(30, remaining))

    def simulate(self, params: List[dict]) -> List[dict]:
        """Run all simulations and write to CSV.  Returns list of result rows."""
        if self.login_expired:
            return []

        os.makedirs(DATA_DIR, exist_ok=True)
        completed: List[dict] = []
        total = len(params)

        if self._job_id:
            JobStore.update(
                self._job_id,
                total_count=total,
                processed_count=0,
                completed_count=0,
                failed_count=0,
                progress_message=f"Running 0/{total}",
                completed_rows=[],
                failed_items=[],
            )

        with open(self._csv_file, "w", newline="", encoding="utf-8") as csv_fh:
            writer = csv.writer(csv_fh)
            writer.writerow(SIM_CSV_HEADER)

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(self._process_one, sim): sim
                    for sim in params
                }
                for fut in as_completed(futures):
                    if self._stop_flag.check():
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    try:
                        result = fut.result()
                        if result and "row" in result:
                            with self._csv_lock:
                                writer.writerow(result["row"])
                                csv_fh.flush()
                            if result.get("status") == "failed":
                                row = result["row"]
                                try:
                                    get_registry().record_simulation(
                                        str(row[14]) if len(row) > 14 else str(result.get("alpha", "")),
                                        job_id=self._job_id,
                                        status="failed",
                                        params=result.get("simulation") or futures.get(fut) or {},
                                        result_link=str(row[13]) if len(row) > 13 else None,
                                        error=result.get("error", "Simulation failed."),
                                    )
                                except Exception as exc:
                                    self._emit(f"Alpha registry update failed: {exc}")
                                if self._job_id:
                                    def _mark_failed_row(job):
                                        completed_rows = list(job.get("completed_rows", []))
                                        failed_items = list(job.get("failed_items", []))
                                        failed_items.append({
                                            "uuid": result.get("uuid"),
                                            "error": result.get("error", "Simulation failed."),
                                            "alpha": result.get("alpha"),
                                            "row": row,
                                        })
                                        processed = len(completed_rows) + len(failed_items)
                                        job["failed_items"] = failed_items
                                        job["completed_count"] = len(completed_rows)
                                        job["failed_count"] = len(failed_items)
                                        job["processed_count"] = processed
                                        job["progress_message"] = f"Running {processed}/{total}"
                                    JobStore.mutate(self._job_id, _mark_failed_row)
                                self._emit(f"Error: {result.get('error', 'Simulation failed.')}")
                            else:
                                completed.append(result)
                                try:
                                    get_registry().record_simulation_row(
                                        result["row"],
                                        job_id=self._job_id,
                                        params=result.get("simulation") or {},
                                    )
                                except Exception as exc:
                                    self._emit(f"Alpha registry update failed: {exc}")
                                if self._job_id:
                                    def _mark_completed(job):
                                        completed_rows = list(job.get("completed_rows", []))
                                        failed_items = list(job.get("failed_items", []))
                                        completed_rows.append({
                                            "uuid": result.get("uuid"),
                                            "row": result["row"],
                                        })
                                        processed = len(completed_rows) + len(failed_items)
                                        job["completed_rows"] = completed_rows
                                        job["completed_count"] = len(completed_rows)
                                        job["failed_count"] = len(failed_items)
                                        job["processed_count"] = processed
                                        job["progress_message"] = f"Running {processed}/{total}"
                                    JobStore.mutate(self._job_id, _mark_completed)
                                self._emit(f"Completed {len(completed)}/{len(params)}: "
                                           f"{str(result['row'][14])[:40]}")
                        elif result and "error" in result:
                            try:
                                get_registry().record_simulation(
                                    str(result.get("alpha", "")),
                                    job_id=self._job_id,
                                    status="failed",
                                    params=futures.get(fut) or {},
                                    error=result["error"],
                                )
                            except Exception as exc:
                                self._emit(f"Alpha registry update failed: {exc}")
                            if self._job_id:
                                def _mark_failed(job):
                                    completed_rows = list(job.get("completed_rows", []))
                                    failed_items = list(job.get("failed_items", []))
                                    failed_items.append({
                                        "uuid": result.get("uuid"),
                                        "error": result["error"],
                                        "alpha": result.get("alpha"),
                                    })
                                    processed = len(completed_rows) + len(failed_items)
                                    job["failed_items"] = failed_items
                                    job["completed_count"] = len(completed_rows)
                                    job["failed_count"] = len(failed_items)
                                    job["processed_count"] = processed
                                    job["progress_message"] = f"Running {processed}/{total}"
                                JobStore.mutate(self._job_id, _mark_failed)
                            self._emit(f"Error: {result['error']}")
                    except Exception as exc:
                        self._emit(f"Future error: {exc}")

        if JobStore.get(self._job_id) is not None:
            JobStore.update(self._job_id, result_file=self._csv_file)
        return completed

    def _process_one(self, simulation: dict) -> Optional[dict]:
        """Submit one alpha simulation, poll for completion, fetch details."""
        if self.login_expired or self._stop_flag.check():
            return None

        alpha        = simulation.get("code", "").strip()
        delay        = simulation.get("delay", 1)
        universe     = simulation.get("universe", "TOP3000")
        truncation   = simulation.get("truncation", 0.1)
        region       = simulation.get("region", "USA")
        decay        = simulation.get("decay", 6)
        neutralization = simulation.get("neutralization", "SUBINDUSTRY").upper()
        pasteurization = simulation.get("pasteurization", "ON")
        nan_handling   = simulation.get("nanHandling", "OFF")
        row_uuid       = simulation.get("uuid", _uuid_mod.uuid4().hex)

        max_retries = 3
        nxt         = None

        for attempt in range(max_retries):
            if self._stop_flag.check():
                return {"uuid": row_uuid, "error": "Stopped by user", "alpha": alpha}
            try:
                with self._submit_lock:
                    if not self._wait_for_simulation_quota():
                        return {"uuid": row_uuid, "error": "Stopped by user", "alpha": alpha}
                    r = self.post(f"{BRAIN_API_BASE}/simulations", json={
                        "regular": alpha,
                        "type":    "REGULAR",
                        "settings": {
                            "nanHandling":    nan_handling,
                            "instrumentType": "EQUITY",
                            "delay":          delay,
                            "universe":       universe,
                            "truncation":     truncation,
                            "unitHandling":   "VERIFY",
                            "pasteurization": pasteurization,
                            "region":         region,
                            "language":       "FASTEXPR",
                            "decay":          decay,
                            "neutralization": neutralization,
                            "visualization":  False,
                        },
                    })
                    self._record_simulation_quota(r)
                if r.status_code == 401:
                    clear_login_state()
                    persona_url = extract_persona_url(r)
                    if persona_url:
                        _notify_login_issue(
                            "Saved session expired while submitting a simulation.",
                            persona_url,
                            cooldown_key="cli-sim-submit-persona",
                        )
                        return {"uuid": row_uuid, "error": f"Persona verification required: {persona_url}", "alpha": alpha}
                    _notify_login_issue(
                        "Saved session expired while submitting a simulation.",
                        "Unauthorized while submitting simulation.",
                        cooldown_key="cli-sim-submit-unauthorized",
                    )
                    return {"uuid": row_uuid, "error": "Unauthorized while submitting simulation.", "alpha": alpha}
                r.raise_for_status()
                location = r.headers.get("Location")
                if not location:
                    return {"uuid": row_uuid, "error": "Simulation response missing Location header.", "alpha": alpha}
                nxt = urljoin(r.url, location)
                break
            except requests.exceptions.HTTPError as exc:
                if exc.response.status_code == 429 and attempt < max_retries - 1:
                    wait_seconds = _retry_after_seconds(exc.response.headers, self._quota_wait_seconds() or 15)
                    self._emit(f"429 rate-limit, retrying in {wait_seconds}s ({attempt+1}/{max_retries})…")
                    deadline = time.monotonic() + wait_seconds
                    while True:
                        if self._stop_flag.check():
                            return {"uuid": row_uuid, "error": "Stopped by user", "alpha": alpha}
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        time.sleep(min(30, remaining))
                    continue
                return {"uuid": row_uuid, "error": str(exc), "alpha": alpha}
            except Exception as exc:
                return {"uuid": row_uuid, "error": str(exc), "alpha": alpha}

        if nxt is None:
            return {"uuid": row_uuid, "error": "Failed to submit simulation.", "alpha": alpha}

        # Poll for completion
        alpha_link = None
        while True:
            if self._stop_flag.check():
                return {"uuid": row_uuid, "error": "Stopped by user", "alpha": alpha}
            try:
                r    = self.get(nxt, timeout=30)
                if r.status_code == 401:
                    clear_login_state()
                    persona_url = extract_persona_url(r)
                    if persona_url:
                        _notify_login_issue(
                            "Saved session expired while polling simulation status.",
                            persona_url,
                            cooldown_key="cli-sim-poll-persona",
                        )
                        return {"uuid": row_uuid, "error": f"Persona verification required: {persona_url}", "alpha": alpha}
                    _notify_login_issue(
                        "Saved session expired while polling simulation status.",
                        "Unauthorized while polling simulation.",
                        cooldown_key="cli-sim-poll-unauthorized",
                    )
                    return {"uuid": row_uuid, "error": "Unauthorized while polling simulation.", "alpha": alpha}
                r.raise_for_status()
                rj   = r.json()
                status = str(rj.get("status", "")).upper()
                if "alpha" in rj:
                    alpha_link = rj["alpha"]
                    break
                if status in SIMULATION_ERROR_STATUSES:
                    message = rj.get("message") or f"Simulation ended with status {status}."
                    return {"uuid": row_uuid, "error": message, "alpha": alpha}
                if status in SIMULATION_DONE_STATUSES:
                    return {"uuid": row_uuid, "error": f"Simulation ended with status {status} but no alpha id was returned.", "alpha": alpha}
                progress = rj.get("progress", 0)
                self._emit(f"  Progress {int(100 * progress)}% — {alpha[:30]}")
                wait_seconds = _retry_after_seconds(r.headers)
            except requests.exceptions.HTTPError as exc:
                if exc.response.status_code == 429:
                    time.sleep(_retry_after_seconds(exc.response.headers, 15))
                    continue
                return {"uuid": row_uuid, "error": str(exc), "alpha": alpha}
            except Exception as exc:
                return {"uuid": row_uuid, "error": str(exc), "alpha": alpha}

            for _ in range(max(int(wait_seconds / 0.2), 1)):
                if self._stop_flag.check():
                    return {"uuid": row_uuid, "error": "Stopped by user", "alpha": alpha}
                time.sleep(0.2)

        # Fetch alpha details
        try:
            r  = self.get(f"{BRAIN_API_BASE}/alphas/{alpha_link}", timeout=30)
            if r.status_code == 401:
                clear_login_state()
                persona_url = extract_persona_url(r)
                if persona_url:
                    _notify_login_issue(
                        "Saved session expired while fetching alpha details.",
                        persona_url,
                        cooldown_key="cli-sim-alpha-persona",
                    )
                    return {"uuid": row_uuid, "error": f"Persona verification required: {persona_url}", "alpha": alpha}
                _notify_login_issue(
                    "Saved session expired while fetching alpha details.",
                    "Unauthorized while fetching alpha details.",
                    cooldown_key="cli-sim-alpha-unauthorized",
                )
                return {"uuid": row_uuid, "error": "Unauthorized while fetching alpha details.", "alpha": alpha}
            r.raise_for_status()
            rj = r.json()
        except Exception as exc:
            row = [0, delay, region, neutralization, decay, truncation,
                   0, 0, 0, "FAIL", 0, -1, universe,
                   f"https://platform.worldquantbrain.com/alpha/{alpha_link}", alpha]
            return {
                "uuid": row_uuid,
                "row": row,
                "simulation": simulation,
                "alpha": alpha,
                "status": "failed",
                "error": f"Failed to fetch alpha details: {exc}",
            }

        passed     = 0
        weight_chk = "N/A"
        subsharpe  = -1
        for chk in rj.get("is", {}).get("checks", []):
            passed += chk.get("result") == "PASS"
            if chk.get("name") == "CONCENTRATED_WEIGHT":
                weight_chk = chk.get("result", "N/A")
            if chk.get("name") == "LOW_SUB_UNIVERSE_SHARPE":
                subsharpe = chk.get("value", -1)

        row = [
            passed, delay, region, neutralization, decay, truncation,
            rj.get("is", {}).get("sharpe", 0),
            rj.get("is", {}).get("fitness", 0),
            round(100 * rj.get("is", {}).get("turnover", 0), 2),
            weight_chk, subsharpe, -1,
            universe,
            f"https://platform.worldquantbrain.com/alpha/{alpha_link}",
            alpha,
        ]
        return {"uuid": row_uuid, "row": row, "simulation": simulation, "status": "done"}


# ---------------------------------------------------------------------------
# Simulate service
# ---------------------------------------------------------------------------

def simulate_enqueue(params: List[dict], credentials_path: str = CREDS_PATH) -> str:
    """Create a new simulation job and return its job_id."""
    job_id = JobStore.create("simulate", {
        "params":           params,
        "credentials_path": credentials_path,
    })
    registry = get_registry()
    for item in params:
        code = str(item.get("code", "")).strip()
        if code:
            registry.record_queued(code, job_id=job_id, params=item)
    return job_id


def simulate_run(job_id: str, progress_cb=None) -> dict:
    """
    Execute a queued simulation job synchronously.
    Updates job state file throughout.
    Returns final job dict.
    """
    job = JobStore.get(job_id)
    if job is None:
        return {"status": "error", "message": f"Job {job_id} not found."}
    if job["status"] not in ("pending",):
        return {"status": "error", "message": f"Job {job_id} is already {job['status']}."}

    JobStore.update(
        job_id,
        status="running",
        pid=os.getpid(),
        total_count=len(job["params"]["params"]),
        processed_count=0,
        completed_count=0,
        failed_count=0,
        progress_message="Queued for worker execution.",
        completed_rows=[],
        failed_items=[],
    )
    JobStore.clear_stop(job_id)

    params           = job["params"]["params"]
    credentials_path = job["params"].get("credentials_path", CREDS_PATH)
    output_csv       = os.path.join(DATA_DIR,
                                    f"job_{job_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

    try:
        session = CLISimulationSession(
            credentials_path=credentials_path,
            job_id=job_id,
            output_csv=output_csv,
            progress_cb=progress_cb,
        )
        if session.login_expired:
            JobStore.update(job_id, status="failed", error="Login failed.", progress_message="Login failed.")
            return JobStore.get(job_id)

        results = session.simulate(params)
        stopped = JobStore.is_stop_requested(job_id)
        JobStore.update(
            job_id,
            status="stopped" if stopped else "done",
            result_file=output_csv,
            progress_message="Stopped." if stopped else "Completed.",
        )
    except Exception as exc:
        JobStore.update(job_id, status="failed", error=str(exc), progress_message=str(exc))

    return JobStore.get(job_id)


def simulate_status(job_id: str) -> Optional[dict]:
    return JobStore.get(job_id)


def simulate_stop(job_id: str) -> dict:
    job = JobStore.get(job_id)
    if job is None:
        return {"status": "error", "message": f"Job {job_id} not found."}
    JobStore.request_stop(job_id)
    return {"status": "ok", "message": f"Stop requested for job {job_id}."}


def simulate_results(job_id: str, limit: int = 100) -> Optional[dict]:
    """Return results from a completed simulation job."""
    job = JobStore.get(job_id)
    if job is None:
        return None
    result_file = job.get("result_file")
    if not result_file or not os.path.exists(result_file):
        return {"job": job, "rows": [], "message": "No result file found."}
    df = pd.read_csv(result_file)
    return {"job": job, "rows": df.head(limit).to_dict(orient="records"), "total": len(df)}


def simulate_list() -> List[dict]:
    """List all simulation jobs."""
    return JobStore.list_jobs("simulate")


# ---------------------------------------------------------------------------
# Alpha registry service
# ---------------------------------------------------------------------------

def alpha_list(status: Optional[str] = None,
               source: Optional[str] = None,
               min_sharpe: Optional[float] = None,
               min_fitness: Optional[float] = None,
               limit: int = 50) -> List[dict]:
    """List alpha records from the SQLite registry."""
    return get_registry().list_alphas(
        status=status,
        source=source,
        min_sharpe=min_sharpe,
        min_fitness=min_fitness,
        limit=limit,
    )


def alpha_show(identifier: str) -> Optional[dict]:
    """Return a single alpha by hash or WQ alpha ID."""
    return get_registry().get_alpha(identifier)


def alpha_history(identifier: str) -> Optional[dict]:
    """Return an alpha plus simulation and event history."""
    return get_registry().history(identifier)


def alpha_promote(identifier: str, reason: Optional[str] = None) -> Optional[dict]:
    """Mark an alpha as promoted and append an event."""
    return get_registry().promote(identifier, reason=reason)


def alpha_reject(identifier: str, reason: str) -> Optional[dict]:
    """Mark an alpha as rejected and append an event."""
    return get_registry().reject(identifier, reason=reason)


# ---------------------------------------------------------------------------
# Evolution service
# ---------------------------------------------------------------------------

def _parse_placeholders(template: str) -> List[str]:
    return list(dict.fromkeys(re.findall(r'\{(\w+)\}', template)))


def _render_candidate(template: str, candidate: Dict[str, str]) -> str:
    result = template
    for k, v in candidate.items():
        result = result.replace(f'{{{k}}}', str(v))
    return result


def _candidate_key(candidate: Dict[str, str]) -> tuple:
    return tuple(sorted(candidate.items()))


def _hamming_diversity(candidate: Dict[str, str], population: List[Dict[str, str]]) -> float:
    if len(population) <= 1:
        return 1.0
    keys = list(candidate.keys())
    if not keys:
        return 0.0
    total = 0.0
    for other in population:
        if other is candidate:
            continue
        diffs = sum(1 for k in keys if candidate.get(k) != other.get(k))
        total += diffs / len(keys)
    return total / (len(population) - 1)


def _uniqueness(candidate: Dict[str, str], population: List[Dict[str, str]]) -> float:
    key  = _candidate_key(candidate)
    dups = sum(1 for c in population if _candidate_key(c) == key) - 1
    return max(0.0, 1.0 - dups * 0.5)


def _normalize_code(code: str) -> str:
    return ' '.join(code.split())


def evolution_run(template: str, pools: Dict[str, List[str]],
                  pop_size: int = 40, generations: int = 10,
                  mutation_rate: float = 0.4,
                  diversity_weight: float = 0.7,
                  top_k: int = 20,
                  seed_population: Optional[List[Dict[str, str]]] = None,
                  known_real_fitness: Optional[Dict[tuple, float]] = None,
                  progress_cb=None) -> List[dict]:
    """
    Run the evolution engine and return top-k unique candidates as dicts.

    Reuses the pure-Python EvolutionEngine from evolution.py without importing
    the GUI module.  Returns list of {score, code, candidate} dicts.
    """
    placeholders = _parse_placeholders(template)
    active       = [p for p in placeholders if p in pools]

    if not active:
        code = _eliminate_dead_code(template)
        return [{"score": 1.0, "code": code, "candidate": {}}]

    pop_size    = max(pop_size, 4)
    parent_num  = max(2, int(pop_size * 0.3))
    known_rf    = dict(known_real_fitness or {})

    def _random_cand():
        return {p: __import__("random").choice(pools[p]) for p in active}

    import random

    def _initial_pop():
        all_combos = list(itertools.product(*[pools[p] for p in active]))
        if len(all_combos) <= pop_size:
            pop = [dict(zip(active, c)) for c in all_combos]
            while len(pop) < pop_size:
                pop.append(_random_cand())
        else:
            sampled = random.sample(all_combos, pop_size)
            pop = [dict(zip(active, c)) for c in sampled]
        return pop

    def _fitness(cand, pop):
        div   = _hamming_diversity(cand, pop)
        uniq  = _uniqueness(cand, pop)
        struct = diversity_weight * div + (1.0 - diversity_weight) * uniq
        real  = known_rf.get(_candidate_key(cand))
        if real is None:
            return struct
        return 0.65 * real + 0.35 * struct

    def _score_pop(pop):
        scored = [(_fitness(c, pop), c) for c in pop]
        scored.sort(key=lambda x: -x[0])
        return scored

    def _crossover(p1, p2):
        n = len(active)
        if n <= 1:
            return p1.copy()
        split = random.randint(1, n - 1)
        return {ph: (p1[ph] if i < split else p2[ph]) for i, ph in enumerate(active)}

    def _mutate(cand):
        mutant = cand.copy()
        for ph in active:
            if random.random() < mutation_rate:
                mutant[ph] = random.choice(pools[ph])
        return mutant

    # Build population
    if seed_population:
        population = [
            {p: c[p] for p in active}
            for c in seed_population if all(p in c for p in active)
        ]
        while len(population) < pop_size:
            population.append(_random_cand())
        population = population[:pop_size]
    else:
        population = _initial_pop()

    for gen in range(generations):
        scored  = _score_pop(population)
        parents = [c for _, c in scored[:parent_num]]
        kids    = []
        attempts = 0
        while len(kids) < pop_size - len(parents) and attempts < pop_size * 20:
            if len(parents) < 2:
                child = _mutate(parents[0])
            else:
                p1, p2 = random.sample(parents, 2)
                child  = _mutate(_crossover(p1, p2))
            kids.append(child)
            attempts += 1
        population = (parents + kids)[:pop_size]
        if progress_cb:
            progress_cb(gen + 1, generations)

    scored = _score_pop(population)
    seen:  set = set()
    results = []
    for score, cand in scored:
        key = _candidate_key(cand)
        if key not in seen:
            seen.add(key)
            code = _render_candidate(template, cand)
            code = _eliminate_dead_code(code)
            results.append({"score": round(score, 6), "code": code, "candidate": cand})
        if len(results) >= top_k:
            break
    return results


def evolution_auto_run(template: str, pools: Dict[str, List[str]],
                       rounds: int = 3,
                       pop_size: int = 40, generations: int = 10,
                       mutation_rate: float = 0.4,
                       diversity_weight: float = 0.7,
                       top_k: int = 20,
                       credentials_path: str = CREDS_PATH,
                       sim_params: Optional[Dict[str, object]] = None,
                       progress_cb=None) -> dict:
    """
    Run a closed loop:
      evolution -> simulation -> backtest-style fitness feedback -> next round
    """
    sim_defaults = {
        "decay": 4,
        "delay": 1,
        "neutralization": "SUBINDUSTRY",
        "region": "USA",
        "truncation": 0.08,
        "universe": "TOP3000",
    }
    if sim_params:
        sim_defaults.update(sim_params)

    seed_population: Optional[List[Dict[str, str]]] = None
    known_real_fitness: Dict[tuple, float] = {}
    history: List[dict] = []
    final_candidates: List[dict] = []
    final_status = "ok"
    final_error: Optional[str] = None

    for round_idx in range(1, max(1, rounds) + 1):
        if progress_cb:
            progress_cb(f"[auto-run] Evolution round {round_idx}/{rounds}")

        candidates = evolution_run(
            template=template,
            pools=pools,
            pop_size=pop_size,
            generations=generations,
            mutation_rate=mutation_rate,
            diversity_weight=diversity_weight,
            top_k=top_k,
            seed_population=seed_population,
            known_real_fitness=known_real_fitness,
            progress_cb=(lambda gen, total: progress_cb(
                f"[auto-run] round {round_idx}: generation {gen}/{total}"
            )) if progress_cb else None,
        )
        final_candidates = candidates
        if not candidates:
            history.append({
                "round": round_idx,
                "candidate_count": 0,
                "simulation_job_id": None,
                "matched_results": 0,
                "top_results": [],
            })
            break

        round_params: List[dict] = []
        candidate_by_code: Dict[str, Dict[str, str]] = {}
        for item in candidates:
            strategy = dict(sim_defaults)
            strategy["code"] = item["code"]
            round_params.append(strategy)
            candidate_by_code[_normalize_code(item["code"])] = item["candidate"]

        sim_job_id = simulate_enqueue(round_params, credentials_path=credentials_path)
        if progress_cb:
            progress_cb(f"[auto-run] round {round_idx}: simulate job {sim_job_id}")
        sim_job = simulate_run(
            sim_job_id,
            progress_cb=(lambda msg: progress_cb(
                f"[auto-run] round {round_idx}: {msg}"
            )) if progress_cb else None,
        )
        sim_status = (sim_job or {}).get("status")
        if sim_status != "done":
            final_status = sim_status or "failed"
            final_error = (sim_job or {}).get("error") or f"Simulation job {sim_job_id} ended with status {sim_status}."
            history.append({
                "round": round_idx,
                "candidate_count": len(candidates),
                "simulation_job_id": sim_job_id,
                "simulation_status": sim_status,
                "matched_results": 0,
                "top_results": final_candidates[:min(5, len(final_candidates))],
                "error": final_error,
            })
            break

        sim_output = simulate_results(sim_job_id, limit=max(1000, top_k * 5)) or {}
        rows = sim_output.get("rows", [])

        matched: List[Tuple[float, Dict[str, str], dict]] = []
        next_known_real_fitness: Dict[tuple, float] = {}
        row_by_code: Dict[str, dict] = {}
        for row in rows:
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            norm_code = _normalize_code(code)
            candidate = candidate_by_code.get(norm_code)
            if candidate is None:
                continue
            score = _compute_composite_score(row)
            matched.append((score, candidate, row))
            next_known_real_fitness[_candidate_key(candidate)] = score
            row_by_code[norm_code] = row

        matched.sort(key=lambda x: -x[0])
        if not matched:
            final_status = "failed"
            final_error = f"Simulation job {sim_job_id} produced no feedback rows for evolution."
            history.append({
                "round": round_idx,
                "candidate_count": len(candidates),
                "simulation_job_id": sim_job_id,
                "simulation_status": sim_status,
                "matched_results": 0,
                "top_results": final_candidates[:min(5, len(final_candidates))],
                "error": final_error,
            })
            break

        seed_population = [candidate for _, candidate, _ in matched[:max(2, min(top_k, pop_size))]]
        known_real_fitness = next_known_real_fitness

        for item in final_candidates:
            matched_row = row_by_code.get(_normalize_code(item["code"]))
            if matched_row is not None:
                item["real_fitness"] = round(_compute_composite_score(matched_row), 6)
                item["simulation"] = {
                    "passed": matched_row.get("passed"),
                    "sharpe": matched_row.get("sharpe"),
                    "fitness": matched_row.get("fitness"),
                    "turnover": matched_row.get("turnover"),
                    "subsharpe": matched_row.get("subsharpe"),
                    "link": matched_row.get("link"),
                }

        history.append({
            "round": round_idx,
            "candidate_count": len(candidates),
            "simulation_job_id": sim_job_id,
            "matched_results": len(matched),
            "top_results": final_candidates[:min(5, len(final_candidates))],
        })

    return {
        "status": final_status,
        "rounds_completed": len(history),
        "history": history,
        "final_results": final_candidates,
        "error": final_error,
    }


def evolution_run_job(job_id: str, progress_cb=None) -> dict:
    """Execute a queued evolution job synchronously."""
    job = JobStore.get(job_id)
    if job is None:
        return {"status": "error", "message": f"Job {job_id} not found."}
    if job["status"] not in ("pending",):
        return {"status": "error", "message": f"Job {job_id} is already {job['status']}."}

    JobStore.update(job_id, status="running", pid=os.getpid())
    p = job["params"]

    def _pcb(gen, total):
        msg = f"Generation {gen}/{total}"
        if progress_cb:
            progress_cb(msg)
        else:
            print(f"[evolution] {msg}", file=sys.stderr)
        # Check stop
        if JobStore.is_stop_requested(job_id):
            raise RuntimeError("Stop requested")

    try:
        results = evolution_run(
            template          = p["template"],
            pools             = p["pools"],
            pop_size          = p.get("pop_size", 40),
            generations       = p.get("generations", 10),
            mutation_rate     = p.get("mutation_rate", 0.4),
            diversity_weight  = p.get("diversity_weight", 0.7),
            top_k             = p.get("top_k", 20),
            seed_population   = p.get("seed_population"),
            known_real_fitness= p.get("known_real_fitness"),
            progress_cb       = _pcb,
        )
        ts          = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = os.path.join(CLI_STATE_DIR, f"evo_{job_id}_{ts}.json")
        with open(result_file, "w", encoding="utf-8") as fh:
            json.dump(results, fh, ensure_ascii=False, indent=2)
        JobStore.update(job_id, status="done", result_file=result_file)
    except RuntimeError as exc:
        if "Stop requested" in str(exc):
            JobStore.update(job_id, status="stopped")
        else:
            JobStore.update(job_id, status="failed", error=str(exc))
    except Exception as exc:
        JobStore.update(job_id, status="failed", error=str(exc))

    return JobStore.get(job_id)


def evolution_from_backtest(template: str, pools: Dict[str, List[str]],
                             backtest_csv: str, top_seed: int = 10,
                             pop_size: int = 40, generations: int = 10,
                             top_k: int = 20,
                             data_dir: str = DATA_DIR) -> dict:
    """
    Load backtest CSV, match to template+pools candidates, seed evolution from
    best performers, run evolution, and return results.
    """
    df = backtest_load(backtest_csv, data_dir)
    if df is None:
        return {"status": "error", "message": f"Backtest file '{backtest_csv}' not found."}

    if "code" not in df.columns:
        return {"status": "error", "message": "Backtest CSV missing 'code' column."}

    # Build norm_code index of backtest rows
    csv_index: Dict[str, dict] = {}
    for _, row in df.iterrows():
        code = str(row.get("code", "")).strip()
        if code:
            csv_index[_normalize_code(code)] = row.to_dict()

    # Enumerate pool combos, find matches
    placeholders = _parse_placeholders(template)
    active       = [p for p in placeholders if p in pools]
    matched: List[Tuple[float, Dict[str, str], str]] = []
    seen_norm: set = set()

    all_combos = list(itertools.product(*[pools[p] for p in active]))
    if len(all_combos) > 20_000:
        import random
        all_combos = random.sample(all_combos, 20_000)

    for combo in all_combos:
        cand = dict(zip(active, combo))
        code = _render_candidate(template, cand)
        norm = _normalize_code(code)
        if norm in csv_index and norm not in seen_norm:
            seen_norm.add(norm)
            score = _compute_composite_score(csv_index[norm])
            matched.append((score, cand, code))

    matched.sort(key=lambda x: -x[0])
    seeds         = [c for _, c, _ in matched[:top_seed]]
    known_rf_map  = {_candidate_key(c): s for s, c, _ in matched}

    results = evolution_run(
        template         = template,
        pools            = pools,
        pop_size         = pop_size,
        generations      = generations,
        top_k            = top_k,
        seed_population  = seeds,
        known_real_fitness = known_rf_map,
    )
    return {
        "status":   "ok",
        "matched":  len(matched),
        "seeds":    len(seeds),
        "results":  results,
    }


def evolution_enqueue(template: str, pools: Dict[str, List[str]],
                      pop_size: int = 40, generations: int = 10,
                      mutation_rate: float = 0.4,
                      diversity_weight: float = 0.7,
                      top_k: int = 20,
                      seed_population: Optional[List[dict]] = None,
                      known_real_fitness: Optional[dict] = None) -> str:
    """Create a queued evolution job and return job_id."""
    return JobStore.create("evolution", {
        "template":           template,
        "pools":              pools,
        "pop_size":           pop_size,
        "generations":        generations,
        "mutation_rate":      mutation_rate,
        "diversity_weight":   diversity_weight,
        "top_k":              top_k,
        "seed_population":    seed_population,
        "known_real_fitness": known_real_fitness,
    })


def evolution_status(job_id: str) -> Optional[dict]:
    return JobStore.get(job_id)


def evolution_stop(job_id: str) -> dict:
    job = JobStore.get(job_id)
    if job is None:
        return {"status": "error", "message": f"Job {job_id} not found."}
    JobStore.request_stop(job_id)
    return {"status": "ok", "message": f"Stop requested for evolution job {job_id}."}


def evolution_results(job_id: str) -> Optional[dict]:
    """Return results from a completed evolution job."""
    job = JobStore.get(job_id)
    if job is None:
        return None
    result_file = job.get("result_file")
    if not result_file or not os.path.exists(result_file):
        return {"job": job, "results": [], "message": "No result file found."}
    with open(result_file, "r", encoding="utf-8") as fh:
        results = json.load(fh)
    return {"job": job, "results": results}


def evolution_list() -> List[dict]:
    return JobStore.list_jobs("evolution")
