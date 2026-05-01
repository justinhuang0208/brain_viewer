#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""SQLite-backed alpha registry for agent-friendly alpha state."""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
import sqlite3
import uuid
from typing import Any, Dict, List, Optional


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CLI_STATE_DIR = os.path.join(SCRIPT_DIR, ".brain_cli")
DEFAULT_DB_PATH = os.path.join(CLI_STATE_DIR, "alphas.sqlite")


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def normalize_code(code: str) -> str:
    return re.sub(r"\s+", " ", str(code or "").strip())


def alpha_hash_for_code(code: str) -> str:
    normalized = normalize_code(code)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _json_dumps(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Optional[str], default: Any = None) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def alpha_id_from_link(link: Optional[str]) -> Optional[str]:
    if not link:
        return None
    match = re.search(r"/alpha/([^/?#]+)", str(link))
    if match:
        return match.group(1)
    match = re.search(r"\balpha/([^/?#]+)", str(link))
    return match.group(1) if match else None


def metrics_from_row(row: List[Any]) -> Dict[str, Any]:
    def at(index: int, default: Any = None) -> Any:
        return row[index] if len(row) > index else default

    return {
        "passed": at(0, 0),
        "delay": at(1),
        "region": at(2),
        "neutralization": at(3),
        "decay": at(4),
        "truncation": at(5),
        "sharpe": at(6, 0),
        "fitness": at(7, 0),
        "turnover": at(8, 0),
        "weight": at(9),
        "subsharpe": at(10, -1),
        "correlation": at(11, -1),
        "universe": at(12),
    }


class AlphaRegistry:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS alphas (
                    alpha_hash TEXT PRIMARY KEY,
                    alpha_id TEXT UNIQUE,
                    code TEXT NOT NULL,
                    normalized_code TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'unknown',
                    template_id TEXT,
                    status TEXT NOT NULL DEFAULT 'candidate',
                    latest_simulation_id TEXT,
                    latest_metrics_json TEXT,
                    latest_result_link TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    promoted_at TEXT,
                    rejected_at TEXT,
                    reject_reason TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_alphas_alpha_id
                    ON alphas(alpha_id);
                CREATE INDEX IF NOT EXISTS idx_alphas_status
                    ON alphas(status);
                CREATE INDEX IF NOT EXISTS idx_alphas_source
                    ON alphas(source);

                CREATE TABLE IF NOT EXISTS simulations (
                    simulation_id TEXT PRIMARY KEY,
                    alpha_hash TEXT NOT NULL,
                    alpha_id TEXT,
                    job_id TEXT,
                    status TEXT NOT NULL,
                    params_json TEXT,
                    metrics_json TEXT,
                    result_link TEXT,
                    error TEXT,
                    source TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    FOREIGN KEY(alpha_hash) REFERENCES alphas(alpha_hash)
                );

                CREATE INDEX IF NOT EXISTS idx_simulations_alpha_hash
                    ON simulations(alpha_hash);
                CREATE INDEX IF NOT EXISTS idx_simulations_job_id
                    ON simulations(job_id);
                CREATE INDEX IF NOT EXISTS idx_simulations_status
                    ON simulations(status);

                CREATE TABLE IF NOT EXISTS alpha_events (
                    event_id TEXT PRIMARY KEY,
                    alpha_hash TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    reason TEXT,
                    payload_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(alpha_hash) REFERENCES alphas(alpha_hash)
                );

                CREATE INDEX IF NOT EXISTS idx_alpha_events_alpha_hash
                    ON alpha_events(alpha_hash);
                CREATE INDEX IF NOT EXISTS idx_alpha_events_type
                    ON alpha_events(event_type);
                """
            )

    def register_alpha(
        self,
        code: str,
        *,
        source: str = "unknown",
        template_id: Optional[str] = None,
        status: str = "candidate",
        alpha_id: Optional[str] = None,
        event_type: Optional[str] = "created",
        event_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized = normalize_code(code)
        if not normalized:
            raise ValueError("Alpha code is empty.")
        alpha_hash = alpha_hash_for_code(normalized)
        now = utc_now()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM alphas WHERE alpha_hash = ?",
                (alpha_hash,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO alphas (
                        alpha_hash, alpha_id, code, normalized_code, source,
                        template_id, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        alpha_hash,
                        alpha_id,
                        code,
                        normalized,
                        source or "unknown",
                        template_id,
                        status,
                        now,
                        now,
                    ),
                )
                if event_type:
                    self._add_event_conn(
                        conn,
                        alpha_hash,
                        event_type,
                        payload=event_payload or {
                            "source": source,
                            "template_id": template_id,
                        },
                    )
            else:
                updates = ["updated_at = ?"]
                values: List[Any] = [now]
                if alpha_id and not existing["alpha_id"]:
                    updates.append("alpha_id = ?")
                    values.append(alpha_id)
                if template_id and not existing["template_id"]:
                    updates.append("template_id = ?")
                    values.append(template_id)
                if source and existing["source"] == "unknown":
                    updates.append("source = ?")
                    values.append(source)
                values.append(alpha_hash)
                conn.execute(
                    f"UPDATE alphas SET {', '.join(updates)} WHERE alpha_hash = ?",
                    values,
                )
            return self.get_alpha(alpha_hash, conn=conn) or {}

    def record_queued(self, code: str, *, job_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        alpha = self.register_alpha(
            code,
            source=(params or {}).get("source", "queued"),
            template_id=(params or {}).get("template_id"),
            status="candidate",
            event_type="created",
        )
        with self._connect() as conn:
            self._add_event_conn(
                conn,
                alpha["alpha_hash"],
                "queued",
                payload={"job_id": job_id, "params": params or {}},
            )
        return alpha

    def record_simulation(
        self,
        code: str,
        *,
        job_id: Optional[str],
        status: str,
        params: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        result_link: Optional[str] = None,
        error: Optional[str] = None,
        source: str = "simulation",
        alpha_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        alpha_id = alpha_id or alpha_id_from_link(result_link)
        alpha = self.register_alpha(
            code,
            source=(params or {}).get("source", source),
            template_id=(params or {}).get("template_id"),
            status="candidate",
            alpha_id=alpha_id,
            event_type="created",
        )
        simulation_id = uuid.uuid4().hex
        now = utc_now()
        alpha_status = "simulated" if status == "done" else "failed"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO simulations (
                    simulation_id, alpha_hash, alpha_id, job_id, status,
                    params_json, metrics_json, result_link, error, source,
                    created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    simulation_id,
                    alpha["alpha_hash"],
                    alpha_id,
                    job_id,
                    status,
                    _json_dumps(params or {}),
                    _json_dumps(metrics or {}),
                    result_link,
                    error,
                    source,
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                UPDATE alphas
                SET alpha_id = COALESCE(?, alpha_id),
                    status = CASE
                        WHEN status IN ('promoted', 'rejected') THEN status
                        ELSE ?
                    END,
                    latest_simulation_id = ?,
                    latest_metrics_json = ?,
                    latest_result_link = COALESCE(?, latest_result_link),
                    updated_at = ?
                WHERE alpha_hash = ?
                """,
                (
                    alpha_id,
                    alpha_status,
                    simulation_id,
                    _json_dumps(metrics or {}),
                    result_link,
                    now,
                    alpha["alpha_hash"],
                ),
            )
            self._add_event_conn(
                conn,
                alpha["alpha_hash"],
                "simulated" if status == "done" else "simulation_failed",
                reason=error,
                payload={
                    "simulation_id": simulation_id,
                    "job_id": job_id,
                    "status": status,
                    "metrics": metrics or {},
                    "result_link": result_link,
                },
            )
        return self.get_alpha(alpha["alpha_hash"]) or {}

    def record_simulation_row(
        self,
        row: List[Any],
        *,
        job_id: Optional[str],
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        code = str(row[14]) if len(row) > 14 else ""
        result_link = str(row[13]) if len(row) > 13 else None
        return self.record_simulation(
            code,
            job_id=job_id,
            status="done",
            params=params,
            metrics=metrics_from_row(row),
            result_link=result_link,
        )

    def promote(self, identifier: str, *, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return self._set_status(identifier, "promoted", "promoted", reason=reason)

    def reject(self, identifier: str, *, reason: str) -> Optional[Dict[str, Any]]:
        return self._set_status(identifier, "rejected", "rejected", reason=reason)

    def _set_status(
        self,
        identifier: str,
        status: str,
        event_type: str,
        *,
        reason: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        alpha = self.get_alpha(identifier)
        if alpha is None:
            return None
        now = utc_now()
        with self._connect() as conn:
            if status == "promoted":
                conn.execute(
                    """
                    UPDATE alphas
                    SET status = ?,
                        promoted_at = ?,
                        rejected_at = NULL,
                        reject_reason = NULL,
                        updated_at = ?
                    WHERE alpha_hash = ?
                    """,
                    (status, now, now, alpha["alpha_hash"]),
                )
            else:
                conn.execute(
                    """
                    UPDATE alphas
                    SET status = ?,
                        rejected_at = ?,
                        promoted_at = NULL,
                        reject_reason = ?,
                        updated_at = ?
                    WHERE alpha_hash = ?
                    """,
                    (status, now, reason, now, alpha["alpha_hash"]),
                )
            self._add_event_conn(
                conn,
                alpha["alpha_hash"],
                event_type,
                reason=reason,
                payload={"status": status},
            )
        return self.get_alpha(alpha["alpha_hash"])

    def list_alphas(
        self,
        *,
        status: Optional[str] = None,
        source: Optional[str] = None,
        min_sharpe: Optional[float] = None,
        min_fitness: Optional[float] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        values: List[Any] = []
        if status:
            clauses.append("status = ?")
            values.append(status)
        if source:
            clauses.append("source = ?")
            values.append(source)

        sql = "SELECT * FROM alphas"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC"

        with self._connect() as conn:
            rows = [self._alpha_row_to_dict(row) for row in conn.execute(sql, values).fetchall()]

        def passes_metric(alpha: Dict[str, Any]) -> bool:
            metrics = alpha.get("latest_metrics") or {}
            if min_sharpe is not None:
                try:
                    if float(metrics.get("sharpe", 0)) < min_sharpe:
                        return False
                except (TypeError, ValueError):
                    return False
            if min_fitness is not None:
                try:
                    if float(metrics.get("fitness", 0)) < min_fitness:
                        return False
                except (TypeError, ValueError):
                    return False
            return True

        return [row for row in rows if passes_metric(row)][:max(int(limit), 1)]

    def get_alpha(self, identifier: str, *, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
        owns_conn = conn is None
        conn = conn or self._connect()
        try:
            row = conn.execute(
                """
                SELECT * FROM alphas
                WHERE alpha_hash = ? OR alpha_id = ?
                """,
                (identifier, identifier),
            ).fetchone()
            return self._alpha_row_to_dict(row) if row else None
        finally:
            if owns_conn:
                conn.close()

    def history(self, identifier: str) -> Optional[Dict[str, Any]]:
        alpha = self.get_alpha(identifier)
        if alpha is None:
            return None
        alpha_hash = alpha["alpha_hash"]
        with self._connect() as conn:
            simulations = [
                self._simulation_row_to_dict(row)
                for row in conn.execute(
                    "SELECT * FROM simulations WHERE alpha_hash = ? ORDER BY created_at DESC",
                    (alpha_hash,),
                ).fetchall()
            ]
            events = [
                self._event_row_to_dict(row)
                for row in conn.execute(
                    "SELECT * FROM alpha_events WHERE alpha_hash = ? ORDER BY created_at DESC",
                    (alpha_hash,),
                ).fetchall()
            ]
        return {"alpha": alpha, "simulations": simulations, "events": events}

    def _add_event_conn(
        self,
        conn: sqlite3.Connection,
        alpha_hash: str,
        event_type: str,
        *,
        reason: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO alpha_events (
                event_id, alpha_hash, event_type, reason, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (uuid.uuid4().hex, alpha_hash, event_type, reason, _json_dumps(payload or {}), utc_now()),
        )

    def _alpha_row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        data["latest_metrics"] = _json_loads(data.pop("latest_metrics_json", None), {})
        return data

    def _simulation_row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        data["params"] = _json_loads(data.pop("params_json", None), {})
        data["metrics"] = _json_loads(data.pop("metrics_json", None), {})
        return data

    def _event_row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        data["payload"] = _json_loads(data.pop("payload_json", None), {})
        return data


def get_registry() -> AlphaRegistry:
    return AlphaRegistry(os.environ.get("BRAIN_ALPHA_REGISTRY_PATH", DEFAULT_DB_PATH))
