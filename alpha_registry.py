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
                    canonical_alpha_id TEXT,
                    latest_alpha_id TEXT,
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

                CREATE TABLE IF NOT EXISTS alpha_platform_ids (
                    alpha_id TEXT PRIMARY KEY,
                    alpha_hash TEXT NOT NULL,
                    simulation_id TEXT,
                    job_id TEXT,
                    result_link TEXT,
                    settings_json TEXT,
                    metrics_json TEXT,
                    role TEXT NOT NULL DEFAULT 'observed',
                    source TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    last_seen_at TEXT NOT NULL,
                    FOREIGN KEY(alpha_hash) REFERENCES alphas(alpha_hash)
                );

                CREATE INDEX IF NOT EXISTS idx_alpha_platform_ids_alpha_hash
                    ON alpha_platform_ids(alpha_hash);
                CREATE INDEX IF NOT EXISTS idx_alpha_platform_ids_role
                    ON alpha_platform_ids(role);

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
                CREATE INDEX IF NOT EXISTS idx_simulations_alpha_id
                    ON simulations(alpha_id);

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
            self._migrate_schema(conn)

    @staticmethod
    def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(row["name"] == column for row in rows)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        if not self._column_exists(conn, "alphas", "canonical_alpha_id"):
            conn.execute("ALTER TABLE alphas ADD COLUMN canonical_alpha_id TEXT")
        if not self._column_exists(conn, "alphas", "latest_alpha_id"):
            conn.execute("ALTER TABLE alphas ADD COLUMN latest_alpha_id TEXT")

        conn.execute(
            """
            UPDATE alphas
            SET latest_alpha_id = COALESCE(latest_alpha_id, alpha_id)
            WHERE alpha_id IS NOT NULL
            """
        )

        rows = conn.execute(
            """
            SELECT
                simulation_id, alpha_hash, alpha_id, job_id, result_link,
                params_json, metrics_json, source, created_at, completed_at
            FROM simulations
            WHERE alpha_id IS NOT NULL AND alpha_id != ''
            ORDER BY COALESCE(completed_at, created_at), created_at
            """
        ).fetchall()
        for row in rows:
            self._upsert_platform_alpha_conn(
                conn,
                alpha_id=row["alpha_id"],
                alpha_hash=row["alpha_hash"],
                simulation_id=row["simulation_id"],
                job_id=row["job_id"],
                result_link=row["result_link"],
                settings_json=row["params_json"],
                metrics_json=row["metrics_json"],
                role="observed",
                source=row["source"],
                created_at=row["created_at"],
                completed_at=row["completed_at"],
            )

        alpha_hashes = [
            row["alpha_hash"]
            for row in conn.execute("SELECT DISTINCT alpha_hash FROM alpha_platform_ids").fetchall()
        ]
        for alpha_hash in alpha_hashes:
            explicit = conn.execute(
                """
                SELECT p.alpha_id
                FROM alpha_platform_ids p
                WHERE p.alpha_hash = ?
                  AND p.role IN ('submitted', 'canonical')
                  AND EXISTS (
                      SELECT 1
                      FROM alpha_events e
                      WHERE e.alpha_hash = p.alpha_hash
                        AND e.event_type = 'promoted'
                        AND json_extract(e.payload_json, '$.alpha_id') = p.alpha_id
                  )
                ORDER BY p.last_seen_at DESC
                LIMIT 1
                """,
                (alpha_hash,),
            ).fetchone()
            first = conn.execute(
                """
                SELECT alpha_id
                FROM alpha_platform_ids
                WHERE alpha_hash = ?
                ORDER BY created_at, COALESCE(completed_at, created_at)
                LIMIT 1
                """,
                (alpha_hash,),
            ).fetchone()
            latest = conn.execute(
                """
                SELECT alpha_id
                FROM alpha_platform_ids
                WHERE alpha_hash = ?
                ORDER BY COALESCE(completed_at, created_at) DESC, created_at DESC
                LIMIT 1
                """,
                (alpha_hash,),
            ).fetchone()
            canonical_alpha_id = (
                explicit["alpha_id"] if explicit else
                first["alpha_id"] if first else
                None
            )
            conn.execute(
                """
                UPDATE alphas
                SET canonical_alpha_id = COALESCE(?, ?),
                    latest_alpha_id = COALESCE(?, latest_alpha_id, alpha_id),
                    alpha_id = COALESCE(?, alpha_id)
                WHERE alpha_hash = ?
                """,
                (
                    canonical_alpha_id,
                    canonical_alpha_id,
                    latest["alpha_id"] if latest else None,
                    latest["alpha_id"] if latest else None,
                    alpha_hash,
                ),
            )
            if canonical_alpha_id:
                conn.execute(
                    """
                    UPDATE alpha_platform_ids
                    SET role = 'observed'
                    WHERE alpha_hash = ?
                      AND role = 'canonical'
                      AND alpha_id != ?
                    """,
                    (alpha_hash, canonical_alpha_id),
                )
                conn.execute(
                    """
                    UPDATE alpha_platform_ids
                    SET role = 'canonical'
                    WHERE alpha_id = ?
                    """,
                    (canonical_alpha_id,),
                )

    def _upsert_platform_alpha_conn(
        self,
        conn: sqlite3.Connection,
        *,
        alpha_id: Optional[str],
        alpha_hash: str,
        simulation_id: Optional[str] = None,
        job_id: Optional[str] = None,
        result_link: Optional[str] = None,
        settings_json: Optional[str] = None,
        metrics_json: Optional[str] = None,
        role: str = "observed",
        source: Optional[str] = None,
        created_at: Optional[str] = None,
        completed_at: Optional[str] = None,
    ) -> None:
        if not alpha_id:
            return
        now = utc_now()
        created_at = created_at or now
        conn.execute(
            """
            INSERT INTO alpha_platform_ids (
                alpha_id, alpha_hash, simulation_id, job_id, result_link,
                settings_json, metrics_json, role, source, created_at,
                completed_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(alpha_id) DO UPDATE SET
                alpha_hash = excluded.alpha_hash,
                simulation_id = COALESCE(excluded.simulation_id, alpha_platform_ids.simulation_id),
                job_id = COALESCE(excluded.job_id, alpha_platform_ids.job_id),
                result_link = COALESCE(excluded.result_link, alpha_platform_ids.result_link),
                settings_json = COALESCE(excluded.settings_json, alpha_platform_ids.settings_json),
                metrics_json = COALESCE(excluded.metrics_json, alpha_platform_ids.metrics_json),
                role = CASE
                    WHEN alpha_platform_ids.role IN ('canonical', 'submitted', 'rejected_variant')
                        THEN alpha_platform_ids.role
                    ELSE COALESCE(excluded.role, alpha_platform_ids.role)
                END,
                source = COALESCE(excluded.source, alpha_platform_ids.source),
                completed_at = COALESCE(excluded.completed_at, alpha_platform_ids.completed_at),
                last_seen_at = excluded.last_seen_at
            """,
            (
                alpha_id,
                alpha_hash,
                simulation_id,
                job_id,
                result_link,
                settings_json,
                metrics_json,
                role or "observed",
                source,
                created_at,
                completed_at,
                now,
            ),
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
                        alpha_hash, alpha_id, canonical_alpha_id, latest_alpha_id,
                        code, normalized_code, source, template_id, status,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        alpha_hash,
                        alpha_id,
                        alpha_id,
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
                if alpha_id and not existing["canonical_alpha_id"]:
                    updates.append("canonical_alpha_id = ?")
                    values.append(alpha_id)
                if alpha_id:
                    updates.append("latest_alpha_id = ?")
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
            if alpha_id:
                self._upsert_platform_alpha_conn(
                    conn,
                    alpha_id=alpha_id,
                    alpha_hash=alpha_hash,
                    role="observed",
                    source=source,
                    created_at=now,
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
                    canonical_alpha_id = COALESCE(canonical_alpha_id, ?),
                    latest_alpha_id = COALESCE(?, latest_alpha_id),
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
                    alpha_id,
                    alpha_id,
                    alpha_status,
                    simulation_id,
                    _json_dumps(metrics or {}),
                    result_link,
                    now,
                    alpha["alpha_hash"],
                ),
            )
            self._upsert_platform_alpha_conn(
                conn,
                alpha_id=alpha_id,
                alpha_hash=alpha["alpha_hash"],
                simulation_id=simulation_id,
                job_id=job_id,
                result_link=result_link,
                settings_json=_json_dumps(params or {}),
                metrics_json=_json_dumps(metrics or {}),
                role="observed",
                source=source,
                created_at=now,
                completed_at=now,
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
                    "alpha_id": alpha_id,
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
        matched_alpha_id = alpha.get("resolved_alpha_id")
        now = utc_now()
        with self._connect() as conn:
            if status == "promoted":
                if matched_alpha_id:
                    conn.execute(
                        """
                        UPDATE alpha_platform_ids
                        SET role = 'observed'
                        WHERE alpha_hash = ?
                          AND role = 'canonical'
                          AND alpha_id != ?
                        """,
                        (alpha["alpha_hash"], matched_alpha_id),
                    )
                conn.execute(
                    """
                    UPDATE alphas
                    SET status = ?,
                        canonical_alpha_id = COALESCE(?, canonical_alpha_id),
                        promoted_at = ?,
                        rejected_at = NULL,
                        reject_reason = NULL,
                        updated_at = ?
                    WHERE alpha_hash = ?
                    """,
                    (status, matched_alpha_id, now, now, alpha["alpha_hash"]),
                )
                if matched_alpha_id:
                    conn.execute(
                        """
                        UPDATE alpha_platform_ids
                        SET role = 'canonical',
                            last_seen_at = ?
                        WHERE alpha_id = ?
                        """,
                        (now, matched_alpha_id),
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
                if matched_alpha_id:
                    conn.execute(
                        """
                        UPDATE alpha_platform_ids
                        SET role = 'rejected_variant',
                            last_seen_at = ?
                        WHERE alpha_id = ?
                        """,
                        (now, matched_alpha_id),
                    )
            self._add_event_conn(
                conn,
                alpha["alpha_hash"],
                event_type,
                reason=reason,
                payload={"status": status, "alpha_id": matched_alpha_id},
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
            rows = [
                self._alpha_row_to_dict(row, conn=conn, include_alpha_ids=False)
                for row in conn.execute(sql, values).fetchall()
            ]

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
                WHERE alpha_hash = ?
                """,
                (identifier,),
            ).fetchone()
            if row:
                data = self._alpha_row_to_dict(row, conn=conn)
                data["resolved_by"] = "alpha_hash"
                return data

            platform = conn.execute(
                """
                SELECT * FROM alpha_platform_ids
                WHERE alpha_id = ?
                """,
                (identifier,),
            ).fetchone()
            if platform:
                row = conn.execute(
                    """
                    SELECT * FROM alphas
                    WHERE alpha_hash = ?
                    """,
                    (platform["alpha_hash"],),
                ).fetchone()
                if row:
                    data = self._alpha_row_to_dict(row, conn=conn)
                    data["resolved_by"] = "platform_alpha_id"
                    data["resolved_alpha_id"] = platform["alpha_id"]
                    data["matched_platform_alpha"] = self._platform_alpha_row_to_dict(platform)
                    return data

            row = conn.execute(
                """
                SELECT * FROM alphas
                WHERE alpha_id = ? OR canonical_alpha_id = ? OR latest_alpha_id = ?
                """,
                (identifier, identifier, identifier),
            ).fetchone()
            if row:
                data = self._alpha_row_to_dict(row, conn=conn)
                data["resolved_by"] = "alpha_legacy_id"
                data["resolved_alpha_id"] = identifier
                return data
            return None
        finally:
            if owns_conn:
                conn.close()

    def history(self, identifier: str) -> Optional[Dict[str, Any]]:
        alpha = self.get_alpha(identifier)
        if alpha is None:
            return None
        alpha_hash = alpha["alpha_hash"]
        with self._connect() as conn:
            platform_alphas = self._platform_alpha_rows_for_hash(conn, alpha_hash)
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
        return {
            "alpha": alpha,
            "platform_alphas": platform_alphas,
            "matched_platform_alpha": alpha.get("matched_platform_alpha"),
            "simulations": simulations,
            "events": events,
        }

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

    def _platform_alpha_row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        data["settings"] = _json_loads(data.pop("settings_json", None), {})
        data["metrics"] = _json_loads(data.pop("metrics_json", None), {})
        return data

    def _platform_alpha_rows_for_hash(
        self,
        conn: sqlite3.Connection,
        alpha_hash: str,
    ) -> List[Dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT *
            FROM alpha_platform_ids
            WHERE alpha_hash = ?
            ORDER BY COALESCE(completed_at, created_at) DESC, created_at DESC
            """,
            (alpha_hash,),
        ).fetchall()
        return [self._platform_alpha_row_to_dict(row) for row in rows]

    def _alpha_row_to_dict(
        self,
        row: sqlite3.Row,
        *,
        conn: Optional[sqlite3.Connection] = None,
        include_alpha_ids: bool = True,
        include_platforms: bool = False,
    ) -> Dict[str, Any]:
        data = dict(row)
        data["latest_metrics"] = _json_loads(data.pop("latest_metrics_json", None), {})
        if conn is not None:
            platform_alphas = self._platform_alpha_rows_for_hash(conn, data["alpha_hash"])
            data["platform_alpha_count"] = len(platform_alphas)
            if include_alpha_ids:
                data["alpha_ids"] = [row["alpha_id"] for row in platform_alphas]
            if include_platforms:
                data["platform_alphas"] = platform_alphas
        else:
            data["platform_alpha_count"] = 0
            if include_alpha_ids:
                data["alpha_ids"] = []
            if include_platforms:
                data["platform_alphas"] = []
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
