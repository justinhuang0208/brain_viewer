from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv, set_key

from wq_session import (
    BRAIN_API_BASE,
    authenticate_with_brain,
    build_session_from_credentials,
    extract_persona_url,
    load_login_cookies,
    load_persisted_session,
    save_login_cookies,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(SCRIPT_DIR, ".brain_cli", "telegram")
OFFSET_FILE = os.path.join(STATE_DIR, "offset.txt")
NOTIFICATION_STATE_FILE = os.path.join(STATE_DIR, "notification_state.json")
DEFAULT_POLL_TIMEOUT = 60
DEFAULT_NOTIFICATION_COOLDOWN = 600
PERSONA_CALLBACK_DATA = "persona_complete"


class TelegramConfigError(RuntimeError):
    pass


def _ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)


def _read_json_file(path: str, default: Dict[str, Any]) -> Dict[str, Any]:
    if not os.path.exists(path):
        return dict(default)
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    return data if isinstance(data, dict) else dict(default)


def _write_json_file(path: str, payload: Dict[str, Any]):
    _ensure_state_dir()
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def _load_config() -> Tuple[str, str]:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_API_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token:
        raise TelegramConfigError("Missing TELEGRAM_BOT_API_TOKEN (or TELEGRAM_BOT_TOKEN) in environment.")
    if not chat_id:
        raise TelegramConfigError("Missing TELEGRAM_CHAT_ID in environment.")
    return token, str(chat_id)


def _load_token() -> str:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_API_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise TelegramConfigError("Missing TELEGRAM_BOT_API_TOKEN (or TELEGRAM_BOT_TOKEN) in environment.")
    return token


def _api_request(method: str,
                 *,
                 http_method: str = "GET",
                 params: Optional[dict] = None,
                 payload: Optional[dict] = None,
                 timeout: int = 30) -> Any:
    token = _load_token()
    url = f"https://api.telegram.org/bot{token}/{method}"
    request = requests.get if http_method == "GET" else requests.post
    response = request(url, params=params, json=payload, timeout=timeout)
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        raise RuntimeError(body.get("description", f"Telegram API {method} failed."))
    return body.get("result")


def send_telegram_message(text: str,
                          *,
                          reply_markup: Optional[dict] = None,
                          disable_web_page_preview: bool = True,
                          chat_id: Optional[str] = None) -> dict:
    _, default_chat_id = _load_config()
    payload = {
        "chat_id": chat_id or default_chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return _api_request("sendMessage", http_method="POST", payload=payload)


def _format_duration(delta: _dt.timedelta) -> str:
    total_seconds = max(int(delta.total_seconds()), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _session_status(credentials_path: str) -> dict:
    try:
        session = load_persisted_session(credentials_path)
    except FileNotFoundError:
        return {"status": "error", "message": f"Credentials file not found: {credentials_path}"}
    except Exception as exc:
        return {"status": "error", "message": f"Unable to prepare session: {exc}"}

    if session is None:
        return {
            "status": "not_logged_in",
            "message": "No persisted WQ session found.",
            "login_age": None,
        }

    _, login_time = load_login_cookies(session)
    login_age = None
    if login_time is not None:
        login_age = _format_duration(_dt.datetime.now() - login_time)

    try:
        response = session.options(f"{BRAIN_API_BASE}/simulations", timeout=10)
    except requests.exceptions.RequestException as exc:
        return {
            "status": "error",
            "message": f"Failed to check session status: {exc}",
            "login_age": login_age,
        }

    if response.status_code == 200:
        return {"status": "valid", "message": "Saved session is valid.", "login_age": login_age}
    if response.status_code == 401:
        persona_url = extract_persona_url(response)
        if persona_url:
            return {
                "status": "persona_required",
                "message": "Saved session expired and Persona verification is required.",
                "persona_url": persona_url,
                "login_age": login_age,
            }
        return {"status": "expired", "message": "Saved session has expired.", "login_age": login_age}
    return {
        "status": "unknown",
        "message": f"Session check returned HTTP {response.status_code}.",
        "login_age": login_age,
    }


def build_status_message(credentials_path: str) -> str:
    import cli_services as svc

    session_info = _session_status(credentials_path)
    dataset_count = len(svc.datasets_list())
    simulate_jobs = svc.simulate_list()
    evolution_jobs = svc.evolution_list()

    def _count_status(jobs, status):
        return sum(1 for job in jobs if job.get("status") == status)

    lines = [
        "brain_viewer 狀態",
        f"Session: {session_info.get('status')}",
        f"Session detail: {session_info.get('message')}",
        f"Login age: {session_info.get('login_age') or 'N/A'}",
        f"Cached datasets: {dataset_count}",
        (
            "Simulation jobs: "
            f"pending={_count_status(simulate_jobs, 'pending')} "
            f"running={_count_status(simulate_jobs, 'running')} "
            f"done={_count_status(simulate_jobs, 'done')} "
            f"failed={_count_status(simulate_jobs, 'failed')} "
            f"stopped={_count_status(simulate_jobs, 'stopped')}"
        ),
        (
            "Evolution jobs: "
            f"pending={_count_status(evolution_jobs, 'pending')} "
            f"running={_count_status(evolution_jobs, 'running')} "
            f"done={_count_status(evolution_jobs, 'done')} "
            f"failed={_count_status(evolution_jobs, 'failed')} "
            f"stopped={_count_status(evolution_jobs, 'stopped')}"
        ),
    ]
    if session_info.get("persona_url"):
        lines.append(f"Persona URL: {session_info['persona_url']}")
    return "\n".join(lines)


def send_status_message(credentials_path: str) -> dict:
    text = build_status_message(credentials_path)
    send_telegram_message(text)
    return {"status": "sent", "message": text}


def _extract_chat_from_update(update: dict) -> Optional[dict]:
    candidates = [
        update.get("message", {}).get("chat"),
        update.get("edited_message", {}).get("chat"),
        update.get("channel_post", {}).get("chat"),
        update.get("edited_channel_post", {}).get("chat"),
        update.get("my_chat_member", {}).get("chat"),
        update.get("chat_member", {}).get("chat"),
        update.get("callback_query", {}).get("message", {}).get("chat"),
    ]
    for chat in candidates:
        if isinstance(chat, dict) and "id" in chat:
            return chat
    return None


def set_telegram_chat_id(chat_id: str, env_path: Optional[str] = None) -> dict:
    target = env_path or os.path.join(SCRIPT_DIR, ".env")
    if not os.path.exists(target):
        with open(target, "a", encoding="utf-8"):
            pass
    set_key(target, "TELEGRAM_CHAT_ID", str(chat_id))
    return {"status": "updated", "env_path": target, "chat_id": str(chat_id)}


def discover_chat_id(*, limit: int = 20, write_env: bool = False, env_path: Optional[str] = None) -> dict:
    updates = _api_request(
        "getUpdates",
        params={"limit": limit, "timeout": 0},
        timeout=30,
    )
    seen_ids = set()
    chats = []
    for update in reversed(updates or []):
        chat = _extract_chat_from_update(update)
        if not chat:
            continue
        chat_id = str(chat.get("id"))
        if chat_id in seen_ids:
            continue
        seen_ids.add(chat_id)
        chats.append({
            "chat_id": chat_id,
            "type": chat.get("type"),
            "title": chat.get("title") or chat.get("username") or chat.get("first_name") or "",
            "username": chat.get("username"),
            "update_id": update.get("update_id"),
        })

    if not chats:
        return {
            "status": "no_updates",
            "message": "No Telegram updates found. Send a message like /start to the bot first.",
            "chats": [],
            "selected_chat_id": None,
            "env_updated": False,
        }

    selected_chat_id = chats[0]["chat_id"]
    result = {
        "status": "ok",
        "message": "Found recent Telegram chat IDs.",
        "chats": chats,
        "selected_chat_id": selected_chat_id,
        "env_updated": False,
    }
    if write_env:
        result["env_write_result"] = set_telegram_chat_id(selected_chat_id, env_path=env_path)
        result["env_updated"] = True
    return result


def send_login_issue_notification(reason: str,
                                  *,
                                  detail: str = "",
                                  cooldown_key: str = "auth-issue",
                                  cooldown_seconds: int = DEFAULT_NOTIFICATION_COOLDOWN) -> dict:
    try:
        _ensure_state_dir()
        state = _read_json_file(NOTIFICATION_STATE_FILE, {})
        now = time.time()
        last_sent = float(state.get(cooldown_key, 0))
        if now - last_sent < cooldown_seconds:
            return {
                "status": "skipped",
                "reason": "cooldown",
                "remaining_seconds": int(cooldown_seconds - (now - last_sent)),
            }

        lines = [
            "brain_viewer 登入狀態通知",
            reason,
        ]
        if detail:
            lines.append(detail)
        lines.append("請在 GUI 按 Check Login，或在 Telegram 使用 /refresh 重新建立 session。")
        send_telegram_message("\n".join(lines))
        state[cooldown_key] = now
        _write_json_file(NOTIFICATION_STATE_FILE, state)
        return {"status": "sent"}
    except TelegramConfigError:
        logging.info("Telegram login notification skipped because Telegram is not configured.")
        return {"status": "disabled"}


class TelegramBotRunner:
    def __init__(self, credentials_path: str, poll_timeout: int = DEFAULT_POLL_TIMEOUT):
        _, chat_id = _load_config()
        self.credentials_path = credentials_path
        self.poll_timeout = poll_timeout
        self.authorized_chat_id = str(chat_id)
        self.pending_persona_session: Optional[requests.Session] = None
        self.pending_persona_url: Optional[str] = None

    def _load_offset(self) -> Optional[int]:
        if not os.path.exists(OFFSET_FILE):
            return None
        with open(OFFSET_FILE, "r", encoding="utf-8") as fh:
            raw = fh.read().strip()
        return int(raw) if raw else None

    def _save_offset(self, offset: int):
        _ensure_state_dir()
        with open(OFFSET_FILE, "w", encoding="utf-8") as fh:
            fh.write(str(offset))

    def _get_updates(self, offset: Optional[int]) -> list:
        params = {
            "timeout": self.poll_timeout,
            "allowed_updates": json.dumps(["message", "callback_query"]),
        }
        if offset is not None:
            params["offset"] = offset
        result = _api_request("getUpdates", params=params, timeout=self.poll_timeout + 10)
        return result if isinstance(result, list) else []

    def _answer_callback(self, callback_id: str, text: str):
        _api_request(
            "answerCallbackQuery",
            http_method="POST",
            payload={"callback_query_id": callback_id, "text": text},
        )

    def _is_authorized_chat(self, chat_id: Optional[str]) -> bool:
        return str(chat_id or "") == self.authorized_chat_id

    def _help_text(self) -> str:
        return (
            "brain_viewer Telegram 指令\n"
            "/refresh 或 /refresh_session - 重新整理 WQ session\n"
            "/status 或 /stat - 查詢目前 session 與 job 狀態\n"
            "/help 或 /start - 顯示這份說明"
        )

    def _handle_refresh(self):
        send_telegram_message("開始刷新 WQ session…")
        try:
            session = load_persisted_session(self.credentials_path)
            if session is None:
                session = build_session_from_credentials(self.credentials_path)
        except FileNotFoundError:
            send_telegram_message(f"找不到憑證檔案: {self.credentials_path}")
            return
        except Exception as exc:
            send_telegram_message(f"無法準備登入 session: {exc}")
            return

        try:
            authed_session, kind, detail = authenticate_with_brain(session)
        except requests.exceptions.RequestException as exc:
            send_telegram_message(f"刷新 session 時發生網路錯誤: {exc}")
            return

        if kind is None and authed_session is not None:
            self.pending_persona_session = None
            self.pending_persona_url = None
            send_telegram_message("Session 刷新成功。")
            return

        if kind == "persona":
            self.pending_persona_session = session
            self.pending_persona_url = detail
            send_telegram_message(
                "請先完成 Persona 生物辨識驗證，完成後按下方按鈕繼續：\n"
                f"{detail}",
                reply_markup={
                    "inline_keyboard": [[
                        {"text": "我已完成驗證", "callback_data": PERSONA_CALLBACK_DATA}
                    ]]
                },
            )
            return

        send_telegram_message(f"Session 刷新失敗：{detail}")

    def _handle_status(self):
        send_telegram_message(build_status_message(self.credentials_path))

    def _handle_persona_complete(self):
        if self.pending_persona_session is None or not self.pending_persona_url:
            send_telegram_message("目前沒有待完成的 Persona 驗證，請重新執行 /refresh。")
            return
        try:
            response = self.pending_persona_session.post(self.pending_persona_url, timeout=15)
            if not response.ok:
                send_telegram_message(f"Persona 驗證提交失敗：HTTP {response.status_code}")
                return
            save_login_cookies(self.pending_persona_session)
            self.pending_persona_session = None
            self.pending_persona_url = None
            send_telegram_message("Persona 驗證完成，Session 已刷新成功。")
        except requests.exceptions.Timeout:
            send_telegram_message("Persona 驗證確認逾時，請重新按一次按鈕或執行 /refresh。")
        except requests.exceptions.RequestException as exc:
            send_telegram_message(f"Persona 驗證確認失敗：{exc}")

    def _handle_message(self, message: dict):
        chat_id = str(message.get("chat", {}).get("id", ""))
        if not self._is_authorized_chat(chat_id):
            return
        text = (message.get("text") or "").strip()
        if not text:
            return

        command = text.split()[0].split("@")[0].lower()
        if command in ("/refresh", "/refresh_session"):
            self._handle_refresh()
        elif command in ("/status", "/stat"):
            self._handle_status()
        elif command in ("/help", "/start"):
            send_telegram_message(self._help_text())
        else:
            send_telegram_message("未知指令。\n\n" + self._help_text())

    def _handle_callback(self, callback: dict):
        message = callback.get("message", {})
        chat_id = str(message.get("chat", {}).get("id", ""))
        if not self._is_authorized_chat(chat_id):
            return

        callback_id = callback.get("id")
        if callback_id:
            self._answer_callback(callback_id, "收到，正在確認驗證狀態…")

        if callback.get("data") == PERSONA_CALLBACK_DATA:
            self._handle_persona_complete()

    def run(self, once: bool = False):
        offset = self._load_offset()
        while True:
            updates = self._get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                self._save_offset(offset)
                if "message" in update:
                    self._handle_message(update["message"])
                elif "callback_query" in update:
                    self._handle_callback(update["callback_query"])
            if once:
                return
            time.sleep(1)
