import datetime
import json
import os
import pickle
from typing import Optional, Tuple
from urllib.parse import urljoin

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CREDENTIALS_PATH = os.path.join(SCRIPT_DIR, "credentials.json")
SESSION_FILE = os.path.join(SCRIPT_DIR, "session.pkl")
LOGIN_TIME_FILE = os.path.join(SCRIPT_DIR, "login_time.pkl")
PENDING_SESSION_FILE = os.path.join(SCRIPT_DIR, "pending_session.pkl")
PENDING_PERSONA_FILE = os.path.join(SCRIPT_DIR, "pending_persona.json")
BRAIN_API_BASE = "https://api.worldquantbrain.com"


def _safe_json(response: requests.Response) -> dict:
    if not response.text:
        return {}
    try:
        body = response.json()
        return body if isinstance(body, dict) else {}
    except ValueError:
        return {}


def _detail_from_response(response: requests.Response, body: Optional[dict] = None) -> str:
    payload = body if body is not None else _safe_json(response)
    return payload.get("detail", f"HTTP {response.status_code}")


def build_session_from_credentials(credentials_path: str = DEFAULT_CREDENTIALS_PATH) -> requests.Session:
    with open(credentials_path, "r") as fh:
        creds = json.load(fh)
    session = requests.Session()
    session.auth = (creds["email"], creds["password"])
    return session


def extract_persona_url(response: requests.Response, body: Optional[dict] = None) -> Optional[str]:
    payload = body if body is not None else _safe_json(response)
    if response.status_code == 401 and (response.headers.get("WWW-Authenticate") or "").lower() == "persona":
        location = response.headers.get("Location")
        if location:
            return urljoin(response.url, location)
    inquiry = payload.get("inquiry")
    if inquiry:
        return f"{response.url}/persona?inquiry={inquiry}"
    return None


def save_login_cookies(session: requests.Session,
                       session_file: str = SESSION_FILE,
                       login_time_file: str = LOGIN_TIME_FILE) -> bool:
    if not session.cookies:
        return False
    with open(session_file, "wb") as fh:
        pickle.dump(requests.utils.dict_from_cookiejar(session.cookies), fh)
    with open(login_time_file, "wb") as fh:
        pickle.dump(datetime.datetime.now(), fh)
    clear_pending_persona_state()
    return True


def save_pending_persona_session(session: requests.Session, persona_url: str,
                                 pending_session_file: str = PENDING_SESSION_FILE,
                                 pending_persona_file: str = PENDING_PERSONA_FILE) -> bool:
    if not session.cookies:
        return False
    with open(pending_session_file, "wb") as fh:
        pickle.dump(requests.utils.dict_from_cookiejar(session.cookies), fh)
    with open(pending_persona_file, "w", encoding="utf-8") as fh:
        json.dump({
            "persona_url": persona_url,
            "created_at": datetime.datetime.now().isoformat(),
        }, fh)
    return True


def load_pending_persona_session(credentials_path: str = DEFAULT_CREDENTIALS_PATH,
                                 pending_session_file: str = PENDING_SESSION_FILE,
                                 pending_persona_file: str = PENDING_PERSONA_FILE) -> Tuple[Optional[requests.Session], Optional[str]]:
    if not (os.path.exists(pending_session_file) and os.path.exists(pending_persona_file)):
        return None, None
    try:
        session = build_session_from_credentials(credentials_path)
        with open(pending_session_file, "rb") as fh:
            session.cookies.update(requests.utils.cookiejar_from_dict(pickle.load(fh)))
        with open(pending_persona_file, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return session, data.get("persona_url")
    except Exception:
        clear_pending_persona_state(pending_session_file, pending_persona_file)
        return None, None


def load_login_cookies(session: requests.Session,
                       session_file: str = SESSION_FILE,
                       login_time_file: str = LOGIN_TIME_FILE) -> Tuple[bool, Optional[datetime.datetime]]:
    loaded = False
    login_time = None
    if os.path.exists(session_file):
        with open(session_file, "rb") as fh:
            cookie_data = pickle.load(fh)
        session.cookies.update(requests.utils.cookiejar_from_dict(cookie_data))
        loaded = True
    if os.path.exists(login_time_file):
        with open(login_time_file, "rb") as fh:
            login_time = pickle.load(fh)
    return loaded, login_time


def clear_login_state(session_file: str = SESSION_FILE,
                      login_time_file: str = LOGIN_TIME_FILE):
    for path in (session_file, login_time_file):
        if os.path.exists(path):
            os.remove(path)
    clear_pending_persona_state()


def clear_pending_persona_state(pending_session_file: str = PENDING_SESSION_FILE,
                                pending_persona_file: str = PENDING_PERSONA_FILE):
    for path in (pending_session_file, pending_persona_file):
        if os.path.exists(path):
            os.remove(path)


def load_persisted_session(credentials_path: str = DEFAULT_CREDENTIALS_PATH,
                           session_file: str = SESSION_FILE,
                           login_time_file: str = LOGIN_TIME_FILE) -> Optional[requests.Session]:
    try:
        session = build_session_from_credentials(credentials_path)
    except Exception:
        return None
    loaded, _ = load_login_cookies(session, session_file=session_file, login_time_file=login_time_file)
    return session if loaded else None


def authenticate_with_brain(session: requests.Session,
                            timeout: int = 15) -> Tuple[Optional[requests.Session], Optional[str], Optional[str]]:
    response = session.post(f"{BRAIN_API_BASE}/authentication", timeout=timeout)
    body = _safe_json(response)
    if response.status_code == 200 and "user" in body:
        save_login_cookies(session)
        return session, None, None
    persona_url = extract_persona_url(response, body)
    if persona_url:
        save_pending_persona_session(session, persona_url)
        return session, "persona", persona_url
    return None, "error", f"Login failed: {_detail_from_response(response, body)}"


def get_session_for_request(credentials_path: str = DEFAULT_CREDENTIALS_PATH) -> Tuple[Optional[requests.Session], Optional[str], Optional[str]]:
    persisted = load_persisted_session(credentials_path)
    if persisted is not None:
        return persisted, None, None
    session = build_session_from_credentials(credentials_path)
    return authenticate_with_brain(session)
