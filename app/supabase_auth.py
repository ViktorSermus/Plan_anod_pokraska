from __future__ import annotations

from typing import Any

import streamlit as st
from supabase import Client, create_client

try:
    from supabase.lib.client_options import ClientOptions
except ImportError:
    ClientOptions = None  # type: ignore[misc, assignment]


class StreamlitAuthStorage:
    """Хранение PKCE code_verifier между перезапусками скрипта Streamlit."""

    def __init__(self, prefix: str = "_sb_gotrue_") -> None:
        self._prefix = prefix

    def get_item(self, key: str) -> str | None:
        full = self._prefix + key
        v = st.session_state.get(full)
        return None if v is None else str(v)

    def set_item(self, key: str, value: str) -> None:
        st.session_state[self._prefix + key] = value

    def remove_item(self, key: str) -> None:
        st.session_state.pop(self._prefix + key, None)


def build_supabase_client(url: str, anon_key: str) -> Client:
    storage = StreamlitAuthStorage()
    if ClientOptions is not None:
        try:
            opts = ClientOptions(storage=storage, flow_type="pkce")
            return create_client(url, anon_key, options=opts)
        except (TypeError, ValueError):
            pass
    return create_client(url, anon_key)


def _persist_session_from_response(session: Any, user: Any) -> None:
    if session is not None:
        st.session_state["sb_access_token"] = session.access_token
        rt = getattr(session, "refresh_token", None)
        st.session_state["sb_refresh_token"] = rt or ""
    if user is not None:
        st.session_state["sb_user_id"] = str(getattr(user, "id", "") or "")
        em = getattr(user, "email", None)
        st.session_state["sb_user_email"] = (em or "") if em else ""


def restore_session(supabase: Client) -> None:
    at = st.session_state.get("sb_access_token")
    rt = st.session_state.get("sb_refresh_token")
    if not at:
        return
    try:
        supabase.auth.set_session(at, rt or "")
    except Exception:
        pass


def current_user() -> dict[str, str] | None:
    if not st.session_state.get("sb_access_token"):
        return None
    uid = st.session_state.get("sb_user_id")
    if not uid:
        return None
    return {
        "id": str(uid),
        "email": str(st.session_state.get("sb_user_email") or ""),
    }


def process_oauth_redirect(supabase: Client) -> bool:
    """Обмен `?code=` на сессию. Возвращает True, если сделан rerun (остановить дальнейшую отрисовку)."""
    code = st.query_params.get("code")
    if not code:
        return False
    err: str | None = None
    try:
        resp = supabase.auth.exchange_code_for_session({"auth_code": code})
        sess = getattr(resp, "session", None)
        usr = getattr(resp, "user", None)
        if sess:
            _persist_session_from_response(sess, usr)
        else:
            err = "Сервер не вернул сессию"
    except Exception as e:
        err = str(e)
    try:
        for k in list(st.query_params.keys()):
            del st.query_params[k]
    except Exception:
        pass
    if err:
        st.error(f"Не удалось завершить вход Google: {err}")
        return False
    st.rerun()
    return True


def render_login_page(supabase: Client, app_base_url: str) -> None:
    st.subheader("Вход")
    redirect_to = app_base_url.rstrip("/") + "/"

    with st.form("email_login"):
        email = st.text_input("Email")
        password = st.text_input("Пароль", type="password")
        submitted = st.form_submit_button("Войти")
        if submitted:
            if not email.strip() or not password:
                st.warning("Введите email и пароль")
            else:
                try:
                    res = supabase.auth.sign_in_with_password({"email": email.strip(), "password": password})
                    sess = getattr(res, "session", None)
                    usr = getattr(res, "user", None)
                    if sess:
                        _persist_session_from_response(sess, usr)
                        st.rerun()
                    else:
                        st.error("Сервер не вернул сессию")
                except Exception as e:
                    st.error(str(e))

    try:
        oauth = supabase.auth.sign_in_with_oauth(
            {
                "provider": "google",
                "options": {
                    "redirect_to": redirect_to,
                    "skip_browser_redirect": True,
                },
            }
        )
        url = getattr(oauth, "url", None)
        if url:
            st.link_button("Войти через Google", url, use_container_width=True)
    except Exception as e:
        st.caption(f"Google: настройте провайдера в Supabase. ({e})")


def logout(supabase: Client) -> None:
    for k in ("sb_access_token", "sb_refresh_token", "sb_user_id", "sb_user_email"):
        st.session_state.pop(k, None)
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and k.startswith("_sb_gotrue_"):
            st.session_state.pop(k, None)
    try:
        supabase.auth.sign_out()
    except Exception:
        pass
    st.rerun()
