from __future__ import annotations

import os
from typing import Any

import requests


SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
SUPABASE_ANON_KEY = (os.getenv("SUPABASE_ANON_KEY") or "").strip()


class SupabaseAuthError(RuntimeError):
    pass


def _require_url() -> str:
    if not SUPABASE_URL:
        raise SupabaseAuthError("SUPABASE_URL is not configured.")
    return SUPABASE_URL


def _require_service_role_key() -> str:
    if not SUPABASE_SERVICE_ROLE_KEY:
        raise SupabaseAuthError("SUPABASE_SERVICE_ROLE_KEY is not configured.")
    return SUPABASE_SERVICE_ROLE_KEY


def _client_headers(api_key: str, bearer: str | None = None) -> dict[str, str]:
    headers = {
        "apikey": api_key,
        "Content-Type": "application/json",
    }
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    return headers


def _request(method: str, path: str, *, api_key: str, bearer: str | None = None, json_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{_require_url()}{path}"
    response = requests.request(
        method=method.upper(),
        url=url,
        headers=_client_headers(api_key, bearer=bearer),
        json=json_payload,
        timeout=25,
    )
    try:
        payload = response.json()
    except Exception:
        payload = {"message": response.text}
    if not response.ok:
        message = (
            payload.get("msg")
            or payload.get("message")
            or payload.get("error_description")
            or payload.get("error")
            or "Supabase request failed."
        )
        raise SupabaseAuthError(str(message))
    if not isinstance(payload, dict):
        raise SupabaseAuthError("Supabase returned an unexpected response payload.")
    return payload


def admin_create_user(*, email: str, password: str, username: str, full_name: str = "", role: str = "", email_confirm: bool = True) -> dict[str, Any]:
    payload = {
        "email": email,
        "password": password,
        "email_confirm": bool(email_confirm),
        "user_metadata": {
            "username": username,
            "full_name": full_name,
            "role": role,
        },
    }
    return _request(
        "POST",
        "/auth/v1/admin/users",
        api_key=_require_service_role_key(),
        bearer=_require_service_role_key(),
        json_payload=payload,
    )


def sign_in_with_password(*, email: str, password: str) -> dict[str, Any]:
    api_key = SUPABASE_ANON_KEY or _require_service_role_key()
    return _request(
        "POST",
        "/auth/v1/token?grant_type=password",
        api_key=api_key,
        json_payload={"email": email, "password": password},
    )


def get_user_from_token(access_token: str) -> dict[str, Any]:
    if not access_token:
        raise SupabaseAuthError("Access token is required.")
    api_key = SUPABASE_ANON_KEY or _require_service_role_key()
    return _request(
        "GET",
        "/auth/v1/user",
        api_key=api_key,
        bearer=access_token,
    )
