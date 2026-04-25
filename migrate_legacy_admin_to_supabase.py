from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from database_service import (
    _connect,
    ensure_legacy_user_record,
    find_app_user_duplicates,
    get_user_by_username,
    init_db,
    insert_user_registration_audit,
    upsert_app_user_profile,
)
from supabase_auth_service import SupabaseAuthError, admin_create_user

BASE_DIR = Path(__file__).resolve().parent
AUTH_SESSION_STORE_PATH = BASE_DIR / "data" / "auth_sessions.json"


def load_auth_session_store() -> dict:
    if not AUTH_SESSION_STORE_PATH.exists():
        return {}
    try:
        payload = json.loads(AUTH_SESSION_STORE_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def latest_password_for_user(username: str) -> str:
    sessions = load_auth_session_store()
    matches = []
    for _, record in sessions.items():
        if not isinstance(record, dict):
            continue
        if str(record.get("username") or "").strip().lower() != username.strip().lower():
            continue
        matches.append(record)
    matches.sort(key=lambda item: str(item.get("created_at") or ""))
    if not matches:
        return ""
    return str(matches[-1].get("password") or "").strip()


def find_auth_user_id_by_email(email: str) -> str:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM auth.users
                WHERE lower(email) = lower(%s)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (email,),
            )
            row = cur.fetchone()
    return str((row or {}).get("id") or "").strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate the current legacy admin account into Supabase Auth + app_users.")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--email", default="admin@iti-feedback.local")
    parser.add_argument("--full-name", default="System Admin")
    args = parser.parse_args()

    init_db()
    legacy_user = get_user_by_username(args.username.strip())
    if not legacy_user:
        raise SystemExit(f"Legacy user '{args.username}' was not found in users table.")

    password = latest_password_for_user(args.username.strip())
    if not password:
        raise SystemExit(f"No saved local session password was found for '{args.username}'. Log in once, then retry.")

    duplicates = find_app_user_duplicates(args.username.strip(), args.email.strip().lower())
    if duplicates["username_exists"] or duplicates["email_exists"]:
        raise SystemExit(
            f"Supabase profile already exists for username/email. username_exists={duplicates['username_exists']} email_exists={duplicates['email_exists']}"
        )

    auth_user_id = ""
    try:
        auth_payload = admin_create_user(
            email=args.email.strip().lower(),
            password=password,
            username=args.username.strip(),
            full_name=args.full_name.strip(),
            role="admin",
        )
        auth_user = auth_payload.get("user") or auth_payload
        auth_user_id = str((auth_user or {}).get("id") or "").strip()
    except SupabaseAuthError as exc:
        if "already been registered" not in str(exc).lower():
            raise SystemExit(f"Supabase admin user creation failed: {exc}") from exc
        auth_user_id = find_auth_user_id_by_email(args.email.strip().lower())
    if not auth_user_id:
        raise SystemExit("Could not resolve the Supabase auth user id for the admin account.")

    profile = upsert_app_user_profile(
        auth_user_id=auth_user_id,
        username=args.username.strip(),
        full_name=args.full_name.strip(),
        email=args.email.strip().lower(),
        role="admin",
        assigned_trade=str(legacy_user.get("assigned_trade") or "").strip(),
        assigned_year=str(legacy_user.get("assigned_year") or "").strip() or None,
        semester=None,
        district="",
        department="",
        status="active",
        created_by=auth_user_id,
    )
    ensure_legacy_user_record(profile)
    insert_user_registration_audit(
        created_user_id=auth_user_id,
        created_by=auth_user_id,
        created_role="admin",
        assigned_trade=str(legacy_user.get("assigned_trade") or "").strip(),
        assigned_year=str(legacy_user.get("assigned_year") or "").strip() or None,
        district="",
        action="bootstrap_admin_migration",
    )

    print("Legacy admin migrated successfully.")
    print(f"Username: {profile.get('username')}")
    print(f"Email: {profile.get('email')}")
    print(f"Auth User ID: {profile.get('id')}")
    print("Log out from Streamlit and log in again with the same username/password.")


if __name__ == "__main__":
    main()
