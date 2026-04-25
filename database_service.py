from __future__ import annotations

import json
import os
import random
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import IntegrityError as PsycopgIntegrityError
except Exception:  # pragma: no cover
    psycopg2 = None
    RealDictCursor = None
    PsycopgIntegrityError = Exception


DB_URL = (os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or "").strip()
LEGACY_PASSWORD_SENTINEL = "SUPABASE_AUTH_ONLY"


def _require_db_url() -> str:
    if not DB_URL:
        raise RuntimeError(
            "SUPABASE_DB_URL is not configured. Set it in .env to use Supabase Postgres."
        )
    return DB_URL


def _connect():
    if psycopg2 is None or RealDictCursor is None:
        raise RuntimeError(
            "psycopg2 is not installed. Run: pip install psycopg2-binary"
        )
    db_url = _require_db_url()
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)


def init_db() -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    assigned_trade TEXT,
                    assigned_year INTEGER,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS assigned_trade TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS assigned_year INTEGER
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS auth_user_id UUID
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS full_name TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS email TEXT
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_users_auth_user_id
                ON users(auth_user_id)
                WHERE auth_user_id IS NOT NULL
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email
                ON users(lower(email))
                WHERE email IS NOT NULL
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.app_users (
                    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
                    username TEXT UNIQUE NOT NULL,
                    full_name TEXT,
                    email TEXT UNIQUE,
                    role TEXT NOT NULL,
                    assigned_trade TEXT,
                    assigned_year TEXT,
                    semester TEXT,
                    district TEXT,
                    department TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_by UUID,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.user_registration_audit (
                    id BIGSERIAL PRIMARY KEY,
                    created_user_id UUID NOT NULL,
                    created_by UUID,
                    created_role TEXT NOT NULL,
                    assigned_trade TEXT,
                    assigned_year TEXT,
                    district TEXT,
                    action TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION public.set_updated_at()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = now();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
                """
            )
            cur.execute(
                """
                DROP TRIGGER IF EXISTS trg_app_users_set_updated_at ON public.app_users
                """
            )
            cur.execute(
                """
                CREATE TRIGGER trg_app_users_set_updated_at
                BEFORE UPDATE ON public.app_users
                FOR EACH ROW
                EXECUTE FUNCTION public.set_updated_at()
                """
            )
            cur.execute(
                """
                ALTER TABLE public.app_users ENABLE ROW LEVEL SECURITY
                """
            )
            cur.execute(
                """
                ALTER TABLE public.user_registration_audit ENABLE ROW LEVEL SECURITY
                """
            )
            cur.execute(
                """
                DROP POLICY IF EXISTS app_users_self_select ON public.app_users
                """
            )
            cur.execute(
                """
                CREATE POLICY app_users_self_select
                ON public.app_users
                FOR SELECT
                USING (auth.uid() = id)
                """
            )
            cur.execute(
                """
                DROP POLICY IF EXISTS app_users_manager_select ON public.app_users
                """
            )
            cur.execute(
                """
                CREATE POLICY app_users_manager_select
                ON public.app_users
                FOR SELECT
                USING (
                    EXISTS (
                        SELECT 1
                        FROM public.app_users me
                        WHERE me.id = auth.uid()
                          AND (
                              me.role = 'admin'
                              OR (
                                  me.role = 'principal'
                                  AND public.app_users.role IN ('trainer', 'ojt_trainer', 'supervisor', 'trainee')
                              )
                              OR (
                                  me.role = 'trainer'
                                  AND public.app_users.role = 'trainee'
                                  AND lower(coalesce(public.app_users.assigned_trade, '')) = lower(coalesce(me.assigned_trade, ''))
                              )
                          )
                    )
                )
                """
            )
            cur.execute(
                """
                DROP POLICY IF EXISTS audit_creator_select ON public.user_registration_audit
                """
            )
            cur.execute(
                """
                CREATE POLICY audit_creator_select
                ON public.user_registration_audit
                FOR SELECT
                USING (
                    created_by = auth.uid()
                    OR EXISTS (
                        SELECT 1
                        FROM public.app_users me
                        WHERE me.id = auth.uid()
                          AND me.role IN ('admin', 'principal')
                    )
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS question_sets (
                    id BIGSERIAL PRIMARY KEY,
                    trainer_user_id BIGINT NOT NULL REFERENCES users(id),
                    month TEXT NOT NULL,
                    district TEXT NOT NULL,
                    institute_name TEXT NOT NULL,
                    trade_name TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    semester INTEGER NOT NULL,
                    question_mode TEXT NOT NULL DEFAULT 'both',
                    target_question_count INTEGER NOT NULL DEFAULT 3,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS question_set_items (
                    id BIGSERIAL PRIMARY KEY,
                    question_set_id BIGINT NOT NULL REFERENCES question_sets(id),
                    subject TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    question TEXT NOT NULL,
                    question_source TEXT,
                    question_image TEXT,
                    option_a TEXT,
                    option_b TEXT,
                    option_c TEXT,
                    option_d TEXT,
                    correct_option TEXT,
                    answer_text TEXT
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE question_set_items
                ADD COLUMN IF NOT EXISTS question_image TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE question_set_items
                ADD COLUMN IF NOT EXISTS question_source TEXT
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS student_technical_feedback (
                    id BIGSERIAL PRIMARY KEY,
                    question_set_id BIGINT NOT NULL REFERENCES question_sets(id),
                    student_user_id BIGINT NOT NULL REFERENCES users(id),
                    submitted_at TEXT NOT NULL,
                    feedback_context_json TEXT NOT NULL,
                    responses_json TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS monthly_feedback_records (
                    id BIGSERIAL PRIMARY KEY,
                    month TEXT NOT NULL,
                    district TEXT NOT NULL,
                    trade_name TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    semester INTEGER NOT NULL,
                    attendance_pct DOUBLE PRECISION NOT NULL,
                    subject_1 TEXT NOT NULL,
                    topic_1 TEXT NOT NULL,
                    question_1 TEXT NOT NULL,
                    topic_1_score DOUBLE PRECISION NOT NULL,
                    subject_2 TEXT NOT NULL,
                    topic_2 TEXT NOT NULL,
                    question_2 TEXT NOT NULL,
                    topic_2_score DOUBLE PRECISION NOT NULL,
                    subject_3 TEXT NOT NULL,
                    topic_3 TEXT NOT NULL,
                    question_3 TEXT NOT NULL,
                    topic_3_score DOUBLE PRECISION NOT NULL,
                    teaching_score DOUBLE PRECISION NOT NULL,
                    practical_score DOUBLE PRECISION NOT NULL,
                    learning_score DOUBLE PRECISION NOT NULL,
                    support_score DOUBLE PRECISION NOT NULL,
                    safety_score DOUBLE PRECISION NOT NULL,
                    weak_topics TEXT NOT NULL,
                    comment_text TEXT NOT NULL,
                    sentiment_label TEXT NOT NULL,
                    sentiment_score DOUBLE PRECISION NOT NULL,
                    submitted_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS category_feedback_records (
                    id BIGSERIAL PRIMARY KEY,
                    submitted_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    form_id TEXT NOT NULL,
                    form_title TEXT NOT NULL,
                    basic_details_json TEXT NOT NULL,
                    parameter_scores_json TEXT NOT NULL,
                    comment_text TEXT NOT NULL,
                    excellent_count INTEGER NOT NULL,
                    good_count INTEGER NOT NULL,
                    average_count INTEGER NOT NULL,
                    poor_count INTEGER NOT NULL,
                    avg_rating_score DOUBLE PRECISION NOT NULL
                )
                """
            )
            cur.execute(
                """
                DELETE FROM student_technical_feedback f
                USING student_technical_feedback newer
                WHERE f.question_set_id = newer.question_set_id
                  AND f.student_user_id = newer.student_user_id
                  AND f.id < newer.id
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_student_qset_submission
                ON student_technical_feedback (question_set_id, student_user_id)
                """
            )
        conn.commit()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _clean_year(value: Any) -> str | None:
    cleaned = _clean_text(value)
    return cleaned or None


def _clean_status(value: Any) -> str:
    status = _clean_text(value).lower()
    return status or "active"


def get_app_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    username,
                    full_name,
                    email,
                    role,
                    assigned_trade,
                    assigned_year,
                    semester,
                    district,
                    department,
                    status,
                    created_by,
                    created_at,
                    updated_at
                FROM public.app_users
                WHERE lower(username) = lower(%s)
                LIMIT 1
                """,
                (username,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def get_app_user_by_auth_id(user_id: str) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    username,
                    full_name,
                    email,
                    role,
                    assigned_trade,
                    assigned_year,
                    semester,
                    district,
                    department,
                    status,
                    created_by,
                    created_at,
                    updated_at
                FROM public.app_users
                WHERE id = %s
                LIMIT 1
                """,
                (user_id,),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def find_app_user_duplicates(username: str, email: str) -> Dict[str, bool]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    EXISTS(SELECT 1 FROM public.app_users WHERE lower(username) = lower(%s)) AS username_exists,
                    EXISTS(SELECT 1 FROM public.app_users WHERE lower(email) = lower(%s)) AS email_exists
                """,
                (username, email),
            )
            row = cur.fetchone() or {}
    return {
        "username_exists": bool((row or {}).get("username_exists")),
        "email_exists": bool((row or {}).get("email_exists")),
    }


def upsert_app_user_profile(
    *,
    auth_user_id: str,
    username: str,
    full_name: str = "",
    email: str = "",
    role: str,
    assigned_trade: str = "",
    assigned_year: str | None = None,
    semester: str | None = None,
    district: str = "",
    department: str = "",
    status: str = "active",
    created_by: str | None = None,
) -> Dict[str, Any]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.app_users (
                    id,
                    username,
                    full_name,
                    email,
                    role,
                    assigned_trade,
                    assigned_year,
                    semester,
                    district,
                    department,
                    status,
                    created_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    username = EXCLUDED.username,
                    full_name = EXCLUDED.full_name,
                    email = EXCLUDED.email,
                    role = EXCLUDED.role,
                    assigned_trade = EXCLUDED.assigned_trade,
                    assigned_year = EXCLUDED.assigned_year,
                    semester = EXCLUDED.semester,
                    district = EXCLUDED.district,
                    department = EXCLUDED.department,
                    status = EXCLUDED.status,
                    created_by = COALESCE(public.app_users.created_by, EXCLUDED.created_by),
                    updated_at = now()
                RETURNING
                    id,
                    username,
                    full_name,
                    email,
                    role,
                    assigned_trade,
                    assigned_year,
                    semester,
                    district,
                    department,
                    status,
                    created_by,
                    created_at,
                    updated_at
                """,
                (
                    auth_user_id,
                    _clean_text(username),
                    _clean_text(full_name) or None,
                    _clean_text(email) or None,
                    _clean_text(role),
                    _clean_text(assigned_trade) or None,
                    _clean_year(assigned_year),
                    _clean_text(semester) or None,
                    _clean_text(district) or None,
                    _clean_text(department) or None,
                    _clean_status(status),
                    created_by,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row)


def insert_user_registration_audit(
    *,
    created_user_id: str,
    created_by: str | None,
    created_role: str,
    assigned_trade: str = "",
    assigned_year: str | None = None,
    district: str = "",
    action: str = "create_user",
) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.user_registration_audit (
                    created_user_id,
                    created_by,
                    created_role,
                    assigned_trade,
                    assigned_year,
                    district,
                    action
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    created_user_id,
                    created_by,
                    _clean_text(created_role),
                    _clean_text(assigned_trade) or None,
                    _clean_year(assigned_year),
                    _clean_text(district) or None,
                    _clean_text(action) or "create_user",
                ),
            )
        conn.commit()


def ensure_legacy_user_record(profile: Dict[str, Any]) -> Dict[str, Any]:
    username = _clean_text(profile.get("username"))
    role = _clean_text(profile.get("role"))
    assigned_trade = _clean_text(profile.get("assigned_trade"))
    assigned_year_raw = _clean_text(profile.get("assigned_year"))
    assigned_year_int = int(assigned_year_raw) if assigned_year_raw.isdigit() else None
    auth_user_id = profile.get("id")
    full_name = _clean_text(profile.get("full_name"))
    email = _clean_text(profile.get("email"))
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM users
                WHERE lower(username) = lower(%s)
                LIMIT 1
                """,
                (username,),
            )
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    """
                    UPDATE users
                    SET
                        auth_user_id = %s,
                        full_name = %s,
                        email = %s,
                        role = %s,
                        assigned_trade = %s,
                        assigned_year = %s
                    WHERE id = %s
                    RETURNING id, username, password_hash, role, assigned_trade, assigned_year, created_at, auth_user_id, full_name, email
                    """,
                    (
                        auth_user_id,
                        full_name or None,
                        email or None,
                        role,
                        assigned_trade or None,
                        assigned_year_int,
                        int(existing["id"]),
                    ),
                )
                row = cur.fetchone()
            else:
                cur.execute(
                    """
                    INSERT INTO users (
                        username,
                        password_hash,
                        role,
                        assigned_trade,
                        assigned_year,
                        created_at,
                        auth_user_id,
                        full_name,
                        email
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, username, password_hash, role, assigned_trade, assigned_year, created_at, auth_user_id, full_name, email
                    """,
                    (
                        username,
                        LEGACY_PASSWORD_SENTINEL,
                        role,
                        assigned_trade or None,
                        assigned_year_int,
                        now,
                        auth_user_id,
                        full_name or None,
                        email or None,
                    ),
                )
                row = cur.fetchone()
        conn.commit()
    return dict(row)


def list_recently_created_users(viewer_profile: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
    viewer_role = _clean_text((viewer_profile or {}).get("role")).lower()
    viewer_id = (viewer_profile or {}).get("id")
    viewer_trade = _clean_text((viewer_profile or {}).get("assigned_trade")).lower()
    if not viewer_role or not viewer_id:
        return []
    role_scope: tuple[str, ...]
    if viewer_role == "admin":
        role_scope = ("admin", "principal", "trainer", "ojt_trainer", "supervisor", "trainee")
    elif viewer_role == "principal":
        role_scope = ("trainer", "ojt_trainer", "supervisor", "trainee")
    elif viewer_role == "trainer":
        role_scope = ("trainee",)
    else:
        return []
    params: list[Any] = [list(role_scope)]
    trade_clause = ""
    if viewer_role == "trainer":
        trade_clause = " AND lower(coalesce(u.assigned_trade, '')) = lower(%s) "
        params.append(viewer_trade)
    params.append(int(limit))
    query = f"""
        SELECT
            u.id AS user_id,
            u.username,
            u.full_name,
            u.email,
            u.role,
            u.assigned_trade,
            u.assigned_year,
            u.semester,
            u.district,
            u.department,
            u.status,
            creator.username AS created_by,
            u.created_at
        FROM public.app_users u
        LEFT JOIN public.app_users creator ON creator.id = u.created_by
        WHERE u.role = ANY(%s)
        {trade_clause}
        ORDER BY u.created_at DESC
        LIMIT %s
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return [dict(row) for row in rows]


def create_user(
    username: str,
    password_hash: str,
    role: str,
    assigned_trade: str = "",
    assigned_year: int | None = None,
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (username, password_hash, role, assigned_trade, assigned_year, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (username, password_hash, role, assigned_trade or None, assigned_year, now),
            )
            user_id = int(cur.fetchone()["id"])
        conn.commit()
    return {
        "id": user_id,
        "username": username,
        "role": role,
        "assigned_trade": assigned_trade or "",
        "assigned_year": int(assigned_year) if assigned_year is not None else None,
        "created_at": now,
    }


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, username, password_hash, role, assigned_trade, assigned_year, created_at
                FROM users
                WHERE username = %s
                """,
                (username,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return dict(row)


def create_question_set(
    trainer_user_id: int,
    month: str,
    district: str,
    institute_name: str,
    trade_name: str,
    year: int,
    semester: int,
    question_mode: str,
    target_question_count: int,
    questions: List[Dict[str, Any]],
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO question_sets
                (trainer_user_id, month, district, institute_name, trade_name, year, semester, question_mode, target_question_count, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    trainer_user_id,
                    month,
                    district,
                    institute_name,
                    trade_name,
                    year,
                    semester,
                    question_mode,
                    int(target_question_count),
                    now,
                ),
            )
            qset_id = int(cur.fetchone()["id"])

            for item in questions:
                cur.execute(
                    """
                    INSERT INTO question_set_items
                    (question_set_id, subject, topic, question, question_source, question_image, option_a, option_b, option_c, option_d, correct_option, answer_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        qset_id,
                        item.get("subject", ""),
                        item.get("topic", ""),
                        item.get("question", ""),
                        item.get("question_source"),
                        item.get("question_image"),
                        item.get("option_a"),
                        item.get("option_b"),
                        item.get("option_c"),
                        item.get("option_d"),
                        item.get("correct_option"),
                        item.get("answer_text"),
                    ),
                )
        conn.commit()
    return qset_id


def get_latest_question_set(
    trade_name: str,
    year: int,
    semester: int,
    student_identity: str = "",
    student_user_id: int | None = None,
) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            if student_user_id is not None:
                cur.execute(
                    """
                    SELECT id, trainer_user_id, month, district, institute_name, trade_name, year, semester, question_mode, target_question_count, created_at
                    FROM question_sets q
                    WHERE lower(q.trade_name) = lower(%s)
                      AND q.year = %s
                      AND q.semester = %s
                      AND NOT EXISTS (
                          SELECT 1
                          FROM student_technical_feedback f
                          WHERE f.question_set_id = q.id
                            AND f.student_user_id = %s
                      )
                    ORDER BY q.id DESC
                    LIMIT 1
                    """,
                    (trade_name, year, semester, int(student_user_id)),
                )
            else:
                cur.execute(
                    """
                    SELECT id, trainer_user_id, month, district, institute_name, trade_name, year, semester, question_mode, target_question_count, created_at
                    FROM question_sets
                    WHERE lower(trade_name) = lower(%s) AND year = %s AND semester = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (trade_name, year, semester),
                )
            qset = cur.fetchone()
            if not qset:
                return None

            cur.execute(
                """
                SELECT subject, topic, question, question_source, question_image, option_a, option_b, option_c, option_d, correct_option, answer_text
                FROM question_set_items
                WHERE question_set_id = %s
                ORDER BY id ASC
                """,
                (int(qset["id"]),),
            )
            items = cur.fetchall()

    result = dict(qset)
    all_questions = [dict(r) for r in items]
    q_mode = str(result.get("question_mode") or "both").strip().lower()
    if q_mode == "mcq":
        q_mode = "practical"

    target_count = int(result.get("target_question_count") or len(all_questions) or 0)
    if target_count <= 0:
        target_count = len(all_questions)

    seed_text = f"{student_identity.strip().lower()}|{result.get('id')}"
    seed = int(hashlib.sha256(seed_text.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)

    if len(all_questions) <= target_count:
        selected = all_questions
    else:
        if q_mode == "both":
            def _question_source(item: Dict[str, Any]) -> str:
                source = str(item.get("question_source") or "").strip().lower()
                if source in {"theory", "practical"}:
                    return source
                subject = str(item.get("subject") or "").strip().lower()
                if "practical" in subject:
                    return "practical"
                return "theory"

            practical_questions = [q for q in all_questions if _question_source(q) == "practical"]
            theory_questions = [q for q in all_questions if _question_source(q) == "theory"]
            practical_target = target_count // 2
            theory_target = target_count - practical_target

            take_practical = min(practical_target, len(practical_questions))
            take_theory = min(theory_target, len(theory_questions))

            selected = []
            if take_practical > 0:
                selected.extend(rng.sample(practical_questions, take_practical))
            if take_theory > 0:
                selected.extend(rng.sample(theory_questions, take_theory))

            remaining = target_count - len(selected)
            if remaining > 0:
                remaining_pool = [q for q in all_questions if q not in selected]
                if remaining_pool:
                    selected.extend(rng.sample(remaining_pool, min(remaining, len(remaining_pool))))
            rng.shuffle(selected)
        else:
            selected = rng.sample(all_questions, target_count)

    result["questions"] = selected
    result["question_pool_size"] = len(all_questions)
    return result


def submit_student_technical_feedback(
    question_set_id: int,
    student_user_id: int,
    feedback_context: Dict[str, Any],
    responses: List[Dict[str, Any]],
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO student_technical_feedback
                    (question_set_id, student_user_id, submitted_at, feedback_context_json, responses_json)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        question_set_id,
                        student_user_id,
                        now,
                        json.dumps(feedback_context, ensure_ascii=False),
                        json.dumps(responses, ensure_ascii=False),
                    ),
                )
                feedback_id = int(cur.fetchone()["id"])
            conn.commit()
            return feedback_id
        except PsycopgIntegrityError as exc:
            conn.rollback()
            raise ValueError("Duplicate technical feedback submission is not allowed.") from exc


def fetch_technical_feedback_rows() -> List[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    f.id AS feedback_id,
                    f.submitted_at,
                    f.feedback_context_json,
                    f.responses_json,
                    q.id AS question_set_id,
                    q.month,
                    q.district,
                    q.institute_name,
                    q.trade_name,
                    q.year,
                    q.semester
                FROM student_technical_feedback f
                JOIN question_sets q ON q.id = f.question_set_id
                ORDER BY f.id DESC
                """
            )
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_question_set_item_rows() -> List[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT question_set_id, subject, topic, question, correct_option, answer_text
                FROM question_set_items
                """
            )
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def exists_monthly_feedback_duplicate(record: Dict[str, Any]) -> bool:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM monthly_feedback_records
                WHERE month = %s
                  AND lower(district) = lower(%s)
                  AND lower(trade_name) = lower(%s)
                  AND year = %s
                  AND semester = %s
                  AND subject_1 = %s AND topic_1 = %s AND question_1 = %s AND topic_1_score = %s
                  AND subject_2 = %s AND topic_2 = %s AND question_2 = %s AND topic_2_score = %s
                  AND subject_3 = %s AND topic_3 = %s AND question_3 = %s AND topic_3_score = %s
                  AND teaching_score = %s AND practical_score = %s AND learning_score = %s AND support_score = %s AND safety_score = %s
                  AND comment_text = %s
                LIMIT 1
                """,
                (
                    str(record.get("month", "")),
                    str(record.get("district", "")),
                    str(record.get("trade_name", "")),
                    int(record.get("year", 0)),
                    int(record.get("semester", 0)),
                    str(record.get("subject_1", "")),
                    str(record.get("topic_1", "")),
                    str(record.get("question_1", "")),
                    float(record.get("topic_1_score", 0.0)),
                    str(record.get("subject_2", "")),
                    str(record.get("topic_2", "")),
                    str(record.get("question_2", "")),
                    float(record.get("topic_2_score", 0.0)),
                    str(record.get("subject_3", "")),
                    str(record.get("topic_3", "")),
                    str(record.get("question_3", "")),
                    float(record.get("topic_3_score", 0.0)),
                    float(record.get("teaching_score", 0.0)),
                    float(record.get("practical_score", 0.0)),
                    float(record.get("learning_score", 0.0)),
                    float(record.get("support_score", 0.0)),
                    float(record.get("safety_score", 0.0)),
                    str(record.get("comment_text", "")),
                ),
            )
            return cur.fetchone() is not None


def insert_monthly_feedback_record(record: Dict[str, Any]) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO monthly_feedback_records (
                    month, district, trade_name, year, semester, attendance_pct,
                    subject_1, topic_1, question_1, topic_1_score,
                    subject_2, topic_2, question_2, topic_2_score,
                    subject_3, topic_3, question_3, topic_3_score,
                    teaching_score, practical_score, learning_score, support_score, safety_score,
                    weak_topics, comment_text, sentiment_label, sentiment_score, submitted_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """,
                (
                    str(record.get("month", "")),
                    str(record.get("district", "")),
                    str(record.get("trade_name", "")),
                    int(record.get("year", 0)),
                    int(record.get("semester", 0)),
                    float(record.get("attendance_pct", 0.0)),
                    str(record.get("subject_1", "")),
                    str(record.get("topic_1", "")),
                    str(record.get("question_1", "")),
                    float(record.get("topic_1_score", 0.0)),
                    str(record.get("subject_2", "")),
                    str(record.get("topic_2", "")),
                    str(record.get("question_2", "")),
                    float(record.get("topic_2_score", 0.0)),
                    str(record.get("subject_3", "")),
                    str(record.get("topic_3", "")),
                    str(record.get("question_3", "")),
                    float(record.get("topic_3_score", 0.0)),
                    float(record.get("teaching_score", 0.0)),
                    float(record.get("practical_score", 0.0)),
                    float(record.get("learning_score", 0.0)),
                    float(record.get("support_score", 0.0)),
                    float(record.get("safety_score", 0.0)),
                    str(record.get("weak_topics", "")),
                    str(record.get("comment_text", "")),
                    str(record.get("sentiment_label", "")),
                    float(record.get("sentiment_score", 0.0)),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        conn.commit()


def fetch_monthly_feedback_rows() -> List[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM monthly_feedback_records ORDER BY id DESC")
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def exists_category_feedback_duplicate(record: Dict[str, Any]) -> bool:
    basics = json.dumps(record.get("basic_details", {}), sort_keys=True, ensure_ascii=False)
    params = json.dumps(record.get("parameter_scores", []), sort_keys=True, ensure_ascii=False)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM category_feedback_records
                WHERE lower(source) = lower(%s)
                  AND lower(form_id) = lower(%s)
                  AND lower(form_title) = lower(%s)
                  AND basic_details_json = %s
                  AND parameter_scores_json = %s
                  AND comment_text = %s
                LIMIT 1
                """,
                (
                    str(record.get("source", "")),
                    str(record.get("form_id", "")),
                    str(record.get("form_title", "")),
                    basics,
                    params,
                    str(record.get("comment_text", "")),
                ),
            )
            return cur.fetchone() is not None


def insert_category_feedback_record(record: Dict[str, Any]) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO category_feedback_records (
                    submitted_at, source, form_id, form_title, basic_details_json, parameter_scores_json,
                    comment_text, excellent_count, good_count, average_count, poor_count, avg_rating_score
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(record.get("submitted_at", datetime.now(timezone.utc).isoformat())),
                    str(record.get("source", "")),
                    str(record.get("form_id", "")),
                    str(record.get("form_title", "")),
                    json.dumps(record.get("basic_details", {}), ensure_ascii=False),
                    json.dumps(record.get("parameter_scores", []), ensure_ascii=False),
                    str(record.get("comment_text", "")),
                    int(record.get("excellent_count", 0)),
                    int(record.get("good_count", 0)),
                    int(record.get("average_count", 0)),
                    int(record.get("poor_count", 0)),
                    float(record.get("avg_rating_score", 0.0)),
                ),
            )
        conn.commit()


def fetch_category_feedback_rows() -> List[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM category_feedback_records ORDER BY id DESC")
            rows = cur.fetchall()
    return [dict(r) for r in rows]
