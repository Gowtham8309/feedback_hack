from __future__ import annotations

import json
import hashlib
import os
import random
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Header, HTTPException, Query

from models import (
    CategoryFeedbackRecord,
    CategoryFeedbackSubmission,
    MonthlyTopicInput,
    QuestionGenerationResponse,
    MonthlyFeedbackSubmission,
    ProcessedFeedbackRecord,
    DashboardSummary,
    RecentlyCreatedUserRow,
    RegisteredUserSummary,
    UserRegisterRequest,
    UserRegisterResponse,
    UserLoginRequest,
    UserAuthResponse,
    TrainerQuestionSetRequest,
    StudentLatestQuestionSetRequest,
    StudentTechnicalFeedbackRequest,
    QuestionBankIngestRequest,
)
from question_generator import (
    generate_questions,
    generate_question_for_subject_topic,
    generate_theory_question_for_subject_topic,
    ingest_question_bank_rows,
)
from sentiment_service import analyze_sentiment, map_score_to_final_label
from storage import (
    append_category_feedback,
    append_feedback,
    init_storage,
    is_duplicate_monthly_feedback,
    is_duplicate_category_feedback,
)
from dashboard_service import get_dashboard_summary, get_dashboard_trend
from category_dashboard_service import get_category_rows, get_category_summary, get_category_trend
from authority_dashboard_service import (
    get_authority_combined_summary,
    get_authority_combined_trend,
    get_technical_feedback_rows_summary,
    get_technical_summary,
    get_technical_trend,
)
from database_service import (
    ensure_legacy_user_record,
    find_app_user_duplicates,
    get_app_user_by_auth_id,
    get_app_user_by_username,
    init_db,
    create_user,
    get_user_by_username,
    insert_user_registration_audit,
    list_recently_created_users,
    upsert_app_user_profile,
    create_question_set,
    get_latest_question_set,
    submit_student_technical_feedback,
)
from llm_analysis_service import evaluate_theory_responses_with_groq
from supabase_auth_service import (
    SupabaseAuthError,
    admin_create_user,
    get_user_from_token,
    sign_in_with_password,
)


app = FastAPI(
    title="ITI Monthly Feedback System",
    version="1.0.0",
)


TEXT_SENTIMENT_WEIGHT = 0.6
RATING_SENTIMENT_WEIGHT = 0.4
RATING_TO_SCORE = {"Excellent": 4.0, "Good": 3.0, "Average": 2.0, "Poor": 1.0}
BASE_DIR = Path(__file__).resolve().parent
FEEDBACK_TEMPLATES_JSON = BASE_DIR / "data" / "feedback_form_templates.json"
ROLE_ALLOWED_FORM_IDS = {
    "iti_trainee": {"iti_trainee_feedback_form"},
    "student": {"iti_trainee_feedback_form", "student_feedback_form_in_plant_training"},
    "trainer": {"iti_training_officer_feedback_form"},
    "principal": {"principal_feedback_form"},
    "ojt_institute_officer": {"institute_feedback_form_in_plant_training_ato_dto_to"},
    "ojt_supervisor": {"supervisor_feedback_form_iti_in_plant_training_evaluation"},
    "admin": set(),
}
LOGIN_ALLOWED_ROLES = set(ROLE_ALLOWED_FORM_IDS.keys())
TRAINER_TECH_ROLES = {"trainer"}
STUDENT_TECH_ROLES = {"student", "iti_trainee", "trainee"}
PUBLIC_SELF_REGISTER_ROLES = {"student", "iti_trainee"}
ADMIN_MANAGED_REGISTER_ROLES = {"trainer", "principal", "ojt_institute_officer", "ojt_supervisor"}
TRAINER_CAN_CREATE_TRAINEE = (os.getenv("TRAINER_CAN_CREATE_TRAINEE") or "false").strip().lower() in {"1", "true", "yes", "on"}
CANONICAL_ROLE_ALIASES = {
    "admin": "admin",
    "principal": "principal",
    "trainer": "trainer",
    "ojt_trainer": "ojt_trainer",
    "ojt_institute_officer": "ojt_trainer",
    "supervisor": "supervisor",
    "ojt_supervisor": "supervisor",
    "trainee": "trainee",
    "student": "trainee",
    "iti_trainee": "trainee",
}


def _score_from_ratings(
    teaching: float,
    practical: float,
    learning: float,
    support: float,
    safety: float,
) -> float:
    avg_rating = (teaching + practical + learning + support + safety) / 5.0
    # Map 1..5 to -1..1 where 3.0 is neutral.
    return round((avg_rating - 3.0) / 2.0, 4)


@lru_cache(maxsize=1)
def _load_feedback_templates() -> list[dict]:
    if not FEEDBACK_TEMPLATES_JSON.exists():
        return []
    with open(FEEDBACK_TEMPLATES_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    return data


@app.on_event("startup")
def startup() -> None:
    init_db()


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _normalize_auth_role(value: str) -> str:
    return CANONICAL_ROLE_ALIASES.get((value or "").strip().lower(), (value or "").strip().lower())


def _assigned_year_int(value: object) -> int | None:
    text = str(value or "").strip()
    return int(text) if text.isdigit() else None


def _build_auth_response(user: dict, message: str) -> UserAuthResponse:
    return UserAuthResponse(
        user_id=str(user.get("auth_user_id") or user.get("id") or ""),
        username=str(user.get("username") or ""),
        role=str(_normalize_auth_role(str(user.get("role") or ""))),
        full_name=str(user.get("full_name") or "") or None,
        email=str(user.get("email") or "") or None,
        assigned_trade=str(user.get("assigned_trade") or "") or None,
        assigned_year=_assigned_year_int(user.get("assigned_year")),
        semester=str(user.get("semester") or "") or None,
        district=str(user.get("district") or "") or None,
        department=str(user.get("department") or "") or None,
        status=str(user.get("status") or "") or None,
        access_token=str(user.get("access_token") or "") or None,
        refresh_token=str(user.get("refresh_token") or "") or None,
        message=message,
    )


def _authenticate(username: str, password: str) -> dict:
    clean_username = username.strip()
    clean_password = password.strip()
    profile = get_app_user_by_username(clean_username)
    if profile and str(profile.get("email") or "").strip():
        try:
            session = sign_in_with_password(email=str(profile.get("email")).strip(), password=clean_password)
        except SupabaseAuthError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        legacy_user = ensure_legacy_user_record(profile)
        combined = dict(profile)
        combined["id"] = int(legacy_user["id"])
        combined["auth_user_id"] = str(profile.get("id") or "")
        combined["access_token"] = session.get("access_token")
        combined["refresh_token"] = session.get("refresh_token")
        return combined

    user = get_user_by_username(clean_username)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    password_hash = str(user.get("password_hash") or "")
    if password_hash == "SUPABASE_AUTH_ONLY":
        raise HTTPException(status_code=401, detail="This account must sign in through Supabase-backed login.")
    if password_hash != _hash_password(clean_password):
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    user["auth_user_id"] = str(user.get("auth_user_id") or "")
    return user


def _extract_bearer_token(authorization: str | None) -> str:
    raw = (authorization or "").strip()
    if not raw.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid bearer token.")
    token = raw.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing or invalid bearer token.")
    return token


def _require_authenticated_profile(authorization: str | None) -> dict:
    token = _extract_bearer_token(authorization)
    try:
        auth_user = get_user_from_token(token)
    except SupabaseAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    auth_user_id = str(auth_user.get("id") or "")
    profile = get_app_user_by_auth_id(auth_user_id)
    if not profile:
        raise HTTPException(status_code=403, detail="No app profile is linked to this authenticated user.")
    profile["role"] = _normalize_auth_role(str(profile.get("role") or ""))
    profile["access_token"] = token
    return profile


def _can_create_role(creator_role: str, target_role: str) -> bool:
    creator = _normalize_auth_role(creator_role)
    target = _normalize_auth_role(target_role)
    if creator == "admin":
        return target in {"admin", "principal", "trainer", "ojt_trainer", "supervisor", "trainee"}
    return False


def _validate_registration_payload(payload: UserRegisterRequest, creator_profile: dict | None) -> tuple[str, str, str | None]:
    username = payload.username.strip()
    email = payload.email.strip().lower()
    password = payload.password.strip()
    role = _normalize_auth_role(payload.role)
    assigned_trade = (payload.assigned_trade or "").strip()
    assigned_year = (payload.assigned_year or "").strip()
    semester = (payload.semester or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="Username is required.")
    if not email:
        raise HTTPException(status_code=400, detail="Email is required.")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters long.")
    if role not in {"admin", "principal", "trainer", "ojt_trainer", "supervisor", "trainee"}:
        raise HTTPException(status_code=400, detail="Invalid role selected.")
    if creator_profile is None:
        if role != "trainee":
            raise HTTPException(status_code=403, detail="Only trainee self-registration is allowed without admin login.")
    elif not _can_create_role(str(creator_profile.get("role") or ""), role):
        raise HTTPException(status_code=403, detail="You are not allowed to create this role.")
    if role in {"trainer", "ojt_trainer", "trainee"} and not assigned_trade:
        raise HTTPException(status_code=400, detail="Assigned trade is required for trainer, OJT trainer, and trainee.")
    if role == "supervisor" and not ((payload.department or "").strip() or assigned_trade):
        raise HTTPException(status_code=400, detail="Department or trade scope is required for supervisor.")
    if role == "trainee":
        if not assigned_year:
            raise HTTPException(status_code=400, detail="Year is required for trainee.")
        if not semester:
            raise HTTPException(status_code=400, detail="Semester is required for trainee.")
    return username, email, role


def _normalize_trade_name(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _require_trainer_trade_access(user: dict, trade_name: str) -> None:
    role = str(user.get("role", "")).lower()
    if role == "admin":
        return
    assigned_trade = _normalize_trade_name(str(user.get("assigned_trade", "")))
    target_trade = _normalize_trade_name(trade_name)
    if role != "trainer":
        raise HTTPException(status_code=403, detail="Only trainer/admin can access trainer functions.")
    if not assigned_trade:
        raise HTTPException(status_code=403, detail="Trainer has no assigned trade. Register or update the trainer with a trade assignment.")
    if target_trade and target_trade != assigned_trade:
        raise HTTPException(
            status_code=403,
            detail=f"Trainer is assigned to trade '{user.get('assigned_trade')}' and cannot manage trade '{trade_name}'.",
        )


def _require_student_scope_access(user: dict, trade_name: str, year: int | None = None) -> None:
    role = str(user.get("role", "")).lower()
    if role not in STUDENT_TECH_ROLES:
        raise HTTPException(status_code=403, detail="Only student can access student functions.")
    assigned_trade = _normalize_trade_name(str(user.get("assigned_trade", "")))
    target_trade = _normalize_trade_name(trade_name)
    if assigned_trade and target_trade and target_trade != assigned_trade:
        raise HTTPException(
            status_code=403,
            detail=f"Student is assigned to trade '{user.get('assigned_trade')}' and cannot access trade '{trade_name}'.",
        )
    assigned_year = user.get("assigned_year")
    assigned_year_int = int(assigned_year) if assigned_year is not None and str(assigned_year).isdigit() else None
    if assigned_year_int is not None and year is not None and int(year) != assigned_year_int:
        raise HTTPException(
            status_code=403,
            detail=f"Student is assigned to year '{assigned_year_int}' and cannot access year '{year}'.",
        )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/register-user", response_model=UserRegisterResponse)
def register_user(payload: UserRegisterRequest, authorization: str | None = Header(default=None)):
    creator_profile = _require_authenticated_profile(authorization) if (authorization or "").strip() else None
    username, email, role = _validate_registration_payload(payload, creator_profile)
    duplicates = find_app_user_duplicates(username, email)
    if duplicates["username_exists"]:
        raise HTTPException(status_code=409, detail="Username already exists.")
    if duplicates["email_exists"]:
        raise HTTPException(status_code=409, detail="Email already exists.")
    try:
        auth_result = admin_create_user(
            email=email,
            password=payload.password.strip(),
            username=username,
            full_name=(payload.full_name or "").strip(),
            role=role,
        )
    except SupabaseAuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    created_auth_user = auth_result.get("user") or auth_result
    created_user_id = str((created_auth_user or {}).get("id") or "")
    if not created_user_id:
        raise HTTPException(status_code=500, detail="Supabase did not return a created user id.")
    profile = upsert_app_user_profile(
        auth_user_id=created_user_id,
        username=username,
        full_name=(payload.full_name or "").strip(),
        email=email,
        role=role,
        assigned_trade=(payload.assigned_trade or "").strip(),
        assigned_year=(payload.assigned_year or "").strip() or None,
        semester=(payload.semester or "").strip() or None,
        district=(payload.district or "").strip(),
        department=((payload.department or "").strip() or (payload.assigned_trade or "").strip()),
        status=(payload.status or "active").strip() or "active",
        created_by=str((creator_profile or {}).get("id") or "") or None,
    )
    ensure_legacy_user_record(profile)
    insert_user_registration_audit(
        created_user_id=created_user_id,
        created_by=str((creator_profile or {}).get("id") or "") or None,
        created_role=role,
        assigned_trade=(payload.assigned_trade or "").strip(),
        assigned_year=(payload.assigned_year or "").strip() or None,
        district=(payload.district or "").strip(),
        action="self_register" if creator_profile is None else "create_user",
    )
    return UserRegisterResponse(
        message="User account created successfully.",
        user=RegisteredUserSummary(
            user_id=created_user_id,
            username=str(profile.get("username") or ""),
            full_name=str(profile.get("full_name") or "") or None,
            email=str(profile.get("email") or "") or None,
            role=str(profile.get("role") or ""),
            assigned_trade=str(profile.get("assigned_trade") or "") or None,
            assigned_year=str(profile.get("assigned_year") or "") or None,
            semester=str(profile.get("semester") or "") or None,
            district=str(profile.get("district") or "") or None,
            department=str(profile.get("department") or "") or None,
            status=str(profile.get("status") or "") or None,
            created_by=str((creator_profile or {}).get("username") or "") or None,
            created_at=str(profile.get("created_at") or ""),
        ),
    )


@app.get("/register-user/recent", response_model=list[RecentlyCreatedUserRow])
def get_recently_created_users(authorization: str | None = Header(default=None), limit: int = Query(default=10, ge=1, le=25)):
    creator_profile = _require_authenticated_profile(authorization)
    rows = list_recently_created_users(creator_profile, limit=limit)
    return [
        RecentlyCreatedUserRow(
            user_id=str(row.get("user_id") or ""),
            username=str(row.get("username") or ""),
            full_name=str(row.get("full_name") or "") or None,
            email=str(row.get("email") or "") or None,
            role=str(row.get("role") or ""),
            assigned_trade=str(row.get("assigned_trade") or "") or None,
            assigned_year=str(row.get("assigned_year") or "") or None,
            semester=str(row.get("semester") or "") or None,
            district=str(row.get("district") or "") or None,
            department=str(row.get("department") or "") or None,
            status=str(row.get("status") or "") or None,
            created_by=str(row.get("created_by") or "") or None,
            created_at=str(row.get("created_at") or ""),
        )
        for row in rows
    ]


@app.post("/auth/login", response_model=UserAuthResponse)
def login_user(payload: UserLoginRequest):
    user = _authenticate(payload.username, payload.password)
    return _build_auth_response(user, "Login successful.")


@app.post("/trainer/generate-question-set")
def trainer_generate_question_set(payload: TrainerQuestionSetRequest):
    user = _authenticate(payload.username, payload.password)
    if str(user.get("role", "")).lower() not in TRAINER_TECH_ROLES:
        raise HTTPException(status_code=403, detail="Only trainer can generate question sets.")
    _require_trainer_trade_access(user, payload.trade_name)

    subject_topic_pairs = [
        (payload.subject_1, payload.topic_1),
        (payload.subject_2, payload.topic_2),
        (payload.subject_3, payload.topic_3),
    ]
    questions = []
    pool = payload.question_pool or []
    q_mode = (payload.question_mode or "both").strip().lower()
    if q_mode == "mcq":
        q_mode = "practical"

    def _mode_source(mode_value: str, subject: str) -> str:
        normalized_mode = (mode_value or "").strip().lower()
        if normalized_mode in {"theory", "practical"}:
            return normalized_mode
        subject_lower = subject.strip().lower()
        if "practical" in subject_lower:
            return "practical"
        return "theory"

    def _generate_by_mode(subject: str, topic: str, desired: str) -> dict:
        desired_source = _mode_source(desired, subject)
        uploaded = _pick_from_uploaded_pool(subject=subject, topic=topic, desired_source=desired_source)
        if uploaded:
            return uploaded
        raise HTTPException(
            status_code=400,
            detail=(
                f"No uploaded {desired_source} question found for topic '{topic}' in trade '{payload.trade_name}'. "
                "Upload matching Trade Theory / Practical bank questions before generating the test."
            ),
        )

    def _pick_from_uploaded_pool(subject: str, topic: str, desired_source: str) -> dict | None:
        if not pool:
            return None
        t = topic.strip().lower()
        trade = payload.trade_name.strip().lower()
        candidates = []
        for row in pool:
            row_topic = str(row.get("topic", "")).strip().lower()
            row_question = str(row.get("question_text", "")).strip()
            if not row_question:
                continue
            if t and t not in row_topic and row_topic not in t:
                continue
            row_trade = str(row.get("trade", "") or row.get("trade_name", "")).strip().lower()
            if trade and row_trade and row_trade != trade:
                continue
            row_source = str(row.get("source", "")).strip().lower()
            if desired_source and row_source and row_source != desired_source:
                continue
            candidates.append(row)
        if not candidates:
            return None
        chosen = random.choice(candidates)
        return {
            "question": str(chosen.get("question_text", "")).strip(),
            "question_source": str(chosen.get("source", "")).strip().lower() or desired_source,
            "question_image": str(chosen.get("question_image", "") or chosen.get("image", "") or chosen.get("image_url", "")).strip() or None,
            "option_a": str(chosen.get("option_a", "")).strip() or None,
            "option_b": str(chosen.get("option_b", "")).strip() or None,
            "option_c": str(chosen.get("option_c", "")).strip() or None,
            "option_d": str(chosen.get("option_d", "")).strip() or None,
            "correct_option": str(chosen.get("correct_option", "")).strip() or None,
            "answer_text": str(chosen.get("answer_text", "")).strip() or None,
        }
    for subject, topic in subject_topic_pairs:
        s = (subject or "").strip()
        t = (topic or "").strip()
        if not s or not t:
            continue
        seed_mode = "auto" if q_mode == "both" else q_mode
        generated = _generate_by_mode(subject=s, topic=t, desired=seed_mode)
        questions.append(
            {
                "subject": s,
                "topic": t,
                "question": generated.get("question", ""),
                "question_image": generated.get("question_image"),
                "option_a": generated.get("option_a"),
                "option_b": generated.get("option_b"),
                "option_c": generated.get("option_c"),
                "option_d": generated.get("option_d"),
                "correct_option": generated.get("correct_option"),
                "answer_text": generated.get("answer_text"),
            }
        )
    if not questions:
        raise HTTPException(status_code=400, detail="At least one subject/topic is required.")

    # Expand questions up to trainer-requested count using round-robin over selected subject/topic pairs.
    base_pairs = [(q["subject"], q["topic"]) for q in questions]
    final_questions = []
    seen_keys: set[tuple[str, str, str]] = set()
    pool_target = int(payload.question_count) if q_mode == "theory" else max(int(payload.question_count), int(payload.question_count) * 3)
    for i in range(pool_target):
        subject, topic = base_pairs[i % len(base_pairs)]
        if q_mode == "both":
            desired = "theory" if i % 2 == 0 else "practical"
        else:
            desired = q_mode
        generated = _generate_by_mode(subject=subject, topic=topic, desired=desired)
        q_text = str(generated.get("question", "")).strip()

        # Try a few times to avoid duplicates when bank has multiple questions for same topic.
        attempts = 0
        key = (subject, topic, q_text)
        while key in seen_keys and attempts < 4:
            generated = _generate_by_mode(subject=subject, topic=topic, desired=desired)
            q_text = str(generated.get("question", "")).strip()
            key = (subject, topic, q_text)
            attempts += 1
        seen_keys.add(key)

        final_questions.append(
            {
                "subject": subject,
                "topic": topic,
                "question": q_text,
                "question_source": generated.get("question_source"),
                "question_image": generated.get("question_image"),
                "option_a": generated.get("option_a"),
                "option_b": generated.get("option_b"),
                "option_c": generated.get("option_c"),
                "option_d": generated.get("option_d"),
                "correct_option": generated.get("correct_option"),
                "answer_text": generated.get("answer_text"),
            }
        )

    question_set_id = create_question_set(
        trainer_user_id=int(user["id"]),
        month=payload.month,
        district=payload.district,
        institute_name=payload.institute_name,
        trade_name=payload.trade_name,
        year=payload.year,
        semester=payload.semester,
        question_mode=q_mode,
        target_question_count=int(payload.question_count),
        questions=final_questions,
    )
    return {
        "question_set_id": question_set_id,
        "month": payload.month,
        "district": payload.district,
        "trade_name": payload.trade_name,
        "year": payload.year,
        "semester": payload.semester,
        "question_mode": q_mode,
        "target_question_count": int(payload.question_count),
        "question_pool_size": len(final_questions),
        "questions": final_questions,
    }


@app.post("/trainer/ingest-question-bank")
def trainer_ingest_question_bank(payload: QuestionBankIngestRequest):
    user = _authenticate(payload.username, payload.password)
    if str(user.get("role", "")).lower() not in TRAINER_TECH_ROLES:
        raise HTTPException(status_code=403, detail="Only trainer can ingest question banks.")
    declared_trade = (payload.default_trade or "").strip()
    row_trades = {
        _normalize_trade_name(str(row.get("trade", "") or row.get("trade_name", "")))
        for row in payload.rows
        if isinstance(row, dict)
    }
    row_trades.discard("")
    if declared_trade:
        _require_trainer_trade_access(user, declared_trade)
    elif len(row_trades) == 1:
        _require_trainer_trade_access(user, next(iter(row_trades)))
    elif row_trades:
        raise HTTPException(status_code=400, detail="Mixed trade uploads are not allowed in one trainer ingest request.")

    if not payload.rows:
        raise HTTPException(status_code=400, detail="No parsed rows provided for ingestion.")

    result = ingest_question_bank_rows(
        rows=payload.rows,
        default_source=(payload.default_source or ""),
        default_trade=(payload.default_trade or ""),
        default_year_level=(payload.default_year_level or ""),
        default_month=(payload.default_month or ""),
    )
    return {
        "message": "Question bank ingestion completed.",
        "inserted": int(result.get("inserted", 0)),
        "skipped": int(result.get("skipped", 0)),
        "total": int(result.get("total", 0)),
    }


@app.post("/student/latest-question-set")
def student_latest_question_set(payload: StudentLatestQuestionSetRequest):
    user = _authenticate(payload.username, payload.password)
    if str(user.get("role", "")).lower() not in STUDENT_TECH_ROLES:
        raise HTTPException(status_code=403, detail="Only student can view student question sets.")
    _require_student_scope_access(user, payload.trade_name, payload.year)

    qset = get_latest_question_set(
        trade_name=payload.trade_name,
        year=payload.year,
        semester=payload.semester,
        student_identity=payload.username,
        student_user_id=int(user["id"]),
    )
    if not qset:
        raise HTTPException(
            status_code=404,
            detail="No new trainer-generated question set is available for your assigned trade/year/semester.",
        )
    return qset


@app.post("/student/submit-technical-feedback")
def student_submit_technical_feedback(payload: StudentTechnicalFeedbackRequest):
    user = _authenticate(payload.username, payload.password)
    if str(user.get("role", "")).lower() not in STUDENT_TECH_ROLES:
        raise HTTPException(status_code=403, detail="Only student can submit student technical feedback.")
    _require_student_scope_access(user, payload.trade_name, payload.year)
    if not payload.responses:
        raise HTTPException(status_code=400, detail="At least one technical response is required.")

    feedback_context = {
        "month": payload.month,
        "district": payload.district,
        "institute_name": payload.institute_name,
        "trade_name": payload.trade_name,
        "year": payload.year,
        "semester": payload.semester,
    }
    response_dicts = [r.model_dump() for r in payload.responses]

    theory_items = []
    for idx, r in enumerate(response_dicts, start=1):
        selected_option = str(r.get("selected_option", "") or "").strip()
        answer_text = str(r.get("response_text", "") or "").strip()
        if not selected_option and answer_text:
            theory_items.append(
                {
                    "index": idx,
                    "question": str(r.get("question", "")).strip(),
                    "answer": answer_text,
                }
            )

    llm_eval = evaluate_theory_responses_with_groq(theory_items) if theory_items else {"ok": True, "results": []}
    eval_map = {
        int(x.get("index", 0)): x
        for x in llm_eval.get("results", [])
        if isinstance(x, dict)
    }
    theory_scores: list[float] = []
    for idx, r in enumerate(response_dicts, start=1):
        e = eval_map.get(idx)
        if e:
            r["theory_llm_score"] = float(e.get("score", 0.0))
            r["theory_llm_feedback"] = str(e.get("feedback", ""))
            r["theory_llm_key_points"] = e.get("key_points", [])
            theory_scores.append(float(e.get("score", 0.0)))

    try:
        feedback_id = submit_student_technical_feedback(
            question_set_id=payload.question_set_id,
            student_user_id=int(user["id"]),
            feedback_context=feedback_context,
            responses=response_dicts,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {
        "message": "Student technical feedback submitted successfully.",
        "feedback_id": feedback_id,
        "question_set_id": payload.question_set_id,
        "student_user_id": int(user["id"]),
        "response_count": len(payload.responses),
        "theory_answers_evaluated": len(theory_items),
        "theory_llm_avg_score": round(sum(theory_scores) / len(theory_scores), 2) if theory_scores else None,
        "theory_llm_status": "ok" if llm_eval.get("ok") else "failed",
        "theory_llm_error": llm_eval.get("error"),
    }


@app.post("/generate-questions", response_model=QuestionGenerationResponse)
def generate_monthly_questions(payload: MonthlyTopicInput):
    """
    Generate subject/topic-related monthly questions
    based on the topics taught in that month.
    """
    return generate_questions(payload)


@app.post("/submit-feedback", response_model=ProcessedFeedbackRecord)
def submit_feedback(payload: MonthlyFeedbackSubmission):
    """
    Submit one monthly student feedback record.
    Also calculates weak topics and sentiment.
    """
    weak_topics = []

    if payload.topic_1_score <= 2.5:
        weak_topics.append(payload.topic_1)
    if payload.topic_2_score <= 2.5:
        weak_topics.append(payload.topic_2)
    if payload.topic_3_score <= 2.5:
        weak_topics.append(payload.topic_3)

    weak_topics_text = "; ".join(sorted(set(weak_topics)))

    # Run sentiment analysis on comment text
    sentiment = analyze_sentiment(payload.comment_text or "")
    rating_score = _score_from_ratings(
        payload.teaching_score,
        payload.practical_score,
        payload.learning_score,
        payload.support_score,
        payload.safety_score,
    )
    blended_score = round(
        (TEXT_SENTIMENT_WEIGHT * float(sentiment["sentiment_score"]))
        + (RATING_SENTIMENT_WEIGHT * rating_score),
        4,
    )
    blended_label = map_score_to_final_label(blended_score)

    record = ProcessedFeedbackRecord(
        month=payload.month,
        district=payload.district,
        trade_name=payload.trade_name,
        year=payload.year,
        semester=payload.semester,
        attendance_pct=payload.attendance_pct,

        subject_1=payload.subject_1,
        topic_1=payload.topic_1,
        question_1=payload.question_1,
        topic_1_score=payload.topic_1_score,

        subject_2=payload.subject_2,
        topic_2=payload.topic_2,
        question_2=payload.question_2,
        topic_2_score=payload.topic_2_score,

        subject_3=payload.subject_3,
        topic_3=payload.topic_3,
        question_3=payload.question_3,
        topic_3_score=payload.topic_3_score,

        teaching_score=payload.teaching_score,
        practical_score=payload.practical_score,
        learning_score=payload.learning_score,
        support_score=payload.support_score,
        safety_score=payload.safety_score,

        weak_topics=weak_topics_text,
        comment_text=payload.comment_text or "",

        sentiment_label=blended_label,
        sentiment_score=float(blended_score),
    )

    if is_duplicate_monthly_feedback(record):
        raise HTTPException(status_code=409, detail="Duplicate monthly feedback submission detected.")

    append_feedback(record)
    return record


@app.get("/feedback/forms")
def list_feedback_forms(source: Optional[str] = None):
    forms = _load_feedback_templates()
    if source:
        source_lower = source.strip().lower()
        forms = [f for f in forms if str(f.get("source", "")).lower() == source_lower]
    return forms


@app.post("/submit-category-feedback", response_model=CategoryFeedbackRecord)
def submit_category_feedback(payload: CategoryFeedbackSubmission):
    forms = _load_feedback_templates()
    if not forms:
        raise HTTPException(status_code=500, detail="Feedback form templates are not available.")

    form = next((f for f in forms if str(f.get("form_id", "")) == payload.form_id), None)
    if not form:
        raise HTTPException(status_code=400, detail=f"Invalid form_id: {payload.form_id}")

    role_key = str(payload.submitted_by_role or "").strip().lower()
    if role_key not in ROLE_ALLOWED_FORM_IDS:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid submitted_by_role. Allowed values: "
                + ", ".join(sorted(ROLE_ALLOWED_FORM_IDS.keys()))
            ),
        )
    allowed_ids = ROLE_ALLOWED_FORM_IDS[role_key]
    if allowed_ids and payload.form_id not in allowed_ids:
        raise HTTPException(
            status_code=403,
            detail=f"Role '{role_key}' is not allowed to submit form '{payload.form_id}'.",
        )

    allowed_ratings = set(form.get("rating_scale", ["Excellent", "Good", "Average", "Poor"]))
    expected_parameters = {
        str(item.get("parameter", "")).strip()
        for item in form.get("parameters", [])
        if str(item.get("parameter", "")).strip()
    }
    submitted_parameters = {item.parameter.strip() for item in payload.parameter_scores if item.parameter.strip()}

    missing = sorted(expected_parameters - submitted_parameters)
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing parameter ratings: {', '.join(missing)}",
        )

    invalid_ratings = [item.rating for item in payload.parameter_scores if item.rating not in allowed_ratings]
    if invalid_ratings:
        raise HTTPException(status_code=400, detail=f"Invalid rating values: {invalid_ratings}")

    excellent_count = sum(1 for item in payload.parameter_scores if item.rating == "Excellent")
    good_count = sum(1 for item in payload.parameter_scores if item.rating == "Good")
    average_count = sum(1 for item in payload.parameter_scores if item.rating == "Average")
    poor_count = sum(1 for item in payload.parameter_scores if item.rating == "Poor")
    numeric_scores = [RATING_TO_SCORE[item.rating] for item in payload.parameter_scores]
    avg_rating_score = round(sum(numeric_scores) / len(numeric_scores), 2) if numeric_scores else 0.0

    record = CategoryFeedbackRecord(
        submitted_at=datetime.now(timezone.utc).isoformat(),
        source=str(payload.source or form.get("source", "")).strip(),
        form_id=payload.form_id,
        form_title=str(payload.form_title or form.get("form_title", payload.form_id)).strip(),
        basic_details={str(k): str(v) for k, v in (payload.basic_details or {}).items()},
        parameter_scores=payload.parameter_scores,
        comment_text=(payload.comment_text or "").strip(),
        excellent_count=excellent_count,
        good_count=good_count,
        average_count=average_count,
        poor_count=poor_count,
        avg_rating_score=avg_rating_score,
    )

    if is_duplicate_category_feedback(record):
        raise HTTPException(status_code=409, detail="Duplicate category feedback submission detected.")

    append_category_feedback(record)
    return record


@app.get("/dashboard/summary", response_model=DashboardSummary)
def dashboard_summary(
    month: Optional[str] = Query(default=None),
    district: Optional[str] = Query(default=None),
    trade_name: Optional[str] = Query(default=None),
):
    """
    Dashboard summary with optional filters.
    """
    return get_dashboard_summary(
        month=month,
        district=district,
        trade_name=trade_name,
    )


@app.get("/dashboard/trend")
def dashboard_trend(
    group_by: str = Query(default="month"),
    district: Optional[str] = Query(default=None),
    trade_name: Optional[str] = Query(default=None),
):
    """
    Trend endpoint for charts.
    Supported group_by:
    - month
    - district
    - trade_name
    """
    try:
        return get_dashboard_trend(
            group_by=group_by,
            district=district,
            trade_name=trade_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/category-feedback/summary")
def category_feedback_summary(
    source: Optional[str] = None,
    form_id: Optional[str] = None,
):
    return get_category_summary(source=source, form_id=form_id)


@app.get("/category-feedback/trend")
def category_feedback_trend(
    group_by: str = "month",
    source: Optional[str] = None,
    form_id: Optional[str] = None,
):
    try:
        return get_category_trend(group_by=group_by, source=source, form_id=form_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/category-feedback/rows")
def category_feedback_rows(
    source: Optional[str] = None,
    form_id: Optional[str] = None,
):
    return get_category_rows(source=source, form_id=form_id)


@app.get("/authority/combined-summary")
def authority_combined_summary(
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
):
    return get_authority_combined_summary(
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )


@app.get("/authority/combined-trend")
def authority_combined_trend(
    group_by: str = "month",
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
):
    try:
        return get_authority_combined_trend(
            group_by=group_by,
            district=district,
            trade_name=trade_name,
            year=year,
            semester=semester,
            institute_name=institute_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/authority/technical-summary")
def authority_technical_summary(
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
):
    return get_technical_summary(
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )


@app.get("/authority/technical-rows")
def authority_technical_rows(
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
):
    return get_technical_feedback_rows_summary(
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )


@app.get("/authority/technical-trend")
def authority_technical_trend(
    group_by: str = "month",
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
):
    try:
        return get_technical_trend(
            group_by=group_by,
            month=month,
            district=district,
            trade_name=trade_name,
            year=year,
            semester=semester,
            institute_name=institute_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
