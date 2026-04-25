from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from datetime import datetime, timezone
from database_service import (
    fetch_technical_feedback_rows,
    fetch_question_set_item_rows,
    fetch_category_feedback_rows,
    fetch_monthly_feedback_rows,
)


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"


def _normalize_text(v: Any) -> str:
    return str(v or "").strip().lower()


def _load_technical_df() -> pd.DataFrame:
    try:
        rows = fetch_technical_feedback_rows()
        item_rows = fetch_question_set_item_rows()
    except Exception:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()

    lookup: dict[tuple[int, str, str, str], dict[str, Any]] = {}
    for r in item_rows:
        key = (
            int(r["question_set_id"]),
            _normalize_text(r["subject"]),
            _normalize_text(r["topic"]),
            _normalize_text(r["question"]),
        )
        lookup[key] = dict(r)

    flat: list[dict[str, Any]] = []
    for row in rows:
        base = dict(row)
        try:
            context = json.loads(str(base.get("feedback_context_json") or "{}"))
            if not isinstance(context, dict):
                context = {}
        except Exception:
            context = {}
        try:
            responses = json.loads(str(base.get("responses_json") or "[]"))
            if not isinstance(responses, list):
                responses = []
        except Exception:
            responses = []

        for resp in responses:
            if not isinstance(resp, dict):
                continue
            subject = str(resp.get("subject", "")).strip()
            topic = str(resp.get("topic", "")).strip()
            question = str(resp.get("question", "")).strip()
            k = (
                int(base["question_set_id"]),
                _normalize_text(subject),
                _normalize_text(topic),
                _normalize_text(question),
            )
            expected = lookup.get(k, {})
            selected_option = str(resp.get("selected_option", "")).strip().upper()
            correct_option = str(expected.get("correct_option", "")).strip().upper()
            is_mcq = bool(selected_option and correct_option)
            is_correct = is_mcq and (selected_option == correct_option)

            flat.append(
                {
                    "feedback_id": int(base["feedback_id"]),
                    "submitted_at": str(base.get("submitted_at", "")),
                    "month": str(context.get("month") or base.get("month") or ""),
                    "district": str(context.get("district") or base.get("district") or ""),
                    "institute_name": str(context.get("institute_name") or base.get("institute_name") or ""),
                    "trade_name": str(context.get("trade_name") or base.get("trade_name") or ""),
                    "year": int(context.get("year") or base.get("year") or 0),
                    "semester": int(context.get("semester") or base.get("semester") or 0),
                    "student_name": str(context.get("student_name") or context.get("name") or "").strip(),
                    "subject": subject,
                    "topic": topic,
                    "question": question,
                    "selected_option": selected_option,
                    "correct_option": correct_option,
                    "is_mcq": bool(is_mcq),
                    "is_correct": bool(is_correct),
                }
            )
    return pd.DataFrame(flat)


def _extract_basic_details_row(s: str) -> dict[str, Any]:
    try:
        obj = json.loads(str(s or "{}"))
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


def _get_basic_details_value(details: dict[str, Any], key_contains: str) -> str:
    for k, v in details.items():
        if key_contains in str(k).strip().lower():
            return str(v or "").strip()
    return ""


def _load_category_df() -> pd.DataFrame:
    try:
        rows = fetch_category_feedback_rows()
    except Exception:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["avg_rating_score"] = pd.to_numeric(df.get("avg_rating_score"), errors="coerce").fillna(0.0)
    basics = df.get("basic_details_json", "").fillna("").astype(str).apply(_extract_basic_details_row)
    df["trade_name"] = basics.apply(lambda d: _get_basic_details_value(d, "trade"))
    df["institute_name"] = basics.apply(lambda d: _get_basic_details_value(d, "institute"))
    df["month"] = pd.to_datetime(df.get("submitted_at"), errors="coerce").dt.strftime("%Y-%m").fillna("")
    return df


def _load_monthly_df() -> pd.DataFrame:
    try:
        rows = fetch_monthly_feedback_rows()
    except Exception:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for c in ["learning_score", "teaching_score", "practical_score", "support_score", "safety_score"]:
        df[c] = pd.to_numeric(df.get(c), errors="coerce").fillna(0.0)
    return df


def _apply_common_filters(
    df: pd.DataFrame,
    month: Optional[str],
    district: Optional[str],
    trade_name: Optional[str],
    year: Optional[int],
    semester: Optional[int],
    institute_name: Optional[str],
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if month and "month" in out.columns:
        out = out[out["month"].astype(str) == str(month)]
    if district and "district" in out.columns:
        out = out[out["district"].astype(str).str.lower() == str(district).strip().lower()]
    if trade_name and "trade_name" in out.columns:
        out = out[out["trade_name"].astype(str).str.lower() == str(trade_name).strip().lower()]
    if year is not None and "year" in out.columns:
        out = out[pd.to_numeric(out["year"], errors="coerce").fillna(0).astype(int) == int(year)]
    if semester is not None and "semester" in out.columns:
        out = out[pd.to_numeric(out["semester"], errors="coerce").fillna(0).astype(int) == int(semester)]
    if institute_name and "institute_name" in out.columns:
        out = out[out["institute_name"].astype(str).str.lower() == str(institute_name).strip().lower()]
    return out


def get_authority_combined_summary(
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
) -> Dict[str, Any]:
    tech_df = _apply_common_filters(
        _load_technical_df(),
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )
    cat_df = _apply_common_filters(
        _load_category_df(),
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )
    mon_df = _apply_common_filters(
        _load_monthly_df(),
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )

    total_responses = int(len(tech_df))
    mcq_df = tech_df[tech_df["is_mcq"] == True] if not tech_df.empty else pd.DataFrame()
    mcq_answered = int(len(mcq_df))
    correct_answers = int(mcq_df["is_correct"].sum()) if not mcq_df.empty else 0
    accuracy_pct = round((correct_answers / mcq_answered) * 100.0, 2) if mcq_answered > 0 else 0.0

    avg_category_rating = round(float(cat_df["avg_rating_score"].mean()), 2) if not cat_df.empty else 0.0
    avg_learning_score = round(float(mon_df["learning_score"].mean()), 2) if not mon_df.empty else 0.0

    category_pct = (avg_category_rating / 4.0) * 100.0 if avg_category_rating > 0 else 0.0
    learning_pct = (avg_learning_score / 5.0) * 100.0 if avg_learning_score > 0 else 0.0
    overall_class_performance_pct = round(
        (accuracy_pct * 0.5) + (category_pct * 0.3) + (learning_pct * 0.2),
        2,
    )
    if overall_class_performance_pct >= 85:
        class_performance_band = "Excellent"
    elif overall_class_performance_pct >= 70:
        class_performance_band = "Good"
    elif overall_class_performance_pct >= 50:
        class_performance_band = "Needs Improvement"
    else:
        class_performance_band = "Critical"

    topic_rows: List[Dict[str, Any]] = []
    if not mcq_df.empty:
        topic_grp = (
            mcq_df.groupby("topic", dropna=False)
            .agg(
                answered=("topic", "count"),
                correct=("is_correct", "sum"),
            )
            .reset_index()
        )
        topic_grp["accuracy_pct"] = (topic_grp["correct"] / topic_grp["answered"] * 100.0).fillna(0.0)
        topic_grp = topic_grp.sort_values(by=["accuracy_pct", "answered"], ascending=[True, False])
        for _, r in topic_grp.head(10).iterrows():
            topic_rows.append(
                {
                    "topic": str(r["topic"]),
                    "answered": int(r["answered"]),
                    "correct": int(r["correct"]),
                    "accuracy_pct": round(float(r["accuracy_pct"]), 2),
                }
            )

    risk_index = round(max(0.0, min(100.0, (100.0 - accuracy_pct) * 0.6 + (4.0 - avg_category_rating) * 10.0)), 2)

    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "technical_submissions": int(tech_df["feedback_id"].nunique()) if not tech_df.empty else 0,
        "total_questions_answered": total_responses,
        "mcq_answered": mcq_answered,
        "correct_answers": correct_answers,
        "technical_accuracy_pct": accuracy_pct,
        "category_feedback_submissions": int(len(cat_df)),
        "avg_category_rating_score": avg_category_rating,
        "monthly_feedback_submissions": int(len(mon_df)),
        "avg_learning_score": avg_learning_score,
        "overall_class_performance_pct": overall_class_performance_pct,
        "class_performance_band": class_performance_band,
        "risk_index": risk_index,
        "top_weak_topics_by_accuracy": topic_rows,
    }


def get_authority_combined_trend(
    group_by: str = "month",
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if group_by not in {"month", "district", "trade_name"}:
        raise ValueError("group_by must be one of: month, district, trade_name")

    tech_df = _apply_common_filters(
        _load_technical_df(),
        month=None,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )
    if tech_df.empty:
        return []

    grouped = (
        tech_df.groupby(group_by, dropna=False)
        .agg(
            technical_submissions=("feedback_id", "nunique"),
            total_questions_answered=("question", "count"),
            mcq_answered=("is_mcq", "sum"),
            correct_answers=("is_correct", "sum"),
        )
        .reset_index()
    )
    grouped["technical_accuracy_pct"] = grouped.apply(
        lambda r: round(float(r["correct_answers"]) / float(r["mcq_answered"]) * 100.0, 2)
        if float(r["mcq_answered"]) > 0
        else 0.0,
        axis=1,
    )

    if group_by == "month":
        grouped["_sort"] = pd.to_datetime(grouped[group_by], errors="coerce")
        grouped = grouped.sort_values(by="_sort").drop(columns=["_sort"])
    else:
        grouped = grouped.sort_values(by=group_by)

    result = []
    for _, r in grouped.iterrows():
        result.append(
            {
                group_by: str(r[group_by]),
                "technical_submissions": int(r["technical_submissions"]),
                "total_questions_answered": int(r["total_questions_answered"]),
                "mcq_answered": int(r["mcq_answered"]),
                "correct_answers": int(r["correct_answers"]),
                "technical_accuracy_pct": round(float(r["technical_accuracy_pct"]), 2),
            }
        )
    return result


def get_technical_summary(
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
) -> Dict[str, Any]:
    tech_df = _apply_common_filters(
        _load_technical_df(),
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )
    if tech_df.empty:
        return {
            "technical_submissions": 0,
            "total_questions_answered": 0,
            "mcq_answered": 0,
            "correct_answers": 0,
            "technical_accuracy_pct": 0.0,
            "subject_performance": [],
            "weak_topics": [],
        }

    mcq_df = tech_df[tech_df["is_mcq"] == True]
    mcq_answered = int(len(mcq_df))
    correct_answers = int(mcq_df["is_correct"].sum()) if not mcq_df.empty else 0
    accuracy_pct = round((correct_answers / mcq_answered) * 100.0, 2) if mcq_answered > 0 else 0.0

    subject_rows: List[Dict[str, Any]] = []
    if not mcq_df.empty:
        sg = (
            mcq_df.groupby("subject", dropna=False)
            .agg(answered=("subject", "count"), correct=("is_correct", "sum"))
            .reset_index()
        )
        sg["accuracy_pct"] = (sg["correct"] / sg["answered"] * 100.0).fillna(0.0)
        sg = sg.sort_values(by="accuracy_pct", ascending=False)
        for _, r in sg.iterrows():
            subject_rows.append(
                {
                    "subject": str(r["subject"]),
                    "answered": int(r["answered"]),
                    "correct": int(r["correct"]),
                    "accuracy_pct": round(float(r["accuracy_pct"]), 2),
                }
            )

    weak_topics: List[Dict[str, Any]] = []
    if not mcq_df.empty:
        tg = (
            mcq_df.groupby("topic", dropna=False)
            .agg(answered=("topic", "count"), correct=("is_correct", "sum"))
            .reset_index()
        )
        tg["accuracy_pct"] = (tg["correct"] / tg["answered"] * 100.0).fillna(0.0)
        tg = tg.sort_values(by=["accuracy_pct", "answered"], ascending=[True, False])
        for _, r in tg.head(10).iterrows():
            weak_topics.append(
                {
                    "topic": str(r["topic"]),
                    "answered": int(r["answered"]),
                    "correct": int(r["correct"]),
                    "accuracy_pct": round(float(r["accuracy_pct"]), 2),
                }
            )

    return {
        "technical_submissions": int(tech_df["feedback_id"].nunique()),
        "total_questions_answered": int(len(tech_df)),
        "mcq_answered": mcq_answered,
        "correct_answers": correct_answers,
        "technical_accuracy_pct": accuracy_pct,
        "subject_performance": subject_rows,
        "weak_topics": weak_topics,
    }


def get_technical_feedback_rows_summary(
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    tech_df = _apply_common_filters(
        _load_technical_df(),
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )
    if tech_df.empty:
        return []

    grouped = (
        tech_df.groupby(
            [
                "feedback_id",
                "submitted_at",
                "month",
                "district",
                "institute_name",
                "trade_name",
                "year",
                "semester",
                "student_name",
            ],
            dropna=False,
        )
        .agg(
            response_count=("question", "count"),
            mcq_answered=("is_mcq", "sum"),
            correct_answers=("is_correct", "sum"),
        )
        .reset_index()
        .sort_values(by="submitted_at", ascending=False)
    )
    grouped["technical_score_pct"] = grouped.apply(
        lambda r: round(float(r["correct_answers"]) / float(r["mcq_answered"]) * 100.0, 2)
        if float(r["mcq_answered"]) > 0
        else 0.0,
        axis=1,
    )
    grouped["technical_rating_4"] = grouped["technical_score_pct"].apply(
        lambda score: round((float(score) / 100.0) * 4.0, 2) if float(score) > 0 else 0.0
    )

    rows: List[Dict[str, Any]] = []
    for _, row in grouped.iterrows():
        rows.append(
            {
                "feedback_id": int(row["feedback_id"]),
                "submitted_at": str(row["submitted_at"]),
                "month": str(row["month"]),
                "district": str(row["district"]),
                "institute_name": str(row["institute_name"]),
                "trade_name": str(row["trade_name"]),
                "year": int(row["year"]),
                "semester": int(row["semester"]),
                "student_name": str(row["student_name"] or "").strip(),
                "response_count": int(row["response_count"]),
                "mcq_answered": int(row["mcq_answered"]),
                "correct_answers": int(row["correct_answers"]),
                "technical_score_pct": round(float(row["technical_score_pct"]), 2),
                "technical_rating_4": round(float(row["technical_rating_4"]), 2),
                "role_group": "Trainee",
            }
        )
    return rows


def get_technical_trend(
    group_by: str = "month",
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
    year: Optional[int] = None,
    semester: Optional[int] = None,
    institute_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if group_by not in {"month", "district", "trade_name"}:
        raise ValueError("group_by must be one of: month, district, trade_name")

    tech_df = _apply_common_filters(
        _load_technical_df(),
        month=month,
        district=district,
        trade_name=trade_name,
        year=year,
        semester=semester,
        institute_name=institute_name,
    )
    if tech_df.empty:
        return []

    grouped = (
        tech_df.groupby(group_by, dropna=False)
        .agg(
            technical_submissions=("feedback_id", "nunique"),
            total_questions_answered=("question", "count"),
            mcq_answered=("is_mcq", "sum"),
            correct_answers=("is_correct", "sum"),
        )
        .reset_index()
    )
    grouped["technical_accuracy_pct"] = grouped.apply(
        lambda r: round(float(r["correct_answers"]) / float(r["mcq_answered"]) * 100.0, 2)
        if float(r["mcq_answered"]) > 0
        else 0.0,
        axis=1,
    )

    if group_by == "month":
        grouped["_sort"] = pd.to_datetime(grouped[group_by], errors="coerce")
        grouped = grouped.sort_values(by="_sort").drop(columns=["_sort"])
    else:
        grouped = grouped.sort_values(by=group_by)

    out: List[Dict[str, Any]] = []
    for _, r in grouped.iterrows():
        out.append(
            {
                group_by: str(r[group_by]),
                "technical_submissions": int(r["technical_submissions"]),
                "total_questions_answered": int(r["total_questions_answered"]),
                "mcq_answered": int(r["mcq_answered"]),
                "correct_answers": int(r["correct_answers"]),
                "technical_accuracy_pct": round(float(r["technical_accuracy_pct"]), 2),
            }
        )
    return out
