from __future__ import annotations

from typing import Any

from models import CategoryFeedbackRecord, ProcessedFeedbackRecord
from database_service import (
    init_db,
    exists_monthly_feedback_duplicate,
    exists_category_feedback_duplicate,
    insert_monthly_feedback_record,
    insert_category_feedback_record,
)


def _norm(v: Any) -> str:
    return str(v if v is not None else "").strip()


def init_storage() -> None:
    # Storage is Supabase/Postgres-backed now.
    init_db()


def is_duplicate_monthly_feedback(record: ProcessedFeedbackRecord) -> bool:
    key = {
        "month": _norm(record.month),
        "district": _norm(record.district),
        "trade_name": _norm(record.trade_name),
        "year": int(record.year),
        "semester": int(record.semester),
        "subject_1": _norm(record.subject_1),
        "topic_1": _norm(record.topic_1),
        "question_1": _norm(record.question_1),
        "topic_1_score": float(record.topic_1_score),
        "subject_2": _norm(record.subject_2),
        "topic_2": _norm(record.topic_2),
        "question_2": _norm(record.question_2),
        "topic_2_score": float(record.topic_2_score),
        "subject_3": _norm(record.subject_3),
        "topic_3": _norm(record.topic_3),
        "question_3": _norm(record.question_3),
        "topic_3_score": float(record.topic_3_score),
        "teaching_score": float(record.teaching_score),
        "practical_score": float(record.practical_score),
        "learning_score": float(record.learning_score),
        "support_score": float(record.support_score),
        "safety_score": float(record.safety_score),
        "comment_text": _norm(record.comment_text),
    }
    return exists_monthly_feedback_duplicate(key)


def is_duplicate_category_feedback(record: CategoryFeedbackRecord) -> bool:
    key = {
        "source": _norm(record.source),
        "form_id": _norm(record.form_id),
        "form_title": _norm(record.form_title),
        "basic_details": {str(k): str(v) for k, v in (record.basic_details or {}).items()},
        "parameter_scores": [item.model_dump() for item in record.parameter_scores],
        "comment_text": _norm(record.comment_text),
    }
    return exists_category_feedback_duplicate(key)


def append_feedback(record: ProcessedFeedbackRecord) -> None:
    insert_monthly_feedback_record(record.model_dump())


def append_category_feedback(record: CategoryFeedbackRecord) -> None:
    payload = record.model_dump()
    payload["parameter_scores"] = [item.model_dump() for item in record.parameter_scores]
    insert_category_feedback_record(payload)
