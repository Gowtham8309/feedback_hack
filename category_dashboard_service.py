from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pandas as pd
from database_service import fetch_category_feedback_rows


def _load_category_df() -> pd.DataFrame:
    expected_cols = [
        "submitted_at",
        "source",
        "form_id",
        "form_title",
        "basic_details_json",
        "parameter_scores_json",
        "comment_text",
        "excellent_count",
        "good_count",
        "average_count",
        "poor_count",
        "avg_rating_score",
    ]
    try:
        rows = fetch_category_feedback_rows()
    except Exception:
        return pd.DataFrame(columns=expected_cols)
    if not rows:
        return pd.DataFrame(columns=expected_cols)
    df = pd.DataFrame(rows)
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    for num_col in ["excellent_count", "good_count", "average_count", "poor_count", "avg_rating_score"]:
        df[num_col] = pd.to_numeric(df[num_col], errors="coerce").fillna(0.0)

    df["source"] = df["source"].fillna("").astype(str)
    df["form_id"] = df["form_id"].fillna("").astype(str)
    df["form_title"] = df["form_title"].fillna("").astype(str)
    df["submitted_at"] = df["submitted_at"].fillna("").astype(str)
    df["comment_text"] = df["comment_text"].fillna("").astype(str)
    df["month"] = pd.to_datetime(df["submitted_at"], errors="coerce").dt.strftime("%Y-%m")
    df["month"] = df["month"].fillna("")
    return df


def _apply_category_filters(df: pd.DataFrame, source: Optional[str], form_id: Optional[str]) -> pd.DataFrame:
    out = df.copy()
    if source:
        src = str(source).strip().lower()
        out = out[out["source"].str.lower() == src]
    if form_id:
        fid = str(form_id).strip().lower()
        out = out[out["form_id"].str.lower() == fid]
    return out


def get_category_summary(source: Optional[str] = None, form_id: Optional[str] = None) -> Dict[str, Any]:
    df = _load_category_df()
    df = _apply_category_filters(df, source=source, form_id=form_id)

    if df.empty:
        return {
            "total_submissions": 0,
            "avg_rating_score": 0.0,
            "excellent_total": 0,
            "good_total": 0,
            "average_total": 0,
            "poor_total": 0,
            "positive_count": 0,
            "neutral_count": 0,
            "negative_count": 0,
            "top_forms": [],
            "available_sources": [],
            "available_form_ids": [],
            "recent_submissions": [],
        }

    grouped = (
        df.groupby(["form_id", "form_title"], dropna=False)
        .agg(
            submissions=("form_id", "count"),
            avg_rating_score=("avg_rating_score", "mean"),
        )
        .reset_index()
        .sort_values(by=["submissions", "avg_rating_score"], ascending=[False, False])
    )

    top_forms = []
    for _, row in grouped.head(10).iterrows():
        top_forms.append(
            {
                "form_id": str(row["form_id"]),
                "form_title": str(row["form_title"]),
                "submissions": int(row["submissions"]),
                "avg_rating_score": round(float(row["avg_rating_score"]), 2),
            }
        )

    recent = (
        df.sort_values(by="submitted_at", ascending=False)
        .head(10)[
            [
                "submitted_at",
                "source",
                "form_id",
                "form_title",
                "avg_rating_score",
                "excellent_count",
                "good_count",
                "average_count",
                "poor_count",
                "comment_text",
                "basic_details_json",
            ]
        ]
        .copy()
    )

    recent_rows: List[Dict[str, Any]] = []
    for _, row in recent.iterrows():
        basic_details: Dict[str, Any] = {}
        try:
            loaded = json.loads(str(row["basic_details_json"]))
            if isinstance(loaded, dict):
                basic_details = loaded
        except Exception:
            basic_details = {}

        recent_rows.append(
            {
                "submitted_at": str(row["submitted_at"]),
                "source": str(row["source"]),
                "form_id": str(row["form_id"]),
                "form_title": str(row["form_title"]),
                "avg_rating_score": round(float(row["avg_rating_score"]), 2),
                "excellent_count": int(row["excellent_count"]),
                "good_count": int(row["good_count"]),
                "average_count": int(row["average_count"]),
                "poor_count": int(row["poor_count"]),
                "comment_text": str(row["comment_text"]),
                "basic_details": basic_details,
            }
        )

    # Category sentiment proxy from avg rating (4-point scale):
    # >= 3.0 -> positive, >= 2.0 -> neutral, else negative.
    rating_series = pd.to_numeric(df["avg_rating_score"], errors="coerce").fillna(0.0)
    positive_count = int((rating_series >= 3.0).sum())
    neutral_count = int(((rating_series >= 2.0) & (rating_series < 3.0)).sum())
    negative_count = int((rating_series < 2.0).sum())

    return {
        "total_submissions": int(len(df)),
        "avg_rating_score": round(float(df["avg_rating_score"].mean()), 2),
        "excellent_total": int(df["excellent_count"].sum()),
        "good_total": int(df["good_count"].sum()),
        "average_total": int(df["average_count"].sum()),
        "poor_total": int(df["poor_count"].sum()),
        "positive_count": positive_count,
        "neutral_count": neutral_count,
        "negative_count": negative_count,
        "top_forms": top_forms,
        "available_sources": sorted([s for s in df["source"].dropna().unique().tolist() if s]),
        "available_form_ids": sorted([s for s in df["form_id"].dropna().unique().tolist() if s]),
        "recent_submissions": recent_rows,
    }


def get_category_trend(
    group_by: str = "month",
    source: Optional[str] = None,
    form_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if group_by not in {"month", "source", "form_id"}:
        raise ValueError("group_by must be one of: month, source, form_id")

    df = _load_category_df()
    df = _apply_category_filters(df, source=source, form_id=form_id)
    if df.empty:
        return []

    grouped = (
        df.groupby(group_by, dropna=False)
        .agg(
            total_submissions=("form_id", "count"),
            avg_rating_score=("avg_rating_score", "mean"),
            excellent_total=("excellent_count", "sum"),
            good_total=("good_count", "sum"),
            average_total=("average_count", "sum"),
            poor_total=("poor_count", "sum"),
        )
        .reset_index()
    )

    if group_by == "month":
        grouped["_sort"] = pd.to_datetime(grouped[group_by], format="%Y-%m", errors="coerce")
        grouped = grouped.sort_values(by="_sort").drop(columns=["_sort"])
    else:
        grouped = grouped.sort_values(by=group_by)

    result: List[Dict[str, Any]] = []
    for _, row in grouped.iterrows():
        result.append(
            {
                group_by: str(row[group_by]),
                "total_submissions": int(row["total_submissions"]),
                "avg_rating_score": round(float(row["avg_rating_score"]), 2),
                "excellent_total": int(row["excellent_total"]),
                "good_total": int(row["good_total"]),
                "average_total": int(row["average_total"]),
                "poor_total": int(row["poor_total"]),
            }
        )
    return result


def get_category_rows(source: Optional[str] = None, form_id: Optional[str] = None) -> List[Dict[str, Any]]:
    df = _load_category_df()
    df = _apply_category_filters(df, source=source, form_id=form_id)
    if df.empty:
        return []

    rows: List[Dict[str, Any]] = []
    for _, row in df.sort_values(by="submitted_at", ascending=False).iterrows():
        basic_details: Dict[str, Any] = {}
        try:
            loaded = json.loads(str(row.get("basic_details_json", "{}")))
            if isinstance(loaded, dict):
                basic_details = loaded
        except Exception:
            basic_details = {}
        parameter_scores: List[Dict[str, Any]] = []
        try:
            loaded_scores = json.loads(str(row.get("parameter_scores_json", "[]")))
            if isinstance(loaded_scores, list):
                parameter_scores = loaded_scores
        except Exception:
            parameter_scores = []
        rows.append(
            {
                "submitted_at": str(row.get("submitted_at", "")),
                "source": str(row.get("source", "")),
                "form_id": str(row.get("form_id", "")),
                "form_title": str(row.get("form_title", "")),
                "basic_details_json": str(row.get("basic_details_json", "")),
                "basic_details": basic_details,
                "parameter_scores_json": str(row.get("parameter_scores_json", "")),
                "parameter_scores": parameter_scores,
                "comment_text": str(row.get("comment_text", "")),
                "excellent_count": int(row.get("excellent_count", 0) or 0),
                "good_count": int(row.get("good_count", 0) or 0),
                "average_count": int(row.get("average_count", 0) or 0),
                "poor_count": int(row.get("poor_count", 0) or 0),
                "avg_rating_score": round(float(row.get("avg_rating_score", 0.0) or 0.0), 2),
            }
        )
    return rows
