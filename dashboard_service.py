from __future__ import annotations

from typing import Optional, List, Dict, Any

import pandas as pd

from models import DashboardSummary
from database_service import fetch_monthly_feedback_rows


def _load_feedback_df() -> pd.DataFrame:
    """
    Load feedback CSV safely.
    Returns an empty DataFrame with expected columns if file does not exist.
    """
    expected_columns = [
        "month",
        "district",
        "trade_name",
        "year",
        "semester",
        "attendance_pct",
        "subject_1",
        "topic_1",
        "question_1",
        "topic_1_score",
        "subject_2",
        "topic_2",
        "question_2",
        "topic_2_score",
        "subject_3",
        "topic_3",
        "question_3",
        "topic_3_score",
        "teaching_score",
        "practical_score",
        "learning_score",
        "support_score",
        "safety_score",
        "weak_topics",
        "comment_text",
        "sentiment_label",
        "sentiment_score",
    ]

    try:
        rows = fetch_monthly_feedback_rows()
    except Exception:
        return pd.DataFrame(columns=expected_columns)
    if not rows:
        return pd.DataFrame(columns=expected_columns)
    df = pd.DataFrame(rows)

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    return df


def _apply_filters(
    df: pd.DataFrame,
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
) -> pd.DataFrame:
    out = df.copy()

    if month:
        out = out[out["month"].astype(str) == str(month)]

    if district:
        out = out[out["district"].astype(str).str.lower() == str(district).lower()]

    if trade_name:
        out = out[out["trade_name"].astype(str).str.lower() == str(trade_name).lower()]

    return out


def get_dashboard_summary(
    month: Optional[str] = None,
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
) -> DashboardSummary:
    """
    Summary metrics for dashboard cards.
    Supports optional filters.
    """
    df = _load_feedback_df()
    df = _apply_filters(df, month=month, district=district, trade_name=trade_name)

    if df.empty:
        return DashboardSummary(
            total_submissions=0,
            avg_teaching_score=0.0,
            avg_practical_score=0.0,
            avg_learning_score=0.0,
            avg_support_score=0.0,
            avg_safety_score=0.0,
            avg_attendance_pct=0.0,
            positive_count=0,
            neutral_count=0,
            negative_count=0,
            top_weak_topics=[],
        )

    weak_topics = (
        df["weak_topics"]
        .fillna("")
        .astype(str)
        .str.split(";")
        .explode()
        .str.strip()
    )
    weak_topics = weak_topics[weak_topics != ""]
    top_weak = weak_topics.value_counts().head(5).index.tolist()

    sentiment_series = (
        df["sentiment_label"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .replace(
            {
                "good": "positive",
                "bad": "negative",
                "average": "neutral",
                "mixed": "neutral",
            }
        )
    )

    return DashboardSummary(
        total_submissions=int(len(df)),
        avg_teaching_score=round(float(df["teaching_score"].mean()), 2),
        avg_practical_score=round(float(df["practical_score"].mean()), 2),
        avg_learning_score=round(float(df["learning_score"].mean()), 2),
        avg_support_score=round(float(df["support_score"].mean()), 2),
        avg_safety_score=round(float(df["safety_score"].mean()), 2),
        avg_attendance_pct=round(float(df["attendance_pct"].mean()), 2),
        positive_count=int((sentiment_series == "positive").sum()),
        neutral_count=int((sentiment_series == "neutral").sum()),
        negative_count=int((sentiment_series == "negative").sum()),
        top_weak_topics=top_weak,
    )


def get_dashboard_trend(
    group_by: str = "month",
    district: Optional[str] = None,
    trade_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Returns grouped trend rows for charts.

    Supported group_by:
    - month
    - district
    - trade_name
    """
    if group_by not in {"month", "district", "trade_name"}:
        raise ValueError("group_by must be one of: month, district, trade_name")

    df = _load_feedback_df()
    df = _apply_filters(df, district=district, trade_name=trade_name)

    if df.empty:
        return []

    grouped = (
        df.groupby(group_by, dropna=False)
        .agg(
            total_submissions=("month", "count"),
            avg_teaching_score=("teaching_score", "mean"),
            avg_practical_score=("practical_score", "mean"),
            avg_learning_score=("learning_score", "mean"),
            avg_support_score=("support_score", "mean"),
            avg_safety_score=("safety_score", "mean"),
            avg_attendance_pct=("attendance_pct", "mean"),
        )
        .reset_index()
    )

    result: List[Dict[str, Any]] = []
    for _, row in grouped.iterrows():
        result.append(
            {
                group_by: row[group_by],
                "total_submissions": int(row["total_submissions"]),
                "avg_teaching_score": round(float(row["avg_teaching_score"]), 2),
                "avg_practical_score": round(float(row["avg_practical_score"]), 2),
                "avg_learning_score": round(float(row["avg_learning_score"]), 2),
                "avg_support_score": round(float(row["avg_support_score"]), 2),
                "avg_safety_score": round(float(row["avg_safety_score"]), 2),
                "avg_attendance_pct": round(float(row["avg_attendance_pct"]), 2),
            }
        )

    return result


if __name__ == "__main__":
    print("Summary:", get_dashboard_summary().model_dump())
    print("Trend:", get_dashboard_trend(group_by="month"))
