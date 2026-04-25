from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SQLITE_PATH = DATA_DIR / "feedback_system.db"
MONTHLY_CSV = DATA_DIR / "monthly_feedback_records.csv"
CATEGORY_CSV = DATA_DIR / "category_feedback_records.csv"


def connect_pg():
    db_url = (os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        raise RuntimeError("Set SUPABASE_DB_URL in environment before running migration.")
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)


def migrate_sqlite_technical_to_pg(cur, sqlite_conn) -> None:
    s_cur = sqlite_conn.cursor()

    s_cur.execute("SELECT id, username, password_hash, role, created_at FROM users ORDER BY id")
    for row in s_cur.fetchall():
        cur.execute(
            """
            INSERT INTO users (id, username, password_hash, role, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (username) DO NOTHING
            """,
            (int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])),
        )

    s_cur.execute(
        """
        SELECT id, trainer_user_id, month, district, institute_name, trade_name, year, semester, question_mode, target_question_count, created_at
        FROM question_sets
        ORDER BY id
        """
    )
    for row in s_cur.fetchall():
        cur.execute(
            """
            INSERT INTO question_sets
            (id, trainer_user_id, month, district, institute_name, trade_name, year, semester, question_mode, target_question_count, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                int(row[0]),
                int(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                str(row[5]),
                int(row[6]),
                int(row[7]),
                str(row[8] or "both"),
                int(row[9] or 3),
                str(row[10]),
            ),
        )

    s_cur.execute(
        """
        SELECT id, question_set_id, subject, topic, question, option_a, option_b, option_c, option_d, correct_option, answer_text
        FROM question_set_items
        ORDER BY id
        """
    )
    for row in s_cur.fetchall():
        cur.execute(
            """
            INSERT INTO question_set_items
            (id, question_set_id, subject, topic, question, option_a, option_b, option_c, option_d, correct_option, answer_text)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                int(row[0]),
                int(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
            ),
        )

    s_cur.execute(
        """
        SELECT id, question_set_id, student_user_id, submitted_at, feedback_context_json, responses_json
        FROM student_technical_feedback
        ORDER BY id
        """
    )
    for row in s_cur.fetchall():
        cur.execute(
            """
            INSERT INTO student_technical_feedback
            (id, question_set_id, student_user_id, submitted_at, feedback_context_json, responses_json)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                int(row[0]),
                int(row[1]),
                int(row[2]),
                str(row[3]),
                str(row[4]),
                str(row[5]),
            ),
        )


def migrate_monthly_csv_to_pg(cur) -> None:
    if not MONTHLY_CSV.exists():
        return
    with open(MONTHLY_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
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
                    %s, %s, %s, %s, now()::text
                )
                """,
                (
                    str(r.get("month", "")),
                    str(r.get("district", "")),
                    str(r.get("trade_name", "")),
                    int(float(r.get("year", 0) or 0)),
                    int(float(r.get("semester", 0) or 0)),
                    float(r.get("attendance_pct", 0) or 0),
                    str(r.get("subject_1", "")),
                    str(r.get("topic_1", "")),
                    str(r.get("question_1", "")),
                    float(r.get("topic_1_score", 0) or 0),
                    str(r.get("subject_2", "")),
                    str(r.get("topic_2", "")),
                    str(r.get("question_2", "")),
                    float(r.get("topic_2_score", 0) or 0),
                    str(r.get("subject_3", "")),
                    str(r.get("topic_3", "")),
                    str(r.get("question_3", "")),
                    float(r.get("topic_3_score", 0) or 0),
                    float(r.get("teaching_score", 0) or 0),
                    float(r.get("practical_score", 0) or 0),
                    float(r.get("learning_score", 0) or 0),
                    float(r.get("support_score", 0) or 0),
                    float(r.get("safety_score", 0) or 0),
                    str(r.get("weak_topics", "")),
                    str(r.get("comment_text", "")),
                    str(r.get("sentiment_label", "")),
                    float(r.get("sentiment_score", 0) or 0),
                ),
            )


def migrate_category_csv_to_pg(cur) -> None:
    if not CATEGORY_CSV.exists():
        return
    with open(CATEGORY_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            basic = r.get("basic_details_json", "{}") or "{}"
            params = r.get("parameter_scores_json", "[]") or "[]"
            try:
                json.loads(basic)
            except Exception:
                basic = "{}"
            try:
                json.loads(params)
            except Exception:
                params = "[]"
            cur.execute(
                """
                INSERT INTO category_feedback_records (
                    submitted_at, source, form_id, form_title, basic_details_json, parameter_scores_json,
                    comment_text, excellent_count, good_count, average_count, poor_count, avg_rating_score
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(r.get("submitted_at", "")),
                    str(r.get("source", "")),
                    str(r.get("form_id", "")),
                    str(r.get("form_title", "")),
                    basic,
                    params,
                    str(r.get("comment_text", "")),
                    int(float(r.get("excellent_count", 0) or 0)),
                    int(float(r.get("good_count", 0) or 0)),
                    int(float(r.get("average_count", 0) or 0)),
                    int(float(r.get("poor_count", 0) or 0)),
                    float(r.get("avg_rating_score", 0) or 0),
                ),
            )


def reset_sequences(cur) -> None:
    tables = [
        "users",
        "question_sets",
        "question_set_items",
        "student_technical_feedback",
        "monthly_feedback_records",
        "category_feedback_records",
    ]
    for t in tables:
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{t}','id'), COALESCE((SELECT MAX(id) FROM {t}), 1), true)"
        )


def main() -> None:
    sqlite_conn = sqlite3.connect(SQLITE_PATH) if SQLITE_PATH.exists() else None
    with connect_pg() as pg_conn:
        with pg_conn.cursor() as cur:
            if sqlite_conn is not None:
                migrate_sqlite_technical_to_pg(cur, sqlite_conn)
            migrate_monthly_csv_to_pg(cur)
            migrate_category_csv_to_pg(cur)
            reset_sequences(cur)
        pg_conn.commit()
    if sqlite_conn is not None:
        sqlite_conn.close()
    print("Migration to Supabase completed.")


if __name__ == "__main__":
    main()
