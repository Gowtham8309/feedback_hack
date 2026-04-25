from __future__ import annotations

import csv
import json
import random
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from models import MonthlyTopicInput, GeneratedQuestion, QuestionGenerationResponse


BASE_DIR = Path(__file__).resolve().parent
THEORY_BANK_CSV = BASE_DIR / "data" / "question_bank_theory.csv"
PRACTICAL_BANK_CSV = BASE_DIR / "data" / "question_bank_practical.csv"
THEORY_BANK_JSON = BASE_DIR / "data" / "question_bank_theory.json"
PRACTICAL_BANK_JSON = BASE_DIR / "data" / "question_bank_practical.json"


def _normalize(value: str) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip().lower())
    if text in {"nan", "none", "nat"}:
        return ""
    return text


def _tokenize(text: str) -> set[str]:
    stop_words = {
        "and",
        "the",
        "for",
        "with",
        "from",
        "into",
        "on",
        "in",
        "of",
        "to",
        "use",
        "using",
        "practice",
        "operation",
        "topic",
        "trade",
    }
    return {
        t
        for t in re.split(r"[^a-z0-9]+", _normalize(text))
        if len(t) > 3 and t not in stop_words
    }


def _subject_hints(subject: str) -> set[str]:
    s = _normalize(subject)
    hints: set[str] = set()
    if "practical" in s:
        hints.update({"practical", "workshop", "operation", "perform", "tools", "equipment"})
    if "theory" in s:
        hints.update({"theory", "concept", "principle", "explain", "define"})
    if "calculation" in s:
        hints.update({"calculation", "equation", "formula", "solve", "estimation", "measurement"})
    return hints


def _month_name_from_payload(month_value: str) -> str:
    text = (month_value or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.strptime(text, "%Y-%m")
        return dt.strftime("%B").upper()
    except ValueError:
        return ""


@lru_cache(maxsize=1)
def _load_question_banks() -> tuple[list[dict], list[dict]]:
    def _load_csv(path: Path) -> list[dict]:
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    return _load_csv(THEORY_BANK_CSV), _load_csv(PRACTICAL_BANK_CSV)


def refresh_question_bank_cache() -> None:
    _load_question_banks.cache_clear()


def _normalize_source(value: str) -> str:
    src = _normalize(value)
    if src in {"theory", "trade theory"}:
        return "theory"
    if src in {"practical", "trade practical"}:
        return "practical"
    return ""


def _normalize_year_level(value: str) -> str:
    v = _normalize(value).upper()
    if v in {"1", "I", "FIRST"}:
        return "I"
    if v in {"2", "II", "SECOND"}:
        return "II"
    return (value or "").strip()


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv_rows(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_json_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def _write_json_rows(path: Path, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def ingest_question_bank_rows(
    rows: list[dict],
    default_source: str = "",
    default_trade: str = "",
    default_year_level: str = "",
    default_month: str = "",
) -> dict:
    csv_columns = [
        "source",
        "year_level",
        "trade",
        "month",
        "topic",
        "question_no",
        "question_text",
        "question_image",
        "option_a",
        "option_b",
        "option_c",
        "option_d",
        "correct_option",
        "answer_text",
    ]
    if not rows:
        return {"inserted": 0, "skipped": 0, "total": 0}

    default_source_n = _normalize_source(default_source)
    theory_csv = _read_csv_rows(THEORY_BANK_CSV)
    practical_csv = _read_csv_rows(PRACTICAL_BANK_CSV)
    theory_json = _read_json_rows(THEORY_BANK_JSON)
    practical_json = _read_json_rows(PRACTICAL_BANK_JSON)

    def _key_strict(row: dict) -> tuple[str, str, str, str, str]:
        return (
            _normalize(str(row.get("source", ""))),
            _normalize(str(row.get("year_level", ""))),
            _normalize(str(row.get("trade", ""))),
            _normalize(str(row.get("topic", ""))),
            _normalize(str(row.get("question_text", ""))),
        )

    def _key_relaxed(row: dict) -> tuple[str, str, str]:
        return (
            _normalize(str(row.get("source", ""))),
            _normalize(str(row.get("topic", ""))),
            _normalize(str(row.get("question_text", ""))),
        )

    existing_strict = set()
    existing_relaxed = set()
    for r in theory_csv + practical_csv:
        existing_strict.add(_key_strict(r))
        existing_relaxed.add(_key_relaxed(r))

    def _next_question_no(bank_rows: list[dict]) -> int:
        max_no = 0
        for r in bank_rows:
            try:
                max_no = max(max_no, int(str(r.get("question_no", "")).strip()))
            except Exception:
                continue
        return max_no + 1

    next_no_theory = _next_question_no(theory_csv)
    next_no_practical = _next_question_no(practical_csv)

    inserted = 0
    skipped = 0

    for raw in rows:
        if not isinstance(raw, dict):
            skipped += 1
            continue

        row_source = _normalize_source(str(raw.get("source", ""))) or default_source_n or "theory"
        question_text = str(raw.get("question_text", "")).strip()
        if not question_text:
            skipped += 1
            continue

        normalized = {
            "source": row_source,
            "year_level": _normalize_year_level(str(raw.get("year_level", "")) or default_year_level),
            "trade": str(raw.get("trade", "") or default_trade).strip(),
            "month": str(raw.get("month", "") or default_month).strip(),
            "topic": str(raw.get("topic", "")).strip(),
            "question_no": "",
            "question_text": question_text,
            "question_image": str(raw.get("question_image", "") or raw.get("image", "") or raw.get("image_url", "")).strip(),
            "option_a": str(raw.get("option_a", "")).strip(),
            "option_b": str(raw.get("option_b", "")).strip(),
            "option_c": str(raw.get("option_c", "")).strip(),
            "option_d": str(raw.get("option_d", "")).strip(),
            "correct_option": str(raw.get("correct_option", "")).strip().upper(),
            "answer_text": str(raw.get("answer_text", "")).strip(),
        }

        s_key = _key_strict(normalized)
        r_key = _key_relaxed(normalized)
        if s_key in existing_strict or r_key in existing_relaxed:
            skipped += 1
            continue

        target_csv = theory_csv
        target_json = theory_json
        if row_source == "practical":
            normalized["question_no"] = str(next_no_practical)
            next_no_practical += 1
            target_csv = practical_csv
            target_json = practical_json
        else:
            normalized["question_no"] = str(next_no_theory)
            next_no_theory += 1

        target_csv.append(normalized.copy())
        normalized_json = normalized.copy()
        try:
            normalized_json["question_no"] = int(normalized_json["question_no"])
        except Exception:
            pass
        target_json.append(normalized_json)

        existing_strict.add(s_key)
        existing_relaxed.add(r_key)
        inserted += 1

    _write_csv_rows(THEORY_BANK_CSV, theory_csv, csv_columns)
    _write_csv_rows(PRACTICAL_BANK_CSV, practical_csv, csv_columns)
    _write_json_rows(THEORY_BANK_JSON, theory_json)
    _write_json_rows(PRACTICAL_BANK_JSON, practical_json)
    refresh_question_bank_cache()

    return {"inserted": inserted, "skipped": skipped, "total": len(rows)}


def _pick_question_from_bank(
    records: list[dict],
    subject: str,
    topic: str,
    month_name: str,
    trade_name: str = "",
    min_score: int = 24,
) -> dict | None:
    if not records:
        return None

    topic_n = _normalize(topic)
    month_n = _normalize(month_name)
    trade_n = _normalize(trade_name)
    topic_tokens = _tokenize(topic_n)
    subject_tokens = _tokenize(subject) | _subject_hints(subject)

    scored: list[tuple[int, dict]] = []
    for row in records:
        row_topic = _normalize(row.get("topic", ""))
        row_month = _normalize(row.get("month", ""))
        row_trade = _normalize(row.get("trade", ""))
        q_text = (row.get("question_text") or "").strip()
        if not q_text:
            continue
        if trade_n and row_trade and trade_n != row_trade:
            continue

        row_text = f"{row_topic} {_normalize(q_text)}".strip()
        score = 0
        if topic_n in {"all theory questions", "all practical questions", "all questions"}:
            score += 80
        elif topic_n and row_topic:
            if topic_n == row_topic:
                score += 100
            elif topic_n in row_topic or row_topic in topic_n:
                score += 60
            else:
                token_hits = sum(1 for tok in topic_tokens if tok in row_text)
                score += token_hits * 8

        if subject_tokens:
            subject_hits = sum(1 for tok in subject_tokens if tok in row_text)
            score += subject_hits * 4

        if month_n and row_month and month_n in row_month:
            score += 15

        if score >= min_score:
            scored.append((score, row))

    if not scored:
        return None

    max_score = max(s for s, _ in scored)
    top_rows = [r for s, r in scored if s == max_score]
    row = random.choice(top_rows)
    q_text = (row.get("question_text") or "").strip()
    if not q_text:
        return None
    return {
        "question": q_text,
        "question_image": (row.get("question_image") or row.get("image") or row.get("image_url") or "").strip() or None,
        "option_a": (row.get("option_a") or "").strip() or None,
        "option_b": (row.get("option_b") or "").strip() or None,
        "option_c": (row.get("option_c") or "").strip() or None,
        "option_d": (row.get("option_d") or "").strip() or None,
        "correct_option": (row.get("correct_option") or "").strip() or None,
        "answer_text": (row.get("answer_text") or "").strip() or None,
    }


def make_subject_question(subject_name: str, topic_name: str) -> str:
    subject_lower = subject_name.lower()

    if "practical" in subject_lower:
        return f"How well were you able to perform the practical activity for {topic_name} in {subject_name}?"
    if "drawing" in subject_lower:
        return f"How clearly did you understand the drawing-related part of {topic_name} in {subject_name}?"
    if "calculation" in subject_lower:
        return f"How well were you able to solve the calculation part of {topic_name} in {subject_name}?"
    if "employability" in subject_lower:
        return f"How well did you understand the application of {topic_name} in {subject_name}?"
    return f"How well did you understand the concept of {topic_name} in {subject_name}?"


def generate_questions(payload: MonthlyTopicInput) -> QuestionGenerationResponse:
    theory_bank, practical_bank = _load_question_banks()
    month_name = _month_name_from_payload(payload.month)

    def _question_for(subject: str, topic: str) -> dict:
        return generate_question_for_subject_topic(
            subject=subject,
            topic=topic,
            month=payload.month,
            trade_name=payload.trade_name,
        )

    questions = [
        GeneratedQuestion(
            subject=payload.subject_1,
            topic=payload.topic_1,
            **_question_for(payload.subject_1, payload.topic_1),
        ),
        GeneratedQuestion(
            subject=payload.subject_2,
            topic=payload.topic_2,
            **_question_for(payload.subject_2, payload.topic_2),
        ),
        GeneratedQuestion(
            subject=payload.subject_3,
            topic=payload.topic_3,
            **_question_for(payload.subject_3, payload.topic_3),
        ),
    ]

    return QuestionGenerationResponse(
        month=payload.month,
        district=payload.district,
        trade_name=payload.trade_name,
        year=payload.year,
        semester=payload.semester,
        questions=questions,
    )


def generate_question_for_subject_topic(subject: str, topic: str, month: str, trade_name: str = "") -> dict:
    theory_bank, practical_bank = _load_question_banks()
    month_name = _month_name_from_payload(month)
    subject_n = _normalize(subject)
    candidate_banks: list[list[dict]]
    if "practical" in subject_n:
        candidate_banks = [practical_bank, theory_bank]
    elif "theory" in subject_n:
        candidate_banks = [theory_bank, practical_bank]
    else:
        candidate_banks = [theory_bank, practical_bank]

    for bank in candidate_banks:
        bank_q = _pick_question_from_bank(
            records=bank,
            subject=subject,
            topic=topic,
            month_name=month_name,
            trade_name=trade_name,
        )
        if bank_q:
            return bank_q
    return {
        "question": make_subject_question(subject, topic),
        "question_image": None,
        "option_a": None,
        "option_b": None,
        "option_c": None,
        "option_d": None,
        "correct_option": None,
        "answer_text": None,
    }


def generate_theory_question_for_subject_topic(subject: str, topic: str) -> dict:
    theory_prompts = [
        f"Explain the concept of {topic} in {subject} with one practical example.",
        f"Write short notes on {topic} and its importance in {subject}.",
        f"Describe how {topic} is applied in real workshop/industry situations for {subject}.",
        f"What are the key steps, precautions, and outcomes related to {topic} in {subject}?",
    ]
    return {
        "question": random.choice(theory_prompts),
        "question_image": None,
        "option_a": None,
        "option_b": None,
        "option_c": None,
        "option_d": None,
        "correct_option": None,
        "answer_text": None,
    }


if __name__ == "__main__":
    sample = MonthlyTopicInput(
        month="2026-03",
        district="Vijayawada",
        institute_name="Govt ITI Vijayawada",
        trade_name="Electrician",
        year=1,
        semester=1,
        subject_1="Trade Theory",
        topic_1="Ohm's Law",
        subject_2="Trade Practical",
        topic_2="Meters and Measurement",
        subject_3="Workshop Calculation",
        topic_3="Series and Parallel Circuits",
    )

    result = generate_questions(sample)
    print(result.model_dump())
