from __future__ import annotations

import re
from functools import lru_cache
from typing import Dict, List

from transformers import pipeline


MODEL_NAME = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"
NEUTRAL_THRESHOLD = 0.60
NEUTRAL_SCORE_BAND = 0.15


@lru_cache(maxsize=1)
def get_sentiment_pipeline():
    return pipeline(
        "sentiment-analysis",
        model=MODEL_NAME,
    )


def normalize_text(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def split_clauses(text: str) -> List[str]:
    pattern = r"\bbut\b|\bhowever\b|\bthough\b|\balthough\b|\byet\b"
    parts = re.split(pattern, text, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p.strip()]


def map_binary_to_score(label: str, score: float) -> float:
    confidence = min(max(float(score), 0.0), 1.0)
    magnitude = max((confidence - 0.5) * 2.0, 0.0)
    if label.upper() == "POSITIVE":
        return round(magnitude, 4)
    return round(-magnitude, 4)


def classify_text(text: str) -> Dict:
    clf = get_sentiment_pipeline()
    result = clf(text)[0]

    raw_label = str(result["label"]).upper()
    confidence = float(result["score"])
    signed_score = map_binary_to_score(raw_label, confidence)

    return {
        "raw_label": raw_label,
        "confidence": round(confidence, 4),
        "signed_score": signed_score,
    }


def map_score_to_final_label(score: float) -> str:
    if score >= NEUTRAL_SCORE_BAND:
        return "positive"
    if score <= -NEUTRAL_SCORE_BAND:
        return "negative"
    return "neutral"


def analyze_sentiment(text: str) -> Dict:
    text = normalize_text(text)

    if not text:
        return {
            "sentiment_label": "neutral",
            "sentiment_score": 0.0,
            "confidence": 0.0,
            "raw_model_label": None,
            "clause_details": [],
        }

    full_pred = classify_text(text)
    clauses = split_clauses(text)

    clause_details = []
    clause_scores = []

    if len(clauses) >= 2:
        for clause in clauses:
            pred = classify_text(clause)

            if pred["confidence"] < NEUTRAL_THRESHOLD:
                clause_score = 0.0
                clause_label = "neutral"
            else:
                clause_score = pred["signed_score"]
                clause_label = map_score_to_final_label(clause_score)

            clause_details.append(
                {
                    "text": clause,
                    "label": clause_label,
                    "confidence": pred["confidence"],
                    "score": clause_score,
                }
            )
            clause_scores.append(clause_score)

        avg_score = sum(clause_scores) / len(clause_scores)
        final_score = round(avg_score, 4)
        final_label = map_score_to_final_label(final_score)
        if final_label == "neutral":
            final_score = 0.0

        return {
            "sentiment_label": final_label,
            "sentiment_score": final_score,
            "confidence": full_pred["confidence"],
            "raw_model_label": full_pred["raw_label"],
            "clause_details": clause_details,
        }

    if full_pred["confidence"] < NEUTRAL_THRESHOLD:
        final_score = 0.0
    else:
        final_score = full_pred["signed_score"]

    final_label = map_score_to_final_label(final_score)
    if final_label == "neutral":
        final_score = 0.0

    return {
        "sentiment_label": final_label,
        "sentiment_score": final_score,
        "confidence": full_pred["confidence"],
        "raw_model_label": full_pred["raw_label"],
        "clause_details": clause_details,
    }


if __name__ == "__main__":
    samples = [
        "Training was very clear and helpful.",
        "Training was useful but I need more practice on meter reading.",
        "I could not understand the practical session and the explanation was not clear.",
        "The teacher explained well, but tools were not available.",
        "",
    ]

    for s in samples:
        print("=" * 80)
        print("TEXT:", s)
        print(analyze_sentiment(s))
