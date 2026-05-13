from __future__ import annotations

import re
from typing import Dict, List


NEUTRAL_SCORE_BAND = 0.15
CLAUSE_SPLIT_PATTERN = r"\bbut\b|\bhowever\b|\bthough\b|\balthough\b|\byet\b"
POSITIVE_TERMS = {
    "amazing",
    "clear",
    "effective",
    "engaging",
    "excellent",
    "good",
    "great",
    "helpful",
    "improved",
    "informative",
    "interactive",
    "nice",
    "positive",
    "practical",
    "satisfied",
    "strong",
    "supportive",
    "understandable",
    "useful",
    "well",
}
NEGATIVE_TERMS = {
    "bad",
    "boring",
    "confusing",
    "difficult",
    "disappointing",
    "hard",
    "issue",
    "lack",
    "missing",
    "negative",
    "not",
    "poor",
    "problem",
    "slow",
    "unclear",
    "unhelpful",
    "unsafe",
    "weak",
    "worse",
    "worst",
}
INTENSIFIERS = {"very", "extremely", "highly", "really", "too"}
NEGATIONS = {"no", "not", "never", "hardly", "scarcely", "barely", "without"}


def normalize_text(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def split_clauses(text: str) -> List[str]:
    parts = re.split(CLAUSE_SPLIT_PATTERN, text, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p.strip()]


def map_score_to_final_label(score: float) -> str:
    if score >= NEUTRAL_SCORE_BAND:
        return "positive"
    if score <= -NEUTRAL_SCORE_BAND:
        return "negative"
    return "neutral"


def _tokenize(text: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9']+", text.lower()) if token]


def classify_text(text: str) -> Dict:
    tokens = _tokenize(text)
    if not tokens:
        return {
            "raw_label": "NEUTRAL",
            "confidence": 0.0,
            "signed_score": 0.0,
        }

    weighted_hits = 0.0
    signed_hits = 0.0

    for idx, token in enumerate(tokens):
        prev = tokens[idx - 1] if idx > 0 else ""
        prev_two = tokens[idx - 2] if idx > 1 else ""
        negated = prev in NEGATIONS or prev_two in NEGATIONS
        weight = 1.5 if prev in INTENSIFIERS else 1.0

        if token in POSITIVE_TERMS:
            direction = -1.0 if negated else 1.0
            signed_hits += direction * weight
            weighted_hits += weight
        elif token in NEGATIVE_TERMS:
            direction = 1.0 if negated else -1.0
            signed_hits += direction * weight
            weighted_hits += weight

    if weighted_hits == 0.0:
        return {
            "raw_label": "NEUTRAL",
            "confidence": 0.0,
            "signed_score": 0.0,
        }

    normalized_score = max(min(signed_hits / weighted_hits, 1.0), -1.0)
    confidence = round(min(abs(normalized_score), 1.0), 4)
    raw_label = "POSITIVE" if normalized_score > 0 else "NEGATIVE"
    return {
        "raw_label": raw_label,
        "confidence": confidence,
        "signed_score": round(normalized_score, 4),
    }


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
            clause_score = pred["signed_score"]
            clause_label = map_score_to_final_label(clause_score)
            if clause_label == "neutral":
                clause_score = 0.0

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
