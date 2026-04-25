from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import requests


GROQ_API_BASE = os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")


def _extract_json(text: str) -> dict:
    raw = (text or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(raw[start : end + 1])
        except Exception:
            return {}
    return {}


def evaluate_theory_responses_with_groq(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    items: [{index, question, answer}]
    returns: {"ok": bool, "results": [{index, score, feedback, key_points}]}
    """
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not api_key:
        return {"ok": False, "error": "GROQ_API_KEY is not configured.", "results": []}

    payload_items = [
        {
            "index": int(i.get("index", 0)),
            "question": str(i.get("question", "")).strip(),
            "answer": str(i.get("answer", "")).strip(),
        }
        for i in items
        if str(i.get("answer", "")).strip()
    ]
    if not payload_items:
        return {"ok": True, "results": []}

    system_prompt = (
        "You are an ITI evaluator. Score each answer from 0 to 5 based on technical correctness, "
        "concept coverage, and clarity. Respond strictly in JSON with this schema: "
        "{\"results\":[{\"index\":1,\"score\":3.5,\"feedback\":\"...\",\"key_points\":[\"...\",\"...\"]}]}"
    )
    user_prompt = json.dumps({"items": payload_items}, ensure_ascii=False)

    resp = requests.post(
        f"{GROQ_API_BASE.rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": GROQ_MODEL,
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {"type": "json_object"},
        },
        timeout=45,
    )
    if not resp.ok:
        return {"ok": False, "error": f"Groq API error: {resp.status_code} {resp.text}", "results": []}
    data = resp.json()
    content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    parsed = _extract_json(content)
    results = parsed.get("results", []) if isinstance(parsed, dict) else []
    clean: List[Dict[str, Any]] = []
    for r in results if isinstance(results, list) else []:
        try:
            score = float(r.get("score", 0.0))
        except Exception:
            score = 0.0
        score = max(0.0, min(5.0, score))
        kp = r.get("key_points", [])
        if not isinstance(kp, list):
            kp = []
        clean.append(
            {
                "index": int(r.get("index", 0)),
                "score": round(score, 2),
                "feedback": str(r.get("feedback", "")).strip(),
                "key_points": [str(x).strip() for x in kp if str(x).strip()],
            }
        )
    return {"ok": True, "results": clean}

