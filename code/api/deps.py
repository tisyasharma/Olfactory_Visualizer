"""
Shared API utilities.
Reason: keep common helpers (DB engine, session resolution, hashing) separate from route wiring.
"""
import hashlib
from pathlib import Path
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text

from code.database.connect import get_engine
from code.database.etl.utils import get_or_create_session_id, load_table, clean_numeric


def resolve_session_id(engine, subject_id: str, experiment_type: str, session_id: Optional[str]) -> str:
    sid = (session_id or "").strip()
    if sid.lower() == "auto" or sid == "":
        with engine.connect() as conn:
            sid = get_or_create_session_id(conn, subject_id, experiment_type)
    return sid


def sha256_path(path: Path, chunk_size: int = 1_048_576) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def fetch_all(query: str, params: dict = None):
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(query), params or {})
        cols = rows.keys()
        return [dict(zip(cols, row)) for row in rows]


__all__ = [
    "get_engine",
    "resolve_session_id",
    "sha256_path",
    "fetch_all",
    "get_or_create_session_id",
    "load_table",
    "clean_numeric",
]
