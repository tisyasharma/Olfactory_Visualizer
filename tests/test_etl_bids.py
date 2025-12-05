import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from code.database.etl import bids, subjects
from code.database.etl.paths import BIDS_ROOT as ORIGINAL_BIDS_ROOT
from code.src.conversion.config_map import SUBJECT_MAP


def make_mem_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE subjects (
                subject_id VARCHAR(50) PRIMARY KEY,
                original_id VARCHAR(100),
                sex CHAR(1),
                experiment_type VARCHAR(50),
                details TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE sessions (
                session_id VARCHAR(50) PRIMARY KEY,
                subject_id VARCHAR(50),
                modality VARCHAR(50),
                session_date DATE,
                protocol TEXT,
                notes TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE microscopy_files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id VARCHAR(50),
                run INT,
                hemisphere VARCHAR(20),
                path TEXT NOT NULL,
                sha256 CHAR(64),
                created_at TEXT,
                UNIQUE(session_id, run, hemisphere)
            );
        """))
        conn.execute(text("""
            CREATE TABLE ingest_log (
                ingest_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT,
                checksum CHAR(64),
                rows_loaded INT,
                status TEXT,
                message TEXT,
                created_at TEXT
            );
        """))
    return engine


def test_bids_scan_picks_up_omero_zarr(tmp_path, monkeypatch):
    engine = make_mem_engine()
    stats = {}
    allowed = {"sub-foo"}

    # Fake BIDS layout with omero.zarr
    zarr_dir = tmp_path / "sub-foo" / "ses-01" / "micr" / "sub-foo_ses-01_sample-brain_stain-native_run-01_omero.zarr"
    (zarr_dir / "0").mkdir(parents=True)
    (zarr_dir / "0" / "dummy.txt").write_text("x")

    # Point BIDS_ROOT to tmp
    monkeypatch.setattr(bids, "BIDS_ROOT", tmp_path)

    try:
        bids.load_bids_files(engine, stats, allowed_subjects=allowed)
    except OperationalError as e:
        if "near \"DO\"" in str(e):
            pytest.skip("SQLite version lacks ON CONFLICT DO support")
        raise

    with engine.connect() as conn:
        sess_count = conn.execute(text("SELECT COUNT(*) FROM sessions")).scalar_one()
        file_row = conn.execute(text("SELECT path FROM microscopy_files")).first()
        subj_row = conn.execute(text("SELECT subject_id FROM subjects")).first()

    assert sess_count == 1
    assert file_row is not None and str(zarr_dir) in file_row[0]
    assert subj_row[0] == "sub-foo"


def test_seed_and_bids_smoke(tmp_path, monkeypatch):
    engine = make_mem_engine()
    stats = {}
    allowed_subjects = {meta["subject"] for meta in SUBJECT_MAP.values()}

    # Seed subjects/sessions from config
    try:
        with engine.begin() as conn:
            subjects.seed_subjects_and_sessions(conn, stats)
    except OperationalError as e:
        if "near \"DO\"" in str(e):
            pytest.skip("SQLite version lacks ON CONFLICT DO support")
        raise

    base_session_count = 0
    with engine.connect() as conn:
        base_session_count = conn.execute(text("SELECT COUNT(*) FROM sessions")).scalar_one()

    # Create a single zarr for a known subject
    zarr_dir = tmp_path / "sub-rab01" / "ses-01" / "micr" / "sub-rab01_ses-01_sample-brain_stain-native_run-01_omero.zarr"
    (zarr_dir / "0").mkdir(parents=True)
    (zarr_dir / "0" / "dummy.txt").write_text("x")
    monkeypatch.setattr(bids, "BIDS_ROOT", tmp_path)

    try:
        bids.load_bids_files(engine, stats, allowed_subjects=allowed_subjects)
    except OperationalError as e:
        if "near \"DO\"" in str(e):
            pytest.skip("SQLite version lacks ON CONFLICT DO support")
        raise

    with engine.connect() as conn:
        sess_count = conn.execute(text("SELECT COUNT(*) FROM sessions")).scalar_one()
        file_count = conn.execute(text("SELECT COUNT(*) FROM microscopy_files")).scalar_one()

    assert sess_count == base_session_count + 1
    assert file_count == 1
