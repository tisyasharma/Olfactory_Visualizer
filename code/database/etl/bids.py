"""
BIDS / OME-Zarr scan stage.
Reason: register microscopy sessions/files from raw_bids with hash dedupe.
"""
import re
import pandas as pd
from sqlalchemy import text, types as satypes
from .paths import BIDS_ROOT
from .utils import file_sha256, detect_hemisphere, get_or_create_session_id


def load_bids_files(engine, stats: dict, existing_hashes: set | None = None, allowed_subjects: set | None = None):
    if not BIDS_ROOT.exists():
        print(f"⚠️ BIDS root not found at {BIDS_ROOT}, skipping file registration.")
        return

    if existing_hashes is None:
        existing_hashes = set()
        with engine.connect() as conn:
            for row in conn.execute(text("SELECT sha256 FROM microscopy_files WHERE sha256 IS NOT NULL")):
                existing_hashes.add(row.sha256)

    existing_sessions = {}
    existing_session_ids = []
    with engine.connect() as conn:
        for row in conn.execute(text("SELECT subject_id, session_id FROM sessions")):
            existing_sessions.setdefault(row.subject_id, []).append(row.session_id)
            existing_session_ids.append(row.session_id)

    records = []
    # Accept any microscopy Zarr (common suffixes: .ome.zarr, _omero.zarr)
    for zarr in BIDS_ROOT.rglob("*.zarr"):
        parts = zarr.relative_to(BIDS_ROOT).parts
        if len(parts) < 4:
            continue
        subject_id = parts[0]
        if allowed_subjects is not None and subject_id not in allowed_subjects:
            stats["microscopy_skipped_unknown_subject"] = stats.get("microscopy_skipped_unknown_subject", 0) + 1
            continue
        session_label = parts[1]
        if not re.match(r"^ses-[A-Za-z0-9]+$", session_label):
            stats["microscopy_skipped_bad_session_label"] = stats.get("microscopy_skipped_bad_session_label", 0) + 1
            print(f"   ⚠️ Skipping {zarr}: invalid session label '{session_label}' (expected ses-XX).")
            continue
        exp_type = "rabies" if "rab" in subject_id else "double_injection"
        modality = "micr"
        session_id = get_or_create_session_id(
            None,
            subject_id,
            exp_type,
            existing_sessions=existing_sessions,
            existing_ids=existing_session_ids,
        )
        run = None
        hemisphere = detect_hemisphere(zarr.parent.as_posix(), zarr.name)
        if "run-" in zarr.name:
            try:
                run = int(zarr.name.split("run-")[1].split("_")[0])
            except Exception:
                run = None
        sha = file_sha256(zarr)
        if sha in existing_hashes:
            stats["microscopy_skipped_dupe"] = stats.get("microscopy_skipped_dupe", 0) + 1
            continue
        existing_hashes.add(sha)
        records.append(
            {
                "subject_id": subject_id,
                "session_id": session_id,
                "modality": modality,
                "exp_type": exp_type,
                "run": run,
                "hemisphere": hemisphere,
                "path": str(zarr),
                "sha256": sha,
            }
        )

    if not records:
        print("⚠️ No OME-Zarr files found under BIDS root.")
        return

    sessions_rows = []
    files_rows = []
    subjects_rows = []
    for r in records:
        subjects_rows.append(
            {
                "subject_id": r["subject_id"],
                "original_id": r["subject_id"],
                "sex": "U",
                "experiment_type": r["exp_type"],
                "details": "",
            }
        )
        sessions_rows.append(
            {
                "session_id": r["session_id"],
                "subject_id": r["subject_id"],
                "modality": "micr",  # microscopy only
                "session_date": None,
                "protocol": None,
                "notes": None,
            }
        )
        files_rows.append(
            {
                "session_id": r["session_id"],
                "run": r["run"],
                "hemisphere": r["hemisphere"],
                "path": r["path"],
                "sha256": r["sha256"],
            }
        )

    with engine.begin() as conn:
        if subjects_rows:
            df_subj = pd.DataFrame(subjects_rows).drop_duplicates(subset=["subject_id"])
            subj_stage = "_subjects_stage"
            df_subj.to_sql(
                subj_stage,
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
                dtype={
                    "subject_id": satypes.String(50),
                    "original_id": satypes.String(100),
                    "sex": satypes.String(1),
                    "experiment_type": satypes.String(50),
                    "details": satypes.Text(),
                },
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO subjects (subject_id, original_id, sex, experiment_type, details)
                    SELECT subject_id, original_id, sex, experiment_type, details
                    FROM {subj_stage}
                    ON CONFLICT (subject_id) DO NOTHING;
                    """
                )
            )
            conn.execute(text(f"DROP TABLE IF EXISTS {subj_stage};"))
        if sessions_rows:
            df_sess = pd.DataFrame(sessions_rows).drop_duplicates(subset=["session_id"])
            sessions_stage = "_sessions_stage"
            df_sess.to_sql(
                sessions_stage,
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
                dtype={
                    "session_id": satypes.String(50),
                    "subject_id": satypes.String(50),
                    "modality": satypes.String(50),
                    "session_date": satypes.Date(),
                    "protocol": satypes.Text(),
                    "notes": satypes.Text(),
                },
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO sessions (session_id, subject_id, modality, session_date, protocol, notes)
                    SELECT session_id, subject_id, modality, session_date, protocol, notes
                    FROM {sessions_stage}
                    ON CONFLICT (session_id) DO NOTHING;
                    """
                )
            )
            conn.execute(text(f"DROP TABLE IF EXISTS {sessions_stage};"))
            stats["sessions_from_bids"] = stats.get("sessions_from_bids", 0) + len(df_sess)
        if files_rows:
            df_files = pd.DataFrame(files_rows).drop_duplicates(subset=["session_id", "run", "hemisphere"])
            files_stage = "_microscopy_files_stage"
            df_files.to_sql(
                files_stage,
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
                dtype={
                    "session_id": satypes.String(50),
                    "run": satypes.Integer(),
                    "hemisphere": satypes.String(20),
                    "path": satypes.Text(),
                    "sha256": satypes.String(64),
                },
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO microscopy_files (session_id, run, hemisphere, path, sha256)
                    SELECT session_id, run, hemisphere, path, sha256
                    FROM {files_stage}
                    ON CONFLICT (session_id, run, hemisphere) DO NOTHING;
                    """
                )
            )
            conn.execute(text(f"DROP TABLE IF EXISTS {files_stage};"))
            stats["microscopy_inserted"] = stats.get("microscopy_inserted", 0) + len(df_files)


def build_file_map(engine):
    """
    Build a map (subject_id, hemisphere) -> file_id (first run) from microscopy_files.
    """
    file_map = {}
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.subject_id, mf.hemisphere, mf.file_id, mf.run
            FROM microscopy_files mf
            JOIN sessions s ON mf.session_id = s.session_id
            ORDER BY COALESCE(mf.run, 0), mf.file_id
        """))
        for r in rows:
            hemi = r.hemisphere or "bilateral"
            key = (r.subject_id, hemi)
            if key not in file_map:
                file_map[key] = r.file_id
    return file_map
