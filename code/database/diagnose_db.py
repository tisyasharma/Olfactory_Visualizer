"""
Quick database diagnostics for microscopy + quant tables.
Reason: provide a fast sanity check after ETL so you can verify coverage without manual SQL.
"""
import sys
from pathlib import Path
from sqlalchemy import text

# Ensure project root is on path so `code` package is importable when run as a script
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from code.database.connect import get_engine
from code.src.conversion.config_map import SUBJECT_MAP


def main():
    engine = get_engine()
    cfg_subjects = {meta["subject"] for meta in SUBJECT_MAP.values()}

    with engine.connect() as conn:
        total_subjects = conn.execute(text("SELECT COUNT(*) FROM subjects")).scalar_one()
        total_sessions = conn.execute(text("SELECT COUNT(*) FROM sessions")).scalar_one()
        total_files = conn.execute(text("SELECT COUNT(*) FROM microscopy_files")).scalar_one()
        total_counts = conn.execute(text("SELECT COUNT(*) FROM region_counts")).scalar_one()

        subjects_in_db = {row[0] for row in conn.execute(text("SELECT subject_id FROM subjects"))}
        missing_subjects = sorted(cfg_subjects - subjects_in_db)
        extra_subjects = sorted(subjects_in_db - cfg_subjects)

        per_subject_files = list(
            conn.execute(
                text(
                    """
                    SELECT s.subject_id, COUNT(*) AS files
                    FROM microscopy_files mf
                    JOIN sessions s ON mf.session_id = s.session_id
                    GROUP BY s.subject_id
                    ORDER BY s.subject_id
                    """
                )
            )
        )

        per_subject_hemi = list(
            conn.execute(
                text(
                    """
                    SELECT subject_id, hemisphere, COUNT(*) AS rows
                    FROM region_counts
                    GROUP BY subject_id, hemisphere
                    ORDER BY subject_id, hemisphere
                    """
                )
            )
        )

        per_subject_file_hemi = list(
            conn.execute(
                text(
                    """
                    SELECT s.subject_id, COALESCE(mf.hemisphere,'bilateral') AS hemisphere, COUNT(*) AS files
                    FROM microscopy_files mf
                    JOIN sessions s ON mf.session_id = s.session_id
                    GROUP BY s.subject_id, COALESCE(mf.hemisphere,'bilateral')
                    ORDER BY s.subject_id, hemisphere
                    """
                )
            )
        )

        per_subject_counts_total = list(
            conn.execute(
                text(
                    """
                    SELECT subject_id, COUNT(*) AS rows
                    FROM region_counts
                    GROUP BY subject_id
                    ORDER BY subject_id
                    """
                )
            )
        )

        ingest_by_status = list(
            conn.execute(
                text(
                    "SELECT status, COUNT(*) AS n FROM ingest_log GROUP BY status ORDER BY status"
                )
            )
        )

    print("\n=== DB Diagnostics ===")
    print(f"Subjects:          {total_subjects}")
    print(f"Sessions:          {total_sessions}")
    print(f"Microscopy files:  {total_files}")
    print(f"Region count rows: {total_counts}")

    if missing_subjects:
        print(f"\n⚠️ Missing subjects (present in config, absent in DB): {', '.join(missing_subjects)}")
    if extra_subjects:
        print(f"\n⚠️ Extra subjects (in DB, not in config): {', '.join(extra_subjects)}")

    if per_subject_files:
        print("\nMicroscopy files per subject:")
        for subj, files in per_subject_files:
            print(f"  {subj}: {files}")
    if per_subject_file_hemi:
        print("\nMicroscopy files per subject/hemisphere:")
        for subj, hemi, files in per_subject_file_hemi:
            print(f"  {subj} [{hemi}]: {files}")

    if per_subject_hemi:
        print("\nRegion counts per subject/hemisphere:")
        for subj, hemi, rows in per_subject_hemi:
            print(f"  {subj} [{hemi}]: {rows}")
    if per_subject_counts_total:
        print("\nRegion counts per subject (all hemispheres):")
        for subj, rows in per_subject_counts_total:
            print(f"  {subj}: {rows}")

    if ingest_by_status:
        print("\nIngest log by status:")
        for status, n in ingest_by_status:
            print(f"  {status}: {n}")


if __name__ == "__main__":
    main()
