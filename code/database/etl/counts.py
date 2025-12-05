"""
Quantification CSV ingest.
Reason: encapsulate checksum dedupe, session creation, and counts insert logic.
"""
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import text, types as satypes
from .utils import (
    clean_numeric,
    detect_hemisphere,
    get_or_create_session_id,
    load_table,
    file_sha256,
)
from code.src.conversion.config_map import SUBJECT_MAP
from .paths import DATA_ROOT


def ingest_counts(engine, unit_map, atlas_map, file_map, stats):
    count_rows = []
    session_rows_from_counts = []
    session_cache = {}
    existing_sessions = {}
    existing_session_ids = []
    with engine.connect() as conn:
        for row in conn.execute(text("SELECT subject_id, session_id FROM sessions")):
            existing_sessions.setdefault(row.subject_id, []).append(row.session_id)
            existing_session_ids.append(row.session_id)
    extra_regions = []

    seen_checksums = set()
    with engine.connect() as conn:
        for row in conn.execute(text("SELECT checksum FROM ingest_log WHERE status = 'success' AND checksum IS NOT NULL")):
            seen_checksums.add(row.checksum)

    for root, dirs, files in os.walk(DATA_ROOT):
        for file in files:
            if not file.endswith(".csv"):
                continue

            matched_key = None
            root_base = os.path.basename(root)
            file_lower = file.lower()
            root_lower = root_base.lower()
            for key in SUBJECT_MAP.keys():
                key_lower = key.lower()
                if key_lower in file_lower or key_lower == root_lower:
                    matched_key = key
                    break
            if not matched_key:
                continue

            subject_id = SUBJECT_MAP[matched_key]["subject"]
            exp_type = "rabies" if "rabies" in matched_key.lower() else "double_injection"
            hemi = detect_hemisphere(root, file)

            print(f"  Processing {file} ({hemi}) -> {subject_id}")

            csv_path = os.path.join(root, file)
            chk = file_sha256(Path(csv_path))
            if chk in seen_checksums:
                stats["counts_skipped_dupe_files"] = stats.get("counts_skipped_dupe_files", 0) + 1
                continue

            df = load_table(csv_path)

            required_cols = {"Region ID", "Region name", "Region pixels", "Region area", "Load"}
            missing = required_cols - set(df.columns)
            if missing:
                print(f"   ⚠️ Skipping {file}: missing required columns {missing}")
                continue

            optional_missing = {"Object count", "Object pixels", "Object area", "Norm load"} - set(df.columns)
            if optional_missing:
                print(f"   ℹ️  {file}: optional columns missing {optional_missing} -> will fill NULLs")

            df = df.rename(columns={
                "Region ID": "region_id",
                "Region name": "region_name",
                "Region pixels": "region_pixels",
                "Region area": "region_area",
                "Object count": "object_count",
                "Object pixels": "object_pixels",
                "Object area": "object_area",
                "Load": "load",
                "Norm load": "norm_load"
            })

            current_map = {int(r.region_id): str(r.region_name) for r in df.itertuples(index=False)}
            for rid, name in current_map.items():
                ref_name = atlas_map.get(rid)
                if ref_name is None:
                    atlas_map[rid] = name
                    extra_regions.append(
                        {
                            "region_id": rid,
                            "name": name,
                            "acronym": name,
                            "parent_id": None,
                            "st_level": None,
                            "atlas_id": None,
                            "ontology_id": None,
                        }
                    )
                elif ref_name != name:
                    raise ValueError(
                        f"Atlas mismatch in {file}: region_id {rid} name '{name}' "
                        f"does not match reference '{ref_name}'."
                    )

            for r in df.itertuples(index=False):
                file_id = file_map.get((subject_id, hemi))
                sess = session_cache.get(subject_id)
                if not sess:
                    sess = get_or_create_session_id(None, subject_id, exp_type, existing_sessions, existing_session_ids)
                    session_cache[subject_id] = sess
                    existing_sessions.setdefault(subject_id, []).append(sess)
                    existing_session_ids.append(sess)
                session_rows_from_counts.append(
                    {
                        "session_id": sess,
                        "subject_id": subject_id,
                        "modality": "micr",
                        "session_date": None,
                        "protocol": None,
                        "notes": None,
                    }
                )
                count_rows.append(
                    {
                        "subject_id": subject_id,
                        "region_id": int(r.region_id),
                        "region_pixels": clean_numeric(r.region_pixels),
                        "region_area_mm": clean_numeric(getattr(r, "region_area", None)),
                        "object_count": clean_numeric(getattr(r, "object_count", None)),
                        "object_pixels": clean_numeric(getattr(r, "object_pixels", None)),
                        "object_area_mm": clean_numeric(getattr(r, "object_area", None)),
                        "load": clean_numeric(r.load),
                        "norm_load": clean_numeric(getattr(r, "norm_load", None)),
                        "hemisphere": hemi,
                        "file_id": file_id,
                        "region_pixels_unit_id": unit_map.get("pixels"),
                        "region_area_unit_id": unit_map.get("pixels"),  # area is in pixels in source
                        "object_count_unit_id": unit_map.get("count"),
                        "object_pixels_unit_id": unit_map.get("pixels"),
                        "object_area_unit_id": unit_map.get("pixels"),
                        "load_unit_id": unit_map.get("pixels"),
                    }
                )

            seen_checksums.add(chk)
            stats["counts_ingested_rows"] = stats.get("counts_ingested_rows", 0) + len(df)
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO ingest_log (source_path, checksum, rows_loaded, status, message) "
                        "VALUES (:p, :c, :r, :s, :m)"
                    ),
                    {"p": str(csv_path), "c": chk, "r": len(df), "s": "success", "m": "ETL counts ingest"},
                )

    return count_rows, session_rows_from_counts, extra_regions


def insert_counts(engine, count_rows, session_rows_from_counts, extra_regions):
    if not count_rows and not extra_regions and not session_rows_from_counts:
        return
    import pandas as pd
    from sqlalchemy import text, types as satypes

    with engine.begin() as conn:
        if session_rows_from_counts:
            df_sess_counts = pd.DataFrame(session_rows_from_counts).drop_duplicates(subset=["session_id"])
            sess_stage = "_sessions_counts_stage"
            df_sess_counts.to_sql(
                sess_stage,
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
                    FROM {sess_stage}
                    ON CONFLICT (session_id) DO NOTHING;
                    """
                )
            )
            conn.execute(text(f"DROP TABLE IF EXISTS {sess_stage};"))

        if extra_regions:
            df_extra = pd.DataFrame(extra_regions).drop_duplicates(subset=["region_id"])
            if not df_extra.empty:
                extra_stage = "_brain_regions_extra_stage"
                df_extra.to_sql(
                    extra_stage,
                    con=conn,
                    if_exists="replace",
                    index=False,
                    method="multi",
                    dtype={
                        "region_id": satypes.Integer(),
                        "name": satypes.String(255),
                        "acronym": satypes.String(50),
                        "parent_id": satypes.Integer(),
                        "st_level": satypes.Integer(),
                        "atlas_id": satypes.Integer(),
                        "ontology_id": satypes.Integer(),
                    },
                )
                conn.execute(text(f"""
                    INSERT INTO brain_regions (region_id, name, acronym, parent_id, st_level, atlas_id, ontology_id)
                    SELECT region_id, name, acronym, parent_id, st_level, atlas_id, ontology_id
                    FROM {extra_stage}
                    ON CONFLICT (region_id) DO NOTHING;
                """))
                conn.execute(text(f"DROP TABLE IF EXISTS {extra_stage};"))

        if count_rows:
            df_counts = pd.DataFrame(count_rows)
            df_counts = df_counts.dropna(subset=["region_pixels", "load"])
            temp_table = "_region_counts_stage"
            df_counts.to_sql(
                temp_table,
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
                dtype={
                    "subject_id": satypes.String(50),
                    "region_id": satypes.Integer(),
                    "file_id": satypes.Integer(),
                    "region_pixels": satypes.BigInteger(),
                    "region_area_mm": satypes.Float(),
                    "object_count": satypes.Integer(),
                    "object_pixels": satypes.BigInteger(),
                    "object_area_mm": satypes.Float(),
                    "load": satypes.Float(),
                    "norm_load": satypes.Float(),
                    "hemisphere": satypes.String(20),
                    "region_pixels_unit_id": satypes.Integer(),
                    "region_area_unit_id": satypes.Integer(),
                    "object_count_unit_id": satypes.Integer(),
                    "object_pixels_unit_id": satypes.Integer(),
                    "object_area_unit_id": satypes.Integer(),
                    "load_unit_id": satypes.Integer(),
                },
            )
            conn.execute(text(f"""
                INSERT INTO region_counts (subject_id, region_id, file_id, region_pixels, region_area_mm, object_count, object_pixels, object_area_mm, load, norm_load, hemisphere,
                                          region_pixels_unit_id, region_area_unit_id, object_count_unit_id, object_pixels_unit_id, object_area_unit_id, load_unit_id)
                SELECT subject_id, region_id, file_id, region_pixels, region_area_mm, object_count, object_pixels, object_area_mm, load, norm_load, hemisphere,
                       region_pixels_unit_id, region_area_unit_id, object_count_unit_id, object_pixels_unit_id, object_area_unit_id, load_unit_id
                FROM {temp_table}
                ON CONFLICT (subject_id, region_id, hemisphere) DO NOTHING;
            """))
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
