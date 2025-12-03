import os
import sys
from typing import Optional
import pandas as pd
import numpy as np
from sqlalchemy import text, types as satypes
from pathlib import Path
import json
import hashlib

# --- ROBUST PATH SETUP ---
# 1. Get the directory of this script (e.g., /capstone/code/database)
THIS_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Add the necessary module folders to sys.path
# a) Add 'code/database' for connect.py
sys.path.append(THIS_DIR)
# b) Add 'code/src/conversion' for config_map.py (Go up one, then down into src/conversion)
sys.path.append(os.path.abspath(os.path.join(THIS_DIR, '../src/conversion'))) 

# 3. Calculate DATA_ROOT
# Project root is two levels up from 'code/database'
project_root = os.path.abspath(os.path.join(THIS_DIR, '../../'))
DATA_ROOT = os.path.join(project_root, "data", "sourcedata", "quantification")
ATLAS_JSON = Path(project_root) / "allen_regions.json"
REQUIREMENTS_FILE = Path(project_root) / "requirements.txt"
BIDS_ROOT = Path(project_root) / "data" / "raw_bids"

# 4. Import critical modules (with error handling)
try:
    from connect import get_engine
    from config_map import SUBJECT_MAP
except ImportError as e:
    print(f"❌ ETL SETUP ERROR: Module import failed.")
    print(f"   Details: {e}")
    sys.exit(1)

# --- ETL CORE LOGIC ---

def clean_numeric(val):
    """Converts 'N/A' or bad strings to None (SQL NULL)"""
    if str(val).strip().upper() == 'N/A' or pd.isna(val):
        return None
    try:
        return float(val)
    except:
        return None

def detect_hemisphere(root: str, filename: str) -> str:
    """Infer hemisphere from folder or filename."""
    parts = [p.lower() for p in Path(root).parts]
    fname = filename.lower()
    if "left" in parts or "left" in fname:
        return "left"
    if "right" in parts or "right" in fname:
        return "right"
    if "both" in parts or "bilateral" in fname:
        return "bilateral"
    return "bilateral"

def load_table(csv_path: str) -> pd.DataFrame:
    """
    Read quantification CSV with delimiter sniffing and sep=; support.
    - Detect 'sep=;' header and skip it.
    - Drop unnamed/empty columns caused by trailing delimiters.
    """
    csv_path = Path(csv_path)
    with csv_path.open("r", errors="ignore") as f:
        first = f.readline()
    skiprows = 0
    sep = None
    if first.lower().startswith("sep="):
        sep = first.strip().split("=", 1)[1] or ";"
        skiprows = 1
    try:
        df = pd.read_csv(csv_path, sep=sep, engine="python", skiprows=skiprows)
    except Exception:
        df = pd.read_csv(csv_path, sep="\t", engine="python", skiprows=skiprows)
    # Drop empty/unnamed columns from trailing separators
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    df = df.dropna(axis=1, how="all")
    df.columns = df.columns.str.strip()
    return df

def file_sha256(path: Path, chunk_size: int = 1_048_576) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def load_bids_files(engine):
    """
    Walk BIDS root, ensure sessions/microscopy_files rows exist.
    """
    if not BIDS_ROOT.exists():
        print(f"⚠️ BIDS root not found at {BIDS_ROOT}, skipping file registration.")
        return

    records = []
    for zarr in BIDS_ROOT.rglob("*.ome.zarr"):
        parts = zarr.relative_to(BIDS_ROOT).parts
        # expect sub-*/ses-*/micr/filename
        if len(parts) < 4:
            continue
        subject_id = parts[0]
        session_id = parts[1]
        run = None
        hemisphere = "bilateral"
        if "run-" in zarr.name:
            try:
                run = int(zarr.name.split("run-")[1].split("_")[0])
            except Exception:
                run = None
        # modality inference
        modality = "micr"
        if "rabies" in session_id:
            modality = "rabies"
        elif "dbl" in session_id:
            modality = "double_injection"
        records.append(
            {
                "subject_id": subject_id,
                "session_id": session_id,
                "modality": modality,
                "run": run,
                "hemisphere": hemisphere,
                "path": str(zarr),
                "sha256": file_sha256(zarr),
            }
        )

    if not records:
        print("⚠️ No OME-Zarr files found under BIDS root.")
        return

    sessions_rows = []
    files_rows = []
    for r in records:
        sessions_rows.append(
            {
                "session_id": r["session_id"],
                "subject_id": r["subject_id"],
                "modality": r["modality"],
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

def flatten_atlas(node, parent_id=None):
    """Recursively flatten Allen atlas JSON tree into list of dicts."""
    rows = []
    rid = int(node["id"])
    rows.append(
        {
            "region_id": rid,
            "name": node["name"],
            "acronym": node["acronym"],
            "parent_id": parent_id,
            "st_level": node.get("st_level"),
            "atlas_id": node.get("atlas_id"),
            "ontology_id": node.get("ontology_id"),
        }
    )
    for child in node.get("children", []) or []:
        rows.extend(flatten_atlas(child, parent_id=rid))
    return rows

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

def run_etl():
    engine = get_engine()
    
    print(f"\n🚀 Starting ETL Pipeline...")
    print(f"Reading data from: {DATA_ROOT}")

    # 1. Insert Subjects (Mice) into DB
    print("\n--- Step 1: Loading Subjects ---")
    with engine.begin() as conn:
        for original_id, meta in SUBJECT_MAP.items():
            exp_type = "rabies" if "Rabies" in original_id else "double_injection"
            stmt = insert_subject(original_id, meta, exp_type)
            conn.execute(stmt)
        conn.execute(text("INSERT INTO ingest_log (source_path, status, message) VALUES (:p, :s, :m)"),
                     {"p": str(DATA_ROOT), "s": "started", "m": "ETL started"})

    # Register BIDS files into sessions/microscopy_files
    print("\n--- Step 2: Registering imaging files (BIDS) ---")
    load_bids_files(engine)
    file_map = build_file_map(engine)

    # 2. Load Allen Atlas regions into brain_regions
    print("\n--- Step 3: Loading Allen Atlas Regions ---")
    if not ATLAS_JSON.exists():
        raise FileNotFoundError(f"Atlas JSON not found at {ATLAS_JSON}")
    atlas_data = json.loads(ATLAS_JSON.read_text())
    root_node = atlas_data["msg"][0]
    atlas_rows = flatten_atlas(root_node)
    atlas_df = pd.DataFrame(atlas_rows).drop_duplicates(subset=["region_id"])
    atlas_map = {row["region_id"]: row["name"] for row in atlas_rows}
    # baseline units
    units_rows = [
        {"name": "pixels", "description": "Raw pixel counts"},
        {"name": "mm2", "description": "Square millimeters"},
        {"name": "count", "description": "Object count"},
    ]
    with engine.begin() as conn:
        brain_stage = "_brain_regions_stage"
        atlas_df.to_sql(
            brain_stage,
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
        conn.execute(
            text(
                f"""
                INSERT INTO brain_regions (region_id, name, acronym, parent_id, st_level, atlas_id, ontology_id)
                SELECT region_id, name, acronym, parent_id, st_level, atlas_id, ontology_id
                FROM {brain_stage}
                ON CONFLICT (region_id) DO NOTHING;
                """
            )
        )
        conn.execute(text(f"DROP TABLE IF EXISTS {brain_stage};"))
        # insert units (ignore conflicts)
        conn.execute(text("""
            INSERT INTO units (name, description) VALUES
            ('pixels','Raw pixel counts'),
            ('mm2','Square millimeters'),
            ('count','Object count')
            ON CONFLICT (name) DO NOTHING;
        """))
        # fetch unit ids
        unit_map = {r._mapping["name"]: r._mapping["unit_id"] for r in conn.execute(text("SELECT unit_id, name FROM units"))}

    # 3. Process CSVs
    print("\n--- Step 4: Processing Quantification Files ---")
    
    # Accumulator for bulk insert
    count_rows = []
    extra_regions = []  # for IDs present in CSVs but not in atlas_json (e.g., Clear Label)

    for root, dirs, files in os.walk(DATA_ROOT):
        for file in files:
            if not file.endswith(".csv"):
                continue

            matched_key = None
            for key in SUBJECT_MAP.keys():
                if key in file or key == os.path.basename(root):
                    matched_key = key
                    break
            if not matched_key:
                continue

            subject_id = SUBJECT_MAP[matched_key]["subject"]
            hemi = detect_hemisphere(root, file)

            print(f"  Processing {file} ({hemi}) -> {subject_id}")

            df = load_table(os.path.join(root, file))

            required_cols = {"Region ID", "Region name", "Region pixels", "Region area", "Load"}
            missing = required_cols - set(df.columns)
            if missing:
                print(f"   ⚠️ Skipping {file}: missing required columns {missing}")
                continue

            optional_missing = {"Object count", "Object pixels", "Object area", "Norm load"} - set(df.columns)
            if optional_missing:
                print(f"   ℹ️  {file}: optional columns missing {optional_missing} -> will fill NULLs")

            # Normalize column names used below
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

            # Atlas consistency check vs canonical map
            current_map = {int(r.region_id): str(r.region_name) for r in df.itertuples(index=False)}
            for rid, name in current_map.items():
                ref_name = atlas_map.get(rid)
                if ref_name is None:
                    # Allow new region (e.g., Clear Label) and track for insertion
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

            # Collect counts rows
            for r in df.itertuples(index=False):
                file_id = file_map.get((subject_id, hemi))
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

    # Bulk insert counts
    with engine.begin() as conn:
        if extra_regions:
            df_extra = pd.DataFrame(extra_regions).drop_duplicates(subset=["region_id"])
            df_extra.to_sql(
                "brain_regions",
                con=conn,
                if_exists="append",
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

        if count_rows:
            df_counts = pd.DataFrame(count_rows)
            # Drop rows with missing required fields
            df_counts = df_counts.dropna(subset=["region_pixels", "load"])
            # Upsert-like: rely on UNIQUE(subject_id, region_id, hemisphere) + ON CONFLICT DO NOTHING
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
        # log success
        conn.execute(text("INSERT INTO ingest_log (source_path, rows_loaded, status, message) VALUES (:p, :r, :s, :m)"),
                     {"p": str(DATA_ROOT), "r": len(count_rows), "s": "success", "m": "ETL complete"})

    print("\n✅ ETL Complete. Database hydrated.")

def ensure_unit_map(conn):
    """Guarantee baseline units exist and return a name->id map."""
    conn.execute(text("""
        INSERT INTO units (name, description) VALUES
        ('pixels','Raw pixel counts'),
        ('mm2','Square millimeters'),
        ('count','Object count')
        ON CONFLICT (name) DO NOTHING;
    """))
    return {row.name: row.unit_id for row in conn.execute(text("SELECT unit_id, name FROM units"))}

def ingest_counts_csv(engine, csv_path: Path, subject_id: str, session_id: Optional[str], hemisphere: str, experiment_type: str = "double_injection") -> int:
    """
    Ingest a single quantification CSV into region_counts for a given subject/session.
    - Validates columns, coerces numerics, and upserts rows.
    - Ensures subject/session/units exist; registers new brain regions if missing.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = load_table(csv_path)
    required_cols = {"Region ID", "Region name", "Region pixels", "Load"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

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

    hemi = hemisphere.lower()
    if hemi not in {"left", "right", "bilateral"}:
        raise ValueError(f"Invalid hemisphere: {hemisphere}")

    # Allow caller to omit session_id
    sess = session_id or f"ses-{experiment_type}"

    with engine.begin() as conn:
        # Ensure subject/session exist
        conn.execute(
            text("""
                INSERT INTO subjects (subject_id, original_id, sex, experiment_type, details)
                VALUES (:subj, :orig, 'U', :exp, '')
                ON CONFLICT (subject_id) DO NOTHING;
            """),
            {"subj": subject_id, "orig": subject_id, "exp": experiment_type},
        )
        conn.execute(
            text("""
                INSERT INTO sessions (session_id, subject_id, modality)
                VALUES (:sid, :subj, :mod)
                ON CONFLICT (session_id) DO NOTHING;
            """),
            {"sid": sess, "subj": subject_id, "mod": "micr"},
        )

        unit_map = ensure_unit_map(conn)

        # Register any unseen brain regions using provided names
        existing_regions = {row.region_id for row in conn.execute(text("SELECT region_id FROM brain_regions"))}
        new_regions = []
        for rid, name in {int(r.region_id): str(r.region_name) for r in df.itertuples(index=False)}.items():
            if rid not in existing_regions:
                new_regions.append(
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
        if new_regions:
            pd.DataFrame(new_regions).to_sql(
                "brain_regions",
                con=conn,
                if_exists="append",
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

        # Optionally link to an existing microscopy file for this subject/hemisphere
        file_id = None
        row = conn.execute(
            text("""
                SELECT mf.file_id
                FROM microscopy_files mf
                JOIN sessions s ON mf.session_id = s.session_id
                WHERE s.subject_id = :subj AND (mf.hemisphere = :hemi OR mf.hemisphere = 'bilateral')
                ORDER BY COALESCE(mf.run, 0), mf.file_id
                LIMIT 1
            """),
            {"subj": subject_id, "hemi": hemi},
        ).fetchone()
        if row:
            file_id = row[0]

        count_rows = []
        for r in df.itertuples(index=False):
            count_rows.append(
                {
                    "subject_id": subject_id,
                    "region_id": int(r.region_id),
                    "file_id": file_id,
                    "region_pixels": clean_numeric(r.region_pixels),
                    "region_area_mm": clean_numeric(getattr(r, "region_area", None)),
                    "object_count": clean_numeric(getattr(r, "object_count", None)),
                    "object_pixels": clean_numeric(getattr(r, "object_pixels", None)),
                    "object_area_mm": clean_numeric(getattr(r, "object_area", None)),
                    "load": clean_numeric(r.load),
                    "norm_load": clean_numeric(getattr(r, "norm_load", None)),
                    "hemisphere": hemi,
                    "region_pixels_unit_id": unit_map.get("pixels"),
                    "region_area_unit_id": unit_map.get("pixels"),
                    "object_count_unit_id": unit_map.get("count"),
                    "object_pixels_unit_id": unit_map.get("pixels"),
                    "object_area_unit_id": unit_map.get("pixels"),
                    "load_unit_id": unit_map.get("pixels"),
                }
            )

        if not count_rows:
            return 0

        df_counts = pd.DataFrame(count_rows).dropna(subset=["region_pixels", "load"])
        temp_table = "_region_counts_upload_stage"
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
            ON CONFLICT (subject_id, region_id, hemisphere)
            DO UPDATE SET
                file_id = EXCLUDED.file_id,
                region_pixels = EXCLUDED.region_pixels,
                region_area_mm = EXCLUDED.region_area_mm,
                object_count = EXCLUDED.object_count,
                object_pixels = EXCLUDED.object_pixels,
                object_area_mm = EXCLUDED.object_area_mm,
                load = EXCLUDED.load,
                norm_load = EXCLUDED.norm_load,
                region_pixels_unit_id = EXCLUDED.region_pixels_unit_id,
                region_area_unit_id = EXCLUDED.region_area_unit_id,
                object_count_unit_id = EXCLUDED.object_count_unit_id,
                object_pixels_unit_id = EXCLUDED.object_pixels_unit_id,
                object_area_unit_id = EXCLUDED.object_area_unit_id,
                load_unit_id = EXCLUDED.load_unit_id;
        """))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
        return len(df_counts)

def insert_subject(original_id: str, meta: dict, exp_type: str):
    stmt = text("""
        INSERT INTO subjects (subject_id, original_id, sex, experiment_type, details)
        VALUES (:sub, :orig, :sex, :exp, :det)
        ON CONFLICT (subject_id) DO NOTHING;
    """)
    return stmt.bindparams(
        sub=meta["subject"],
        orig=original_id,
        sex=meta.get("sex", "U"),
        exp=exp_type,
        det=meta.get("details", ""),
    )

if __name__ == "__main__":
    run_etl()
