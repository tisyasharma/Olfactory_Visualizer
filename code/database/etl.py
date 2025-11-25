import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import text
from pathlib import Path

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

def run_etl():
    engine = get_engine()
    
    print(f"\n🚀 Starting ETL Pipeline...")
    print(f"Reading data from: {DATA_ROOT}")

    # 1. Insert Subjects (Mice) into DB
    print("\n--- Step 1: Loading Subjects ---")
    with engine.connect() as conn:
        for original_id, meta in SUBJECT_MAP.items():
            print(f"  Registering {original_id} -> {meta['subject']}")
            
            exp_type = "rabies" if "Rabies" in original_id else "double_injection"
            
            conn.execute(text("""
                INSERT INTO subjects (subject_id, original_id, sex, experiment_type, details)
                VALUES (:sub, :orig, :sex, :exp, :det)
                ON CONFLICT (subject_id) DO NOTHING;
            """), {
                "sub": meta['subject'],
                "orig": original_id,
                "sex": meta.get('sex', 'U'),
                "exp": exp_type,
                "det": meta.get('details', '')
            })
        conn.commit()

    # 2. Process CSVs
    print("\n--- Step 2: Processing Quantification Files ---")
    
    # Walk through all folders in quantification
    with engine.connect() as conn:
        for root, dirs, files in os.walk(DATA_ROOT):
            for file in files:
                if not file.endswith(".csv"): continue
                
                # Find matching subject ID from folder/filename
                matched_key = None
                for key in SUBJECT_MAP.keys():
                    if key in file or key == os.path.basename(root):
                        matched_key = key
                        break
                
                if not matched_key: continue

                subject_id = SUBJECT_MAP[matched_key]['subject']
                
                # Determine Hemisphere
                hemi = detect_hemisphere(root, file)

                print(f"  Processing {file} ({hemi}) -> {subject_id}")

                df = load_table(os.path.join(root, file))

                # Minimal required columns
                required_cols = {"Region ID", "Region name", "Region pixels", "Region area", "Load"}
                missing = required_cols - set(df.columns)
                if missing:
                    print(f"   ⚠️ Skipping {file}: missing required columns {missing}")
                    continue

                optional_missing = {"Object count", "Object pixels", "Object area", "Norm load"} - set(df.columns)
                if optional_missing:
                    print(f"   ℹ️  {file}: optional columns missing {optional_missing} -> will fill NULLs")

                for _, row in df.iterrows():
                    
                    # A. Ensure Region Exists in Dictionary before inserting data
                    conn.execute(text("""
                        INSERT INTO brain_regions (region_id, name)
                        VALUES (:rid, :rname)
                        ON CONFLICT (region_id) DO NOTHING;
                    """), {"rid": int(row['Region ID']), "rname": str(row['Region name'])})

                    # B. Insert Data into region_counts
                    conn.execute(text("""
                        INSERT INTO region_counts 
                        (subject_id, region_id, region_pixels, region_area_mm, object_count, object_pixels, object_area_mm, load, norm_load, hemisphere)
                        VALUES (:sid, :rid, :rpix, :rarea, :ocount, :opix, :oarea, :load, :nload, :hemi)
                    """), {
                        "sid": subject_id,
                        "rid": int(row['Region ID']),
                        "rpix": clean_numeric(row['Region pixels']),
                        "rarea": clean_numeric(row['Region area']),
                        "ocount": clean_numeric(row.get('Object count')),
                        "opix": clean_numeric(row.get('Object pixels')),
                        "oarea": clean_numeric(row.get('Object area')),
                        "load": clean_numeric(row['Load']),
                        "nload": clean_numeric(row.get('Norm load')),
                        "hemi": hemi
                    })
        conn.commit()

    print("\n✅ ETL Complete. Database hydrated.")

if __name__ == "__main__":
    run_etl()
