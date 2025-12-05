"""
Upload endpoints for microscopy (OME-Zarr) and quantification CSVs.
Reason: isolate write paths from read-only routes.
"""
from typing import List, Optional
import tempfile
import shutil
from pathlib import Path
import pandas as pd

from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form
from sqlalchemy import text, types as satypes

from code.api.deps import (
    get_engine,
    resolve_session_id,
    sha256_path,
    load_table,
    clean_numeric,
)
from code.database.ingest_upload import ingest

router = APIRouter()


def ingest_counts_csv(engine, csv_path: Path, subject_id: str, session_id: str, hemisphere: str, experiment_type: str) -> int:
    """
    Simple CSV ingest used by the upload endpoint.
    Mirrors the ETL column mapping and writes into region_counts with a staging table.
    """
    df = load_table(csv_path)
    required_cols = {"Region ID", "Region name", "Region pixels", "Region area", "Load"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"{csv_path.name}: missing required columns {missing}")

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

    df_rows = []
    with engine.connect() as conn:
        unit_map = {r._mapping["name"]: r._mapping["unit_id"] for r in conn.execute(text("SELECT unit_id, name FROM units"))}
        file_row = conn.execute(
            text(
                """
                SELECT mf.file_id
                FROM microscopy_files mf
                WHERE mf.session_id = :sid AND (mf.hemisphere = :hemi OR mf.hemisphere IS NULL)
                ORDER BY COALESCE(mf.run,0), mf.file_id
                LIMIT 1
                """
            ),
            {"sid": session_id, "hemi": hemisphere},
        ).first()
        file_id = file_row.file_id if file_row else None

    for r in df.itertuples(index=False):
        df_rows.append({
            "subject_id": subject_id,
            "region_id": int(getattr(r, "region_id")),
            "file_id": file_id,
            "region_pixels": clean_numeric(getattr(r, "region_pixels")),
            "region_area_mm": clean_numeric(getattr(r, "region_area", None)),
            "object_count": clean_numeric(getattr(r, "object_count", None)),
            "object_pixels": clean_numeric(getattr(r, "object_pixels", None)),
            "object_area_mm": clean_numeric(getattr(r, "object_area", None)),
            "load": clean_numeric(getattr(r, "load")),
            "norm_load": clean_numeric(getattr(r, "norm_load", None)),
            "hemisphere": hemisphere,
            "region_pixels_unit_id": unit_map.get("pixels"),
            "region_area_unit_id": unit_map.get("pixels"),
            "object_count_unit_id": unit_map.get("count"),
            "object_pixels_unit_id": unit_map.get("pixels"),
            "object_area_unit_id": unit_map.get("pixels"),
            "load_unit_id": unit_map.get("pixels"),
        })

    if not df_rows:
        return 0

    df_counts = pd.DataFrame(df_rows)
    df_counts = df_counts.dropna(subset=["region_pixels", "load"])
    temp_table = "_region_counts_upload_stage"
    with engine.begin() as conn:
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
        inserted = conn.execute(
            text(
                f"""
                INSERT INTO region_counts (subject_id, region_id, file_id, region_pixels, region_area_mm, object_count, object_pixels, object_area_mm, load, norm_load, hemisphere,
                                          region_pixels_unit_id, region_area_unit_id, object_count_unit_id, object_pixels_unit_id, object_area_unit_id, load_unit_id)
                SELECT subject_id, region_id, file_id, region_pixels, region_area_mm, object_count, object_pixels, object_area_mm, load, norm_load, hemisphere,
                       region_pixels_unit_id, region_area_unit_id, object_count_unit_id, object_pixels_unit_id, object_area_unit_id, load_unit_id
                FROM {temp_table}
                ON CONFLICT (subject_id, region_id, hemisphere) DO NOTHING;
                """
            )
        ).rowcount
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
    return inserted or 0


@router.post("/upload/microscopy")
async def upload_microscopy(
    subject_id: str = Form(..., description="BIDS subject id (e.g., sub-DBL_A)"),
    session_id: str = Form(..., description="BIDS session id (e.g., ses-dbl or 'auto')"),
    hemisphere: str = Form("bilateral", regex="^(left|right|bilateral)$"),
    pixel_size_um: float = Form(1.0),
    experiment_type: str = Form("double_injection", regex="^(double_injection|rabies)$"),
    files: List[UploadFile] = File(...),
):
    """
    Accept microscopy uploads, convert to OME-Zarr, register sessions/files.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    image_ext = (".png", ".jpg", ".jpeg", ".tif", ".tiff", ".ome.tif", ".ome.tiff", ".zarr", ".ome.zarr")

    tmpdir = Path(tempfile.mkdtemp())
    saved_paths: List[Path] = []
    try:
        engine = get_engine()
        session_id = resolve_session_id(engine, subject_id, experiment_type, session_id)
        # prevent duplicate experiment loads
        with engine.connect() as conn:
            already = conn.execute(
                text("SELECT 1 FROM microscopy_files WHERE session_id = :sid LIMIT 1"),
                {"sid": session_id},
            ).first()
            if already:
                raise HTTPException(
                    status_code=409,
                    detail=f"Session {session_id} already has microscopy files registered. Duplicate ingest blocked.",
                )
        # Stage uploads to temp
        for uf in files:
            fname = uf.filename or ""
            lower = fname.lower()
            if not lower.endswith(image_ext):
                raise HTTPException(status_code=400, detail=f"Unsupported file type for {fname}. Upload images only.")
            dest = tmpdir / fname
            with dest.open("wb") as f:
                shutil.copyfileobj(uf.file, f)
            saved_paths.append(dest)

        if not saved_paths:
            raise HTTPException(status_code=400, detail="No valid image files found in upload.")

        # Stable order so run numbering is deterministic when folder uploads are used
        all_images = sorted(saved_paths, key=lambda p: p.name)

        try:
            ingested = ingest(
                subject=subject_id,
                session=session_id,
                hemisphere=hemisphere,
                files=all_images,
                pixel_size_um=pixel_size_um,
                experiment_type=experiment_type,
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Microscopy ingest failed: {e}")
        return {"status": "ok", "ingested": [str(p) for p in ingested], "files_processed": [p.name for p in all_images]}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@router.post("/upload/region-counts")
async def upload_region_counts(
    subject_id: str = Form(..., description="BIDS subject id (e.g., sub-DBL_A)"),
    session_id: Optional[str] = Form(None, description="BIDS session id (e.g., ses-dbl)"),
    hemisphere: str = Form("auto", regex="^(left|right|bilateral|auto)$"),
    experiment_type: str = Form("double_injection", regex="^(double_injection|rabies)$"),
    files: List[UploadFile] = File(...),
):
    """
    Accept quantification CSV uploads and load them into region_counts.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    engine = get_engine()
    sess = resolve_session_id(engine, subject_id, experiment_type, session_id)

    tmpdir = Path(tempfile.mkdtemp())
    rows = 0
    try:
        saved = []
        saved_hashes = {}
        for uf in files:
            if not uf.filename.lower().endswith(".csv"):
                raise HTTPException(status_code=400, detail=f"Unsupported file type for {uf.filename}. Upload CSV only.")
            dest = tmpdir / uf.filename
            with dest.open("wb") as f:
                shutil.copyfileobj(uf.file, f)
            saved.append(dest)
            saved_hashes[dest] = sha256_path(dest)

        with engine.connect() as conn:
            # Block duplicate ingest for the same session if any region_counts already linked
            dup = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM region_counts rc
                    JOIN microscopy_files mf ON rc.file_id = mf.file_id
                    WHERE mf.session_id = :sid
                    LIMIT 1
                    """
                ),
                {"sid": sess},
            ).first()
            if dup:
                raise HTTPException(
                    status_code=409,
                    detail=f"Session {sess} already has quantification rows registered. Duplicate ingest blocked.",
                )
            # Block identical file contents if checksum already ingested
            for chk in saved_hashes.values():
                existing = conn.execute(
                    text("SELECT 1 FROM ingest_log WHERE checksum = :c AND status = 'success' LIMIT 1"),
                    {"c": chk},
                ).first()
                if existing:
                    raise HTTPException(
                        status_code=409,
                        detail="This quantification file matches a previously ingested file (checksum duplicate).",
                    )
        for path in saved:
            try:
                rows += ingest_counts_csv(engine, path, subject_id, sess, hemisphere, experiment_type)
                # log checksum for dedupe
                chk = saved_hashes.get(path)
                if chk:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                "INSERT INTO ingest_log (source_path, checksum, rows_loaded, status, message) "
                                "VALUES (:p, :c, :r, :s, :m)"
                            ),
                            {"p": str(path), "c": chk, "r": rows, "s": "success", "m": f"upload {sess}"},
                        )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
        return {"status": "ok", "rows_ingested": rows}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
