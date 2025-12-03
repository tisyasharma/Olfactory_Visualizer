from typing import List, Optional
import tempfile
import shutil
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import text

from code.database.connect import get_engine
from code.database.ingest_upload import ingest
from code.database.etl import ingest_counts_csv

WEB_DIR = Path(__file__).resolve().parents[1] / "web"

app = FastAPI(title="Olfactory Data API", version="0.1.0")
app.mount("/code/web", StaticFiles(directory=WEB_DIR, html=True), name="web")


@app.get("/")
def root():
    """Redirect root to the web dashboard."""
    return RedirectResponse(url="/code/web/index.html")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    ico = WEB_DIR / "favicon.ico"
    if ico.exists():
        return FileResponse(str(ico))
    raise HTTPException(status_code=404, detail="favicon not found")


def fetch_all(query: str, params: dict = None):
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(query), params or {})
        cols = rows.keys()
        return [dict(zip(cols, row)) for row in rows]


class Subject(BaseModel):
    subject_id: str
    sex: Optional[str] = None
    experiment_type: Optional[str] = None
    details: Optional[str] = None


@app.get("/subjects", response_model=List[Subject])
def list_subjects():
    rows = fetch_all("SELECT subject_id, sex, experiment_type, details FROM subjects ORDER BY subject_id")
    return rows


@app.get("/sessions")
def list_sessions(subject_id: Optional[str] = None):
    q = "SELECT session_id, subject_id, modality, session_date, protocol, notes FROM sessions"
    params = {}
    if subject_id:
        q += " WHERE subject_id = :sid"
        params["sid"] = subject_id
    q += " ORDER BY session_id"
    return fetch_all(q, params)


@app.get("/regions/tree")
def regions_tree():
    rows = fetch_all(
        "SELECT region_id, name, acronym, parent_id, st_level, atlas_id, ontology_id FROM brain_regions ORDER BY region_id"
    )
    return rows


@app.get("/files")
def list_files(session_id: Optional[str] = None, subject_id: Optional[str] = None):
    q = """
    SELECT mf.file_id, mf.session_id, s.subject_id, mf.run, mf.hemisphere, mf.path, mf.sha256, mf.created_at
    FROM microscopy_files mf
    JOIN sessions s ON mf.session_id = s.session_id
    """
    params = {}
    where = []
    if session_id:
        where.append("mf.session_id = :sess")
        params["sess"] = session_id
    if subject_id:
        where.append("s.subject_id = :subj")
        params["subj"] = subject_id
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY mf.session_id, mf.run NULLS LAST"
    return fetch_all(q, params)


@app.get("/fluor/counts")
def fluor_counts(
    subject_id: Optional[str] = None,
    region_id: Optional[int] = None,
    hemisphere: Optional[str] = Query(None, regex="^(left|right|bilateral)$"),
    limit: int = Query(500, ge=1, le=5000),
):
    q = """
    SELECT rc.subject_id, rc.region_id, br.name AS region_name, rc.region_pixels, rc.region_area_mm,
           rc.object_count, rc.object_pixels, rc.object_area_mm, rc.load, rc.norm_load,
           rc.hemisphere, rc.file_id
    FROM region_counts rc
    JOIN brain_regions br ON rc.region_id = br.region_id
    """
    params = {}
    where = []
    if subject_id:
        where.append("rc.subject_id = :sid")
        params["sid"] = subject_id
    if region_id:
        where.append("rc.region_id = :rid")
        params["rid"] = region_id
    if hemisphere:
        where.append("rc.hemisphere = :hemi")
        params["hemi"] = hemisphere
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY rc.subject_id, rc.region_id LIMIT :lim"
    params["lim"] = limit
    return fetch_all(q, params)


@app.get("/fluor/summary")
def fluor_summary(
    experiment_type: Optional[str] = Query(None, regex="^(double_injection|rabies)$"),
    hemisphere: Optional[str] = Query(None, regex="^(left|right|bilateral)$"),
    subject_id: Optional[str] = None,
    region_id: Optional[int] = None,
    limit: int = Query(500, ge=1, le=5000),
):
    """
    Aggregated region-level summary to drive charts without client-side recompute.
    """
    q = """
    SELECT br.region_id,
           br.name AS region_name,
           rc.hemisphere,
           COUNT(*) AS records,
           SUM(rc.region_pixels) AS region_pixels_sum,
           AVG(rc.region_pixels) AS region_pixels_avg,
           SUM(rc.load) AS load_sum,
           AVG(rc.load) AS load_avg,
           SUM(rc.object_count) AS object_count_sum,
           AVG(rc.object_count) AS object_count_avg
    FROM region_counts rc
    JOIN brain_regions br ON rc.region_id = br.region_id
    JOIN subjects s ON rc.subject_id = s.subject_id
    """
    params = {}
    where = []
    if experiment_type:
        where.append("s.experiment_type = :exp")
        params["exp"] = experiment_type
    if hemisphere:
        where.append("rc.hemisphere = :hemi")
        params["hemi"] = hemisphere
    if subject_id:
        where.append("rc.subject_id = :sid")
        params["sid"] = subject_id
    if region_id:
        where.append("rc.region_id = :rid")
        params["rid"] = region_id
    if where:
        q += " WHERE " + " AND ".join(where)
    q += """
    GROUP BY br.region_id, br.name, rc.hemisphere
    ORDER BY br.region_id
    LIMIT :lim
    """
    params["lim"] = limit
    return fetch_all(q, params)


@app.get("/status")
def status():
    rows = fetch_all("SELECT count(*) AS subjects FROM subjects")
    subs = rows[0]["subjects"]
    rows = fetch_all("SELECT count(*) AS files FROM microscopy_files")
    files = rows[0]["files"]
    rows = fetch_all("SELECT count(*) AS counts FROM region_counts")
    counts = rows[0]["counts"]
    return {"subjects": subs, "files": files, "counts": counts}


@app.get("/scrna/samples")
def scrna_samples():
    return fetch_all("SELECT sample_id, subject_id, tissue, prep, date_run, protocol FROM scrna_samples ORDER BY sample_id")


@app.get("/scrna/clusters")
def scrna_clusters(sample_id: Optional[str] = None):
    q = "SELECT sample_id, cluster_id, n_cells, description FROM scrna_clusters"
    params = {}
    if sample_id:
        q += " WHERE sample_id = :sid"
        params["sid"] = sample_id
    q += " ORDER BY sample_id, cluster_id"
    return fetch_all(q, params)


@app.get("/scrna/markers")
def scrna_markers(sample_id: str, cluster_id: str, limit: int = Query(50, ge=1, le=500)):
    q = """
    SELECT sample_id, cluster_id, gene, logfc, pval_adj
    FROM scrna_cluster_markers
    WHERE sample_id = :sid AND cluster_id = :cid
    ORDER BY logfc DESC
    LIMIT :lim
    """
    return fetch_all(q, {"sid": sample_id, "cid": cluster_id, "lim": limit})


@app.post("/upload/microscopy")
async def upload_microscopy(
    subject_id: str = Form(..., description="BIDS subject id (e.g., sub-DBL_A)"),
    session_id: str = Form(..., description="BIDS session id (e.g., ses-dbl)"),
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
    allowed_ext = (".png", ".jpg", ".jpeg", ".tif", ".tiff", ".ome.tif", ".ome.tiff", ".zarr", ".ome.zarr")

    tmpdir = Path(tempfile.mkdtemp())
    saved_paths = []
    try:
        for uf in files:
            fname = uf.filename or ""
            if not fname.lower().endswith(allowed_ext):
                raise HTTPException(status_code=400, detail=f"Unsupported file type for {fname}. Upload images only.")
            dest = tmpdir / uf.filename
            with dest.open("wb") as f:
                shutil.copyfileobj(uf.file, f)
            saved_paths.append(dest)

        ingested = ingest(
            subject=subject_id,
            session=session_id,
            hemisphere=hemisphere,
            files=saved_paths,
            pixel_size_um=pixel_size_um,
            experiment_type=experiment_type,
        )
        return {"ingested": [str(p) for p in ingested]}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@app.post("/upload/region-counts")
async def upload_region_counts(
    subject_id: str = Form(..., description="BIDS subject id (e.g., sub-DBL_A)"),
    session_id: Optional[str] = Form(None, description="BIDS session id (e.g., ses-dbl)"),
    hemisphere: str = Form("bilateral", regex="^(left|right|bilateral)$"),
    experiment_type: str = Form("double_injection", regex="^(double_injection|rabies)$"),
    files: List[UploadFile] = File(...),
):
    """
    Accept quantification CSV uploads and load them into region_counts.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    tmpdir = Path(tempfile.mkdtemp())
    rows = 0
    try:
        saved = []
        for uf in files:
            if not uf.filename.lower().endswith(".csv"):
                raise HTTPException(status_code=400, detail=f"Unsupported file type for {uf.filename}. Upload CSV only.")
            dest = tmpdir / uf.filename
            with dest.open("wb") as f:
                shutil.copyfileobj(uf.file, f)
            saved.append(dest)

        engine = get_engine()
        for path in saved:
            try:
                rows += ingest_counts_csv(engine, path, subject_id, session_id, hemisphere, experiment_type)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
        return {"rows_ingested": rows}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
