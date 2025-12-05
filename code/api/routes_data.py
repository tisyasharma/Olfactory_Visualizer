"""
Core data read endpoints (subjects, sessions, files, fluor metrics).
Reason: separate read-only API routes from uploads and main wiring.
"""
from typing import List, Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel

from code.api.deps import fetch_all

router = APIRouter()


class Subject(BaseModel):
    subject_id: str
    sex: Optional[str] = None
    experiment_type: Optional[str] = None
    details: Optional[str] = None


@router.get("/subjects", response_model=List[Subject])
def list_subjects():
    rows = fetch_all("SELECT subject_id, sex, experiment_type, details FROM subjects ORDER BY subject_id")
    return rows


@router.get("/sessions")
def list_sessions(subject_id: Optional[str] = None):
    q = "SELECT session_id, subject_id, modality, session_date, protocol, notes FROM sessions"
    params = {}
    if subject_id:
        q += " WHERE subject_id = :sid"
        params["sid"] = subject_id
    q += " ORDER BY session_id"
    return fetch_all(q, params)


@router.get("/regions/tree")
def regions_tree():
    rows = fetch_all(
        "SELECT region_id, name, acronym, parent_id, st_level, atlas_id, ontology_id FROM brain_regions ORDER BY region_id"
    )
    return rows


@router.get("/files")
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


@router.get("/fluor/counts")
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


@router.get("/fluor/summary")
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


@router.get("/status")
def status():
    rows = fetch_all("SELECT count(*) AS subjects FROM subjects")
    subs = rows[0]["subjects"]
    rows = fetch_all("SELECT count(*) AS files FROM microscopy_files")
    files = rows[0]["files"]
    rows = fetch_all("SELECT count(*) AS counts FROM region_counts")
    counts = rows[0]["counts"]
    return {"subjects": subs, "files": files, "counts": counts}
