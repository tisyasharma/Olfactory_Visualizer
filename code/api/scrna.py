"""
scRNA endpoints backed by CSV reference files (cluster, terms, membership).
Reason: isolate RNA loading and routes from the main API wiring.
"""
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RNA_DIR = Path(__file__).resolve().parents[2] / "data" / "RNAseq_data"
_clusters_df = None
_terms_df = None
_membership_df = None


def load_rna_tables():
    global _clusters_df, _terms_df, _membership_df
    if _clusters_df is not None and _terms_df is not None and _membership_df is not None:
        return
    cluster_path = RNA_DIR / "cluster.csv"
    term_path = RNA_DIR / "cluster_annotation_term.csv"
    membership_path = RNA_DIR / "cluster_to_cluster_annotation_membership.csv"
    if not (cluster_path.exists() and term_path.exists() and membership_path.exists()):
        raise HTTPException(status_code=500, detail="scRNA reference files missing in data/RNAseq_data")
    _clusters_df = pd.read_csv(cluster_path)
    _terms_df = pd.read_csv(term_path)
    _membership_df = pd.read_csv(membership_path)


def scrna_samples_data():
    load_rna_tables()
    return [{
        "sample_id": "WMB-10Xv2-OLF",
        "modality": "rna_seq",
        "n_clusters": int(len(_clusters_df)),
        "notes": "Clusters and annotations loaded from CSV; expression in .h5ad stored on disk."
    }]


def scrna_clusters_data():
    load_rna_tables()
    return [
        {
            "sample_id": "WMB-10Xv2-OLF",
            "cluster_id": str(row.cluster_alias),
            "n_cells": int(row.number_of_cells),
            "label": row.label,
        }
        for row in _clusters_df.itertuples()
    ]


def scrna_markers_data(cluster_id: str, limit: int):
    load_rna_tables()
    try:
        cid_int = int(cluster_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="cluster_id must be an integer")
    m = _membership_df[_membership_df["cluster_alias"] == cid_int]
    if m.empty:
        return []
    merged = m.merge(_terms_df, left_on="cluster_annotation_term_label", right_on="label", how="left")
    merged = merged.head(limit)
    results = []
    for row in merged.itertuples():
        results.append({
            "cluster_id": cluster_id,
            "gene": getattr(row, "cluster_annotation_term_label"),
            "name": getattr(row, "name_x", None) or getattr(row, "name_y", None),
            "logfc": None,
            "pval_adj": None,
            "color": getattr(row, "color_hex_triplet_x", None) or getattr(row, "color_hex_triplet_y", None),
        })
    return results


@router.get("/scrna/samples")
def scrna_samples():
    return scrna_samples_data()


@router.get("/scrna/clusters")
def scrna_clusters(sample_id: Optional[str] = None):
    return scrna_clusters_data()


@router.get("/scrna/markers")
def scrna_markers(sample_id: str, cluster_id: str, limit: int = Query(50, ge=1, le=500)):
    return scrna_markers_data(cluster_id, limit)
