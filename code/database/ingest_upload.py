"""
Helper to ingest uploaded microscopy images:
- Converts PNG/JPG/TIFF (and other imageio-readable formats) to OME-Zarr.
- Writes into BIDS-style layout under data/raw_bids/sub-*/ses-*/micr/.
- Registers sessions and microscopy_files in the database with SHA256 hashes.

Usage (example):
  python -m code.database.ingest_upload --subject sub-DBL_A --session ses-dbl --hemisphere right \
    --pixel-size-um 0.5 path/to/image1.png path/to/image2.tif
"""

import argparse
import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path

import dask.array as da
import warnings
from PIL import Image, ImageFile
import imageio.v3 as iio
import numpy as np
import zarr
from ome_zarr.io import parse_url
from ome_zarr.writer import write_image
from sqlalchemy import text, types as satypes

from code.database.connect import get_engine

ROOT = Path(__file__).resolve().parents[2]  # project root
BIDS_ROOT = ROOT / "data" / "raw_bids"


def file_sha256(path: Path, chunk_size: int = 1_048_576) -> str:
    h = hashlib.sha256()
    if path.is_dir():
        # Deterministic walk so the same tree hashes identically
        for sub in sorted(p for p in path.rglob("*") if p.is_file()):
            h.update(str(sub.relative_to(path)).encode())
            with sub.open("rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    h.update(chunk)
    else:
        with path.open("rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
    return h.hexdigest()


def load_image(path: Path) -> np.ndarray:
    # Allow large images but guard against pathological cases; suppress PIL warnings
    Image.MAX_IMAGE_PIXELS = None
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=Image.DecompressionBombWarning)
        arr = iio.imread(path)
    # No hard cap; callers can downsample/tile manually if memory becomes an issue
    if arr.ndim == 2:
        arr = arr[np.newaxis, ...]
    elif arr.ndim == 3:
        if arr.shape[2] == 4:
            arr = arr[:, :, :3]
        arr = np.moveaxis(arr, -1, 0)
    else:
        raise ValueError(f"Unsupported image shape {arr.shape} for {path}")
    return arr


def write_omezarr(data: np.ndarray, dest: Path, pixel_size_um: float) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    store = parse_url(dest, mode="w").store
    root = zarr.group(store=store)
    darr = da.from_array(data, chunks=(data.shape[0], 512, 512))
    ps_m = pixel_size_um * 1e-6
    write_image(
        darr,
        group=root,
        axes="cyx",
        scaler=None,
        coordinate_transformations=[[{"type": "scale", "scale": [1.0, ps_m, ps_m]}]],
    )

def write_sidecar(dest: Path, subject: str, session: str, run: int, hemisphere: str, experiment_type: str, pixel_size_um: float):
    sidecar = dest.with_suffix(dest.suffix + ".json")
    meta = {
        "BIDSVersion": "1.8.0",
        "Modality": "microscopy",
        "Subject": subject,
        "Session": session,
        "Run": run,
        "Hemisphere": hemisphere,
        "ExperimentType": experiment_type,
        "PixelSizeMicrons": pixel_size_um,
        "GeneratedAt": datetime.utcnow().isoformat() + "Z",
    }
    sidecar.write_text(json.dumps(meta, indent=2))

def ensure_dataset_files():
    """Ensure dataset_description exists at BIDS root."""
    BIDS_ROOT.mkdir(parents=True, exist_ok=True)
    dd = BIDS_ROOT / "dataset_description.json"
    if not dd.exists():
        dd.write_text(json.dumps({
            "Name": "Olfactory Data Visualizer",
            "BIDSVersion": "1.8.0",
            "DatasetType": "raw",
            "GeneratedBy": [{"Name": "ingest_upload.py", "Version": "0.1"}],
        }, indent=2))

def validate_outputs(dest: Path):
    sidecar = dest.with_suffix(dest.suffix + ".json")
    if not dest.exists():
        raise FileNotFoundError(f"OME-Zarr not found at {dest}")
    if not sidecar.exists():
        raise FileNotFoundError(f"Missing sidecar JSON for {dest}")
    dd = BIDS_ROOT / "dataset_description.json"
    if not dd.exists():
        raise FileNotFoundError(f"Missing dataset_description.json at {dd}")

def ingest(subject: str, session: str, hemisphere: str, files: list[Path], pixel_size_um: float = 1.0, experiment_type: str = "double_injection"):
    engine = get_engine()
    inserted = []
    with engine.begin() as conn:
        # ensure subject exists (generic placeholder if new)
        conn.execute(
            text("""
                INSERT INTO subjects (subject_id, original_id, sex, experiment_type, details)
                VALUES (:subj, :orig, 'U', :exp_type, '')
                ON CONFLICT (subject_id) DO NOTHING;
            """),
            {"subj": subject, "orig": subject, "exp_type": experiment_type},
        )
        # ensure session
        conn.execute(
            text("""
                INSERT INTO sessions (session_id, subject_id, modality)
                VALUES (:sid, :subj, :mod)
                ON CONFLICT (session_id) DO NOTHING;
            """),
            {"sid": session, "subj": subject, "mod": "micr"},
        )

        for idx, src in enumerate(files, start=1):
            if not src.exists():
                raise FileNotFoundError(f"Input file not found: {src}")
            data = load_image(src)
            ensure_dataset_files()
            dest = BIDS_ROOT / subject / session / "microscopy" / f"{subject}_{session}_run-{idx:02d}_micr.ome.zarr"
            # Clean up any stale store from prior attempts so the writer can proceed
            if dest.exists():
                shutil.rmtree(dest, ignore_errors=True)
                sidecar_stale = dest.with_suffix(dest.suffix + ".json")
                sidecar_stale.unlink(missing_ok=True)
            write_omezarr(data, dest, pixel_size_um)
            write_sidecar(dest, subject, session, idx, hemisphere, experiment_type, pixel_size_um)
            validate_outputs(dest)
            sha = file_sha256(dest)
            # reject duplicate content
            dup = conn.execute(
                text("SELECT 1 FROM microscopy_files WHERE sha256 = :sha LIMIT 1"),
                {"sha": sha},
            ).first()
            if dup:
                # clean up created files to avoid orphaned duplicates
                shutil.rmtree(dest, ignore_errors=True)
                sidecar = dest.with_suffix(dest.suffix + ".json")
                sidecar.unlink(missing_ok=True)
                raise ValueError(f"Duplicate microscopy content detected (sha256 already exists) for {src.name}")

            # register file
            conn.execute(
                text("""
                    INSERT INTO microscopy_files (session_id, run, hemisphere, path, sha256)
                    VALUES (:sid, :run, :hemi, :path, :sha)
                    ON CONFLICT (session_id, run, hemisphere) DO NOTHING;
                """),
                {"sid": session, "run": idx, "hemi": hemisphere, "path": str(dest), "sha": sha},
            )
            inserted.append(dest)
    return inserted


def main():
    ap = argparse.ArgumentParser(description="Ingest microscopy uploads and convert to OME-Zarr.")
    ap.add_argument("--subject", required=True, help="BIDS subject id (e.g., sub-DBL_A)")
    ap.add_argument("--session", required=True, help="BIDS session id (e.g., ses-dbl)")
    ap.add_argument("--hemisphere", default="bilateral", choices=["left", "right", "bilateral"])
    ap.add_argument("--pixel-size-um", type=float, default=1.0, help="Pixel size in micrometers")
    ap.add_argument("--experiment-type", default="double_injection", choices=["double_injection", "rabies"], help="Experiment type to satisfy subjects constraint")
    ap.add_argument("files", nargs="+", type=Path, help="Input image files")
    args = ap.parse_args()

    ingested = ingest(args.subject, args.session, args.hemisphere, args.files, args.pixel_size_um, args.experiment_type)
    print(f"Ingested {len(ingested)} file(s):")
    for p in ingested:
        print(" -", p)


if __name__ == "__main__":
    main()
