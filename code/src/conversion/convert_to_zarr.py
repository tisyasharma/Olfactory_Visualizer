import os
import re
import sys
import numpy as np
import zarr
from skimage.io import imread
from ome_zarr.io import parse_url
from ome_zarr.writer import write_image

# Attempt to import the map we just created
try:
    from config_map import SUBJECT_MAP
except ImportError:
    # Fix python path if running from root
    sys.path.append(os.path.join(os.getcwd(), 'src', 'conversion'))
    from config_map import SUBJECT_MAP

# --- PATH CONFIGURATION (Matches your screenshot) ---
SOURCE_ROOT = os.path.join("data", "sourcedata", "images")
BIDS_ROOT = os.path.join("data", "raw_bids")

def extract_slice_number(filename):
    """
    Robustly finds the slice number.
    Targeting patterns like: '...s001.png' or '...s59.png'
    """
    # Regex: Look for 's' followed by digits, right before the extension
    match = re.search(r's(\d+)\.png$', filename, re.IGNORECASE)
    if match:
        return int(match.group(1))
    else:
        # Fallback: Just look for any sequence of digits at the end
        match_fallback = re.search(r'(\d+)\.png$', filename, re.IGNORECASE)
        if match_fallback:
            return int(match_fallback.group(1))
    return 9999 # Push to end if unrecognizable

def convert_subject(folder_name, metadata):
    source_dir = os.path.join(SOURCE_ROOT, folder_name)
    
    if not os.path.exists(source_dir):
        print(f"[SKIP] {folder_name}: Folder not found in {SOURCE_ROOT}")
        return

    print(f"\nProcessing {folder_name} -> {metadata['subject']}...")

    # 1. Get and Sort Files
    files = [f for f in os.listdir(source_dir) if f.endswith('.png')]
    # Sort files by slice number
    files.sort(key=extract_slice_number)

    if not files:
        print("  [ERROR] No PNGs found!")
        return

    print(f"  Found {len(files)} slices. Range: {files[0]} ... {files[-1]}")

    # 2. Load Images into Memory (Stacking)
    # Read first image to get dimensions
    first_path = os.path.join(source_dir, files[0])
    first_img = imread(first_path)
    
    # Handle dimensions (Height, Width)
    if len(first_img.shape) == 3:
        # If image is RGB, take just one channel or convert to grayscale
        # For neuroscience anatomical slices, usually grayscale is sufficient
        # Here we convert RGB -> Grayscale
        height, width = first_img.shape[:2]
        dtype = first_img.dtype
    else:
        height, width = first_img.shape
        dtype = first_img.dtype
    
    # Create the volume (Z, Y, X)
    print("  Stacking images into 3D volume...")
    volume = np.zeros((len(files), height, width), dtype=dtype)

    for i, f in enumerate(files):
        img_path = os.path.join(source_dir, f)
        img = imread(img_path)
        
        if len(img.shape) == 3: 
            # Convert RGB to Grayscale
            img = np.mean(img, axis=2).astype(dtype)
            
        volume[i, :, :] = img

    # 3. Define Output Path (BIDS Structure)
    # structure: raw_bids/sub-XX/ses-XX/micr/
    output_dir = os.path.join(
        BIDS_ROOT, 
        metadata['subject'], 
        metadata['session'], 
        'micr'
    )
    os.makedirs(output_dir, exist_ok=True)
    
    # BIDS Filename convention
    zarr_filename = f"{metadata['subject']}_{metadata['session']}_sample-brain_stain-native_run-01_omero.zarr"
    store_path = os.path.join(output_dir, zarr_filename)

    # 4. Write to OME-Zarr
    print(f"  Writing OME-Zarr to {store_path}...")
    store = parse_url(store_path, mode="w").store
    root = zarr.group(store=store)
    
    # This writes the pyramidal levels (Zoom levels) automatically
    # chunks=(1, 1024, 1024) is standard for 2D slicing performance
    write_image(image=volume, group=root, axes="zyx", storage_options=dict(chunks=(1, 1024, 1024)))
    print("  Done.")

def main():
    # Ensure output root exists
    if not os.path.exists(BIDS_ROOT):
        os.makedirs(BIDS_ROOT)

    # Loop through the map
    for raw_folder, meta in SUBJECT_MAP.items():
        convert_subject(raw_folder, meta)

if __name__ == "__main__":
    main()