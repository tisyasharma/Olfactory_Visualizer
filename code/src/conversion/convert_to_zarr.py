import os
import re
import sys
import shutil  # Added for auto-cleaning old folders
import numpy as np
import zarr
from skimage.io import imread
from ome_zarr.io import parse_url
from ome_zarr.writer import write_image

# Attempt to import the map
# This handles the import whether you run from root or src/
try:
    from config_map import SUBJECT_MAP
except ImportError:
    sys.path.append(os.path.join(os.getcwd(), 'src', 'conversion'))
    from config_map import SUBJECT_MAP

# --- PATH CONFIGURATION ---
# Based on your screenshot: 'images' is lowercase
SOURCE_ROOT = os.path.join("data", "sourcedata", "images")
BIDS_ROOT = os.path.join("data", "raw_bids")

def extract_slice_number(filename):
    """
    Robustly finds the slice number to sort images correctly.
    Targeting patterns like: '...s001.png' or '...s59.png'
    """
    # Strategy 1: Look for 's' followed by digits (e.g., s001)
    match = re.search(r's(\d+)\.png$', filename, re.IGNORECASE)
    if match:
        return int(match.group(1))
    
    # Strategy 2: Fallback to any digits at the end
    match_fallback = re.search(r'(\d+)\.png$', filename, re.IGNORECASE)
    if match_fallback:
        return int(match_fallback.group(1))
    
    return 9999 # If no number found, push to end

def convert_subject(folder_name, metadata):
    source_dir = os.path.join(SOURCE_ROOT, folder_name)
    
    # Safety Check: Does source exist?
    if not os.path.exists(source_dir):
        print(f"[SKIP] {folder_name}: Folder not found in {SOURCE_ROOT}")
        return

    print(f"\nProcessing {folder_name} -> {metadata['subject']}...")

    # 1. Get and Sort Files
    files = [f for f in os.listdir(source_dir) if f.endswith('.png')]
    files.sort(key=extract_slice_number)

    if not files:
        print("  [ERROR] No PNGs found!")
        return

    print(f"  Found {len(files)} slices. Range: {files[0]} ... {files[-1]}")

    # 2. DEFINE OUTPUT & CLEAN UP
    # Structure: raw_bids/sub-XX/ses-XX/micr/
    output_dir = os.path.join(
        BIDS_ROOT, 
        metadata['subject'], 
        metadata['session'], 
        'micr'
    )
    
    # AUTO-CLEAN: If this folder exists from a failed run, delete it first.
    if os.path.exists(output_dir):
        print(f"  Cleaning up old data in {output_dir}...")
        shutil.rmtree(output_dir)
    
    os.makedirs(output_dir, exist_ok=True)

    # 3. PRE-SCAN: Find Max Dimensions (The Canvas Method)
    # We must scan all images to ensure the volume is big enough for the largest slice.
    print("  Scanning dimensions to create a unified volume...")
    max_h, max_w = 0, 0
    temp_dtype = None

    for f in files:
        img_path = os.path.join(source_dir, f)
        img = imread(img_path)
        
        # Get shape (Handle RGB vs Grayscale)
        if len(img.shape) == 3:
            h, w = img.shape[:2]
        else:
            h, w = img.shape
            
        if h > max_h: max_h = h
        if w > max_w: max_w = w
        
        if temp_dtype is None:
            temp_dtype = img.dtype

    print(f"  Max Canvas Size Detected: {max_h} x {max_w}")

    # 4. Create the Empty Volume (Black Canvas)
    # This creates a block of Zeros
    volume = np.zeros((len(files), max_h, max_w), dtype=temp_dtype)

    # 5. Load and Center Images
    print("  Stacking and Centering images...")
    for i, f in enumerate(files):
        img = imread(os.path.join(source_dir, f))
        
        # Convert RGB to Grayscale if needed
        if len(img.shape) == 3: 
            img = np.mean(img, axis=2).astype(temp_dtype)
            
        h, w = img.shape
        
        # CALCULATE CENTERING OFFSETS
        # This math puts the small image exactly in the middle of the black canvas
        y_off = (max_h - h) // 2
        x_off = (max_w - w) // 2
        
        # Insert image into the volume using slicing
        volume[i, y_off:y_off+h, x_off:x_off+w] = img

    # 6. Write to OME-Zarr
    zarr_filename = f"{metadata['subject']}_{metadata['session']}_sample-brain_stain-native_run-01_omero.zarr"
    store_path = os.path.join(output_dir, zarr_filename)

    print(f"  Writing OME-Zarr to {store_path}...")
    store = parse_url(store_path, mode="w").store
    root = zarr.group(store=store)
    
    # Write the image with 3D chunks. 
    # (1, 1024, 1024) means "Load 1 slice at a time, in 1024x1024 pixel tiles"
    write_image(image=volume, group=root, axes="zyx", storage_options=dict(chunks=(1, 1024, 1024)))
    print("  Done.")

def main():
    # Ensure output root exists
    if not os.path.exists(BIDS_ROOT):
        os.makedirs(BIDS_ROOT)

    # Loop through every mouse defined in config_map.py
    for raw_folder, meta in SUBJECT_MAP.items():
        convert_subject(raw_folder, meta)

if __name__ == "__main__":
    main()