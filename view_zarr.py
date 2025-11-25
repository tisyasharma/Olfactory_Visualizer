import napari
import os

# POINT THIS TO ONE OF YOUR NEW FOLDERS
# Example: data/raw_bids/sub-dbl01/ses-01/micr/sub-dbl01_ses-01_sample-brain_stain-native_run-01_omero.zarr
zarr_path = os.path.join(
    "data", "raw_bids", 
    "sub-dbl01", "ses-01", "micr", 
    "sub-dbl01_ses-01_sample-brain_stain-native_run-01_omero.zarr"
)

print(f"Opening {zarr_path}...")

# Open the viewer
viewer = napari.Viewer()

# Load the Zarr file
# The plugin='napari-ome-zarr' is critical—it tells Napari how to read the folder structure
viewer.open(zarr_path, plugin='napari-ome-zarr')

print("Viewer open. Use the slider at the bottom to scroll through slices!")
napari.run()