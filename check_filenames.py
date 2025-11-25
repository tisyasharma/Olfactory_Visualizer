import os

# Adjust this path if your images aren't exactly here
base_path = os.path.join("data", "sourcedata", "Images")

def scan_folder(folder_name):
    target = os.path.join(base_path, folder_name)
    if os.path.exists(target):
        files = sorted([f for f in os.listdir(target) if f.endswith('.png')])
        print(f"\n--- Files in {folder_name} ---")
        print(f"Total count: {len(files)}")
        print("First 3 files:", files[:3])
        print("Last 3 files: ", files[-3:])
    else:
        print(f"\n[ERROR] Could not find folder: {target}")

# Check one Double Injection mouse and one Rabies mouse
scan_folder("DBL_A")
scan_folder("RabiesA_Vglut1")