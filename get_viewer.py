import urllib.request
import os
import sys

# Define where we want the file
save_path = os.path.join("code", "web", "viv.js")

# List of potential locations for the library (The "Hunter" List)
urls_to_try = [
    "https://unpkg.com/@hms-dbmi/viv@0.13.3/dist/bundle.min.js",
    "https://unpkg.com/viv@0.12.4/dist/bundle.min.js",
    "https://cdn.jsdelivr.net/npm/@hms-dbmi/viv@0.13.3/dist/bundle.min.js",
    "https://unpkg.com/@hms-dbmi/viv/dist/viv.min.js",  # Alternate naming
]

print(f"--- Viewer Library Downloader ---")
print(f"Target: {save_path}\n")

success = False

for url in urls_to_try:
    print(f"Trying: {url} ...")
    try:
        # Try to open the URL first to check if it exists
        with urllib.request.urlopen(url) as response:
            if response.getcode() == 200:
                # If 200 OK, download it
                urllib.request.urlretrieve(url, save_path)
                print(f"SUCCESS! Downloaded library from:\n  -> {url}")
                success = True
                break
    except Exception as e:
        print(f"  Failed (404 or Network Error)")

if success:
    print("\nDONE. You can now refresh your webpage.")
else:
    print("\n[CRITICAL FAIL] Could not download from any source.")
    print("Check your internet connection or firewall.")