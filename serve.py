# serve.py
# A robust server that handles CORS and complex data requests for OME-Zarr
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import sys

class RobustHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        # Allow everything. We are in a safe local environment.
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'X-Requested-With, Content-Type, Range')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        super().end_headers()

    def do_OPTIONS(self):
        # Browsers ask "Can I read this?" before reading. We must say "YES".
        self.send_response(200)
        self.end_headers()

if __name__ == '__main__':
    port = 8000
    # Ensure we are serving from the PROJECT ROOT (NeuroCapstone)
    # This lets us see both 'code/' and 'data/'
    root_dir = os.getcwd()
    
    # Check if we are in the right place
    if not os.path.exists(os.path.join(root_dir, "data")):
        print("WARNING: 'data' folder not found. Are you running this from the 'capstone' root folder?")

    print(f"\n--- CAPSTONE SERVER ONLINE ---")
    print(f"Root: {root_dir}")
    print(f"Dashboard: http://localhost:{port}/code/web/")
    print(f"------------------------------\n")
    
    try:
        HTTPServer(('', port), RobustHandler).serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server.")
        sys.exit(0)