import sys
import os
from sqlalchemy import text

# Add the database folder to path so we can grab the connection tool
sys.path.append(os.path.join(os.getcwd(), 'code', 'database'))

try:
    from connect import get_engine
except ImportError:
    print("❌ Could not find connect.py. Are you running from the root 'capstone' folder?")
    sys.exit(1)

def verify_data():
    engine = get_engine()
    
    print("\n--- 📊 DATABASE STATUS REPORT ---")
    
    with engine.connect() as conn:
        # 1. Count Mice
        mice_count = conn.execute(text("SELECT count(*) FROM subjects")).scalar()
        print(f"🐭 Mice Registered:    {mice_count}")
        
        # 2. Count Brain Regions
        region_count = conn.execute(text("SELECT count(*) FROM brain_regions")).scalar()
        print(f"🧠 Regions Learned:    {region_count}")
        
        # 3. Count Data Points (The big one)
        data_count = conn.execute(text("SELECT count(*) FROM region_counts")).scalar()
        print(f"📈 Data Rows Loaded:   {data_count}")
        
        # 4. Sample Data
        if data_count > 0:
            print("\n--- Sample Data (Top 3 Rows) ---")
            result = conn.execute(text("""
                SELECT s.subject_id, b.name, r.region_pixels, r.hemisphere 
                FROM region_counts r
                JOIN subjects s ON r.subject_id = s.subject_id
                JOIN brain_regions b ON r.region_id = b.region_id
                LIMIT 3
            """))
            for row in result:
                print(f"  - Mouse {row[0]} | Region: {row[1][:20]}... | Pixels: {row[2]} | Side: {row[3]}")

if __name__ == "__main__":
    verify_data()
