import sys
import os
from sqlalchemy import text

# Fix path so we can import 'connect.py' from the same folder
sys.path.append(os.path.join(os.getcwd(), 'code', 'database'))
try:
    from connect import get_engine
except ImportError:
    print("❌ ERROR: Could not find connect.py. Make sure you run this from the 'capstone' root folder.")
    sys.exit(1)

def init_database():
    engine = get_engine()
    schema_path = os.path.join("code", "database", "schema.sql")
    
    print(f"Reading schema from: {schema_path}")
    
    try:
        with open(schema_path, 'r') as f:
            sql_commands = f.read()

        with engine.connect() as conn:
            # Execute the entire SQL script
            conn.execute(text(sql_commands))
            conn.commit()
            print("✅ SUCCESS: Tables created! (public imaging + rna schema)")
            
    except FileNotFoundError:
        print("❌ ERROR: schema.sql not found. Did you create it in code/database/?")

if __name__ == "__main__":
    init_database()
