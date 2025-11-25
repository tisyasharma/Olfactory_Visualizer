'''
File responsible for connecting the Postgres server to the Python Client.
Now modularized so other scripts (like etl.py) can import the connection.
'''
import sys
from sqlalchemy import create_engine, text

# CONNECTION SETTINGS
# Since you are on localhost with your user, this URL is correct based on your previous file.
# If you ever add a password, it would look like: tisyasharma:password@localhost...
DB_URL = "postgresql+psycopg2://tisyasharma@localhost:5432/murthy_db"

def get_engine():
    """Creates and returns a database engine."""
    try:
        engine = create_engine(DB_URL)
        return engine
    except Exception as e:
        print(f"❌ Error creating engine: {e}")
        sys.exit(1)

def test_connection():
    """Runs a quick check to see if Postgres is awake."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"✅ SUCCESS: Connected to Postgres!")
            print(f"   Version: {version}")
            return True
    except Exception as e:
        print(f"❌ CONNECTION FAILED: {e}")
        print("   Is the Postgres app running?")
        return False

# This block only runs if you execute this file directly (python connect.py)
if __name__ == "__main__":
    test_connection()