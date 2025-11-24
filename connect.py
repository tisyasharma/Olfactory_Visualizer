'''
File responsible for connecting the Posgres server to the Python Client
'''

from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://tisyasharma@localhost:5432/murthy_db")

with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print(result.fetchone())
