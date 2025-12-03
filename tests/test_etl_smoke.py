import os
import pytest
from sqlalchemy import text

def test_counts_nonzero():
    # assumes DB is already loaded; this is a smoke test
    from code.database.connect import get_engine
    engine = get_engine()
    with engine.connect() as conn:
        regions = conn.execute(text("SELECT COUNT(*) FROM brain_regions")).scalar()
        rows = conn.execute(text("SELECT COUNT(*) FROM region_counts")).scalar()
        assert regions >= 1328
        assert rows > 0

def test_unique_constraint():
    from code.database.connect import get_engine
    engine = get_engine()
    with engine.connect() as conn:
        dup = conn.execute(text("""
            SELECT COUNT(*) FROM (
              SELECT subject_id, region_id, hemisphere, COUNT(*)
              FROM region_counts
              GROUP BY subject_id, region_id, hemisphere
              HAVING COUNT(*) > 1
            ) t
        """)).scalar()
        assert dup == 0
