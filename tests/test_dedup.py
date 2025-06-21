"""
Unit and integration tests for the Deduplicator.

For local testing without Docker:
1. Install PostgreSQL with PostGIS extension
2. Create test database: createdb flood_test
3. Set: export TEST_DATABASE_URL="postgresql://postgres:password@localhost:5432/flood_test"
4. Run: python -m pytest tests/ -v

For CI testing, GitHub Actions will provide PostgreSQL automatically.
"""

import asyncio
import os
import random
import time
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import pytest_asyncio

# Import the functions we want to test
from dedup import (
    acquire_advisory_lock,
    prune_raw_table,
    refresh_marketable_view,
    release_advisory_lock,
    upsert_unique_pixels,
)


# Test database configuration
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "postgresql://postgres:password@localhost:5432/flood_test")


@pytest_asyncio.fixture
async def db_connection():
    """Provide a test database connection with clean tables."""
    try:
        conn = await asyncpg.connect(TEST_DATABASE_URL)
    except asyncpg.exceptions.InvalidCatalogNameError:
        # Database doesn't exist - provide helpful error
        pytest.skip(f"Test database not found. Please create it or set TEST_DATABASE_URL environment variable.\n"
                   f"For local testing:\n"
                   f"  1. Install PostgreSQL with PostGIS\n"
                   f"  2. Run: createdb flood_test\n"
                   f"  3. Set: export TEST_DATABASE_URL='{TEST_DATABASE_URL}'\n"
                   f"  4. Run: python -m pytest tests/ -v")
    except Exception as e:
        pytest.skip(f"Could not connect to test database: {e}\n"
                   f"Please ensure PostgreSQL is running and TEST_DATABASE_URL is correct.")
    
    # Load schema from fixtures
    schema_path = os.path.join(os.path.dirname(__file__), "fixtures", "schema.sql")
    if os.path.exists(schema_path):
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        await conn.execute(schema_sql)
    else:
        # Fallback: create minimal schema for testing
        await conn.execute("""
            CREATE EXTENSION IF NOT EXISTS postgis;
            
            DROP TABLE IF EXISTS flood_pixels_marketable;
            DROP TABLE IF EXISTS flood_pixels_unique;
            DROP TABLE IF EXISTS flood_pixels_raw;
            
            CREATE TABLE flood_pixels_raw (
                id bigserial PRIMARY KEY,
                segment_id bigint NOT NULL,
                score smallint NOT NULL,
                homes integer NOT NULL,
                qpe_1h numeric NOT NULL,
                ffw boolean NOT NULL,
                geom geometry(Point, 4326) NOT NULL,
                first_seen timestamptz NOT NULL DEFAULT now()
            );
            
            CREATE TABLE flood_pixels_unique (
                segment_id bigint PRIMARY KEY,
                score smallint NOT NULL,
                homes integer NOT NULL,
                qpe_1h numeric NOT NULL,
                ffw boolean NOT NULL,
                geom geometry(Point, 4326) NOT NULL,
                first_seen timestamptz NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            );
            
            CREATE TABLE flood_pixels_marketable (
                segment_id bigint PRIMARY KEY,
                score smallint NOT NULL,
                homes integer NOT NULL,
                qpe_1h numeric NOT NULL,
                ffw boolean NOT NULL,
                geom geometry(Point, 4326) NOT NULL,
                first_seen timestamptz NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            );
        """)
    
    # Clean tables before each test
    await conn.execute("TRUNCATE flood_pixels_raw, flood_pixels_unique, flood_pixels_marketable")
    
    yield conn
    
    await conn.close()


async def insert_test_data(conn: asyncpg.Connection, count: int, base_time: datetime = None):
    """Insert synthetic test data into flood_pixels_raw."""
    if base_time is None:
        base_time = datetime.now(timezone.utc)
    
    data = []
    for i in range(count):
        segment_id = 1000000 + i
        score = random.randint(0, 100)
        homes = random.randint(0, 800)
        qpe_1h = round(random.uniform(0, 100), 2)
        ffw = random.choice([True, False])
        # Simple point geometry
        lon, lat = random.uniform(-180, 180), random.uniform(-90, 90)
        first_seen = base_time - timedelta(minutes=random.randint(0, 180))
        
        data.append((segment_id, score, homes, qpe_1h, ffw, f'POINT({lon} {lat})', first_seen))
    
    await conn.executemany(
        """
        INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen)
        VALUES ($1, $2, $3, $4, $5, ST_GeomFromText($6, 4326), $7)
        """,
        data
    )


@pytest.mark.asyncio
async def test_advisory_lock(db_connection):
    """Test advisory lock acquisition and release."""
    conn = db_connection
    
    # Should be able to acquire lock
    assert await acquire_advisory_lock(conn) is True
    
    # Second attempt should fail (lock already held)
    conn2 = await asyncpg.connect(TEST_DATABASE_URL)
    try:
        assert await acquire_advisory_lock(conn2) is False
    finally:
        await conn2.close()
    
    # Release lock
    await release_advisory_lock(conn)
    
    # Should be able to acquire again
    assert await acquire_advisory_lock(conn) is True
    await release_advisory_lock(conn)


@pytest.mark.asyncio
async def test_prune_raw_table(db_connection):
    """Test pruning of old records from flood_pixels_raw."""
    conn = db_connection
    base_time = datetime.now(timezone.utc)
    
    # Insert mix of old and new records with clear time boundaries
    old_time = base_time - timedelta(hours=5)     # 5 hours ago (definitely old)
    recent_time = base_time - timedelta(minutes=30)  # 30 minutes ago (definitely recent)
    
    # Insert old records
    await insert_test_data(conn, 5, old_time)
    # Insert recent records  
    await insert_test_data(conn, 3, recent_time)
    
    # Verify we have 8 total records
    count_before = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw")
    assert count_before == 8
    
    # Prune old records
    pruned_count = await prune_raw_table(conn)
    
    # Should have pruned the 5 old records
    assert pruned_count == 5
    
    # Should have 3 records remaining
    count_after = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw")
    assert count_after == 3


@pytest.mark.asyncio
async def test_upsert_wins_on_higher_score(db_connection):
    """Test that UPSERT only updates when new score is higher."""
    conn = db_connection
    
    # Insert initial record with high score
    await conn.execute("""
        INSERT INTO flood_pixels_unique (segment_id, score, homes, qpe_1h, ffw, geom, first_seen, updated_at)
        VALUES (12345, 80, 300, 25.5, true, ST_GeomFromText('POINT(-95.3698 29.7604)', 4326), now(), now())
    """)
    
    # Insert raw record with lower score for same segment_id
    await conn.execute("""
        INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen)
        VALUES (12345, 60, 250, 20.0, false, ST_GeomFromText('POINT(-95.3700 29.7600)', 4326), now())
    """)
    
    # Run UPSERT
    inserts, updates = await upsert_unique_pixels(conn)
    
    # Should be 0 inserts (segment exists) and 0 updates (lower score)
    assert updates == 0
    
    # Verify original record unchanged
    record = await conn.fetchrow("SELECT score, homes FROM flood_pixels_unique WHERE segment_id = 12345")
    assert record['score'] == 80
    assert record['homes'] == 300
    
    # Now insert raw record with higher score
    await conn.execute("""
        INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen)
        VALUES (12345, 90, 400, 30.0, true, ST_GeomFromText('POINT(-95.3695 29.7608)', 4326), now())
    """)
    
    # Run UPSERT again
    inserts, updates = await upsert_unique_pixels(conn)
    
    # Should be 0 inserts and 1 update (higher score)
    assert updates == 1
    
    # Verify record was updated
    record = await conn.fetchrow("SELECT score, homes FROM flood_pixels_unique WHERE segment_id = 12345")
    assert record['score'] == 90
    assert record['homes'] == 400


@pytest.mark.asyncio
async def test_mv_row_limit(db_connection):
    """Test that materialized view respects 10-row limit."""
    conn = db_connection
    
    # Insert 15 records into unique table, with varying scores and homes
    data = []
    for i in range(15):
        segment_id = 20000 + i
        score = 45 + i  # Scores from 45 to 59 (all >= 40)
        homes = 250 + i * 10  # Homes from 250 to 390 (all >= 200)
        data.append((segment_id, score, homes, 15.5, True, f'POINT(-95.{3700+i} 29.7604)', 
                    datetime.now(timezone.utc), datetime.now(timezone.utc)))
    
    await conn.executemany("""
        INSERT INTO flood_pixels_unique (segment_id, score, homes, qpe_1h, ffw, geom, first_seen, updated_at)
        VALUES ($1, $2, $3, $4, $5, ST_GeomFromText($6, 4326), $7, $8)
    """, data)
    
    # Refresh materialized view
    marketable_count = await refresh_marketable_view(conn)
    
    # Should have exactly 10 rows (limited by LIMIT 10)
    assert marketable_count == 10
    
    # Verify the top 10 scores were selected (should be 59 down to 50)
    scores = await conn.fetch("SELECT score FROM flood_pixels_marketable ORDER BY score DESC")
    expected_scores = list(range(59, 49, -1))  # [59, 58, 57, ..., 50]
    actual_scores = [row['score'] for row in scores]
    assert actual_scores == expected_scores


@pytest.mark.asyncio
async def test_latency_budget(db_connection):
    """Test that processing 10,000 synthetic rows completes within 30 seconds."""
    conn = db_connection
    
    # Insert 10,000 synthetic raw rows
    print("Inserting 10,000 test records...")
    start_insert = time.time()
    await insert_test_data(conn, 10000)
    insert_time = time.time() - start_insert
    print(f"Insert completed in {insert_time:.1f}s")
    
    # Add some records with high scores/homes to ensure MV has content
    high_score_data = []
    for i in range(20):
        segment_id = 50000 + i
        score = 60 + random.randint(0, 30)  # 60-90 range
        homes = 300 + random.randint(0, 200)  # 300-500 range
        high_score_data.append((segment_id, score, homes, 25.0, True, 
                               f'POINT(-95.{3700+i} 29.{7600+i})', 
                               datetime.now(timezone.utc)))
    
    await conn.executemany("""
        INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen)
        VALUES ($1, $2, $3, $4, $5, ST_GeomFromText($6, 4326), $7)
    """, high_score_data)
    
    # Time the deduplication process
    start_time = time.time()
    
    # Acquire lock
    assert await acquire_advisory_lock(conn) is True
    
    try:
        # Run deduplication steps in transaction
        async with conn.transaction(isolation='serializable'):
            pruned = await prune_raw_table(conn)
            inserts, updates = await upsert_unique_pixels(conn)
            marketable_rows = await refresh_marketable_view(conn)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"Deduplication completed in {duration:.1f}s")
        print(f"Pruned: {pruned}, Inserts: {inserts}, Updates: {updates}, Marketable: {marketable_rows}")
        
        # Should complete within 30 seconds
        assert duration < 30.0, f"Deduplication took {duration:.1f}s, exceeding 30s budget"
        
        # Verify we have marketable rows
        assert marketable_rows > 0, "Should have some marketable rows"
        assert marketable_rows <= 10, "Should not exceed 10 marketable rows"
        
    finally:
        await release_advisory_lock(conn)


@pytest.mark.asyncio 
async def test_materialized_view_cleanup(db_connection):
    """Test that materialized view refresh handles leftover tables."""
    conn = db_connection
    
    # Create leftover tables that might exist from previous failed runs
    await conn.execute("CREATE TABLE IF NOT EXISTS _mv (test_col int)")
    await conn.execute("CREATE TABLE IF NOT EXISTS _old (test_col int)")
    
    # Insert test data into unique table
    await conn.execute("""
        INSERT INTO flood_pixels_unique (segment_id, score, homes, qpe_1h, ffw, geom, first_seen, updated_at)
        VALUES (99999, 75, 350, 20.0, true, ST_GeomFromText('POINT(-95.3698 29.7604)', 4326), now(), now())
    """)
    
    # Should successfully refresh despite leftover tables
    marketable_count = await refresh_marketable_view(conn)
    assert marketable_count == 1
    
    # Verify leftover tables were cleaned up
    mv_exists = await conn.fetchval("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '_mv')")
    old_exists = await conn.fetchval("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '_old')")
    assert not mv_exists
    assert not old_exists