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
    prune_raw_table_if_configured,
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
async def test_prune_raw_table_with_retention_configured(db_connection, monkeypatch):
    """Test pruning of old records from flood_pixels_raw when retention is configured."""
    conn = db_connection
    
    # Set retention to 3 hours for this test
    monkeypatch.setenv("RAW_RETENTION_HOURS", "3")
    
    # Clean the table completely to avoid interference from other tests
    await conn.execute("TRUNCATE flood_pixels_raw, flood_pixels_unique")
    
    # Use much clearer time boundaries to avoid edge cases
    now = datetime.now(timezone.utc)
    very_old_time = now - timedelta(hours=6)    # 6 hours ago (definitely old)
    very_recent_time = now - timedelta(minutes=10)  # 10 minutes ago (definitely recent)
    
    # Insert specific test data with known segment_ids
    old_data = []
    recent_data = []
    
    # Insert 5 old records
    for i in range(5):
        segment_id = 9000 + i  # Use specific range to avoid conflicts
        old_data.append((segment_id, 50, 100, 15.0, False, f'POINT(-95.{3700+i} 29.7604)', very_old_time))
    
    # Insert 3 recent records  
    for i in range(3):
        segment_id = 9010 + i  # Use different range
        recent_data.append((segment_id, 60, 200, 20.0, True, f'POINT(-95.{3710+i} 29.7604)', very_recent_time))
    
    # Insert all test data
    all_data = old_data + recent_data
    await conn.executemany(
        """
        INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen)
        VALUES ($1, $2, $3, $4, $5, ST_GeomFromText($6, 4326), $7)
        """,
        all_data
    )
    
    # Verify we have exactly 8 total records
    count_before = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw")
    assert count_before == 8, f"Expected 8 records before pruning, got {count_before}"
    
    # Prune old records
    pruned_count = await prune_raw_table_if_configured(conn)
    
    # Should have pruned exactly the 5 old records
    assert pruned_count == 5, f"Expected 5 records pruned, got {pruned_count}"
    
    # Should have exactly 3 records remaining
    count_after = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw")
    assert count_after == 3, f"Expected 3 records after pruning, got {count_after}"
    
    # Verify that only recent records remain
    old_remaining = await conn.fetchval(
        "SELECT COUNT(*) FROM flood_pixels_raw WHERE segment_id BETWEEN 9000 AND 9004"
    )
    recent_remaining = await conn.fetchval(
        "SELECT COUNT(*) FROM flood_pixels_raw WHERE segment_id BETWEEN 9010 AND 9012"
    )
    
    assert old_remaining == 0, "No old records should remain"
    assert recent_remaining == 3, "All recent records should remain"


@pytest.mark.asyncio
async def test_prune_raw_table_without_retention_configured(db_connection, monkeypatch):
    """Test that no pruning occurs when retention is not configured."""
    conn = db_connection
    
    # Ensure RAW_RETENTION_HOURS is not set
    monkeypatch.delenv("RAW_RETENTION_HOURS", raising=False)
    
    # Use much clearer time boundaries to avoid edge cases
    now = datetime.now(timezone.utc)
    very_old_time = now - timedelta(hours=6)    # 6 hours ago (definitely old)
    very_recent_time = now - timedelta(minutes=10)  # 10 minutes ago (definitely recent)
    
    # Insert old records
    await insert_test_data(conn, 5, very_old_time)
    # Insert recent records  
    await insert_test_data(conn, 3, very_recent_time)
    
    # Verify we have 8 total records
    count_before = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw")
    assert count_before == 8
    
    # Attempt to prune - should not delete anything
    pruned_count = await prune_raw_table_if_configured(conn)
    
    # Should have pruned 0 records (append-only mode)
    assert pruned_count == 0
    
    # Should still have all 8 records
    count_after = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw")
    assert count_after == 8


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
async def test_latency_budget(db_connection, monkeypatch):
    """Test that processing 10,000 synthetic rows completes within 30 seconds."""
    conn = db_connection
    
    # Set retention to avoid pruning during this performance test
    monkeypatch.setenv("RAW_RETENTION_HOURS", "24")
    
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
            pruned = await prune_raw_table_if_configured(conn)
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


# FL-134: Tests for append-only raw table behavior
@pytest.mark.asyncio
async def test_dedup_does_not_delete_raw(db_connection, monkeypatch):
    """Test that deduplication runs without deleting raw records when RAW_RETENTION_HOURS is not set."""
    conn = db_connection
    
    # Ensure RAW_RETENTION_HOURS is not set
    monkeypatch.delenv("RAW_RETENTION_HOURS", raising=False)
    
    # Clean up any existing test data
    await conn.execute("DELETE FROM flood_pixels_raw WHERE segment_id IN (1, 2)")
    await conn.execute("DELETE FROM flood_pixels_unique WHERE segment_id IN (1, 2)")
    
    # Insert test data - one recent, one 4h old
    await conn.execute(
        "INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen) "
        "VALUES (1, 10, 0, 0, false, ST_SetSRID(ST_MakePoint(0,0), 4326), now() - INTERVAL '4 hours'), "
        "       (2, 10, 0, 0, false, ST_SetSRID(ST_MakePoint(0,0), 4326), now())"
    )
    
    # Test the pruning function directly (should not delete anything)
    pruned_count = await prune_raw_table_if_configured(conn)
    assert pruned_count == 0, "No records should be pruned when RAW_RETENTION_HOURS is not set"
    
    # Verify both records still exist in raw table
    rows = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw WHERE segment_id IN (1, 2)")
    assert rows == 2, "Raw table must remain append-only - both old and new records should be preserved"


@pytest.mark.asyncio
async def test_dedup_prunes_when_retention_configured(db_connection, monkeypatch):
    """Test that deduplication prunes old records when RAW_RETENTION_HOURS is explicitly set."""
    conn = db_connection
    
    # Set retention to 2 hours
    monkeypatch.setenv("RAW_RETENTION_HOURS", "2")
    
    # Clean up any existing test data
    await conn.execute("DELETE FROM flood_pixels_raw WHERE segment_id IN (3, 4)")
    await conn.execute("DELETE FROM flood_pixels_unique WHERE segment_id IN (3, 4)")
    
    # Insert test data - one recent, one 4h old
    await conn.execute(
        "INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen) "
        "VALUES (3, 10, 0, 0, false, ST_SetSRID(ST_MakePoint(0,0), 4326), now() - INTERVAL '4 hours'), "
        "       (4, 10, 0, 0, false, ST_SetSRID(ST_MakePoint(0,0), 4326), now())"
    )
    
    # Test the pruning function directly (should delete old record)
    pruned_count = await prune_raw_table_if_configured(conn)
    assert pruned_count == 1, "One old record should be pruned when retention is configured"
    
    # Verify only the recent record remains
    total_rows = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw WHERE segment_id IN (3, 4)")
    recent_rows = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw WHERE segment_id = 4")
    
    assert total_rows == 1, "Only recent record should remain when retention is configured"
    assert recent_rows == 1, "Recent record should be preserved"


@pytest.mark.asyncio
async def test_dedup_handles_invalid_retention_config(db_connection, monkeypatch):
    """Test that deduplication handles invalid RAW_RETENTION_HOURS gracefully."""
    conn = db_connection
    
    # Set invalid retention value
    monkeypatch.setenv("RAW_RETENTION_HOURS", "invalid")
    
    # Clean up any existing test data
    await conn.execute("DELETE FROM flood_pixels_raw WHERE segment_id IN (5, 6)")
    await conn.execute("DELETE FROM flood_pixels_unique WHERE segment_id IN (5, 6)")
    
    # Insert test data
    await conn.execute(
        "INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen) "
        "VALUES (5, 10, 0, 0, false, ST_SetSRID(ST_MakePoint(0,0), 4326), now() - INTERVAL '4 hours'), "
        "       (6, 10, 0, 0, false, ST_SetSRID(ST_MakePoint(0,0), 4326), now())"
    )
    
    # Test the pruning function directly - should not raise exception and should not delete
    pruned_count = await prune_raw_table_if_configured(conn)
    assert pruned_count == 0, "No records should be pruned with invalid config"
    
    # Verify both records still exist (fallback to append-only)
    rows = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw WHERE segment_id IN (5, 6)")
    assert rows == 2, "Invalid retention config should fallback to append-only mode"


@pytest.mark.asyncio
async def test_dedup_handles_zero_retention_config(db_connection, monkeypatch):
    """Test that deduplication treats zero retention as disabled."""
    conn = db_connection
    
    # Set retention to 0 (disabled)
    monkeypatch.setenv("RAW_RETENTION_HOURS", "0")
    
    # Clean up any existing test data
    await conn.execute("DELETE FROM flood_pixels_raw WHERE segment_id IN (7, 8)")
    await conn.execute("DELETE FROM flood_pixels_unique WHERE segment_id IN (7, 8)")
    
    # Insert test data
    await conn.execute(
        "INSERT INTO flood_pixels_raw (segment_id, score, homes, qpe_1h, ffw, geom, first_seen) "
        "VALUES (7, 10, 0, 0, false, ST_SetSRID(ST_MakePoint(0,0), 4326), now() - INTERVAL '4 hours'), "
        "       (8, 10, 0, 0, false, ST_SetSRID(ST_MakePoint(0,0), 4326), now())"
    )
    
    # Test the pruning function directly - should not delete anything
    pruned_count = await prune_raw_table_if_configured(conn)
    assert pruned_count == 0, "No records should be pruned when retention is 0"
    
    # Verify both records still exist
    rows = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_raw WHERE segment_id IN (7, 8)")
    assert rows == 2, "Zero retention should be treated as disabled (append-only mode)"