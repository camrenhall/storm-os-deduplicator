#!/usr/bin/env python3
"""
Flood-Lead Intelligence - Deduplicator (Container 2)

Prunes flood_pixels_raw to 3-hour window, deduplicates to flood_pixels_unique,
and refreshes flood_pixels_marketable materialized view.
"""

import asyncio
import logging
import os
import sys
import time
from typing import Optional

import asyncpg

from db import get_pool

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("deduplicator")

ADVISORY_LOCK_ID = 987655


class DeduplicationMetrics:
    """Track metrics for the deduplication run."""
    
    def __init__(self):
        self.start_time = time.time()
        self.raw_pruned_rows = 0
        self.upsert_inserts = 0
        self.upsert_updates = 0
        self.marketable_rows = 0
    
    @property
    def latency_seconds(self) -> float:
        return time.time() - self.start_time
    
    def log_metrics(self):
        """Log final metrics in Prometheus-compatible format."""
        print(f"dedup_latency_seconds={self.latency_seconds:.1f} "
              f"raw_pruned_rows={self.raw_pruned_rows} "
              f"upsert_inserts={self.upsert_inserts} "
              f"upsert_updates={self.upsert_updates} "
              f"marketable_rows={self.marketable_rows}")


async def acquire_advisory_lock(conn: asyncpg.Connection) -> bool:
    """
    Acquire session-level advisory lock to prevent concurrent runs.
    
    Returns:
        True if lock acquired, False if another instance is running.
    """
    acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1)", ADVISORY_LOCK_ID)
    return bool(acquired)


async def release_advisory_lock(conn: asyncpg.Connection):
    """Release the advisory lock."""
    await conn.execute("SELECT pg_advisory_unlock($1)", ADVISORY_LOCK_ID)


async def prune_raw_table(conn: asyncpg.Connection) -> int:
    """
    Delete records older than 3 hours from flood_pixels_raw.
    
    Returns:
        Number of rows deleted.
    """
    result = await conn.execute(
        "DELETE FROM flood_pixels_raw WHERE first_seen < now() - INTERVAL '180 minutes'"
    )
    # Extract row count from result string like "DELETE 123"
    return int(result.split()[-1]) if result.split()[-1].isdigit() else 0


async def upsert_unique_pixels(conn: asyncpg.Connection) -> tuple[int, int]:
    """UPSERT from flood_pixels_raw to flood_pixels_unique."""
    
    # Get counts to calculate metrics
    count_before = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_unique")
    
    # Get raw records that would be processed
    raw_records = await conn.fetch("SELECT segment_id, score FROM flood_pixels_raw")
    
    # Perform the UPSERT with the critical WHERE clause
    result = await conn.execute("""
        INSERT INTO flood_pixels_unique AS u (segment_id, score, homes, qpe_1h, ffw,
                                              geom, first_seen, updated_at)
        SELECT segment_id, score, homes, qpe_1h, ffw, geom, first_seen, now()
        FROM   flood_pixels_raw
        ON CONFLICT (segment_id) DO UPDATE
          SET (score, homes, qpe_1h, ffw, geom, updated_at)
              = (EXCLUDED.score, EXCLUDED.homes, EXCLUDED.qpe_1h,
                 EXCLUDED.ffw, EXCLUDED.geom, now())
          WHERE EXCLUDED.score > u.score
    """)
    
    count_after = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_unique")
    
    # Calculate actual inserts and updates
    inserts = count_after - count_before
    
    # Count how many raw records had higher scores than existing unique records
    updates = 0
    for raw_record in raw_records:
        existing = await conn.fetchval(
            "SELECT score FROM flood_pixels_unique WHERE segment_id = $1", 
            raw_record['segment_id']
        )
        if existing is not None and raw_record['score'] > existing:
            updates += 1
    
    return inserts, updates


async def refresh_marketable_view(conn: asyncpg.Connection) -> int:
    """
    Non-blocking refresh of flood_pixels_marketable materialized view.
    Uses table swap technique to avoid locking the view during refresh.
    
    Returns:
        Number of rows in the new marketable view.
    """
    # Clean up any leftover tables from previous failed runs
    await conn.execute("DROP TABLE IF EXISTS _mv CASCADE")
    await conn.execute("DROP TABLE IF EXISTS _old CASCADE")
    
    # Create new materialized view as temporary table
    await conn.execute("""
        CREATE TABLE _mv AS
          SELECT *
          FROM   flood_pixels_unique
          WHERE  score >= 40
            AND  homes >= 200
          ORDER  BY score DESC
          LIMIT  10
    """)
    
    # Atomic swap: rename existing view to _old, new table to view name
    await conn.execute("ALTER TABLE flood_pixels_marketable RENAME TO _old")
    await conn.execute("ALTER TABLE _mv RENAME TO flood_pixels_marketable")
    await conn.execute("DROP TABLE _old")
    
    # Return count of marketable rows
    return await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_marketable")


async def run_deduplication():
    """Main deduplication logic with transaction and error handling."""
    metrics = DeduplicationMetrics()
    pool = None
    conn = None
    
    try:
        # Get database connection pool
        pool = await get_pool()
        
        # Acquire connection and advisory lock
        conn = await pool.acquire()
        
        if not await acquire_advisory_lock(conn):
            logger.warning("Another Deduplicator is running; aborting.")
            return
        
        logger.info("Starting deduplication cycle")
        
        # Execute all operations in a single serializable transaction
        async with conn.transaction(isolation='serializable'):
            # 1. Prune old records
            logger.info("Pruning old records from flood_pixels_raw")
            metrics.raw_pruned_rows = await prune_raw_table(conn)
            logger.info(f"Pruned {metrics.raw_pruned_rows} old records")
            
            # 2. UPSERT to unique table
            logger.info("Upserting to flood_pixels_unique")
            metrics.upsert_inserts, metrics.upsert_updates = await upsert_unique_pixels(conn)
            logger.info(f"UPSERT: {metrics.upsert_inserts} inserts, {metrics.upsert_updates} updates")
            
            # 3. Refresh materialized view
            logger.info("Refreshing flood_pixels_marketable view")
            metrics.marketable_rows = await refresh_marketable_view(conn)
            logger.info(f"Materialized view refreshed: {metrics.marketable_rows} marketable rows")
        
        logger.info(f"Deduplication completed in {metrics.latency_seconds:.1f}s")
        
        # Log metrics for Prometheus scraping
        metrics.log_metrics()
        
    except Exception as e:
        logger.error(f"Deduplication failed: {e}")
        raise
    
    finally:
        # Always release the advisory lock and connection
        if conn:
            try:
                await release_advisory_lock(conn)
            except Exception as e:
                logger.warning(f"Failed to release advisory lock: {e}")
            
            try:
                await pool.release(conn)
            except Exception as e:
                logger.warning(f"Failed to release connection: {e}")
        
        if pool:
            await pool.close()


async def run_with_retry():
    """Run deduplication with single retry on transient errors."""
    transient_errors = (
        asyncpg.exceptions.CannotConnectNowError,
        asyncpg.exceptions.ConnectionDoesNotExistError,
        asyncpg.exceptions.ConnectionFailureError,
        asyncpg.exceptions.TooManyConnectionsError,
        asyncpg.exceptions.SerializationError,
        asyncpg.exceptions.DeadlockDetectedError,
    )
    
    try:
        await run_deduplication()
    except transient_errors as e:
        logger.warning(f"Transient error occurred: {e}. Retrying in 30 seconds...")
        await asyncio.sleep(30)
        try:
            await run_deduplication()
        except transient_errors as retry_e:
            logger.error(f"Retry failed with transient error: {retry_e}")
            sys.exit(1)
        except Exception as retry_e:
            logger.error(f"Retry failed with permanent error: {retry_e}")
            sys.exit(1)
    except (
        asyncpg.exceptions.UndefinedTableError,
        asyncpg.exceptions.SyntaxOrAccessError,
    ) as e:
        logger.error(f"Permanent database error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(run_with_retry())