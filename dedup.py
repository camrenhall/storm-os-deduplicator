#!/usr/bin/env python3
"""
Flood-Lead Intelligence - Deduplicator (Container 2)

Deduplicates flood_pixels_raw to flood_pixels_unique,
and refreshes flood_pixels_marketable materialized view.
Raw table is now append-only by default.
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


async def prune_raw_table_if_configured(conn: asyncpg.Connection) -> int:
    """
    Delete records older than configured retention period from flood_pixels_raw.
    Only runs if RAW_RETENTION_HOURS environment variable is set and > 0.
    
    Returns:
        Number of rows deleted (0 if pruning disabled).
    """
    raw_retention_hours = os.getenv("RAW_RETENTION_HOURS")
    
    # Skip pruning if not configured or set to None/empty
    if not raw_retention_hours:
        logger.info("RAW_RETENTION_HOURS not set - skipping raw table pruning (append-only mode)")
        return 0
    
    try:
        retention_hours = float(raw_retention_hours)
        if retention_hours <= 0:
            logger.info("RAW_RETENTION_HOURS <= 0 - skipping raw table pruning (append-only mode)")
            return 0
    except (ValueError, TypeError):
        logger.warning(f"Invalid RAW_RETENTION_HOURS value '{raw_retention_hours}' - skipping pruning")
        return 0
    
    logger.info(f"RAW_RETENTION_HOURS={retention_hours} - pruning old records")
    
    try:
        result = await conn.execute(
            f"DELETE FROM flood_pixels_raw WHERE first_seen < now() - INTERVAL '{retention_hours} hours'"
        )
        # Extract row count from result string like "DELETE 123"
        rows_deleted = int(result.split()[-1]) if result.split()[-1].isdigit() else 0
        logger.info(f"Pruned {rows_deleted} records older than {retention_hours} hours")
        return rows_deleted
    except asyncpg.exceptions.InsufficientPrivilegeError:
        logger.warning("No DELETE privilege on flood_pixels_raw - continuing in append-only mode")
        return 0
    except Exception as e:
        logger.error(f"Failed to prune raw table: {e}")
        # Don't fail the entire process if pruning fails
        return 0


async def upsert_unique_pixels(conn: asyncpg.Connection) -> tuple[int, int]:
    """
    UPSERT from flood_pixels_raw to flood_pixels_unique.
    Only updates existing records if new score is higher.
    
    Returns:
        Tuple of (inserts, updates) performed.
    """
    # Get counts before
    count_before = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_unique")
    
    # Store existing records to calculate updates properly
    existing_segments = await conn.fetch("""
        SELECT segment_id, score 
        FROM flood_pixels_unique 
        WHERE segment_id IN (SELECT segment_id FROM flood_pixels_raw)
    """)
    existing_dict = {row['segment_id']: row['score'] for row in existing_segments}
    
    # Get raw records that will be processed
    raw_records = await conn.fetch("SELECT segment_id, score FROM flood_pixels_raw")
    
    # Perform the UPSERT
    await conn.execute("""
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
    
    # Calculate metrics
    count_after = await conn.fetchval("SELECT COUNT(*) FROM flood_pixels_unique")
    inserts = count_after - count_before
    
    # Count actual updates (raw records with higher scores than existing)
    updates = 0
    for raw_record in raw_records:
        segment_id = raw_record['segment_id']
        if segment_id in existing_dict and raw_record['score'] > existing_dict[segment_id]:
            updates += 1
    
    return inserts, updates


async def refresh_marketable_view(conn: asyncpg.Connection) -> int:
    """
    Non-blocking refresh of flood_pixels_marketable materialized view.
    Uses table swap technique to avoid locking the view during refresh.
    
    Returns:
        Number of rows in the new marketable view.
    """
    # Clean up any leftover tables/views from previous failed runs
    await conn.execute("DROP TABLE IF EXISTS _mv CASCADE")
    await conn.execute("DROP TABLE IF EXISTS _old CASCADE")
    await conn.execute("DROP MATERIALIZED VIEW IF EXISTS _old CASCADE")
    
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
    
    # Check what type of object flood_pixels_marketable is
    object_type = await conn.fetchval("""
        SELECT 
            CASE 
                WHEN c.relkind = 'r' THEN 'table'
                WHEN c.relkind = 'm' THEN 'materialized_view'
                ELSE 'unknown'
            END as object_type
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'flood_pixels_marketable'
        AND n.nspname = 'public'
    """)
    
    logger.info(f"flood_pixels_marketable is a {object_type}")
    
    # Handle the swap based on object type
    if object_type == 'materialized_view':
        logger.info("Dropping existing materialized view")
        await conn.execute("DROP MATERIALIZED VIEW flood_pixels_marketable")
    elif object_type == 'table':
        logger.info("Renaming existing table to _old")
        await conn.execute("ALTER TABLE flood_pixels_marketable RENAME TO _old")
    else:
        # Object doesn't exist, which is fine for first run
        logger.info("flood_pixels_marketable doesn't exist, will create new")
    
    # Rename new table to target name
    await conn.execute("ALTER TABLE _mv RENAME TO flood_pixels_marketable")
    
    # Clean up old table if it exists
    await conn.execute("DROP TABLE IF EXISTS _old CASCADE")
    
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
            # 1. Conditionally prune old records (only if configured)
            logger.info("Checking if raw table pruning is configured")
            metrics.raw_pruned_rows = await prune_raw_table_if_configured(conn)
            if metrics.raw_pruned_rows > 0:
                logger.info(f"Pruned {metrics.raw_pruned_rows} old records")
            else:
                logger.info("No records pruned - raw table operating in append-only mode")
            
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