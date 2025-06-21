"""
Database connection pool helper - copied from Generator repo.
Provides async connection pooling for PostgreSQL with PostGIS.
"""

import os
from typing import Optional

import asyncpg


_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """
    Get or create the database connection pool.
    
    Returns:
        asyncpg.Pool: Connection pool instance.
    """
    global _pool
    
    if _pool is None:
        # Determine which database URL to use based on environment
        env = os.getenv("ENV", "dev").lower()
        if env == "prod":
            database_url = os.getenv("DATABASE_URL_PROD")
        else:
            database_url = os.getenv("DATABASE_URL_DEV")
        
        if not database_url:
            raise ValueError(f"DATABASE_URL_{env.upper()} environment variable not set")
        
        # Create connection pool with appropriate settings
        _pool = await asyncpg.create_pool(
            database_url,
            min_size=1,
            max_size=5,
            command_timeout=60,
            server_settings={
                'application_name': 'flood-lead-deduplicator',
                'timezone': 'UTC'
            }
        )
    
    return _pool


async def close_pool():
    """Close the database connection pool."""
    global _pool
    
    if _pool:
        await _pool.close()
        _pool = None