"""
Module entry point for running deduplicator as: python -m dedup
"""

import asyncio
from dedup import run_with_retry

if __name__ == "__main__":
    asyncio.run(run_with_retry())