"""Utility functions for the NIST ISODB scraper."""

import hashlib
import json
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

# Rate limiting
_last_request_time: float = 0
MIN_REQUEST_INTERVAL = 1.0  # seconds


def rate_limited_get(url: str, timeout: int = 30) -> requests.Response:
    """Make a GET request with rate limiting (1 second between requests)."""
    global _last_request_time

    # Wait if needed to respect rate limit
    elapsed = time.time() - _last_request_time
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)

    response = requests.get(url, timeout=timeout)
    _last_request_time = time.time()

    response.raise_for_status()
    return response


def calculate_checksum(data: dict) -> str:
    """Calculate MD5 checksum for a material record.

    Only includes fields that indicate the record has changed.
    """
    # Normalize the data for consistent hashing
    relevant_fields = {
        "name": data.get("name", ""),
        "synonyms": data.get("synonyms", ""),
        "formula": data.get("formula", ""),
        "category": data.get("category", ""),
        "isotherm_count": data.get("isotherm_count", 0),
    }
    serialized = json.dumps(relevant_fields, sort_keys=True)
    return hashlib.md5(serialized.encode()).hexdigest()


def backup_database(db_path: Path, backup_dir: Path = None) -> Path:
    """Create a timestamped backup of the database.

    Returns the path to the backup file.
    """
    if backup_dir is None:
        backup_dir = db_path.parent.parent / "backups"

    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"materials_{timestamp}.db"

    shutil.copy2(db_path, backup_path)
    return backup_path


def restore_database(backup_path: Path, db_path: Path) -> None:
    """Restore the database from a backup file."""
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup file not found: {backup_path}")

    shutil.copy2(backup_path, db_path)


def list_backups(backup_dir: Path = None, db_path: Path = None) -> list[Path]:
    """List all available backup files, sorted by date (newest first)."""
    if backup_dir is None:
        if db_path is None:
            from src.database import DEFAULT_DB_PATH
            db_path = DEFAULT_DB_PATH
        backup_dir = db_path.parent.parent / "backups"

    if not backup_dir.exists():
        return []

    backups = list(backup_dir.glob("materials_*.db"))
    return sorted(backups, reverse=True)


def format_timestamp(iso_timestamp: str) -> str:
    """Format an ISO timestamp for display."""
    if not iso_timestamp:
        return "Never"
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return iso_timestamp
