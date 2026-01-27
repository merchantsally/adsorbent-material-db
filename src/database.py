"""SQLite database operations for NIST ISODB materials."""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

# Default database path
DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "materials.db"


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Get a database connection, creating the database if needed."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Initialize the database schema."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Materials table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS materials (
            material_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            synonyms TEXT,
            formula TEXT,
            category TEXT,
            isotherm_count INTEGER DEFAULT 0,
            last_updated TEXT,
            local_updated TEXT NOT NULL,
            checksum TEXT NOT NULL
        )
    """)

    # Audit log table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            action TEXT NOT NULL,
            material_id TEXT NOT NULL,
            old_values TEXT,
            new_values TEXT
        )
    """)

    # Sync metadata table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # Isotherms table - EXPANDED SCHEMA
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS isotherms (
            filename TEXT PRIMARY KEY,
            doi TEXT,
            adsorbent_id TEXT,
            adsorbent_name TEXT,
            adsorbates TEXT,
            category TEXT,
            temperature REAL,
            tabular_data INTEGER,
            isotherm_type TEXT,
            article_source TEXT,
            date TEXT,
            digitizer TEXT,
            adsorption_units TEXT,
            pressure_units TEXT,
            composition_type TEXT,
            concentration_units TEXT,
            data_point_count INTEGER DEFAULT 0,
            local_updated TEXT NOT NULL,
            checksum TEXT NOT NULL
        )
    """)

    # Isotherm data points table - stores actual measurement data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS isotherm_data_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            isotherm_filename TEXT NOT NULL,
            pressure REAL NOT NULL,
            total_adsorption REAL,
            species_data TEXT,
            FOREIGN KEY (isotherm_filename) REFERENCES isotherms(filename)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_isotherm_data_filename
        ON isotherm_data_points(isotherm_filename)
    """)

    # Gases (adsorbates) table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gases (
            inchikey TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            synonyms TEXT,
            local_updated TEXT NOT NULL,
            checksum TEXT NOT NULL
        )
    """)

    # Bibliography table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bibliography (
            doi TEXT PRIMARY KEY,
            title TEXT,
            journal TEXT,
            year INTEGER,
            authors TEXT,
            categories TEXT,
            adsorbents TEXT,
            adsorbates TEXT,
            temperatures TEXT,
            pressures TEXT,
            local_updated TEXT NOT NULL,
            checksum TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def get_all_materials(db_path: Path = DEFAULT_DB_PATH) -> list[dict]:
    """Get all materials from the database."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM materials ORDER BY name")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_material(material_id: str, db_path: Path = DEFAULT_DB_PATH) -> Optional[dict]:
    """Get a single material by ID."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM materials WHERE material_id = ?", (material_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_checksums(db_path: Path = DEFAULT_DB_PATH) -> dict[str, str]:
    """Get a mapping of material_id -> checksum for all materials."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT material_id, checksum FROM materials")
    rows = cursor.fetchall()
    conn.close()
    return {row["material_id"]: row["checksum"] for row in rows}


def insert_material(material: dict, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Insert a new material and log the action."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO materials (
            material_id, name, synonyms, formula, category,
            isotherm_count, last_updated, local_updated, checksum
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        material["material_id"],
        material["name"],
        material.get("synonyms"),
        material.get("formula"),
        material.get("category"),
        material.get("isotherm_count", 0),
        material.get("last_updated"),
        datetime.utcnow().isoformat(),
        material["checksum"],
    ))

    # Log the insert
    cursor.execute("""
        INSERT INTO audit_log (timestamp, action, material_id, new_values)
        VALUES (?, 'INSERT', ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        material["material_id"],
        json.dumps(material),
    ))

    conn.commit()
    conn.close()


def update_material(material: dict, old_material: dict, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Update an existing material and log the action."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE materials SET
            name = ?,
            synonyms = ?,
            formula = ?,
            category = ?,
            isotherm_count = ?,
            last_updated = ?,
            local_updated = ?,
            checksum = ?
        WHERE material_id = ?
    """, (
        material["name"],
        material.get("synonyms"),
        material.get("formula"),
        material.get("category"),
        material.get("isotherm_count", 0),
        material.get("last_updated"),
        datetime.utcnow().isoformat(),
        material["checksum"],
        material["material_id"],
    ))

    # Log the update
    cursor.execute("""
        INSERT INTO audit_log (timestamp, action, material_id, old_values, new_values)
        VALUES (?, 'UPDATE', ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        material["material_id"],
        json.dumps(old_material),
        json.dumps(material),
    ))

    conn.commit()
    conn.close()


def delete_material(material_id: str, old_material: dict, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Delete a material and log the action."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM materials WHERE material_id = ?", (material_id,))

    # Log the deletion
    cursor.execute("""
        INSERT INTO audit_log (timestamp, action, material_id, old_values)
        VALUES (?, 'DELETE', ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        material_id,
        json.dumps(old_material),
    ))

    conn.commit()
    conn.close()


def get_material_count(db_path: Path = DEFAULT_DB_PATH) -> int:
    """Get the total number of materials in the database."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM materials")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_last_sync_time(db_path: Path = DEFAULT_DB_PATH) -> Optional[str]:
    """Get the timestamp of the last successful sync."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sync_metadata WHERE key = 'last_sync'")
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def set_last_sync_time(timestamp: str, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Set the timestamp of the last successful sync."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO sync_metadata (key, value)
        VALUES ('last_sync', ?)
    """, (timestamp,))
    conn.commit()
    conn.close()


def get_recent_audit_logs(limit: int = 50, db_path: Path = DEFAULT_DB_PATH) -> list[dict]:
    """Get recent audit log entries."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM audit_log
        ORDER BY timestamp DESC
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


# =============================================================================
# Generic table operations for isotherms, gases, bibliography
# =============================================================================

def get_table_checksums(table: str, id_column: str, db_path: Path = DEFAULT_DB_PATH) -> dict[str, str]:
    """Get a mapping of id -> checksum for a table."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT {id_column}, checksum FROM {table}")
    rows = cursor.fetchall()
    conn.close()
    return {row[id_column]: row["checksum"] for row in rows}


def get_all_from_table(table: str, db_path: Path = DEFAULT_DB_PATH) -> list[dict]:
    """Get all records from a table."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_table_count(table: str, db_path: Path = DEFAULT_DB_PATH) -> int:
    """Get the total number of records in a table."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def upsert_record(table: str, record: dict, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Insert or replace a record in a table."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    columns = ", ".join(record.keys())
    placeholders = ", ".join(["?"] * len(record))
    values = list(record.values())

    cursor.execute(f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})", values)
    conn.commit()
    conn.close()


def delete_record(table: str, id_column: str, record_id: str, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Delete a record from a table."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table} WHERE {id_column} = ?", (record_id,))
    conn.commit()
    conn.close()


def bulk_upsert(table: str, records: list[dict], db_path: Path = DEFAULT_DB_PATH) -> None:
    """Bulk insert or replace records in a table."""
    if not records:
        return

    conn = get_connection(db_path)
    cursor = conn.cursor()

    columns = ", ".join(records[0].keys())
    placeholders = ", ".join(["?"] * len(records[0]))

    for record in records:
        values = list(record.values())
        cursor.execute(f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})", values)

    conn.commit()
    conn.close()


def bulk_delete(table: str, id_column: str, record_ids: list[str], db_path: Path = DEFAULT_DB_PATH) -> None:
    """Bulk delete records from a table."""
    if not record_ids:
        return

    conn = get_connection(db_path)
    cursor = conn.cursor()

    for record_id in record_ids:
        cursor.execute(f"DELETE FROM {table} WHERE {id_column} = ?", (record_id,))

    conn.commit()
    conn.close()


def delete_isotherm_data_points(filename: str, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Delete all data points for a specific isotherm."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM isotherm_data_points WHERE isotherm_filename = ?", (filename,))
    conn.commit()
    conn.close()


# =============================================================================
# Individual isotherm data point fetching support
# =============================================================================

def migrate_add_data_fetched_column(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Add data_fetched tracking columns to isotherms table if not exists."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Check if column exists
    cursor.execute("PRAGMA table_info(isotherms)")
    columns = [row[1] for row in cursor.fetchall()]

    if "data_fetched" not in columns:
        cursor.execute("ALTER TABLE isotherms ADD COLUMN data_fetched INTEGER DEFAULT 0")
        cursor.execute("ALTER TABLE isotherms ADD COLUMN data_fetched_at TEXT")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_isotherms_data_fetched ON isotherms(data_fetched)")
        print("Added data_fetched tracking columns to isotherms table")

    # Create failed fetches table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS failed_isotherm_fetches (
            filename TEXT PRIMARY KEY,
            failure_count INTEGER DEFAULT 1,
            last_error TEXT,
            last_attempt_at TEXT,
            FOREIGN KEY (filename) REFERENCES isotherms(filename)
        )
    """)

    conn.commit()
    conn.close()


def get_unfetched_isotherms(limit: Optional[int] = None, db_path: Path = DEFAULT_DB_PATH) -> list[str]:
    """Get filenames of isotherms that haven't had their data points fetched yet.

    Args:
        limit: Maximum number of filenames to return (None for all)

    Returns:
        List of isotherm filenames
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    query = "SELECT filename FROM isotherms WHERE data_fetched = 0 OR data_fetched IS NULL ORDER BY filename"
    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query)
    filenames = [row[0] for row in cursor.fetchall()]
    conn.close()

    return filenames


def mark_isotherms_data_fetched(filenames: list[str], db_path: Path = DEFAULT_DB_PATH) -> None:
    """Mark isotherms as having their data points fetched."""
    if not filenames:
        return

    conn = get_connection(db_path)
    cursor = conn.cursor()
    timestamp = datetime.utcnow().isoformat()

    for filename in filenames:
        cursor.execute("""
            UPDATE isotherms
            SET data_fetched = 1, data_fetched_at = ?
            WHERE filename = ?
        """, (timestamp, filename))

    conn.commit()
    conn.close()


def record_failed_isotherm_fetch(filename: str, error: str, db_path: Path = DEFAULT_DB_PATH) -> None:
    """Record a failed isotherm fetch attempt."""
    conn = get_connection(db_path)
    cursor = conn.cursor()
    timestamp = datetime.utcnow().isoformat()

    cursor.execute("""
        INSERT INTO failed_isotherm_fetches (filename, failure_count, last_error, last_attempt_at)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(filename) DO UPDATE SET
            failure_count = failure_count + 1,
            last_error = excluded.last_error,
            last_attempt_at = excluded.last_attempt_at
    """, (filename, error, timestamp))

    conn.commit()
    conn.close()


def get_data_fetch_progress(db_path: Path = DEFAULT_DB_PATH) -> dict:
    """Get progress statistics for data point fetching."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM isotherms WHERE data_fetched = 1")
    fetched = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM isotherms")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM failed_isotherm_fetches")
    failed = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM isotherm_data_points")
    total_points = cursor.fetchone()[0]

    conn.close()

    return {
        "fetched": fetched,
        "total": total,
        "remaining": total - fetched,
        "failed": failed,
        "total_data_points": total_points,
        "percent_complete": (fetched / total * 100) if total > 0 else 0,
    }


def update_isotherm_metadata(metadata_list: list[dict], db_path: Path = DEFAULT_DB_PATH) -> None:
    """Update isotherms with unit metadata from individual API fetches.

    Args:
        metadata_list: List of dicts with filename, pressure_units, adsorption_units
        db_path: Database path
    """
    if not metadata_list:
        return

    conn = get_connection(db_path)
    cursor = conn.cursor()

    for metadata in metadata_list:
        cursor.execute("""
            UPDATE isotherms
            SET pressure_units = ?, adsorption_units = ?
            WHERE filename = ?
        """, (
            metadata.get("pressure_units"),
            metadata.get("adsorption_units"),
            metadata["filename"],
        ))

    conn.commit()
    conn.close()
