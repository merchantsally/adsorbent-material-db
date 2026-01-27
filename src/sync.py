"""Sync engine for detecting and applying changes from NIST ISODB."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable

from src import database
from src.scraper import (
    fetch_materials, fetch_isotherms_count, enrich_with_isotherm_counts,
    fetch_isotherms, fetch_gases, fetch_bibliography
)
from src.utils import backup_database


@dataclass
class TableChanges:
    """Summary of changes detected for a single table."""
    table_name: str
    id_column: str
    new: list[dict] = field(default_factory=list)
    modified: list[dict] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)  # For storing additional data like isotherm data points

    @property
    def total_changes(self) -> int:
        return len(self.new) + len(self.modified) + len(self.deleted)

    @property
    def is_empty(self) -> bool:
        return self.total_changes == 0


@dataclass
class SyncResult:
    """Result of a sync operation."""
    success: bool
    table_changes: dict[str, TableChanges] = field(default_factory=dict)
    backup_path: Optional[Path] = None
    error: Optional[str] = None

    @property
    def total_changes(self) -> int:
        return sum(tc.total_changes for tc in self.table_changes.values())


def detect_table_changes(
    fetched_records: list[dict],
    table: str,
    id_column: str,
    db_path: Path = database.DEFAULT_DB_PATH,
) -> TableChanges:
    """Compare fetched records against a database table and detect changes."""
    # Get current checksums from database
    local_checksums = database.get_table_checksums(table, id_column, db_path)

    # Build lookup for fetched records
    fetched_by_id = {r[id_column]: r for r in fetched_records}

    new_records = []
    modified_records = []
    deleted_ids = []

    # Check for new and modified records
    for record_id, record in fetched_by_id.items():
        if record_id not in local_checksums:
            new_records.append(record)
        elif record["checksum"] != local_checksums[record_id]:
            modified_records.append(record)

    # Check for deleted records
    for record_id in local_checksums:
        if record_id not in fetched_by_id:
            deleted_ids.append(record_id)

    return TableChanges(
        table_name=table,
        id_column=id_column,
        new=new_records,
        modified=modified_records,
        deleted=deleted_ids,
    )


def check_suspicious_changes(
    changes: TableChanges,
    current_count: int,
    deletion_threshold: float = 0.1,
) -> Optional[str]:
    """Check for suspicious changes that require confirmation."""
    if current_count == 0:
        return None

    deletion_ratio = len(changes.deleted) / current_count

    if deletion_ratio > deletion_threshold:
        return (
            f"Warning: {len(changes.deleted)} {changes.table_name} ({deletion_ratio:.1%}) "
            f"would be deleted. This exceeds the {deletion_threshold:.0%} threshold. "
            f"Use --force to apply these changes."
        )

    return None


def apply_table_changes(
    changes: TableChanges,
    db_path: Path = database.DEFAULT_DB_PATH,
) -> None:
    """Apply detected changes to a database table."""
    # Add local_updated timestamp to new and modified records
    timestamp = datetime.utcnow().isoformat()

    for record in changes.new:
        record["local_updated"] = timestamp

    for record in changes.modified:
        record["local_updated"] = timestamp

    # Bulk operations
    database.bulk_upsert(changes.table_name, changes.new + changes.modified, db_path)
    database.bulk_delete(changes.table_name, changes.id_column, changes.deleted, db_path)

    # Handle isotherm data points specially
    if changes.table_name == "isotherms" and "data_points" in changes.metadata:
        data_points = changes.metadata["data_points"]

        # Delete old data points for modified/new isotherms
        for record in changes.new + changes.modified:
            database.delete_isotherm_data_points(record["filename"], db_path)

        # Delete data points for deleted isotherms
        for filename in changes.deleted:
            database.delete_isotherm_data_points(filename, db_path)

        # Insert new data points
        if data_points:
            database.bulk_upsert("isotherm_data_points", data_points, db_path)
            print(f"    Inserted {len(data_points):,} data points")


def sync_table(
    table: str,
    id_column: str,
    fetch_func: Callable,
    db_path: Path,
    force: bool = False,
) -> tuple[TableChanges, Optional[str]]:
    """Sync a single table from NIST API."""
    print(f"\nFetching {table}...")

    # Special handling for isotherms (returns tuple of metadata and data points)
    if table == "isotherms":
        records, data_points = fetch_func()
        print(f"  Fetched {len(records)} {table} with {len(data_points):,} data points")
    else:
        records = fetch_func()
        print(f"  Fetched {len(records)} {table}")
        data_points = None

    changes = detect_table_changes(records, table, id_column, db_path)
    print(f"  Changes: {len(changes.new)} new, {len(changes.modified)} modified, {len(changes.deleted)} deleted")

    # Check for suspicious changes
    current_count = database.get_table_count(table, db_path)
    warning = check_suspicious_changes(changes, current_count)

    if warning and not force:
        return changes, warning

    # Store data points for later insertion (isotherms only)
    if data_points is not None:
        changes.metadata["data_points"] = data_points

    return changes, None


def sync(
    dry_run: bool = False,
    force: bool = False,
    tables: Optional[list[str]] = None,
    db_path: Path = database.DEFAULT_DB_PATH,
) -> SyncResult:
    """Perform a full sync from NIST ISODB.

    Args:
        dry_run: If True, detect changes but don't apply them.
        force: If True, apply changes even if they look suspicious.
        tables: List of tables to sync. Default: all tables.
        db_path: Path to the SQLite database.

    Returns:
        SyncResult with details about the operation.
    """
    # Initialize database if needed
    database.init_db(db_path)

    # Define all syncable tables (materials handled specially)
    all_tables = {
        "materials": ("material_id", None),  # Special handling
        "isotherms": ("filename", fetch_isotherms),
        "gases": ("inchikey", fetch_gases),
        "bibliography": ("doi", fetch_bibliography),
    }

    # Filter tables if specified
    if tables:
        tables_to_sync = {k: v for k, v in all_tables.items() if k in tables}
    else:
        tables_to_sync = all_tables

    result = SyncResult(success=True)
    all_changes: dict[str, TableChanges] = {}
    warnings = []

    # Sync each table
    for table_name, (id_column, fetch_func) in tables_to_sync.items():
        if table_name == "materials" or fetch_func is None:
            # Materials has special handling for isotherm counts
            changes, warning = sync_materials_table(db_path, force)
        else:
            changes, warning = sync_table(table_name, id_column, fetch_func, db_path, force)

        all_changes[table_name] = changes
        if warning:
            warnings.append(warning)

    result.table_changes = all_changes

    # Check for warnings
    if warnings and not force:
        result.success = False
        result.error = "\n".join(warnings)
        for warning in warnings:
            print(warning)
        return result

    # Check if any changes
    if result.total_changes == 0:
        print("\nNo changes to apply across all tables.")
        return result

    if dry_run:
        print("\nDry run - no changes applied.")
        return result

    # Create backup before applying changes
    if db_path.exists():
        print("\nCreating backup...")
        result.backup_path = backup_database(db_path)
        print(f"Backup created: {result.backup_path}")

    # Apply changes to all tables
    print("\nApplying changes...")
    for table_name, changes in all_changes.items():
        if not changes.is_empty:
            apply_table_changes(changes, db_path)
            print(f"  Applied {changes.total_changes} changes to {table_name}")

    # Update sync timestamp
    database.set_last_sync_time(datetime.utcnow().isoformat(), db_path)
    print("\nSync complete.")

    return result


def sync_materials_table(db_path: Path, force: bool) -> tuple[TableChanges, Optional[str]]:
    """Sync materials table with isotherm count enrichment."""
    print("\nFetching materials...")
    materials = fetch_materials()
    print(f"  Fetched {len(materials)} materials")

    print("  Enriching with isotherm counts...")
    counts = fetch_isotherms_count()
    materials = enrich_with_isotherm_counts(materials, counts)
    print(f"  Enriched {len(counts)} materials with isotherm counts")

    changes = detect_table_changes(materials, "materials", "material_id", db_path)
    print(f"  Changes: {len(changes.new)} new, {len(changes.modified)} modified, {len(changes.deleted)} deleted")

    # Check for suspicious changes
    current_count = database.get_table_count("materials", db_path)
    warning = check_suspicious_changes(changes, current_count)

    if warning and not force:
        return changes, warning

    return changes, None


def print_sync_summary(result: SyncResult, verbose: bool = False) -> None:
    """Print a human-readable summary of sync results."""
    print(f"\n{'='*60}")
    print("SYNC SUMMARY")
    print(f"{'='*60}")

    for table_name, changes in result.table_changes.items():
        print(f"\n{table_name.upper()}")
        print(f"  New: {len(changes.new)}")
        print(f"  Modified: {len(changes.modified)}")
        print(f"  Deleted: {len(changes.deleted)}")

        if verbose and changes.new:
            print(f"  New records:")
            for r in changes.new[:5]:
                id_val = r.get(changes.id_column, "unknown")
                name = r.get("name", r.get("title", id_val))
                print(f"    + {name}")
            if len(changes.new) > 5:
                print(f"    ... and {len(changes.new) - 5} more")

    print(f"\n{'='*60}")
    print(f"Total changes: {result.total_changes}")
    print(f"{'='*60}")


# Legacy compatibility - keep old function signature working
def print_changes_summary(changes, verbose: bool = False) -> None:
    """Legacy function for backward compatibility."""
    if isinstance(changes, TableChanges):
        print(f"\n{changes.table_name}: {len(changes.new)} new, "
              f"{len(changes.modified)} modified, {len(changes.deleted)} deleted")
