"""Sync engine for detecting and applying changes from NIST ISODB."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from src import database
from src.scraper import fetch_materials, fetch_isotherms_count, enrich_with_isotherm_counts
from src.utils import backup_database


@dataclass
class SyncChanges:
    """Summary of changes detected during sync."""
    new: list[dict]
    modified: list[tuple[dict, dict]]  # (new_material, old_material)
    deleted: list[dict]

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
    changes: SyncChanges
    backup_path: Optional[Path] = None
    error: Optional[str] = None


def detect_changes(
    fetched_materials: list[dict],
    db_path: Path = database.DEFAULT_DB_PATH,
) -> SyncChanges:
    """Compare fetched materials against the database and detect changes."""
    # Get current checksums from database
    local_checksums = database.get_all_checksums(db_path)
    local_materials = {m["material_id"]: m for m in database.get_all_materials(db_path)}

    # Build lookup for fetched materials
    fetched_by_id = {m["material_id"]: m for m in fetched_materials}

    new_materials = []
    modified_materials = []
    deleted_materials = []

    # Check for new and modified materials
    for material_id, material in fetched_by_id.items():
        if material_id not in local_checksums:
            new_materials.append(material)
        elif material["checksum"] != local_checksums[material_id]:
            old_material = local_materials[material_id]
            modified_materials.append((material, old_material))

    # Check for deleted materials
    for material_id in local_checksums:
        if material_id not in fetched_by_id:
            deleted_materials.append(local_materials[material_id])

    return SyncChanges(
        new=new_materials,
        modified=modified_materials,
        deleted=deleted_materials,
    )


def check_suspicious_changes(
    changes: SyncChanges,
    current_count: int,
    deletion_threshold: float = 0.1,
) -> Optional[str]:
    """Check for suspicious changes that require confirmation.

    Returns a warning message if changes are suspicious, None otherwise.
    """
    if current_count == 0:
        return None  # First sync, nothing suspicious

    deletion_ratio = len(changes.deleted) / current_count

    if deletion_ratio > deletion_threshold:
        return (
            f"Warning: {len(changes.deleted)} materials ({deletion_ratio:.1%}) "
            f"would be deleted. This exceeds the {deletion_threshold:.0%} threshold. "
            f"Use --force to apply these changes."
        )

    return None


def apply_changes(
    changes: SyncChanges,
    db_path: Path = database.DEFAULT_DB_PATH,
) -> None:
    """Apply detected changes to the database."""
    # Insert new materials
    for material in changes.new:
        database.insert_material(material, db_path)

    # Update modified materials
    for new_material, old_material in changes.modified:
        database.update_material(new_material, old_material, db_path)

    # Delete removed materials
    for material in changes.deleted:
        database.delete_material(material["material_id"], material, db_path)

    # Update sync timestamp
    database.set_last_sync_time(datetime.utcnow().isoformat(), db_path)


def sync(
    dry_run: bool = False,
    force: bool = False,
    skip_isotherm_counts: bool = False,
    db_path: Path = database.DEFAULT_DB_PATH,
) -> SyncResult:
    """Perform a full sync from NIST ISODB.

    Args:
        dry_run: If True, detect changes but don't apply them.
        force: If True, apply changes even if they look suspicious.
        skip_isotherm_counts: If True, skip fetching isotherm counts (faster).
        db_path: Path to the SQLite database.

    Returns:
        SyncResult with details about the operation.
    """
    # Initialize database if needed
    database.init_db(db_path)

    # Fetch materials from NIST
    print("Fetching materials from NIST ISODB...")
    materials = fetch_materials()
    print(f"Fetched {len(materials)} materials")

    # Optionally enrich with isotherm counts
    if not skip_isotherm_counts:
        print("Fetching isotherm counts...")
        counts = fetch_isotherms_count()
        materials = enrich_with_isotherm_counts(materials, counts)
        print(f"Enriched with isotherm counts for {len(counts)} materials")

    # Detect changes
    print("Detecting changes...")
    changes = detect_changes(materials, db_path)

    print(f"Changes detected: {len(changes.new)} new, "
          f"{len(changes.modified)} modified, {len(changes.deleted)} deleted")

    if changes.is_empty:
        print("No changes to apply.")
        return SyncResult(success=True, changes=changes)

    # Check for suspicious changes
    current_count = database.get_material_count(db_path)
    warning = check_suspicious_changes(changes, current_count)

    if warning and not force:
        print(warning)
        return SyncResult(
            success=False,
            changes=changes,
            error=warning,
        )

    if dry_run:
        print("Dry run - no changes applied.")
        return SyncResult(success=True, changes=changes)

    # Create backup before applying changes
    backup_path = None
    if db_path.exists():
        print("Creating backup...")
        backup_path = backup_database(db_path)
        print(f"Backup created: {backup_path}")

    # Apply changes
    print("Applying changes...")
    apply_changes(changes, db_path)
    print("Sync complete.")

    return SyncResult(
        success=True,
        changes=changes,
        backup_path=backup_path,
    )


def print_changes_summary(changes: SyncChanges, verbose: bool = False) -> None:
    """Print a human-readable summary of changes."""
    print(f"\n{'='*50}")
    print("SYNC CHANGES SUMMARY")
    print(f"{'='*50}")

    print(f"\nNew materials: {len(changes.new)}")
    if verbose and changes.new:
        for m in changes.new[:10]:
            print(f"  + {m['name']} ({m['material_id']})")
        if len(changes.new) > 10:
            print(f"  ... and {len(changes.new) - 10} more")

    print(f"\nModified materials: {len(changes.modified)}")
    if verbose and changes.modified:
        for new_m, old_m in changes.modified[:10]:
            print(f"  ~ {new_m['name']} ({new_m['material_id']})")
        if len(changes.modified) > 10:
            print(f"  ... and {len(changes.modified) - 10} more")

    print(f"\nDeleted materials: {len(changes.deleted)}")
    if verbose and changes.deleted:
        for m in changes.deleted[:10]:
            print(f"  - {m['name']} ({m['material_id']})")
        if len(changes.deleted) > 10:
            print(f"  ... and {len(changes.deleted) - 10} more")

    print(f"\n{'='*50}")
