#!/usr/bin/env python3
"""CLI interface for the NIST ISODB Adsorbent Materials Scraper."""

import argparse
import sys
from pathlib import Path

from src import database
from src.sync import sync, print_sync_summary
from src.utils import backup_database, restore_database, list_backups, format_timestamp


def cmd_sync(args):
    """Execute the sync command."""
    # Parse tables argument
    tables = None
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",")]

    result = sync(
        dry_run=args.dry_run,
        force=args.force,
        tables=tables,
    )

    if args.verbose or args.dry_run:
        print_sync_summary(result, verbose=True)

    if not result.success:
        print(f"\nSync failed: {result.error}", file=sys.stderr)
        return 1

    return 0


def cmd_status(args):
    """Show database status."""
    db_path = database.DEFAULT_DB_PATH

    if not db_path.exists():
        print("Database not initialized. Run 'sync' to create it.")
        return 0

    last_sync = database.get_last_sync_time(db_path)

    print(f"Database: {db_path}")
    print(f"Last sync: {format_timestamp(last_sync)}")
    print()

    # Show counts for all tables
    tables = [
        ("materials", "Materials"),
        ("isotherms", "Isotherms"),
        ("isotherm_data_points", "Isotherm Data Points"),
        ("gases", "Gases (Adsorbates)"),
        ("bibliography", "Bibliography"),
    ]

    for table, label in tables:
        try:
            count = database.get_table_count(table, db_path)
            print(f"{label}: {count:,}")
        except Exception:
            print(f"{label}: (table not created)")

    # Show recent audit log if verbose
    if args.verbose:
        logs = database.get_recent_audit_logs(limit=10)
        if logs:
            print(f"\nRecent changes:")
            for log in logs:
                print(f"  [{log['timestamp'][:19]}] {log['action']} {log['material_id']}")

    return 0


def cmd_backup(args):
    """Create a manual backup."""
    db_path = database.DEFAULT_DB_PATH

    if not db_path.exists():
        print("Database not found. Nothing to backup.", file=sys.stderr)
        return 1

    backup_path = backup_database(db_path)
    print(f"Backup created: {backup_path}")
    return 0


def cmd_restore(args):
    """Restore from a backup."""
    db_path = database.DEFAULT_DB_PATH
    backup_path = Path(args.backup_file)

    if not backup_path.exists():
        print(f"Backup file not found: {backup_path}", file=sys.stderr)
        return 1

    restore_database(backup_path, db_path)
    print(f"Database restored from: {backup_path}")
    return 0


def cmd_list_backups(args):
    """List available backups."""
    backups = list_backups()

    if not backups:
        print("No backups found.")
        return 0

    print("Available backups:")
    for backup in backups:
        print(f"  {backup}")

    return 0


def cmd_sync_data_points(args):
    """Execute the sync-data-points command."""
    from src.sync import sync_data_points

    stats = sync_data_points(
        limit=args.limit,
        resume=not args.no_resume,
        force_refetch=args.force_refetch,
        batch_size=args.batch_size,
    )

    return 0


def cmd_data_status(args):
    """Show data point fetch status."""
    from src.database import get_data_fetch_progress, get_connection

    progress = get_data_fetch_progress()

    print(f"\n{'='*60}")
    print("DATA POINT FETCH STATUS")
    print(f"{'='*60}")
    print(f"Isotherms with data fetched: {progress['fetched']:,}/{progress['total']:,}")
    print(f"Remaining: {progress['remaining']:,}")
    print(f"Failed: {progress['failed']:,}")
    print(f"Progress: {progress['percent_complete']:.1f}%")
    print(f"Total data points: {progress['total_data_points']:,}")
    print(f"{'='*60}")

    # Show failed isotherms if any
    if progress['failed'] > 0:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT filename, failure_count, last_error, last_attempt_at
            FROM failed_isotherm_fetches
            ORDER BY failure_count DESC
            LIMIT 10
        """)
        print("\nMost problematic isotherms:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} failures - {row[2][:50]}")
        conn.close()

    return 0


def cmd_create_views(args):
    """Create normalized views and analysis schema."""
    from src.normalization import setup_normalized_schema

    db_path = database.DEFAULT_DB_PATH

    if not db_path.exists():
        print("Database not found. Run 'sync' first.", file=sys.stderr)
        return 1

    setup_normalized_schema(db_path)
    return 0


def cmd_refresh_matrix(args):
    """Refresh the gas_material_matrix materialized table."""
    from src.normalization import refresh_gas_material_matrix

    db_path = database.DEFAULT_DB_PATH

    if not db_path.exists():
        print("Database not found. Run 'sync' first.", file=sys.stderr)
        return 1

    refresh_gas_material_matrix(db_path)
    return 0


def cmd_normalization_status(args):
    """Show normalization and view statistics."""
    from src.normalization import get_normalization_stats

    db_path = database.DEFAULT_DB_PATH

    if not db_path.exists():
        print("Database not initialized.", file=sys.stderr)
        return 1

    stats = get_normalization_stats(db_path)

    print(f"\n{'='*60}")
    print("NORMALIZATION STATUS")
    print(f"{'='*60}")

    print(f"\nViews created: {len(stats['views_created'])}/3")
    for view in stats['views_created']:
        print(f"  ✓ {view}")

    print(f"\nData coverage:")
    print(f"  Isotherms with unit metadata: {stats['isotherms_with_units']:,}")
    print(f"  Normalized data points: {stats['normalized_data_points']:,}")
    print(f"  Gas-material matrix entries: {stats['matrix_entries']:,}")

    if stats['pressure_units']:
        print(f"\nPressure units found:")
        for unit, count in sorted(stats['pressure_units'].items(), key=lambda x: -x[1]):
            print(f"  {unit}: {count:,} isotherms")

    if stats['adsorption_units']:
        print(f"\nAdsorption units found:")
        for unit, count in sorted(stats['adsorption_units'].items(), key=lambda x: -x[1]):
            print(f"  {unit}: {count:,} isotherms")

    print(f"\n{'='*60}")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="NIST ISODB Adsorbent Materials Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # sync command
    sync_parser = subparsers.add_parser("sync", help="Sync all data from NIST ISODB")
    sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without applying",
    )
    sync_parser.add_argument(
        "--force",
        action="store_true",
        help="Apply changes even if they look suspicious",
    )
    sync_parser.add_argument(
        "--tables",
        type=str,
        help="Comma-separated list of tables to sync (materials,isotherms,gases,bibliography)",
    )
    sync_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show detailed change information",
    )
    sync_parser.set_defaults(func=cmd_sync)

    # status command
    status_parser = subparsers.add_parser("status", help="Show database status")
    status_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show recent audit log",
    )
    status_parser.set_defaults(func=cmd_status)

    # backup command
    backup_parser = subparsers.add_parser("backup", help="Create a manual backup")
    backup_parser.set_defaults(func=cmd_backup)

    # restore command
    restore_parser = subparsers.add_parser("restore", help="Restore from a backup")
    restore_parser.add_argument("backup_file", help="Path to the backup file")
    restore_parser.set_defaults(func=cmd_restore)

    # list-backups command
    list_parser = subparsers.add_parser("list-backups", help="List available backups")
    list_parser.set_defaults(func=cmd_list_backups)

    # sync-data-points command
    sync_dp_parser = subparsers.add_parser(
        "sync-data-points",
        help="Fetch individual isotherm data points"
    )
    sync_dp_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of isotherms to process"
    )
    sync_dp_parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't skip already-fetched isotherms"
    )
    sync_dp_parser.add_argument(
        "--force-refetch",
        action="store_true",
        help="Re-fetch all isotherms even if already fetched"
    )
    sync_dp_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Checkpoint progress every N isotherms (default: 1000)"
    )
    sync_dp_parser.set_defaults(func=cmd_sync_data_points)

    # data-status command
    data_status_parser = subparsers.add_parser(
        "data-status",
        help="Show data point fetch progress"
    )
    data_status_parser.set_defaults(func=cmd_data_status)

    # create-views command
    create_views_parser = subparsers.add_parser(
        "create-views",
        help="Create normalized views and analysis schema"
    )
    create_views_parser.set_defaults(func=cmd_create_views)

    # refresh-matrix command
    refresh_matrix_parser = subparsers.add_parser(
        "refresh-matrix",
        help="Refresh the gas_material_matrix materialized table"
    )
    refresh_matrix_parser.set_defaults(func=cmd_refresh_matrix)

    # normalization-status command
    norm_status_parser = subparsers.add_parser(
        "normalization-status",
        help="Show normalization and view statistics"
    )
    norm_status_parser.set_defaults(func=cmd_normalization_status)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
