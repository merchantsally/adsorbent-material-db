#!/usr/bin/env python3
"""CLI interface for the NIST ISODB Adsorbent Materials Scraper."""

import argparse
import sys
from pathlib import Path

from src import database
from src.sync import sync, print_changes_summary
from src.utils import backup_database, restore_database, list_backups, format_timestamp


def cmd_sync(args):
    """Execute the sync command."""
    result = sync(
        dry_run=args.dry_run,
        force=args.force,
        skip_isotherm_counts=args.skip_isotherms,
    )

    if args.verbose or args.dry_run:
        print_changes_summary(result.changes, verbose=True)

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

    count = database.get_material_count(db_path)
    last_sync = database.get_last_sync_time(db_path)

    print(f"Database: {db_path}")
    print(f"Materials: {count}")
    print(f"Last sync: {format_timestamp(last_sync)}")

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


def main():
    parser = argparse.ArgumentParser(
        description="NIST ISODB Adsorbent Materials Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # sync command
    sync_parser = subparsers.add_parser("sync", help="Sync materials from NIST ISODB")
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
        "--skip-isotherms",
        action="store_true",
        help="Skip fetching isotherm counts (faster)",
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

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
