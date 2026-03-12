#!/usr/bin/env python3
"""
benchmark.db → Supabase PostgreSQL data insertion
Inserts all rows from SQLite into Supabase via the REST API (supabase-py).

Prerequisites:
  1. Run migration.sql in Supabase SQL Editor first (creates the tables).
  2. pip install supabase

Usage:
  python3 insert_data.py

Tables inserted in dependency order:
  entities → assets → entity_asset_stakes → asset_metrics → portfolio_metrics → ingestion_log
"""

import os
import sqlite3
import sys
from datetime import datetime

try:
    from supabase import create_client, Client
except ImportError:
    print("ERROR: supabase not installed. Run: pip3 install supabase")
    sys.exit(1)

# ── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL  = "https://mjyssladxasvjssuixfz.supabase.co"
SERVICE_KEY   = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1qeXNzbGFkeGFzdmpzc3VpeGZ6Iiwicm9sZSI6"
    "InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjA2MTM2NiwiZXhwIjoyMDg3NjM3MzY2fQ"
    ".u-NLH_Ntev2M4cPIVRKRxJpbJj1O7tqTB0FID5mop1E"
)
SQLITE_PATH   = os.path.expanduser("~/data/benchmark/benchmark.db")
BATCH_SIZE    = 500

# Insertion order respects foreign-key dependencies
TABLE_ORDER = [
    "entities",
    "assets",
    "entity_asset_stakes",
    "asset_metrics",
    "portfolio_metrics",
    "ingestion_log",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def sqlite_rows_as_dicts(conn: sqlite3.Connection, table: str) -> list[dict]:
    """Return all rows from a SQLite table as a list of plain dicts."""
    conn.row_factory = sqlite3.Row
    cur = conn.execute(f'SELECT * FROM "{table}"')
    rows = [dict(r) for r in cur.fetchall()]
    # Replace None with None (supabase-py handles it as JSON null)
    return rows


def clean_row(row: dict) -> dict:
    """
    Coerce types that PostgREST may reject:
      - SQLite stores timestamps as TEXT; keep as-is (PostgreSQL accepts ISO strings).
      - NaN floats become None (JSON null).
    """
    import math
    cleaned = {}
    for k, v in row.items():
        if isinstance(v, float) and math.isnan(v):
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned


def insert_table(supabase: Client, table: str, rows: list[dict]) -> int:
    """
    Upsert rows in batches. Returns total rows inserted/updated.
    Uses upsert so the script is idempotent (safe to re-run).
    """
    if not rows:
        print(f"  {table}: 0 rows — skipping")
        return 0

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = [clean_row(r) for r in rows[i : i + BATCH_SIZE]]
        result = (
            supabase.table(table)
            .upsert(batch, on_conflict=get_pk_columns(table))
            .execute()
        )
        total += len(batch)
        print(f"  {table}: inserted batch {i//BATCH_SIZE + 1} "
              f"({i+1}–{min(i+BATCH_SIZE, len(rows))} / {len(rows)})")

    return total


def get_pk_columns(table: str) -> str:
    """Return comma-separated PK columns for upsert conflict resolution."""
    pk_map = {
        "entities":             "id",
        "assets":               "id",
        "entity_asset_stakes":  "entity_id,asset_id,quarter",
        "asset_metrics":        "asset_id,entity_id,quarter,period_type",
        "portfolio_metrics":    "entity_id,quarter,period_type",
        "ingestion_log":        "id",
    }
    return pk_map.get(table, "id")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"=== benchmark.db → Supabase migration ===")
    print(f"Started: {datetime.now().isoformat()}\n")

    # Connect to SQLite
    print(f"Opening SQLite: {SQLITE_PATH}")
    try:
        sqlite_conn = sqlite3.connect(SQLITE_PATH)
    except Exception as e:
        print(f"ERROR opening SQLite: {e}")
        sys.exit(1)

    # Connect to Supabase
    print(f"Connecting to Supabase: {SUPABASE_URL}\n")
    supabase: Client = create_client(SUPABASE_URL, SERVICE_KEY)

    grand_total = 0
    for table in TABLE_ORDER:
        print(f"Reading '{table}' from SQLite...")
        try:
            rows = sqlite_rows_as_dicts(sqlite_conn, table)
            print(f"  {len(rows)} rows read from SQLite")
            inserted = insert_table(supabase, table, rows)
            grand_total += inserted
        except Exception as e:
            print(f"  ERROR on table '{table}': {e}")
            # Continue with remaining tables instead of aborting
            continue

    sqlite_conn.close()
    print(f"\nDone. Total rows upserted: {grand_total}")
    print(f"Finished: {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
