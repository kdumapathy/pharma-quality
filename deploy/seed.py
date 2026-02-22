"""
seed.py — Run sample data and validation queries against Databricks
====================================================================
Executes sample_data/seed_e2e_sample.sql:
  - INSERT statements load E2E sample data across all layers
  - SELECT statements run validation queries and print results

Usage:
    python deploy/seed.py                  # seed + validate
    python deploy/seed.py --validate-only  # skip INSERTs, run SELECTs only
    python deploy/seed.py --dry-run        # print statements, do not execute
"""

import sys
import argparse
from pathlib import Path
from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).parent.parent
_cfg         = dotenv_values(PROJECT_ROOT / ".env")

DATABRICKS_HOST      = _cfg.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN     = _cfg.get("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = _cfg.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_CATALOG   = _cfg.get("DATABRICKS_CATALOG", "pharma_quality")

SEED_FILE = PROJECT_ROOT / "sample_data" / "seed_e2e_sample.sql"


# ---------------------------------------------------------------------------
# SQL parsing — identical to deploy.py but also returns SELECT statements
# ---------------------------------------------------------------------------

def split_sql_statements(sql_text: str):
    """Split SQL file into statements; keeps SELECTs (they're not commented out)."""
    statements = []
    current = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt and stmt != ";":
                statements.append(stmt)
            current = []
    remainder = "\n".join(current).strip()
    if remainder and not remainder.startswith("--"):
        statements.append(remainder)
    return [s for s in statements if len(s.replace(";", "").strip()) > 0]


def is_select(stmt: str) -> bool:
    return stmt.lstrip().upper().startswith("SELECT")


def is_use(stmt: str) -> bool:
    upper = stmt.lstrip().upper()
    return upper.startswith("USE CATALOG") or upper.startswith("USE SCHEMA")


# ---------------------------------------------------------------------------
# Pretty-print query results
# ---------------------------------------------------------------------------

def print_results(cursor, stmt_preview: str):
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description] if cursor.description else []

    if not cols:
        print("  (no columns returned)")
        return

    # Compute column widths
    widths = [len(c) for c in cols]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val) if val is not None else "NULL"))

    sep  = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    hdr  = "| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols)) + " |"

    print(f"\n  Query: {stmt_preview[:80]}{'...' if len(stmt_preview) > 80 else ''}")
    print(f"  Rows returned: {len(rows)}")
    print(f"  {sep}")
    print(f"  {hdr}")
    print(f"  {sep}")
    for row in rows:
        line = "| " + " | ".join(
            str(v if v is not None else "NULL").ljust(widths[i])
            for i, v in enumerate(row)
        ) + " |"
        print(f"  {line}")
    print(f"  {sep}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Seed E2E sample data to Databricks")
    p.add_argument("--validate-only", action="store_true",
                   help="Skip INSERT/USE statements, run only SELECT queries")
    p.add_argument("--dry-run", action="store_true",
                   help="Print statements without executing")
    return p.parse_args()


def main():
    args = parse_args()

    print("=" * 65)
    print("  Pharma Quality — E2E Sample Data Seed")
    print("=" * 65)
    print(f"  Catalog      : {DATABRICKS_CATALOG}")
    print(f"  Seed file    : {SEED_FILE.name}")
    print(f"  Validate only: {args.validate_only}")
    print(f"  Dry run      : {args.dry_run}")
    print("=" * 65)

    if not SEED_FILE.exists():
        print(f"ERROR: Seed file not found: {SEED_FILE}")
        sys.exit(1)

    statements = split_sql_statements(SEED_FILE.read_text(encoding="utf-8"))
    print(f"\n  Parsed {len(statements)} SQL statement(s) from seed file.\n")

    if args.dry_run:
        for i, stmt in enumerate(statements, 1):
            kind = "SELECT" if is_select(stmt) else ("USE" if is_use(stmt) else "DML")
            print(f"  [{i:02d}] [{kind}] {stmt[:100].replace(chr(10),' ')}...")
        return

    # Connect
    try:
        from databricks import sql as dbsql
    except ImportError:
        print("ERROR: Run: pip install databricks-sql-connector")
        sys.exit(1)

    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    print(f"  Connected to {DATABRICKS_HOST}\n")

    ok = fail = selects = 0
    with conn.cursor() as cur:
        for i, stmt in enumerate(statements, 1):
            preview = stmt[:80].replace("\n", " ")
            select  = is_select(stmt)
            use_stmt = is_use(stmt)

            if args.validate_only and not select:
                continue  # skip non-SELECT in validate-only mode

            if select:
                print(f"\n{'='*65}")
                print(f"  VALIDATION QUERY #{selects + 1}")
                print(f"{'='*65}")
                try:
                    cur.execute(stmt)
                    print_results(cur, preview)
                    selects += 1
                    ok += 1
                except Exception as exc:
                    print(f"  ERROR: {exc}")
                    fail += 1
            else:
                # DML / USE — execute silently unless it errors
                try:
                    cur.execute(stmt)
                    if not use_stmt:
                        ok += 1
                        print(f"  [OK ] {preview}")
                except Exception as exc:
                    fail += 1
                    print(f"  [ERR] {preview}")
                    print(f"        {exc}")

    conn.close()

    print(f"\n{'='*65}")
    print(f"  Seed complete.")
    print(f"  DML OK     : {ok}")
    print(f"  SELECT runs: {selects}")
    print(f"  Failures   : {fail}")
    print("=" * 65)
    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
