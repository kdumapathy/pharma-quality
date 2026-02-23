"""
full_deploy.py — Full Pharma Quality deployment orchestrator
===========================================================
Runs end-to-end deployment after schema/DDL changes:
1) Deploy schemas and table DDLs
2) Seed sample raw data
3) Run L2/L3 data-load SQL notebooks
4) Optionally execute validation SQL

Usage:
    python deploy/full_deploy.py
    python deploy/full_deploy.py --dry-run
    python deploy/full_deploy.py --skip-seed
    python deploy/full_deploy.py --skip-validation
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_cfg = dotenv_values(PROJECT_ROOT / ".env")

DATABRICKS_HOST = _cfg.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = _cfg.get("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = _cfg.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_CATALOG = _cfg.get("DATABRICKS_CATALOG", "pharma_quality")

DATA_LOAD_FILES = [
    PROJECT_ROOT / "notebooks/03_data_load/00_populate_reference_data.sql",
    PROJECT_ROOT / "notebooks/03_data_load/00_populate_l2_1.sql",
    PROJECT_ROOT / "notebooks/03_data_load/00_populate_l2_2_dims_facts.sql",
    PROJECT_ROOT / "notebooks/03_data_load/01_populate_dspec.sql",
    PROJECT_ROOT / "notebooks/03_data_load/02_populate_l3.sql",
]
VALIDATION_FILE = PROJECT_ROOT / "notebooks/04_validation/01_validation_queries.sql"


def split_sql_statements(sql_text: str) -> list[str]:
    """Split Databricks-exported SQL notebook text into executable SQL statements."""
    statements: list[str] = []
    current: list[str] = []

    for line in sql_text.splitlines():
        stripped = line.strip()

        if stripped.startswith("-- COMMAND"):
            continue
        if stripped.startswith("-- MAGIC"):
            continue
        if stripped.startswith("--"):
            continue

        current.append(line)

        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt and stmt != ";":
                statements.append(stmt)
            current = []

    remainder = "\n".join(current).strip()
    if remainder:
        statements.append(remainder)

    return [s for s in statements if s.replace(";", "").strip()]


def replace_catalog(stmt: str, catalog: str) -> str:
    return stmt.replace("USE CATALOG pharma_quality", f"USE CATALOG {catalog}")


def is_select(stmt: str) -> bool:
    return stmt.lstrip().upper().startswith("SELECT")


def run_subprocess(cmd: list[str], *, dry_run: bool) -> None:
    rendered = " ".join(cmd)
    print(f"\n→ {rendered}")
    if dry_run:
        print("  [DRY RUN] skipped")
        return

    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def validate_files(paths: list[Path]) -> None:
    missing = [p for p in paths if not p.exists()]
    if missing:
        print("ERROR: Missing required SQL files:")
        for path in missing:
            print(f"  - {path}")
        sys.exit(1)


def validate_config() -> None:
    missing = []
    if not DATABRICKS_HOST:
        missing.append("DATABRICKS_HOST")
    if not DATABRICKS_TOKEN:
        missing.append("DATABRICKS_TOKEN")
    if not DATABRICKS_HTTP_PATH:
        missing.append("DATABRICKS_HTTP_PATH")

    if missing:
        print("ERROR: Missing Databricks configuration in .env:")
        for key in missing:
            print(f"  - {key}")
        sys.exit(1)


def execute_sql_files(files: list[Path], *, validation_only: bool, dry_run: bool) -> tuple[int, int]:
    statements: list[str] = []
    for path in files:
        sql_text = path.read_text(encoding="utf-8")
        file_stmts = split_sql_statements(sql_text)
        statements.extend(replace_catalog(stmt, DATABRICKS_CATALOG) for stmt in file_stmts)
        print(f"  Parsed {len(file_stmts)} statement(s) from {path.relative_to(PROJECT_ROOT)}")

    if dry_run:
        for i, stmt in enumerate(statements, 1):
            print(f"  [DRY RUN:{i:03d}] {stmt[:120].replace(chr(10), ' ')}")
        return len(statements), 0

    try:
        from databricks import sql as dbsql
    except ImportError:
        print("ERROR: databricks-sql-connector not installed. Run: pip install databricks-sql-connector")
        sys.exit(1)

    ok, fail = 0, 0
    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )

    with conn.cursor() as cur:
        for stmt in statements:
            if validation_only and not is_select(stmt):
                continue
            try:
                cur.execute(stmt)
                if is_select(stmt):
                    _ = cur.fetchall()
                ok += 1
            except Exception as exc:
                print(f"  [ERR] {stmt[:100].replace(chr(10), ' ')}")
                print(f"        {exc}")
                fail += 1

    conn.close()
    return ok, fail


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full Pharma Quality deployment")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    parser.add_argument("--skip-seed", action="store_true", help="Skip sample data seed")
    parser.add_argument("--skip-validation", action="store_true", help="Skip validation SQL execution")
    parser.add_argument(
        "--validation-only",
        action="store_true",
        help="Run only validation SELECT statements (skips deployment and loading)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    sql_files = list(DATA_LOAD_FILES)
    if not args.skip_validation:
        sql_files.append(VALIDATION_FILE)
    validate_files(sql_files)

    print("=" * 70)
    print("  Pharma Quality — Full Deployment Orchestrator")
    print("=" * 70)
    print(f"  Catalog          : {DATABRICKS_CATALOG}")
    print(f"  Dry run          : {args.dry_run}")
    print(f"  Validation only  : {args.validation_only}")
    print(f"  Skip seed        : {args.skip_seed}")
    print(f"  Skip validation  : {args.skip_validation}")
    print("=" * 70)

    if args.validation_only:
        validate_config()
        files = [VALIDATION_FILE]
        ok, fail = execute_sql_files(files, validation_only=True, dry_run=args.dry_run)
        print(f"\nValidation complete — ok={ok}, failed={fail}")
        sys.exit(0 if fail == 0 else 1)

    deploy_cmd = [sys.executable, "deploy/deploy.py"]
    if args.dry_run:
        deploy_cmd.append("--dry-run")
    run_subprocess(deploy_cmd, dry_run=False)

    if not args.skip_seed:
        seed_cmd = [sys.executable, "deploy/seed.py"]
        if args.dry_run:
            seed_cmd.append("--dry-run")
        run_subprocess(seed_cmd, dry_run=False)

    validate_config()
    load_files = list(DATA_LOAD_FILES)
    if not args.skip_validation:
        load_files.append(VALIDATION_FILE)

    ok, fail = execute_sql_files(load_files, validation_only=False, dry_run=args.dry_run)

    print("\n" + "=" * 70)
    print(f"  Full deployment completed — ok={ok}, failed={fail}")
    print("=" * 70)
    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
