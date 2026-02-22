"""
deploy.py — Databricks DDL Deployment Script
=============================================
Deploys the Pharma Quality Unified Data Model DDL to a Databricks workspace.

Usage:
    python deploy/deploy.py                  # deploy all layers
    python deploy/deploy.py --layer l2_2     # deploy L2.2 only
    python deploy/deploy.py --layer l3       # deploy L3 only
    python deploy/deploy.py --dry-run        # print statements, do not execute
    python deploy/deploy.py --table dim_specification  # single table only

Prerequisites:
    pip install databricks-sql-connector python-dotenv

Configuration (create a .env file in the project root — see .env.example):
    DATABRICKS_HOST       Workspace URL  (e.g. https://dbc-xxx.cloud.databricks.com)
    DATABRICKS_TOKEN      Personal Access Token (dapi...)
    DATABRICKS_HTTP_PATH  SQL Warehouse HTTP path (/sql/1.0/warehouses/...)
    DATABRICKS_CATALOG    Target Unity Catalog name (default: pharma_quality)
"""

import os
import sys
import argparse
import time
from pathlib import Path
from dotenv import dotenv_values

# ---------------------------------------------------------------------------
# Load environment configuration — always read directly from .env file
# (avoids stale tokens set in the system/shell environment)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
_cfg = dotenv_values(PROJECT_ROOT / ".env")

DATABRICKS_HOST      = _cfg.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN     = _cfg.get("DATABRICKS_TOKEN", "")
DATABRICKS_HTTP_PATH = _cfg.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_CATALOG   = _cfg.get("DATABRICKS_CATALOG", "pharma_quality")

# ---------------------------------------------------------------------------
# DDL execution order
# Dependencies flow: reference dims → MDM dims → spec dims → fact → denorm → OBT
# ---------------------------------------------------------------------------
DDL_EXECUTION_ORDER = [
    # ── L1 Schema bootstrap ────────────────────────────────────────────────
    ("bootstrap", "CREATE_SCHEMA_L1",     None),   # inline, not a file

    # ── L1 Raw landing tables (LIMS — append-only, source-faithful) ───────
    ("l1",  "raw_lims_specification",     "ddl/l1_raw/raw_lims_specification.sql"),
    ("l1",  "raw_lims_spec_item",         "ddl/l1_raw/raw_lims_spec_item.sql"),
    ("l1",  "raw_lims_spec_limit",        "ddl/l1_raw/raw_lims_spec_limit.sql"),

    # ── L2.1 Schema bootstrap ──────────────────────────────────────────────
    ("bootstrap", "CREATE_SCHEMA_L2_1",   None),   # inline, not a file

    # ── L2.1 Source conform tables (LIMS cleansed / typed) ────────────────
    ("l2_1", "src_lims_specification",    "ddl/l2_1_source_conform/lims/src_lims_specification.sql"),
    ("l2_1", "src_lims_spec_item",        "ddl/l2_1_source_conform/lims/src_lims_spec_item.sql"),
    ("l2_1", "src_lims_spec_limit",       "ddl/l2_1_source_conform/lims/src_lims_spec_limit.sql"),

    # ── L2.2 Schema bootstrap ──────────────────────────────────────────────
    ("bootstrap", "CREATE_SCHEMA_L2_2",   None),   # inline, not a file
    ("bootstrap", "CREATE_SCHEMA_L3",     None),   # inline, not a file

    # ── L2.2 Reference dimensions (no foreign key dependencies) ───────────
    ("l2_2", "dim_date",                  "ddl/l2_2_unified_model/dimensions/dim_date.sql"),
    ("l2_2", "dim_uom",                   "ddl/l2_2_unified_model/dimensions/dim_uom.sql"),
    ("l2_2", "dim_limit_type",            "ddl/l2_2_unified_model/dimensions/dim_limit_type.sql"),
    ("l2_2", "dim_regulatory_context",    "ddl/l2_2_unified_model/dimensions/dim_regulatory_context.sql"),

    # ── L2.2 MDM dimensions ────────────────────────────────────────────────
    ("l2_2", "dim_product",               "ddl/l2_2_unified_model/dimensions/dim_product.sql"),
    ("l2_2", "dim_material",              "ddl/l2_2_unified_model/dimensions/dim_material.sql"),
    ("l2_2", "dim_test_method",           "ddl/l2_2_unified_model/dimensions/dim_test_method.sql"),
    ("l2_2", "dim_site",                  "ddl/l2_2_unified_model/dimensions/dim_site.sql"),
    ("l2_2", "dim_market",                "ddl/l2_2_unified_model/dimensions/dim_market.sql"),

    # ── L2.2 Core specification dimensions ────────────────────────────────
    ("l2_2", "dim_specification",         "ddl/l2_2_unified_model/dimensions/dim_specification.sql"),
    ("l2_2", "dim_specification_item",    "ddl/l2_2_unified_model/dimensions/dim_specification_item.sql"),

    # ── L2.2 Fact table ────────────────────────────────────────────────────
    ("l2_2", "fact_specification_limit",  "ddl/l2_2_unified_model/facts/fact_specification_limit.sql"),

    # ── L2.2 Denormalized (depends on all dims + fact) ────────────────────
    ("l2_2", "dspec_specification",       "ddl/l2_2_unified_model/denormalized/dspec_specification.sql"),

    # ── L3 Schema bootstrap is handled above ───────────────────────────────
    # ── L3 OBT tables ─────────────────────────────────────────────────────
    ("l3",   "obt_specification_ctd",     "ddl/l3_final/obt_specification_ctd.sql"),
    ("l3",   "obt_acceptance_criteria",   "ddl/l3_final/obt_acceptance_criteria.sql"),
]

# Schema names per layer
L1_SCHEMA   = "l1_raw"
L2_1_SCHEMA = "l2_1_lims"
L2_2_SCHEMA = "l2_2_spec_unified"
L3_SCHEMA   = "l3_spec_products"

# Inline bootstrap statements (run before any DDL files)
BOOTSTRAP_STATEMENTS = {
    "CREATE_SCHEMA_L1": [
        f"CREATE CATALOG IF NOT EXISTS {{catalog}}",
        f"CREATE SCHEMA IF NOT EXISTS {{catalog}}.{L1_SCHEMA} "
        f"COMMENT 'L1 Raw landing zone — immutable source-faithful ingestion'",
    ],
    "CREATE_SCHEMA_L2_1": [
        f"CREATE SCHEMA IF NOT EXISTS {{catalog}}.{L2_1_SCHEMA} "
        f"COMMENT 'L2.1 Source conform layer — LIMS cleansed, typed, and standardised'",
    ],
    "CREATE_SCHEMA_L2_2": [
        f"CREATE SCHEMA IF NOT EXISTS {{catalog}}.{L2_2_SCHEMA} "
        f"COMMENT 'L2.2 Pharma Quality Unified Data Model — Specifications domain'",
    ],
    "CREATE_SCHEMA_L3": [
        f"CREATE SCHEMA IF NOT EXISTS {{catalog}}.{L3_SCHEMA} "
        f"COMMENT 'L3 Final Data Products — Specifications domain (CTD-ready OBTs)'",
    ],
}


# ---------------------------------------------------------------------------
# SQL file parsing
# ---------------------------------------------------------------------------

def _strip_sql_comments(sql: str) -> str:
    """Remove single-line (--) comments from SQL text."""
    lines = []
    for line in sql.splitlines():
        stripped = line.rstrip()
        # Keep lines that are not pure comment lines
        # (inline comments are fine; they don't affect statement splitting)
        lines.append(stripped)
    return "\n".join(lines)


def split_sql_statements(sql_text: str) -> list[str]:
    """
    Split a SQL file into individual executable statements.

    Rules:
    - Split on semicolons (;) at the end of a logical statement
    - Skip blank statements and pure comment blocks
    - Skip population/commented-out queries (lines starting with --)
    - Respect that some DDL files contain both CREATE TABLE and INSERT statements
    """
    # Remove the commented-out population queries (blocks wrapped in --)
    # These are the large INSERT/SELECT blocks at the bottom of DDL files
    # We identify them as comment blocks and skip execution.
    statements = []
    current = []

    for line in sql_text.splitlines():
        stripped = line.strip()

        # Skip pure comment lines
        if stripped.startswith("--"):
            continue

        current.append(line)

        # A semicolon on its own line or at end of a non-comment line = statement boundary
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt and stmt != ";":
                statements.append(stmt)
            current = []

    # Catch any trailing statement without a semicolon
    remainder = "\n".join(current).strip()
    if remainder and not remainder.startswith("--"):
        statements.append(remainder)

    return [s for s in statements if len(s.replace(";", "").strip()) > 0]


def load_ddl_file(file_path: Path, catalog: str) -> list[str]:
    """Read a DDL file and return a list of executable SQL statements."""
    raw = file_path.read_text(encoding="utf-8")
    statements = split_sql_statements(raw)

    # Replace catalog placeholder if the file uses a catalog-qualified name.
    # Our DDL files use schema-only names (no catalog prefix) by design,
    # so we prepend USE CATALOG at the top of each file execution.
    # But we do a quick scan for any {catalog} tokens just in case.
    statements = [s.replace("{catalog}", catalog) for s in statements]
    return statements


# ---------------------------------------------------------------------------
# Deployment engine
# ---------------------------------------------------------------------------

class DatabricksDeployer:
    def __init__(self, host: str, token: str, http_path: str, catalog: str, dry_run: bool = False):
        self.host     = host
        self.token    = token
        self.http_path = http_path
        self.catalog  = catalog
        self.dry_run  = dry_run
        self._conn    = None

    def connect(self):
        if self.dry_run:
            print("  [DRY RUN] Skipping actual connection.")
            return
        try:
            from databricks import sql as dbsql
            self._conn = dbsql.connect(
                server_hostname=self.host.replace("https://", ""),
                http_path=self.http_path,
                access_token=self.token,
            )
            print(f"  Connected to {self.host}")
        except ImportError:
            print("ERROR: databricks-sql-connector not installed.")
            print("       Run: pip install databricks-sql-connector")
            sys.exit(1)
        except Exception as exc:
            print(f"ERROR: Could not connect to Databricks: {exc}")
            sys.exit(1)

    def close(self):
        if self._conn:
            self._conn.close()

    def execute(self, statement: str, context: str = "") -> bool:
        """Execute a single SQL statement. Returns True on success."""
        display_stmt = statement[:120].replace("\n", " ") + ("..." if len(statement) > 120 else "")

        if self.dry_run:
            print(f"  [DRY RUN] {display_stmt}")
            return True

        try:
            with self._conn.cursor() as cur:
                cur.execute(statement)
            return True
        except Exception as exc:
            print(f"\n  ERROR in {context}: {exc}")
            print(f"  Statement: {display_stmt}")
            return False

    def set_catalog(self) -> bool:
        """Switch to the target catalog context."""
        return self.execute(f"USE CATALOG {self.catalog}", "USE CATALOG")

    def set_schema(self, schema: str) -> bool:
        """Switch catalog then schema — Unity Catalog requires them as separate commands."""
        self.execute(f"USE CATALOG {self.catalog}", "USE CATALOG")
        return self.execute(f"USE SCHEMA {schema}", f"USE SCHEMA {schema}")

    def deploy_bootstrap(self, name: str) -> tuple[int, int]:
        """Execute inline bootstrap statements (catalog/schema creation)."""
        ok, fail = 0, 0
        stmts = BOOTSTRAP_STATEMENTS.get(name, [])
        for raw_stmt in stmts:
            stmt = raw_stmt.format(catalog=self.catalog)
            success = self.execute(stmt, name)
            if success:
                ok += 1
            else:
                fail += 1
        return ok, fail

    def deploy_file(self, table_name: str, file_path: Path, schema: str) -> tuple[int, int]:
        """Load, parse, and execute all statements from one DDL file."""
        if not file_path.exists():
            print(f"  WARNING: File not found — {file_path}")
            return 0, 1

        statements = load_ddl_file(file_path, self.catalog)
        ok, fail = 0, 0

        # Set schema context before executing file statements
        self.set_schema(schema)

        for i, stmt in enumerate(statements, 1):
            success = self.execute(stmt, f"{table_name}[{i}]")
            if success:
                ok += 1
            else:
                fail += 1
                # Continue on error (don't abort the whole deployment)

        return ok, fail


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Deploy Pharma Quality Unified Data Model DDL to Databricks"
    )
    parser.add_argument(
        "--layer",
        choices=["l1", "l2_1", "l2_2", "l3", "all"],
        default="all",
        help="Deploy a specific layer (default: all)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Deploy a single table by name (e.g. dim_specification)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print statements without executing them",
    )
    return parser.parse_args()


def validate_config():
    missing = []
    if not DATABRICKS_HOST:
        missing.append("DATABRICKS_HOST")
    if not DATABRICKS_TOKEN:
        missing.append("DATABRICKS_TOKEN")
    if not DATABRICKS_HTTP_PATH:
        missing.append("DATABRICKS_HTTP_PATH")
    if missing:
        print("ERROR: Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nCreate a .env file in the project root based on .env.example")
        sys.exit(1)


def get_schema_for_layer(layer: str) -> str:
    if layer == "l1":
        return L1_SCHEMA
    if layer == "l2_1":
        return L2_1_SCHEMA
    if layer in ("l2_2", "bootstrap"):
        return L2_2_SCHEMA
    return L3_SCHEMA


def main():
    args = parse_args()
    validate_config()

    print("=" * 65)
    print("  Pharma Quality — Databricks DDL Deployment")
    print("=" * 65)
    print(f"  Workspace : {DATABRICKS_HOST}")
    print(f"  Catalog   : {DATABRICKS_CATALOG}")
    print(f"  Layer     : {args.layer}")
    print(f"  Dry run   : {args.dry_run}")
    print("=" * 65)

    deployer = DatabricksDeployer(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
        http_path=DATABRICKS_HTTP_PATH,
        catalog=DATABRICKS_CATALOG,
        dry_run=args.dry_run,
    )
    deployer.connect()

    total_ok, total_fail = 0, 0
    start_time = time.time()

    for layer, table_name, relative_path in DDL_EXECUTION_ORDER:
        # Filter by --layer
        if args.layer != "all" and layer not in (args.layer, "bootstrap"):
            continue

        # Filter by --table
        if args.table and table_name != args.table:
            continue

        print(f"\n  [{layer.upper():10s}] {table_name}")

        if relative_path is None:
            # Bootstrap inline statement
            ok, fail = deployer.deploy_bootstrap(table_name)
        else:
            file_path = PROJECT_ROOT / relative_path
            schema = get_schema_for_layer(layer)
            ok, fail = deployer.deploy_file(table_name, file_path, schema)

        status = "OK" if fail == 0 else f"PARTIAL ({fail} errors)"
        print(f"            -> {ok} statement(s) executed — {status}")
        total_ok   += ok
        total_fail += fail

    deployer.close()

    elapsed = time.time() - start_time
    print("\n" + "=" * 65)
    print(f"  Deployment complete in {elapsed:.1f}s")
    print(f"  Statements OK    : {total_ok}")
    print(f"  Statements FAILED: {total_fail}")
    print("=" * 65)

    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
