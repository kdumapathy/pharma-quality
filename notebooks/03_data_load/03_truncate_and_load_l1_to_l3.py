# Databricks notebook source
"""
Truncate and reload curated layers (L2.1 -> L2.2 -> L3) from existing SQL notebooks.

Usage (Databricks notebook):
- Run as-is to truncate curated targets and execute load SQL in dependency order.
- Optionally set `DRY_RUN = True` to print actions without applying changes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

# COMMAND ----------

CATALOG = "pharma_quality"
SQL_NOTEBOOKS_IN_ORDER = [
    "00_populate_l2_1.sql",
    "00_populate_reference_data.sql",
    "00_populate_l2_2_dims_facts.sql",
    "01_populate_dspec.sql",
    "02_populate_l3.sql",
]

# Set to True to preview only.
DRY_RUN = False

# Curated schemas to truncate prior to full reload.
TARGET_SCHEMAS = [
    "l2_1_scl",
    "l2_2_unified_model",
    "l3_data_product",
]

# COMMAND ----------


def _script_dir() -> Path:
    """Resolve this script directory when __file__ is unavailable (e.g., notebooks)."""
    script_file = globals().get("__file__")
    if script_file:
        return Path(script_file).resolve().parent
    return Path.cwd()


def _workspace_path() -> Path:
    """Best-effort resolve of current notebook folder in Databricks."""
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # type: ignore[name-defined]
        return Path("/Workspace") / Path(notebook_path).parent
    except Exception:
        # Local/CI fallback: relative to repository file location.
        return _script_dir()


def _find_notebook_dir(*, files_in_order: list[str]) -> Path:
    """Resolve a SQL notebook directory that actually contains expected files."""
    candidates: list[Path] = [
        _workspace_path(),
        _script_dir(),
        Path.cwd() / "notebooks" / "03_data_load",
    ]

    # Search upward from current working directory for a repo-like notebooks folder.
    for parent in [Path.cwd(), *Path.cwd().parents]:
        candidates.append(parent / "notebooks" / "03_data_load")

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)

        if all((resolved / file_name).exists() for file_name in files_in_order):
            return resolved

    searched = "\n".join(f" - {path.resolve()}" for path in candidates)
    raise FileNotFoundError(
        "Unable to locate SQL notebook directory containing required files. "
        f"Looked for: {files_in_order}\nSearched:\n{searched}"
    )


def _execute_sql(statements: Iterable[str], *, dry_run: bool = False) -> None:
    for stmt in statements:
        clean_stmt = stmt.strip()
        if not clean_stmt:
            continue
        if dry_run:
            print(f"[DRY RUN] SQL: {clean_stmt[:120]}{'...' if len(clean_stmt) > 120 else ''}")
        else:
            spark.sql(clean_stmt)  # type: ignore[name-defined]


def _split_sql_statements(sql_block: str) -> list[str]:
    """Split a SQL block into individual statements by semicolon delimiters."""
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False

    for char in sql_block:
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current.clear()
            continue

        current.append(char)

    trailing_statement = "".join(current).strip()
    if trailing_statement:
        statements.append(trailing_statement)

    return statements


def _extract_sql_statements(sql_text: str) -> list[str]:
    """
    Parse Databricks-exported SQL notebook text into executable SQL statements.

    Skips:
    - `-- MAGIC` markdown/control lines
    - `-- COMMAND ----------` separators
    - blank lines and plain SQL comments
    """
    statements: list[str] = []
    buffer: list[str] = []

    def flush() -> None:
        joined = "\n".join(buffer).strip()
        if joined:
            statements.extend(_split_sql_statements(joined))
        buffer.clear()

    for raw_line in sql_text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()

        if stripped.startswith("-- COMMAND"):
            flush()
            continue
        if stripped.startswith("-- MAGIC"):
            continue
        if stripped.startswith("--"):
            continue

        buffer.append(line)

    flush()
    return statements


def _list_schema_tables(catalog: str, schema: str) -> list[str]:
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()  # type: ignore[name-defined]
    return [row.tableName for row in rows]


def truncate_curated_targets(*, catalog: str, schemas: list[str], dry_run: bool = False) -> None:
    print(f"Using catalog: {catalog}")
    _execute_sql([f"USE CATALOG {catalog}"], dry_run=dry_run)

    for schema in schemas:
        tables = _list_schema_tables(catalog, schema)
        if not tables:
            print(f"No tables found in {catalog}.{schema}; skipping")
            continue

        print(f"Truncating {len(tables)} tables in {catalog}.{schema}")
        for table in tables:
            sql = f"TRUNCATE TABLE {catalog}.{schema}.{table}"
            if dry_run:
                print(f"[DRY RUN] {sql}")
            else:
                spark.sql(sql)  # type: ignore[name-defined]


def run_sql_notebooks(*, notebook_dir: Path, files_in_order: list[str], dry_run: bool = False) -> None:
    for file_name in files_in_order:
        path = notebook_dir / file_name
        if not path.exists():
            raise FileNotFoundError(f"Expected SQL notebook not found: {path}")

        print(f"Executing: {path}")
        sql_text = path.read_text(encoding="utf-8")
        statements = _extract_sql_statements(sql_text)
        _execute_sql(statements, dry_run=dry_run)


# COMMAND ----------

base_dir = _find_notebook_dir(files_in_order=SQL_NOTEBOOKS_IN_ORDER)
print(f"Resolved SQL notebook directory: {base_dir}")

truncate_curated_targets(catalog=CATALOG, schemas=TARGET_SCHEMAS, dry_run=DRY_RUN)
run_sql_notebooks(notebook_dir=base_dir, files_in_order=SQL_NOTEBOOKS_IN_ORDER, dry_run=DRY_RUN)

print("L2.1 -> L2.2 -> L3 truncate + reload completed.")
