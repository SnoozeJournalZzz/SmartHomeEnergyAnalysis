#!/usr/bin/env python3
"""
smartthings.py — CLI tool to load SmartThings device messages into the database.

Usage:
    smartthings.py [OPTIONS] FILE [FILE ...]

    -d DBURL    SQLAlchemy database URL (e.g. sqlite:///myhome.db)
    --info      Show current row count in the database and exit
    --dry-run   Parse files and report row counts without writing to the database
    -v          Verbose output (show per-file statistics)

Examples:
    # Load one file
    python smartthings.py -d sqlite:///myhome.db data/data/smartthings/smartthings.20230107.tsv.gz

    # Load all SmartThings files
    python smartthings.py -d sqlite:///myhome.db data/data/smartthings/smartthings.*.tsv.gz

    # Preview without writing
    python smartthings.py --dry-run data/data/smartthings/smartthings.20230107.tsv.gz
"""

import gzip
import sys
from pathlib import Path

import click
import pandas as pd

from home_messages_db import HomeMessagesDB


def parse_smartthings_file(filepath: str | Path) -> pd.DataFrame:
    """
    Read a single SmartThings TSV file (gzip-compressed) and return a clean
    DataFrame ready for database insertion.

    Key observations about this data source:
    - Tab-separated values (TSV), always gzip-compressed.
    - The ``epoch`` column is already in UTC ISO 8601 format (e.g.
      ``'2022-10-09T20:13:08Z'``), so no timezone conversion is needed.
    - Duplicate rows (identical in all columns) appear within a single file.
    - The same event can also appear across multiple files.

    Parameters
    ----------
    filepath : str or Path

    Returns
    -------
    pd.DataFrame
        Columns: ``loc``, ``level``, ``name``, ``epoch`` (int, UTC seconds),
        ``capability``, ``attribute``, ``value``, ``unit``.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If required columns are missing.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    REQUIRED_COLS = {"loc", "level", "name", "epoch", "capability", "attribute", "value", "unit"}

    opener = gzip.open if filepath.suffix == ".gz" else open
    with opener(filepath, "rt", encoding="utf-8") as f:
        # sep='\t' for TSV; keep_default_na=False so empty 'unit' stays as ''
        df = pd.read_csv(f, sep="\t", keep_default_na=False)

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"{filepath.name}: missing columns: {missing}")

    # --- Drop rows where mandatory fields are empty -------------------------
    df = df[df["name"].str.strip() != ""].copy()
    df = df[df["epoch"].str.strip() != ""].copy()

    # --- Convert ISO 8601 UTC string → integer epoch ------------------------
    # The source format is e.g. '2022-10-09T20:13:08Z' (always UTC, 'Z' suffix).
    # pd.to_datetime with utc=True handles the 'Z' suffix correctly.
    df["epoch"] = (
        pd.to_datetime(df["epoch"], utc=True)
        .dt.tz_localize(None)
        .astype("datetime64[s]")
        .astype("int64")
    )

    # --- Normalise text columns ---------------------------------------------
    # Strip leading/trailing whitespace that sometimes appears in the source.
    for col in ["loc", "level", "name", "capability", "attribute", "value", "unit"]:
        df[col] = df[col].str.strip()

    # --- Drop duplicates within this file -----------------------------------
    # Deduplication key matches the UNIQUE constraint in the database:
    # same device + same moment + same capability + same attribute.
    df.drop_duplicates(
        subset=["name", "epoch", "capability", "attribute"],
        keep="first",
        inplace=True,
    )

    cols = ["loc", "level", "name", "epoch", "capability", "attribute", "value", "unit"]
    return df[cols].reset_index(drop=True)


@click.command()
@click.argument("files", nargs=-1, required=False, type=click.Path())
@click.option("-d", "--db-url", default=None, metavar="DBURL",
              help="SQLAlchemy database URL (e.g. sqlite:///myhome.db).")
@click.option("--info", is_flag=True, default=False,
              help="Show current SmartThings row count in the database and exit.")
@click.option("--dry-run", is_flag=True, default=False,
              help="Parse files and show statistics without writing to the database.")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show per-file statistics.")
def main(files, db_url, info, dry_run, verbose):
    """Load SmartThings device messages into the project database.

    \b
    FILE arguments should be .tsv.gz files.
    Duplicates within and across files are automatically handled.

    \b
    Examples:
      python smartthings.py -d sqlite:///myhome.db data/data/smartthings/smartthings.20230107.tsv.gz
      python smartthings.py --dry-run data/data/smartthings/smartthings.*.tsv.gz
      python smartthings.py --info -d sqlite:///myhome.db
    """
    if info:
        if not db_url:
            raise click.UsageError("--info requires -d DBURL.")
        db = HomeMessagesDB(db_url)
        stats = db.get_stats()
        click.echo(f"smartthings_messages: {stats['smartthings_messages']:,} rows")
        return

    if not files:
        raise click.UsageError("Please provide at least one FILE argument.")

    if not dry_run and not db_url:
        raise click.UsageError(
            "Please provide -d DBURL, or use --dry-run to skip database writing."
        )

    db = HomeMessagesDB(db_url) if (db_url and not dry_run) else None

    total_parsed   = 0
    total_inserted = 0

    for filepath in files:
        try:
            df = parse_smartthings_file(filepath)
            n_parsed = len(df)
            total_parsed += n_parsed

            if dry_run:
                if verbose:
                    click.echo(f"[dry-run] {Path(filepath).name}: {n_parsed:,} rows parsed")
            else:
                records = df.to_dict(orient="records")
                n_inserted = db.insert_smartthings(records)
                total_inserted += n_inserted
                if verbose:
                    click.echo(
                        f"{Path(filepath).name}: "
                        f"{n_parsed:,} parsed, {n_inserted:,} new rows inserted"
                    )

        except (FileNotFoundError, ValueError) as exc:
            click.echo(f"ERROR: {exc}", err=True)
            sys.exit(1)

    if dry_run:
        click.echo(f"[dry-run] Total rows parsed: {total_parsed:,} (nothing written)")
    else:
        click.echo(
            f"Done. Total parsed: {total_parsed:,} | "
            f"New rows inserted: {total_inserted:,} | "
            f"Duplicates skipped: {total_parsed - total_inserted:,}"
        )


if __name__ == "__main__":
    main()
