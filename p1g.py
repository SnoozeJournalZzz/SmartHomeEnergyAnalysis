#!/usr/bin/env python3
"""
p1g.py — CLI tool to load P1g gas data into the SmartHomeEnergyAnalysis database.

Usage:
    p1g.py [OPTIONS] FILE [FILE ...]

    -d DBURL    SQLAlchemy database URL (e.g. sqlite:///myhome.db)
    --info      Show current row count in the database and exit
    --dry-run   Parse files and report row counts without writing to the database
    -v          Verbose output (show per-file statistics)

Examples:
    # Load one file
    python p1g.py -d sqlite:///myhome.db data/data/P1g/P1g-2022-12-01-2022-12-30.csv.gz

    # Load all P1g files
    python p1g.py -d sqlite:///myhome.db data/data/P1g/P1g-*.csv.gz data/data/P1g/P1g-*.csv

    # Preview without writing
    python p1g.py --dry-run data/data/P1g/P1g-2022-12-01-2022-12-30.csv.gz
"""

import gzip
import sys
import warnings
from pathlib import Path

import click
import pandas as pd

from home_messages_db import HomeMessagesDB


def parse_p1g_file(filepath: str | Path) -> pd.DataFrame:
    """
    Read a single P1g file (compressed or plain CSV) and return a clean
    DataFrame ready for database insertion.

    Parameters
    ----------
    filepath : str or Path
        Path to the P1g file.

    Returns
    -------
    pd.DataFrame
        Columns: ``epoch`` (int, UTC), ``total`` (float, m³).

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file has fewer than 2 columns.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    opener = gzip.open if filepath.suffix == ".gz" else open
    with opener(filepath, "rt", encoding="utf-8") as f:
        df_raw = pd.read_csv(f, header=0)

    if df_raw.shape[1] < 2:
        raise ValueError(
            f"{filepath.name}: expected at least 2 columns, got {df_raw.shape[1]}"
        )

    # Use positional access: col 0 = time, col 1 = cumulative gas [m³]
    df = df_raw.iloc[:, [0, 1]].copy()
    df.columns = ["time_str", "total"]

    df.dropna(subset=["time_str", "total"], inplace=True)

    # Convert Amsterdam local time → UTC epoch (same pattern as p1e.py).
    local_times_raw = pd.to_datetime(df["time_str"], format="%Y-%m-%d %H:%M")

    try:
        local_times = local_times_raw.dt.tz_localize(
            "Europe/Amsterdam",
            ambiguous="infer",
            nonexistent="shift_forward",
        )
    except Exception:
        local_times = local_times_raw.dt.tz_localize(
            "Europe/Amsterdam",
            ambiguous="NaT",
            nonexistent="shift_forward",
        )

    nat_mask = local_times.isna()
    if nat_mask.any():
        n_nat = nat_mask.sum()
        warnings.warn(
            f"{filepath.name}: {n_nat} DST-ambiguous timestamp(s) could not be "
            f"inferred and will be dropped. "
            f"Affected: {df.loc[nat_mask, 'time_str'].tolist()[:5]}",
            UserWarning,
            stacklevel=2,
        )
        df = df[~nat_mask].copy()
        local_times = local_times[~nat_mask]

    df["epoch"] = (
        local_times.dt.tz_convert("UTC")
        .dt.tz_localize(None)
        .astype("datetime64[s]")
        .astype("int64")
    )

    # Physical sanity check: cumulative gas counter is always non-negative.
    df["total"] = pd.to_numeric(df["total"], errors="coerce")
    df.dropna(subset=["total"], inplace=True)

    invalid_mask = df["total"] < 0
    if invalid_mask.any():
        warnings.warn(
            f"{filepath.name}: {invalid_mask.sum()} row(s) with negative gas "
            f"readings removed.",
            UserWarning,
            stacklevel=2,
        )
        df = df[~invalid_mask].copy()

    df.drop_duplicates(subset=["epoch"], keep="first", inplace=True)

    return df[["epoch", "total"]].reset_index(drop=True)


@click.command()
@click.argument("files", nargs=-1, required=False, type=click.Path())
@click.option("-d", "--db-url", default=None, metavar="DBURL",
              help="SQLAlchemy database URL (e.g. sqlite:///myhome.db).")
@click.option("--info", is_flag=True, default=False,
              help="Show current gas row count in the database and exit.")
@click.option("--dry-run", is_flag=True, default=False,
              help="Parse files and show statistics without writing to the database.")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show per-file statistics.")
def main(files, db_url, info, dry_run, verbose):
    """Load P1g gas meter data into the project database.

    \b
    FILE arguments can be .csv or .csv.gz files.
    Duplicate rows across files are automatically handled.

    \b
    Examples:
      python p1g.py -d sqlite:///myhome.db data/data/P1g/P1g-2022-12-01-2022-12-30.csv.gz
      python p1g.py --dry-run data/data/P1g/P1g-*.csv.gz
      python p1g.py --info -d sqlite:///myhome.db
    """
    if info:
        if not db_url:
            raise click.UsageError("--info requires -d DBURL.")
        db = HomeMessagesDB(db_url)
        stats = db.get_stats()
        click.echo(f"gas_readings: {stats['gas_readings']:,} rows")
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
            df = parse_p1g_file(filepath)
            n_parsed = len(df)
            total_parsed += n_parsed

            if dry_run:
                if verbose:
                    click.echo(f"[dry-run] {Path(filepath).name}: {n_parsed:,} rows parsed")
            else:
                records = df.to_dict(orient="records")
                n_inserted = db.insert_gas(records)
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
