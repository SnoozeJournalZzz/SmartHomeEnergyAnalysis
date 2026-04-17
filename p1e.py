#!/usr/bin/env python3
"""
p1e.py — CLI tool to load P1e electricity data into the SmartHomeEnergyAnalysis database.

Usage:
    p1e.py [OPTIONS] FILE [FILE ...]

    -d DBURL    SQLAlchemy database URL (e.g. sqlite:///myhome.db)
    --info      Show current row count in the database and exit
    --dry-run   Parse files and report row counts without writing to the database
    -v          Verbose output (show per-file statistics)

Examples:
    # Load one file
    python p1e.py -d sqlite:///myhome.db data/data/P1e/P1e-2022-12-01-2022-12-30.csv.gz

    # Load all P1e files (gzipped and plain CSV)
    python p1e.py -d sqlite:///myhome.db data/data/P1e/P1e-*.csv.gz data/data/P1e/P1e-*.csv

    # Preview what would be inserted without touching the database
    python p1e.py --dry-run data/data/P1e/P1e-2022-12-01-2022-12-30.csv.gz
"""

import gzip
import sys
import warnings
from pathlib import Path

import click
import pandas as pd

from home_messages_db import HomeMessagesDB, amsterdam_str_to_epoch


# ---------------------------------------------------------------------------
# Core parsing logic (separated from CLI so it can be tested independently)
# ---------------------------------------------------------------------------

def parse_p1e_file(filepath: str | Path) -> pd.DataFrame:
    """
    Read a single P1e file (compressed or plain CSV) and return a clean
    DataFrame ready for database insertion.

    The function handles:
    - Both ``.csv.gz`` (gzip-compressed) and ``.csv`` (plain) files.
    - Two column-name formats used across different collection periods
      (the format changed over time, but column *positions* are stable).
    - Conversion of Amsterdam local time to UTC epoch integers.
    - Removal of duplicate timestamps within the file.

    Parameters
    ----------
    filepath : str or Path
        Path to the P1e file.

    Returns
    -------
    pd.DataFrame
        Columns: ``epoch`` (int, UTC), ``t1`` (float, kWh), ``t2`` (float, kWh).

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file has fewer than 3 columns.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # --- Read file (auto-detect compression) --------------------------------
    # Why use positional access (iloc)?
    # Column names changed between 2022 and 2024 files:
    #   Old: "Electricity imported T1" / "Electricity imported T2"
    #   New: "Import T1 kWh" / "Import T2 kWh"
    # Using positions (0, 1, 2) makes the parser format-agnostic.

    opener = gzip.open if filepath.suffix == ".gz" else open
    with opener(filepath, "rt", encoding="utf-8") as f:
        df_raw = pd.read_csv(f, header=0)

    if df_raw.shape[1] < 3:
        raise ValueError(
            f"{filepath.name}: expected at least 3 columns, got {df_raw.shape[1]}"
        )

    # Keep only the three columns we need (by position, not name)
    df = df_raw.iloc[:, [0, 1, 2]].copy()
    df.columns = ["time_str", "t1", "t2"]

    # --- Drop rows with missing values --------------------------------------
    # Real-world data sometimes has blank lines or partial rows.
    df.dropna(subset=["time_str", "t1", "t2"], inplace=True)

    # --- Convert time string to UTC epoch -----------------------------------
    # Source timezone: Europe/Amsterdam (both CET=UTC+1 and CEST=UTC+2 occur
    # in this dataset depending on the season).
    # We use pandas tz_localize which handles DST transitions automatically.
    #
    # NOTE: pd.to_datetime is used first for vectorised parsing (much faster
    # than calling amsterdam_str_to_epoch row-by-row).

    local_times_raw = pd.to_datetime(df["time_str"], format="%Y-%m-%d %H:%M")

    # tz_localize: tell pandas these times are Amsterdam local times.
    # First attempt: 'infer' resolves the DST fall-back overlap by checking
    # that the sequence is monotonically increasing.  This works for normal
    # month-spanning files that include full context around the transition.
    # nonexistent='shift_forward': handle the spring-forward gap (02:00–03:00
    # skipped) by moving the non-existent time to the next valid instant.
    try:
        local_times = local_times_raw.dt.tz_localize(
            "Europe/Amsterdam",
            ambiguous="infer",
            nonexistent="shift_forward",
        )
    except Exception:
        # 'infer' failed entirely — most likely because the file starts inside
        # the ambiguous DST window (02:00–03:00 on fall-back day) so pandas
        # has no prior-row context to determine CET vs CEST.
        # Fall back to NaT for every ambiguous timestamp; we handle them below.
        local_times = local_times_raw.dt.tz_localize(
            "Europe/Amsterdam",
            ambiguous="NaT",
            nonexistent="shift_forward",
        )

    # Detect NaT rows produced by the NaT fallback path and drop them with a
    # warning.  Silently accepting a wrong epoch (off by 3600 s) is worse than
    # dropping a handful of ambiguous readings.
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

    # tz_convert: shift to UTC, then extract integer seconds.
    # The tz_localize(None) step is required before astype("datetime64[s]")
    # in pandas 3.0, which forbids direct casting from tz-aware to naive.
    # This approach is robust across pandas 2.x and 3.x.
    df["epoch"] = (
        local_times.dt.tz_convert("UTC")
        .dt.tz_localize(None)
        .astype("datetime64[s]")
        .astype("int64")
    )

    # --- Physical sanity checks on meter readings ---------------------------
    # Coerce non-numeric strings to NaN (e.g. garbled CSV rows), then drop.
    df["t1"] = pd.to_numeric(df["t1"], errors="coerce")
    df["t2"] = pd.to_numeric(df["t2"], errors="coerce")
    df.dropna(subset=["t1", "t2"], inplace=True)

    # Cumulative meter counters are always non-negative.
    invalid_mask = (df["t1"] < 0) | (df["t2"] < 0)
    if invalid_mask.any():
        warnings.warn(
            f"{filepath.name}: {invalid_mask.sum()} row(s) with negative meter "
            f"readings removed.",
            UserWarning,
            stacklevel=2,
        )
        df = df[~invalid_mask].copy()

    # --- Drop duplicate timestamps within this file -------------------------
    # P1e files overlap in time (same reading can appear in multiple files).
    # We keep the first occurrence and drop exact duplicates.
    df.drop_duplicates(subset=["epoch"], keep="first", inplace=True)

    return df[["epoch", "t1", "t2"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# CLI definition (Click)
# ---------------------------------------------------------------------------

@click.command()
@click.argument("files", nargs=-1, required=False, type=click.Path())
@click.option(
    "-d", "--db-url",
    default=None,
    metavar="DBURL",
    help="SQLAlchemy database URL (e.g. sqlite:///myhome.db).",
)
@click.option(
    "--info",
    is_flag=True,
    default=False,
    help="Show current electricity row count in the database and exit.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Parse files and show statistics without writing to the database.",
)
@click.option(
    "-v", "--verbose",
    is_flag=True,
    default=False,
    help="Show per-file statistics.",
)
def main(files, db_url, info, dry_run, verbose):
    """Load P1e electricity meter data into the project database.

    \b
    FILE arguments can be .csv or .csv.gz files.
    Duplicate rows across files are automatically handled.

    \b
    Examples:
      python p1e.py -d sqlite:///myhome.db data/data/P1e/P1e-2022-12-01-2022-12-30.csv.gz
      python p1e.py --dry-run data/data/P1e/P1e-*.csv.gz
      python p1e.py --info -d sqlite:///myhome.db
    """

    # --- --info flag: just show DB stats and exit ---------------------------
    if info:
        if not db_url:
            raise click.UsageError("--info requires -d DBURL.")
        db = HomeMessagesDB(db_url)
        stats = db.get_stats()
        click.echo(f"electricity_readings: {stats['electricity_readings']:,} rows")
        return

    # --- Validate arguments -------------------------------------------------
    if not files:
        raise click.UsageError("Please provide at least one FILE argument.")

    if not dry_run and not db_url:
        raise click.UsageError(
            "Please provide -d DBURL, or use --dry-run to skip database writing."
        )

    # --- Process files ------------------------------------------------------
    db = HomeMessagesDB(db_url) if (db_url and not dry_run) else None

    total_parsed   = 0
    total_inserted = 0

    for filepath in files:
        try:
            df = parse_p1e_file(filepath)
            n_parsed = len(df)
            total_parsed += n_parsed

            if dry_run:
                if verbose:
                    click.echo(f"[dry-run] {Path(filepath).name}: {n_parsed:,} rows parsed")
            else:
                records = df.to_dict(orient="records")
                n_inserted = db.insert_electricity(records)
                total_inserted += n_inserted
                if verbose:
                    click.echo(
                        f"{Path(filepath).name}: "
                        f"{n_parsed:,} parsed, {n_inserted:,} new rows inserted"
                    )

        except (FileNotFoundError, ValueError) as exc:
            click.echo(f"ERROR: {exc}", err=True)
            sys.exit(1)

    # --- Summary ------------------------------------------------------------
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
