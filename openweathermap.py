#!/usr/bin/env python3
"""
openweathermap.py — CLI tool to fetch and store weather data from Open-Meteo.

Fetches hourly temperature and relative humidity for Nordwijk, NL from the
Open-Meteo Historical Weather API (https://open-meteo.com/) — free, no API
key required.

Usage:
    openweathermap.py [OPTIONS]

    -d DBURL        SQLAlchemy database URL (e.g. sqlite:///myhome.db)
    --start DATE    Start date in YYYY-MM-DD format (default: 2022-01-01)
    --end DATE      End date in YYYY-MM-DD format (default: today)
    --info          Show current weather row count in the database and exit
    --dry-run       Fetch data and report row counts without writing to the database
    -v              Verbose output

Examples:
    # Fetch the full dataset range and store it
    python openweathermap.py -d sqlite:///myhome.db

    # Fetch a specific period
    python openweathermap.py -d sqlite:///myhome.db --start 2023-01-01 --end 2023-12-31

    # Preview without writing
    python openweathermap.py --dry-run --start 2022-12-01 --end 2022-12-07
"""

import sys
from datetime import date, datetime

import click
import pandas as pd
import requests

from home_messages_db import HomeMessagesDB

# ---------------------------------------------------------------------------
# Constants — Nordwijk, NL (fixed for this project)
# ---------------------------------------------------------------------------

LATITUDE  = 52.2985
LONGITUDE = 4.4552
TIMEZONE  = "Europe/Amsterdam"

API_URL = "https://archive-api.open-meteo.com/v1/archive"

# The smart home dataset spans from 2022-01-01.
DEFAULT_START = "2022-01-01"


# ---------------------------------------------------------------------------
# Core fetch + parse logic (separated from CLI for testability)
# ---------------------------------------------------------------------------

def fetch_weather(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch hourly weather data from Open-Meteo for Nordwijk, NL.

    Parameters
    ----------
    start_date : str
        Start date in ``YYYY-MM-DD`` format.
    end_date : str
        End date in ``YYYY-MM-DD`` format (inclusive).

    Returns
    -------
    pd.DataFrame
        Columns: ``epoch`` (int, UTC seconds), ``temperature`` (float, °C),
        ``humidity`` (float, %).

    Raises
    ------
    ValueError
        If start_date > end_date or the API returns an error.
    requests.RequestException
        If the network request fails.

    Notes
    -----
    **Timezone handling:**
    We request ``timezone="UTC"`` from the Open-Meteo API, so the returned
    time strings are already in UTC (e.g. ``'2022-12-01T00:00'``, no offset
    suffix).  ``pd.to_datetime(..., utc=True)`` attaches the UTC timezone to
    these naive-UTC strings directly — no ``tz_localize`` step is needed.
    DST handling is irrelevant here because we never work with Amsterdam local
    time in this function.
    """
    # Validate date order before making a network call
    if start_date > end_date:
        raise ValueError(
            f"start_date ({start_date}) must be <= end_date ({end_date})."
        )

    params = {
        "latitude":  LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date,
        "end_date":   end_date,
        "hourly":    "temperature_2m,relative_humidity_2m",
        # Request UTC so timestamps are unambiguous — no DST transitions
        # to worry about. The 'timezone' parameter only affects how the
        # API labels its output times; all epochs we store are UTC anyway.
        "timezone":  "UTC",
    }

    resp = requests.get(API_URL, params=params, timeout=60)
    resp.raise_for_status()   # raises HTTPError for 4xx / 5xx responses
    data = resp.json()

    # Check for API-level error message (Open-Meteo returns 200 with an
    # "error" key when the request is invalid)
    if "error" in data:
        raise ValueError(f"Open-Meteo API error: {data['error']}")

    hourly = data["hourly"]
    times       = hourly["time"]                  # UTC, no tz suffix
    temperatures = hourly["temperature_2m"]        # float or None
    humidities   = hourly["relative_humidity_2m"]  # int or None

    df = pd.DataFrame({
        "time_str":    times,
        "temperature": temperatures,
        "humidity":    humidities,
    })

    # Drop rows where the API returned null values
    df.dropna(subset=["time_str"], inplace=True)

    # --- Convert UTC time string → epoch ------------------------------------
    # We request timezone=UTC from the API, so times are already UTC with
    # no DST ambiguity. pd.to_datetime with utc=True handles the 'Z' suffix
    # and plain ISO strings equally well.
    df["epoch"] = (
        pd.to_datetime(df["time_str"], utc=True)
        .dt.tz_localize(None)
        .astype("datetime64[s]")
        .astype("int64")
    )

    df.drop_duplicates(subset=["epoch"], keep="first", inplace=True)

    return df[["epoch", "temperature", "humidity"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# CLI definition
# ---------------------------------------------------------------------------

@click.command()
@click.option("-d", "--db-url", default=None, metavar="DBURL",
              help="SQLAlchemy database URL (e.g. sqlite:///myhome.db).")
@click.option("--start", "start_date",
              default=DEFAULT_START,
              metavar="DATE",
              show_default=True,
              help="Start date (YYYY-MM-DD).")
@click.option("--end", "end_date",
              default=None,
              metavar="DATE",
              help="End date (YYYY-MM-DD). Defaults to today.")
@click.option("--info", is_flag=True, default=False,
              help="Show current weather row count in the database and exit.")
@click.option("--dry-run", is_flag=True, default=False,
              help="Fetch data and show statistics without writing to the database.")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show detailed statistics.")
def main(db_url, start_date, end_date, info, dry_run, verbose):
    """Fetch hourly weather data for Nordwijk, NL and store it in the database.

    \b
    Data source: Open-Meteo Historical Weather API (free, no API key needed).
    Variables fetched: temperature [°C] and relative humidity [%].
    Resolution: 1 hour.

    \b
    Examples:
      python openweathermap.py -d sqlite:///myhome.db
      python openweathermap.py -d sqlite:///myhome.db --start 2023-01-01 --end 2023-12-31
      python openweathermap.py --dry-run
    """
    # --- --info flag --------------------------------------------------------
    if info:
        if not db_url:
            raise click.UsageError("--info requires -d DBURL.")
        db = HomeMessagesDB(db_url)
        stats = db.get_stats()
        click.echo(f"weather_readings: {stats['weather_readings']:,} rows")
        return

    # --- Defaults -----------------------------------------------------------
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    # --- Validate date format -----------------------------------------------
    for label, val in [("--start", start_date), ("--end", end_date)]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            raise click.BadParameter(
                f"'{val}' is not a valid date. Use YYYY-MM-DD format.",
                param_hint=label,
            )

    # --- Validate write mode ------------------------------------------------
    if not dry_run and not db_url:
        raise click.UsageError(
            "Please provide -d DBURL, or use --dry-run to skip database writing."
        )

    # --- Fetch --------------------------------------------------------------
    if verbose:
        click.echo(
            f"Fetching weather data: {start_date} → {end_date} "
            f"(Nordwijk, NL  {LATITUDE}°N {LONGITUDE}°E)"
        )

    try:
        df = fetch_weather(start_date, end_date)
    except (ValueError, requests.RequestException) as exc:
        click.echo(f"ERROR: {exc}", err=True)
        sys.exit(1)

    n_fetched = len(df)

    # --- Store or report ----------------------------------------------------
    if dry_run:
        click.echo(
            f"[dry-run] Fetched {n_fetched:,} hourly records "
            f"({start_date} → {end_date}). Nothing written."
        )
        if verbose:
            click.echo(f"  Temperature range: "
                       f"{df['temperature'].min():.1f} – {df['temperature'].max():.1f} °C")
            click.echo(f"  Humidity range:    "
                       f"{df['humidity'].min():.0f} – {df['humidity'].max():.0f} %")
    else:
        db = HomeMessagesDB(db_url)
        records = df.to_dict(orient="records")
        n_inserted = db.insert_weather(records)
        click.echo(
            f"Done. Fetched: {n_fetched:,} | "
            f"New rows inserted: {n_inserted:,} | "
            f"Duplicates skipped: {n_fetched - n_inserted:,}"
        )
        if verbose:
            click.echo(f"  Temperature range: "
                       f"{df['temperature'].min():.1f} – {df['temperature'].max():.1f} °C")
            click.echo(f"  Humidity range:    "
                       f"{df['humidity'].min():.0f} – {df['humidity'].max():.0f} %")


if __name__ == "__main__":
    main()
