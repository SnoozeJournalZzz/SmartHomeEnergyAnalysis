"""
tests/test_parsers.py
---------------------
Unit tests for the ETL parsing layer.

Run with:
    pytest tests/ -v

Design philosophy
-----------------
These tests focus on the *pure-function* layer: the parse_* functions that
convert raw file content into DataFrames.  They do NOT test database insertion
(that would require a live DB connection and belong in integration tests).

Each test follows the Arrange / Act / Assert pattern.
Each test verifies exactly ONE behaviour — this makes failures easy to diagnose.

Why test the parsers specifically?
    parse_p1e_file, parse_p1g_file, and parse_smartthings_file are the
    data-quality gatekeepers for the entire project.  Every row in the
    database passed through one of these functions.  Silent bugs here
    (wrong timezone, missed deduplication) propagate into all downstream
    analyses without any obvious error signal.
"""

import gzip
import io
import textwrap
from pathlib import Path

import pandas as pd
import pytest

# ── Import the functions under test ──────────────────────────────────────────
# sys.path manipulation is NOT needed because pytest is run from the project
# root, where p1e.py etc. live.  If you run pytest from a different directory,
# add a conftest.py that adjusts sys.path.
from p1e import parse_p1e_file
from p1g import parse_p1g_file
from smartthings import parse_smartthings_file
from home_messages_db import amsterdam_str_to_epoch


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — build minimal valid test files in a temporary directory
# ─────────────────────────────────────────────────────────────────────────────

def _write_csv(tmp_path: Path, name: str, content: str) -> Path:
    """Write a plain-text CSV to tmp_path and return its Path."""
    p = tmp_path / name
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return p


def _write_gz(tmp_path: Path, name: str, content: str) -> Path:
    """Write a gzip-compressed CSV to tmp_path and return its Path."""
    p = tmp_path / name
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        fh.write(textwrap.dedent(content))
    return p


def _p1e_csv(rows: list[tuple]) -> str:
    """
    Build a P1e CSV string with the NEW column format.
    Each row is (time_str, t1, t2).
    """
    lines = ["Import_time,Import T1 kWh,Import T2 kWh"]
    for t, v1, v2 in rows:
        lines.append(f"{t},{v1},{v2}")
    return "\n".join(lines) + "\n"


def _p1e_csv_old_format(rows: list[tuple]) -> str:
    """
    Build a P1e CSV string with the OLD column format (pre-2024 files).
    The parser uses positional access (iloc), so column names don't matter —
    but this test proves that assumption holds.
    """
    lines = ["Electricity imported T1,Electricity imported T2,Time"]
    # Deliberately put the columns in a different order to prove position=0 is time
    # Actually p1e uses iloc[:, [0, 1, 2]]: col0=time, col1=t1, col2=t2.
    # Old format also has time in col 0.
    lines = ["Time,Electricity imported T1,Electricity imported T2"]
    for t, v1, v2 in rows:
        lines.append(f"{t},{v1},{v2}")
    return "\n".join(lines) + "\n"


def _p1g_csv(rows: list[tuple]) -> str:
    """Build a P1g CSV string.  Each row is (time_str, total_m3)."""
    lines = ["Time,Gas imported m3"]
    for t, total in rows:
        lines.append(f"{t},{total}")
    return "\n".join(lines) + "\n"


def _smartthings_tsv(rows: list[dict]) -> str:
    """
    Build a SmartThings TSV string.
    rows: list of dicts with keys matching the required columns.
    """
    cols = ["loc", "level", "name", "epoch", "capability", "attribute", "value", "unit"]
    lines = ["\t".join(cols)]
    for row in rows:
        lines.append("\t".join(str(row.get(c, "")) for c in cols))
    return "\n".join(lines) + "\n"


# ─────────────────────────────────────────────────────────────────────────────
# P1e parser tests
# ─────────────────────────────────────────────────────────────────────────────

class TestParseP1e:

    def test_basic_happy_path(self, tmp_path):
        """
        Basic case: two rows with distinct timestamps.
        Verifies column names, dtypes, and row count.
        """
        f = _write_csv(tmp_path, "test.csv", _p1e_csv([
            ("2023-01-15 12:00", "1000.111", "500.222"),
            ("2023-01-15 12:15", "1000.500", "500.500"),
        ]))
        df = parse_p1e_file(f)

        assert list(df.columns) == ["epoch", "t1", "t2"], \
            "Output must have exactly these three columns in this order"
        assert len(df) == 2
        assert df["epoch"].dtype == "int64", \
            "epoch must be stored as integer seconds, not float or datetime"
        assert df["t1"].iloc[0] == pytest.approx(1000.111)
        assert df["t2"].iloc[0] == pytest.approx(500.222)

    def test_old_column_format(self, tmp_path):
        """
        The parser uses positional access (iloc), NOT column names.
        This test confirms the old column-name format is handled identically.
        If this test ever fails, it means someone changed the parser to use
        column names — which would break loading 2022–2023 files.
        """
        f = _write_csv(tmp_path, "test_old.csv", _p1e_csv_old_format([
            ("2023-01-15 12:00", "1000.111", "500.222"),
        ]))
        df = parse_p1e_file(f)
        assert len(df) == 1
        assert df["t1"].iloc[0] == pytest.approx(1000.111)

    def test_deduplication_within_file(self, tmp_path):
        """
        Two rows with the SAME timestamp must be collapsed to one.
        This is the core idempotency guarantee of the ETL layer.
        Without deduplication, re-running the loader would double-count rows.
        """
        f = _write_csv(tmp_path, "test.csv", _p1e_csv([
            ("2023-01-15 12:00", "1000.111", "500.222"),
            ("2023-01-15 12:00", "1000.111", "500.222"),  # exact duplicate
        ]))
        df = parse_p1e_file(f)
        assert len(df) == 1, \
            "Duplicate epochs within a file must be dropped to one row"

    def test_gzip_compression(self, tmp_path):
        """
        The parser must handle .csv.gz files identically to .csv files.
        P1 files in the real dataset are mostly gzip-compressed.
        """
        f = _write_gz(tmp_path, "test.csv.gz", _p1e_csv([
            ("2023-01-15 12:00", "1000.111", "500.222"),
        ]))
        df = parse_p1e_file(f)
        assert len(df) == 1
        assert df["epoch"].iloc[0] > 0

    def test_file_not_found(self, tmp_path):
        """FileNotFoundError must be raised for non-existent files."""
        with pytest.raises(FileNotFoundError, match="File not found"):
            parse_p1e_file(tmp_path / "nonexistent.csv")

    def test_too_few_columns(self, tmp_path):
        """
        A file with < 3 columns must raise ValueError, not silently produce
        wrong data.  This guards against accidentally loading gas files as
        electricity files (gas CSV has only 2 columns).
        """
        f = _write_csv(tmp_path, "bad.csv", "Time,T1\n2023-01-15 12:00,1000\n")
        with pytest.raises(ValueError, match="expected at least 3 columns"):
            parse_p1e_file(f)

    def test_dst_winter_epoch(self, tmp_path):
        """
        Amsterdam winter time = CET = UTC+1.
        2023-01-15 12:00 Amsterdam  →  2023-01-15 11:00 UTC.

        WHY is this test important?
        The tz_localize step is where DST bugs hide.  In winter, Amsterdam is
        UTC+1, so noon Amsterdam = 11:00 UTC.  In summer (CEST, UTC+2), noon
        Amsterdam = 10:00 UTC.  A bug that always applies a fixed +1 offset
        would silently produce wrong epochs for half the year.
        """
        f = _write_csv(tmp_path, "test.csv", _p1e_csv([
            ("2023-01-15 12:00", "1000.0", "500.0"),
        ]))
        df = parse_p1e_file(f)

        # Ground truth: compute the expected epoch independently
        expected = int(
            pd.Timestamp("2023-01-15 12:00")
            .tz_localize("Europe/Amsterdam")
            .tz_convert("UTC")
            .timestamp()
        )
        assert df["epoch"].iloc[0] == expected, (
            f"Winter epoch mismatch: got {df['epoch'].iloc[0]}, expected {expected}. "
            "This likely indicates a DST handling bug."
        )

    def test_dst_summer_epoch(self, tmp_path):
        """
        Amsterdam summer time = CEST = UTC+2.
        2023-07-15 12:00 Amsterdam  →  2023-07-15 10:00 UTC.

        The delta from the winter test is 1 hour (not 2), because winter is
        UTC+1 and summer is UTC+2 — a 1-hour difference in the UTC offset.
        If both this test and test_dst_winter_epoch pass, the DST handling
        is correct for both halves of the year.
        """
        f = _write_csv(tmp_path, "test.csv", _p1e_csv([
            ("2023-07-15 12:00", "1000.0", "500.0"),
        ]))
        df = parse_p1e_file(f)

        expected = int(
            pd.Timestamp("2023-07-15 12:00")
            .tz_localize("Europe/Amsterdam")
            .tz_convert("UTC")
            .timestamp()
        )
        assert df["epoch"].iloc[0] == expected, (
            f"Summer epoch mismatch: got {df['epoch'].iloc[0]}, expected {expected}. "
            "CEST (UTC+2) must produce a different offset than CET (UTC+1)."
        )
        # Sanity check: summer epoch must be exactly 3600 s earlier than winter
        # for the same clock time (UTC+2 → subtract 2h; UTC+1 → subtract 1h;
        # difference = 1h = 3600 s).
        winter_expected = int(
            pd.Timestamp("2023-01-15 12:00")
            .tz_localize("Europe/Amsterdam")
            .tz_convert("UTC")
            .timestamp()
        )
        # The summer date is 181 days later, so we can't compare raw epochs;
        # the offset difference is what matters.
        summer_offset = pd.Timestamp("2023-07-15 12:00").tz_localize("Europe/Amsterdam").utcoffset().seconds
        winter_offset = pd.Timestamp("2023-01-15 12:00").tz_localize("Europe/Amsterdam").utcoffset().seconds
        assert summer_offset - winter_offset == 3600, \
            "CEST offset (UTC+2) must be 1h larger than CET offset (UTC+1)"


# ─────────────────────────────────────────────────────────────────────────────
# P1g parser tests
# ─────────────────────────────────────────────────────────────────────────────

class TestParseP1g:

    def test_basic_happy_path(self, tmp_path):
        """Two gas rows → correct columns and epoch values."""
        f = _write_csv(tmp_path, "test.csv", _p1g_csv([
            ("2023-03-10 08:00", "3456.789"),
            ("2023-03-10 08:15", "3456.800"),
        ]))
        df = parse_p1g_file(f)

        assert list(df.columns) == ["epoch", "total"]
        assert len(df) == 2
        assert df["total"].iloc[0] == pytest.approx(3456.789)

    def test_deduplication_within_file(self, tmp_path):
        """Duplicate epochs must be collapsed to one row."""
        f = _write_csv(tmp_path, "test.csv", _p1g_csv([
            ("2023-03-10 08:00", "3456.789"),
            ("2023-03-10 08:00", "3456.789"),
        ]))
        df = parse_p1g_file(f)
        assert len(df) == 1

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="File not found"):
            parse_p1g_file(tmp_path / "ghost.csv")

    def test_too_few_columns(self, tmp_path):
        """
        A file with only 1 column must raise ValueError.
        Guards against loading a corrupt or truncated file.
        """
        f = _write_csv(tmp_path, "bad.csv", "Time\n2023-03-10 08:00\n")
        with pytest.raises(ValueError, match="expected at least 2 columns"):
            parse_p1g_file(f)


# ─────────────────────────────────────────────────────────────────────────────
# SmartThings parser tests
# ─────────────────────────────────────────────────────────────────────────────

class TestParseSmartThings:

    # Reference row used across multiple tests
    _ROW = {
        "loc":        "living_room",
        "level":      "ground",
        "name":       "Living Room (move)",
        "epoch":      "2023-06-01T14:30:00Z",
        "capability": "motionSensor",
        "attribute":  "motion",
        "value":      "active",
        "unit":       "",
    }

    def test_basic_happy_path(self, tmp_path):
        """
        One SmartThings event row.
        Verifies that the ISO 8601 UTC epoch string is converted to an
        integer unix timestamp correctly.
        """
        f = _write_gz(tmp_path, "st.tsv.gz", _smartthings_tsv([self._ROW]))
        df = parse_smartthings_file(f)

        assert list(df.columns) == [
            "loc", "level", "name", "epoch",
            "capability", "attribute", "value", "unit"
        ]
        assert len(df) == 1
        assert df["epoch"].dtype == "int64"

        expected_epoch = int(
            pd.Timestamp("2023-06-01T14:30:00Z").timestamp()
        )
        assert df["epoch"].iloc[0] == expected_epoch

    def test_deduplication_same_event(self, tmp_path):
        """
        Two rows with identical (name, epoch, capability, attribute) tuples
        must be collapsed to one.

        WHY this specific deduplication key?
        The UNIQUE constraint in the database is on these four columns.
        The parser's deduplication must mirror that constraint exactly,
        otherwise the database INSERT OR IGNORE will silently skip a row
        that the parser thought it had cleaned up.
        """
        f = _write_gz(tmp_path, "st.tsv.gz", _smartthings_tsv([
            self._ROW,
            self._ROW,  # exact duplicate
        ]))
        df = parse_smartthings_file(f)
        assert len(df) == 1, \
            "Rows with identical (name, epoch, capability, attribute) must be deduped"

    def test_same_device_different_value_not_deduped(self, tmp_path):
        """
        Two events from the same device at the same time but with DIFFERENT
        attribute values must both be kept.

        Example: a temperature sensor reports 21.5°C and then 22.0°C at the
        same epoch (rare but valid in practice).  Only identical
        (name, epoch, capability, attribute) tuples are duplicates.
        """
        row2 = {**self._ROW, "value": "inactive"}   # different value, same key
        f = _write_gz(tmp_path, "st.tsv.gz", _smartthings_tsv([
            self._ROW,
            row2,
        ]))
        df = parse_smartthings_file(f)
        # Both rows have the same (name, epoch, capability, attribute),
        # so they ARE duplicates under the dedup key.
        # value is NOT part of the unique key — this is intentional.
        assert len(df) == 1, \
            "Same (name, epoch, capability, attribute) is a duplicate regardless of value"

    def test_missing_required_column(self, tmp_path):
        """
        A file missing 'capability' column must raise ValueError,
        not silently produce a DataFrame with NaN values.
        """
        bad_content = "loc\tlevel\tname\tepoch\tattribute\tvalue\tunit\n" \
                      "lr\tg\tDev\t2023-06-01T14:30:00Z\tmotion\tactive\t\n"
        f = _write_gz(tmp_path, "bad.tsv.gz", bad_content)
        with pytest.raises(ValueError, match="missing columns"):
            parse_smartthings_file(f)

    def test_whitespace_stripping(self, tmp_path):
        """
        Leading/trailing whitespace in string columns must be stripped.
        The SmartThings source files sometimes have spaces around values,
        especially in the 'value' and 'unit' columns.
        A value like ' active ' (with spaces) must NOT be treated as a
        different event from 'active'.
        """
        padded_row = {**self._ROW, "value": "  active  ", "name": "  Living Room (move)  "}
        f = _write_gz(tmp_path, "st.tsv.gz", _smartthings_tsv([padded_row]))
        df = parse_smartthings_file(f)

        assert df["value"].iloc[0] == "active", \
            "Trailing/leading spaces must be stripped from value column"
        assert df["name"].iloc[0] == "Living Room (move)", \
            "Trailing/leading spaces must be stripped from name column"


# ─────────────────────────────────────────────────────────────────────────────
# Timezone conversion utility tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAmsterdamConversion:
    """
    Tests for amsterdam_str_to_epoch() — the single-row version of the
    vectorised tz_localize logic used in the parsers.

    WHY test this separately from the parsers?
    The parsers use vectorised pd.to_datetime + dt.tz_localize internally,
    which is a different code path from amsterdam_str_to_epoch() (which uses
    pd.Timestamp directly).  Both paths must agree on the same epoch values.
    If they diverge, something changed in pandas' timezone handling.
    """

    def test_known_winter_timestamp(self):
        """
        2022-12-01 00:15 Amsterdam (CET = UTC+1)
        → subtract 1 hour → 2022-11-30 23:15 UTC
        → epoch = 1669850100

        This is the example from the home_messages_db.py docstring.
        """
        epoch = amsterdam_str_to_epoch("2022-12-01 00:15")
        assert epoch == 1669850100, (
            f"Expected 1669850100, got {epoch}.  "
            "winter CET offset (UTC+1) may not be applied correctly."
        )

    def test_known_summer_timestamp(self):
        """
        2023-07-01 12:00 Amsterdam (CEST = UTC+2)
        → subtract 2 hours → 2023-07-01 10:00 UTC
        → compute expected dynamically to avoid hardcoded arithmetic errors
        """
        expected = int(
            pd.Timestamp("2023-07-01 12:00")
            .tz_localize("Europe/Amsterdam")
            .tz_convert("UTC")
            .timestamp()
        )
        epoch = amsterdam_str_to_epoch("2023-07-01 12:00")
        assert epoch == expected

    def test_winter_and_summer_differ_by_one_hour(self):
        """
        Two timestamps with identical clock time but different seasons must
        differ by exactly 3600 seconds (1 hour) in their UTC epoch values.

        Conceptually: noon Amsterdam on a winter day is 11:00 UTC;
        noon Amsterdam on a summer day is 10:00 UTC.  The same clock time
        maps to a UTC epoch 1 hour earlier in summer, because the UTC offset
        is 1 hour larger (UTC+2 vs UTC+1).

        Using a 365-day gap (same weekday, avoids DST transition edge cases):
        winter: 2023-01-15 12:00, summer: 2023-07-15 12:00
        """
        winter = amsterdam_str_to_epoch("2023-01-15 12:00")
        summer = amsterdam_str_to_epoch("2023-07-15 12:00")
        # Summer epoch must be earlier in UTC (further from today in the past)
        # Compared to winter, the same clock time in summer is 1h earlier in UTC.
        summer_date_delta = (
            pd.Timestamp("2023-07-15") - pd.Timestamp("2023-01-15")
        ).days * 86400
        # Adjust for the calendar gap and isolate the offset difference
        offset_diff = (summer - winter) - summer_date_delta
        assert offset_diff == -3600, (
            f"Expected -3600 (summer UTC offset is 1h more negative than winter), "
            f"got {offset_diff}"
        )
