"""
home_messages_db.py
-------------------
Database access layer for the SmartHomeEnergyAnalysis project.

All database interaction MUST go through the HomeMessagesDB class.
No SQL or SQLAlchemy code is allowed outside this module.

Timezone convention
-------------------
All timestamps are stored as UTC Unix epoch integers (seconds since
1970-01-01 00:00:00 UTC).  Raw source data in Europe/Amsterdam timezone
must be converted to UTC *before* calling any insert method.
The conversion helpers at the bottom of this file should be used by
the CLI tools to keep the logic consistent.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import (
    Column, Integer, Float, String, Text,
    UniqueConstraint, create_engine, text,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

Base = declarative_base()


# ---------------------------------------------------------------------------
# ORM Models
# ---------------------------------------------------------------------------

class ElectricityReading(Base):
    """One 15-minute electricity meter reading."""

    __tablename__ = "electricity_readings"

    epoch = Column(Integer, primary_key=True,
                   doc="UTC Unix timestamp (seconds).")
    t1    = Column(Float, nullable=False,
                   doc="Cumulative low-tariff import [kWh].")
    t2    = Column(Float, nullable=False,
                   doc="Cumulative high-tariff import [kWh].")


class GasReading(Base):
    """One 15-minute gas meter reading."""

    __tablename__ = "gas_readings"

    epoch = Column(Integer, primary_key=True,
                   doc="UTC Unix timestamp (seconds).")
    total = Column(Float, nullable=False,
                   doc="Cumulative gas consumption [m³].")


class SmartThingsMessage(Base):
    """One message received from a SmartThings smart-home device."""

    __tablename__ = "smartthings_messages"
    __table_args__ = (
        # Deduplication: same device, same moment, same capability+attribute
        # can appear in multiple source files — keep only one copy.
        UniqueConstraint("name", "epoch", "capability", "attribute",
                         name="uq_smartthings_event"),
    )

    id         = Column(Integer, primary_key=True, autoincrement=True)
    loc        = Column(String(64),  nullable=True,  doc="Physical location (e.g. 'kitchen').")
    level      = Column(String(32),  nullable=True,  doc="House floor (e.g. 'ground').")
    name       = Column(String(128), nullable=False,  doc="Device name given by the owner.")
    epoch      = Column(Integer,     nullable=False,  doc="UTC Unix timestamp (seconds).")
    capability = Column(String(128), nullable=False,  doc="SmartThings capability (e.g. 'switch').")
    attribute  = Column(String(128), nullable=False,  doc="Measured attribute (e.g. 'temperature').")
    value      = Column(Text,        nullable=True,   doc="Reading value as text.")
    unit       = Column(String(32),  nullable=True,   doc="Unit of the reading (e.g. '°C').")


class WeatherReading(Base):
    """One hourly weather observation from Open-Meteo."""

    __tablename__ = "weather_readings"

    epoch       = Column(Integer, primary_key=True,
                         doc="UTC Unix timestamp (seconds).")
    temperature = Column(Float, nullable=True,
                         doc="Air temperature [°C].")
    humidity    = Column(Float, nullable=True,
                         doc="Relative humidity [%].")


# ---------------------------------------------------------------------------
# Database access class
# ---------------------------------------------------------------------------

class HomeMessagesDB:
    """
    Single access point to the SmartHomeEnergyAnalysis SQLite database.

    Parameters
    ----------
    db_url : str
        SQLAlchemy database URL, e.g. ``'sqlite:///myhome.db'``.

    Examples
    --------
    >>> db = HomeMessagesDB('sqlite:///myhome.db')
    >>> db.get_stats()
    """

    def __init__(self, db_url: str) -> None:
        self._engine = create_engine(db_url, echo=False)
        self._Session = sessionmaker(bind=self._engine)
        self.create_tables()

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def create_tables(self) -> None:
        """Create all tables if they do not already exist."""
        Base.metadata.create_all(self._engine)

    # ------------------------------------------------------------------
    # Insert methods
    # ------------------------------------------------------------------

    def insert_electricity(self, records: list[dict]) -> int:
        """
        Insert electricity readings into the database.

        Duplicate epochs (same primary key) are silently ignored so that
        overlapping source files can be loaded repeatedly without errors.

        Parameters
        ----------
        records : list[dict]
            Each dict must have keys: ``epoch`` (int, UTC), ``t1`` (float),
            ``t2`` (float).

        Returns
        -------
        int
            Number of new rows actually inserted.
        """
        if not records:
            return 0
        return self._upsert_ignore(ElectricityReading, records)

    def insert_gas(self, records: list[dict]) -> int:
        """
        Insert gas readings into the database.

        Duplicate epochs are silently ignored.

        Parameters
        ----------
        records : list[dict]
            Each dict must have keys: ``epoch`` (int, UTC), ``total`` (float).

        Returns
        -------
        int
            Number of new rows actually inserted.
        """
        if not records:
            return 0
        return self._upsert_ignore(GasReading, records)

    def insert_smartthings(self, records: list[dict]) -> int:
        """
        Insert SmartThings device messages into the database.

        Duplicates (same name + epoch + capability + attribute) are ignored.

        Parameters
        ----------
        records : list[dict]
            Each dict must have keys: ``name``, ``epoch`` (int, UTC),
            ``capability``, ``attribute``.  Optional keys: ``loc``,
            ``level``, ``value``, ``unit``.

        Returns
        -------
        int
            Number of new rows actually inserted.
        """
        if not records:
            return 0
        return self._upsert_ignore(SmartThingsMessage, records)

    def insert_weather(self, records: list[dict]) -> int:
        """
        Insert weather observations into the database.

        Duplicate epochs are silently ignored.

        Parameters
        ----------
        records : list[dict]
            Each dict must have keys: ``epoch`` (int, UTC).  Optional:
            ``temperature`` (float), ``humidity`` (float).

        Returns
        -------
        int
            Number of new rows actually inserted.
        """
        if not records:
            return 0
        return self._upsert_ignore(WeatherReading, records)

    # ------------------------------------------------------------------
    # Query methods (all return pd.DataFrame)
    # ------------------------------------------------------------------

    def get_electricity(
        self,
        start_epoch: int | None = None,
        end_epoch:   int | None = None,
    ) -> pd.DataFrame:
        """
        Return electricity readings as a DataFrame.

        Parameters
        ----------
        start_epoch, end_epoch : int, optional
            UTC epoch bounds (inclusive).  If omitted, all rows are returned.

        Returns
        -------
        pd.DataFrame
            Columns: ``epoch``, ``t1``, ``t2``.
        """
        query = "SELECT epoch, t1, t2 FROM electricity_readings"
        query, params = self._apply_epoch_filter(query, start_epoch, end_epoch)
        query += " ORDER BY epoch"
        with self._engine.connect() as conn:
            return pd.read_sql_query(text(query), conn, params=params)

    def get_gas(
        self,
        start_epoch: int | None = None,
        end_epoch:   int | None = None,
    ) -> pd.DataFrame:
        """
        Return gas readings as a DataFrame.

        Returns
        -------
        pd.DataFrame
            Columns: ``epoch``, ``total``.
        """
        query = "SELECT epoch, total FROM gas_readings"
        query, params = self._apply_epoch_filter(query, start_epoch, end_epoch)
        query += " ORDER BY epoch"
        with self._engine.connect() as conn:
            return pd.read_sql_query(text(query), conn, params=params)

    def get_smartthings(
        self,
        start_epoch: int | None = None,
        end_epoch:   int | None = None,
        loc:         str | None = None,
        name:        str | None = None,
        capability:  str | None = None,
        attribute:   str | None = None,
        value:       str | None = None,
    ) -> pd.DataFrame:
        """
        Return SmartThings messages as a DataFrame, with optional filters.

        Parameters
        ----------
        start_epoch, end_epoch : int, optional
            UTC epoch bounds (inclusive).
        loc, name, capability, attribute, value : str, optional
            Exact-match filters.  ``value`` filters the ``value`` column
            (e.g. ``value='active'`` for motion events, ``value='open'``
            for contact events).  All filters are combined with AND.

        Returns
        -------
        pd.DataFrame
            Columns: ``id``, ``loc``, ``level``, ``name``, ``epoch``,
            ``capability``, ``attribute``, ``value``, ``unit``.

        Notes
        -----
        Why ``WHERE 1=1``?
            It is a common SQL pattern that lets us unconditionally append
            ``AND <condition>`` clauses without needing to track whether a
            WHERE keyword has already been emitted.  The query planner
            optimises it away at zero cost.

        Examples
        --------
        # All motion-active events for a specific epoch range
        >>> db.get_smartthings(
        ...     capability='motionSensor', attribute='motion', value='active',
        ...     start_epoch=1666000000, end_epoch=1740000000,
        ... )

        # All door-open events (no epoch filter)
        >>> db.get_smartthings(
        ...     capability='contactSensor', name='Door (main)', value='open',
        ... )
        """
        query = (
            "SELECT id, loc, level, name, epoch, capability, attribute, value, unit "
            "FROM smartthings_messages WHERE 1=1"
        )
        params: dict = {}

        query, params = self._apply_epoch_filter(query, start_epoch, end_epoch, params)

        if loc is not None:
            query += " AND loc = :loc"
            params["loc"] = loc
        if name is not None:
            query += " AND name = :name"
            params["name"] = name
        if capability is not None:
            query += " AND capability = :capability"
            params["capability"] = capability
        if attribute is not None:
            query += " AND attribute = :attribute"
            params["attribute"] = attribute
        # WHY add value filter here rather than a separate method:
        # All four notebook queries that previously used db._engine.connect()
        # are filtering the same smartthings_messages table with varying
        # combinations of these exact columns.  Adding one optional parameter
        # is the minimal change that eliminates all four raw-SQL violations
        # without introducing redundant helper methods.
        if value is not None:
            query += " AND value = :value"
            params["value"] = value

        query += " ORDER BY epoch"
        with self._engine.connect() as conn:
            return pd.read_sql_query(text(query), conn, params=params)

    def get_weather(
        self,
        start_epoch: int | None = None,
        end_epoch:   int | None = None,
    ) -> pd.DataFrame:
        """
        Return weather observations as a DataFrame.

        Returns
        -------
        pd.DataFrame
            Columns: ``epoch``, ``temperature``, ``humidity``.
        """
        query = "SELECT epoch, temperature, humidity FROM weather_readings"
        query, params = self._apply_epoch_filter(query, start_epoch, end_epoch)
        query += " ORDER BY epoch"
        with self._engine.connect() as conn:
            return pd.read_sql_query(text(query), conn, params=params)

    def get_devices(self) -> pd.DataFrame:
        """
        Return a summary of all unique devices found in the SmartThings data.

        Returns
        -------
        pd.DataFrame
            Columns: ``name``, ``loc``, ``level``, ``message_count``.
        """
        query = """
            SELECT name, loc, level, COUNT(*) AS message_count
            FROM smartthings_messages
            GROUP BY name, loc, level
            ORDER BY message_count DESC
        """
        with self._engine.connect() as conn:
            return pd.read_sql_query(text(query), conn)

    def get_stats(self) -> dict[str, int]:
        """
        Return row counts for each table.  Useful for CLI --info option
        and for verifying that data was loaded correctly.

        Returns
        -------
        dict
            Keys are table names, values are row counts.
        """
        tables = [
            "electricity_readings",
            "gas_readings",
            "smartthings_messages",
            "weather_readings",
        ]
        stats: dict[str, int] = {}
        with self._engine.connect() as conn:
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                stats[table] = result.scalar()
        return stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _upsert_ignore(self, model, records: list[dict]) -> int:
        """
        Insert records using INSERT OR IGNORE semantics (SQLite).
        Returns the number of rows actually inserted.

        SQLite enforces a limit of 999 bound variables per statement.
        We therefore split large record lists into batches whose size is
        computed dynamically from the number of columns so we always stay
        under the limit.
        """
        if not records:
            return 0

        # SQLite limit: 999 variables per statement.
        # Each record occupies len(record) variables, so:
        n_cols = len(records[0])
        batch_size = max(1, 999 // n_cols)

        total_inserted = 0
        with self._engine.begin() as conn:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                stmt = sqlite_insert(model).prefix_with("OR IGNORE").values(batch)
                result = conn.execute(stmt)
                total_inserted += result.rowcount  # ignored rows are not counted
        return total_inserted

    def _count(self, model) -> int:
        """Return the current row count for a model's table."""
        with self._engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {model.__tablename__}")
            )
            return result.scalar()

    @staticmethod
    def _apply_epoch_filter(
        query: str,
        start_epoch: int | None,
        end_epoch:   int | None,
        params:      dict | None = None,
    ) -> tuple[str, dict]:
        """Append WHERE epoch conditions and return updated (query, params)."""
        if params is None:
            params = {}
        has_where = "WHERE" in query.upper()
        connector = "AND" if has_where else "WHERE"
        if start_epoch is not None:
            query += f" {connector} epoch >= :start_epoch"
            params["start_epoch"] = start_epoch
            connector = "AND"
        if end_epoch is not None:
            query += f" {connector} epoch <= :end_epoch"
            params["end_epoch"] = end_epoch
        return query, params


# ---------------------------------------------------------------------------
# Timezone conversion utilities
# ---------------------------------------------------------------------------
# These helpers are used by the CLI tools to convert source timestamps to
# UTC epoch integers before inserting into the database.
# ---------------------------------------------------------------------------

def amsterdam_str_to_epoch(dt_str: str, fmt: str = "%Y-%m-%d %H:%M") -> int:
    """
    Parse a datetime string in the Europe/Amsterdam timezone and return
    the corresponding UTC Unix epoch (integer seconds).

    Parameters
    ----------
    dt_str : str
        Datetime string, e.g. ``'2022-12-01 00:15'``.
    fmt : str
        strptime format string matching ``dt_str``.

    Returns
    -------
    int
        UTC Unix timestamp in seconds.

    Examples
    --------
    >>> amsterdam_str_to_epoch('2022-12-01 00:15')
    1669849500
    """
    ts = pd.Timestamp(dt_str).tz_localize("Europe/Amsterdam")
    return int(ts.tz_convert("UTC").timestamp())


def epoch_to_amsterdam(epoch: int) -> pd.Timestamp:
    """
    Convert a UTC epoch integer to a timezone-aware Timestamp in
    Europe/Amsterdam.

    Parameters
    ----------
    epoch : int
        UTC Unix timestamp in seconds.

    Returns
    -------
    pd.Timestamp
        Timezone-aware timestamp in Europe/Amsterdam.
    """
    return pd.Timestamp(epoch, unit="s", tz="UTC").tz_convert("Europe/Amsterdam")
