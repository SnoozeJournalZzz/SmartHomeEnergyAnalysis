# Analysis Log — SmartHomeEnergyAnalysis

> **Purpose:** This log records analytical decisions, bugs encountered, surprising findings,
> open questions, and reflections throughout the project. It is a thinking record, not a
> summary of results. Entries are dated and honest — including failures and reversals.
>
> **Audience:** Future self, interviewers, collaborators.

---

## Phase 1 — ETL Pipeline (2026-04-10)

### Decision: Epoch storage format
**Decision:** Store all timestamps as UTC Unix epoch integers (seconds).
**Why:** Avoids timezone ambiguity at query time. Timezone conversion is done once at
ingestion, consistently, rather than scattered across every downstream analysis.
**Trade-off:** Slightly less readable in raw SQL queries. Mitigated by `epoch_to_amsterdam()`
utility function in `home_messages_db.py`.

---

### Bug: pandas 3.0 datetime precision change
**What happened:** All stored epochs were ~1000x too small (e.g. 1,647,609 instead of
1,669,849,200), causing all timestamps to appear as "1970-01-20".
**Root cause:** In pandas 3.0, the default datetime precision changed from `datetime64[ns]`
(nanoseconds) to `datetime64[us]` (microseconds). The conversion chain
`.astype("int64") // 10**9` had assumed nanoseconds — dividing microseconds by 10^9
gives milliseconds, not seconds.
**Fix:** Added `.dt.tz_localize(None).astype("datetime64[s]").astype("int64")` — casting
to second-precision first makes the final integer always represent seconds, regardless of
the underlying pandas precision.
**Lesson:** Never assume the internal precision of a pandas datetime. Always cast to an
explicit target precision before converting to integer.

---

### Decision: DST handling strategy
**What happened:** `openweathermap.py` crashed on 2022-10-30 (DST fall-back in Amsterdam)
because `ambiguous='infer'` cannot resolve ambiguous hourly timestamps — only sub-hourly
data has enough context for inference.
**Fix:** Requested UTC directly from the Open-Meteo API (`timezone=UTC`). No DST
ambiguity possible in UTC.
**Lesson:** Applied a hierarchy of DST strategies:
1. Get UTC from source if possible (best — zero DST exposure)
2. Use `ambiguous='infer'` for sub-hourly data (pandas can infer direction from neighbors)
3. Explicit `True/False` only when the transition direction is known from domain knowledge

---

### Bug: SQLite 999-variable limit
**What happened:** Inserting 29,899 SmartThings rows in one statement raised
`sqlite3.OperationalError: too many SQL variables`. SQLite limits bound parameters
per statement to 999.
**Fix:** Batched inserts in `_upsert_ignore()` with `batch_size = 999 // n_columns`.
**Lesson:** Always batch large inserts. The limit is a SQLite hard constraint, not a
configuration issue. The fix belongs in the database layer, not the caller.

---

### Decision: Deduplication at DB layer
**Design choice:** Duplicates are handled via `PRIMARY KEY` / `UNIQUE` constraints +
`INSERT OR IGNORE`, not by Python-level pre-filtering.
**Why:** This approach is O(1) per record (hash lookup in SQLite index), vs. O(n) for
loading the full table into memory to check. With 1.7M SmartThings rows, memory
pre-filtering would be impractical and fragile.
**Result:** Loading all P1e files produced 223,245 parsed rows but only 106,013 unique
epochs — 52% were cross-file duplicates, correctly discarded.

---

## Phase 2 — Data Quality Report (2026-04-10)

### Finding: Electricity & gas share the same coverage gap
**Observed:** Both `electricity_readings` and `gas_readings` have exactly **1 gap > 1 day**.
Given both are from the same physical P1 smart meter, this almost certainly reflects
the same hardware/collection outage, not a data processing error.
**Implication:** Any analysis spanning that gap should be treated with caution. The gap
period should be explicitly excluded from daily/weekly pattern analyses.
**Status:** Need to identify exact dates — see notebook Section 2.

### Finding: SmartThings data starts Oct 2022, electricity/gas from Mar 2022
**Observed:** Electricity and gas data start 2022-03-18; SmartThings starts 2022-10-09.
**Implication:** There is a ~7 month window (Mar–Oct 2022) where energy data exists but
device behavior data does not. Cross-source analyses must respect this.
**Decision:** Cross-source analyses will use 2022-10-09 as the common start date.

### Finding: Extreme device activity imbalance
**Observed:** Garden air sensor: 276,593 messages; Garden (ground): 52 messages.
Ratio: ~5,300:1 across 40 devices.
**Implication:** Naive "per-device" averages are meaningless. Any aggregation must be
aware of this imbalance. Some devices may have been installed/removed mid-dataset.
**Open question:** Do the low-activity devices reflect broken sensors or genuinely
quiet devices (e.g. a door that's rarely opened)?

### Finding: SmartThings 2022-10-09 has only 17 messages
**Observation:** First day of SmartThings data has 17 messages vs. median of 1,845.
**Assessment:** This is almost certainly a partial-day artifact (data collection started
partway through the day), not a sensor malfunction. Not a quality issue.

---

## Open Questions (to be addressed in upcoming analyses)

1. **What are the exact dates of the electricity/gas gap?** Is it a single continuous
   outage, or multiple short ones above the 30-min threshold only?
2. **Do any SmartThings devices have multi-day gaps mid-dataset?** Which ones, and when?
3. **Are temperature sensor readings from SmartThings consistent with Open-Meteo weather?**
   The garden sensor should correlate with outdoor weather data.
4. **Is the 2024-08-15 peak (4,667 messages) a real activity spike or a data artifact?**
5. **Does the electricity/gas gap coincide with any SmartThings anomaly?**

---

*Log continues in subsequent phases...*
