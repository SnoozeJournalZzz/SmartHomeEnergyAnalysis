#!/usr/bin/env python3
"""
export_dashboard_data.py — Pre-compute dashboard data from myhome.db.

Run once locally before deploying:
    python export_dashboard_data.py

Outputs four small CSV files to data_cache/ that the Dash app reads.
The database itself is never deployed.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from home_messages_db import HomeMessagesDB

# ── Constants (match report_energy_analysis.ipynb exactly) ────────────────────
TZ           = 'Europe/Amsterdam'
EPOCH_START  = 1664575200   # 2022-10-01 Amsterdam
EPOCH_END    = 1743375600   # 2025-03-31 Amsterdam
HDD_BASE     = 15.5
K            = 6
MOTION_DEVICES = [
    'Living Room (move)', 'Bathroom (sensor eye)',
    'Kitchen (stairs)', 'Kitchen (move)', 'Blue room (move aeotec)',
]

Path('data_cache').mkdir(exist_ok=True)
db = HomeMessagesDB('sqlite:///myhome.db')

print('Loading electricity...')
df_e = db.get_electricity(start_epoch=EPOCH_START, end_epoch=EPOCH_END)
df_e['dt'] = pd.to_datetime(df_e['epoch'], unit='s', utc=True).dt.tz_convert(TZ)
df_e = df_e.sort_values('dt').set_index('dt')
df_e['total_kwh'] = df_e['t1'] + df_e['t2']
df_e['gap_min']   = df_e['epoch'].diff() / 60
df_e['delta_kwh'] = df_e['total_kwh'].diff()
df_e.loc[df_e['gap_min'] > 20, 'delta_kwh'] = np.nan
df_e.loc[df_e['delta_kwh'] < 0, 'delta_kwh'] = np.nan

print('Loading gas...')
df_g = db.get_gas(start_epoch=EPOCH_START, end_epoch=EPOCH_END)
df_g['dt'] = pd.to_datetime(df_g['epoch'], unit='s', utc=True).dt.tz_convert(TZ)
df_g = df_g.sort_values('dt').set_index('dt')
df_g['gap_min']  = df_g['epoch'].diff() / 60
df_g['delta_m3'] = df_g['total'].diff()
df_g.loc[df_g['gap_min'] > 20, 'delta_m3'] = np.nan
df_g.loc[df_g['delta_m3'] < 0, 'delta_m3'] = np.nan

print('Loading weather...')
df_w = db.get_weather(start_epoch=EPOCH_START, end_epoch=EPOCH_END)
df_w['dt'] = pd.to_datetime(df_w['epoch'], unit='s', utc=True).dt.tz_convert(TZ)
df_w = df_w.sort_values('dt').set_index('dt')

# ── 1. Daily aggregates → data_cache/daily_energy.csv ─────────────────────────
print('Exporting daily_energy.csv...')
daily_elec = df_e['delta_kwh'].resample('D').sum(min_count=80)
daily_gas  = df_g['delta_m3'].resample('D').sum(min_count=80)
daily_temp = df_w['temperature'].resample('D').mean()

daily = pd.concat([
    daily_elec.rename('elec_kwh'),
    daily_gas.rename('gas_m3'),
    daily_temp.rename('temp_c'),
], axis=1).dropna(subset=['elec_kwh', 'gas_m3'])

daily['hdd'] = (HDD_BASE - daily['temp_c']).clip(lower=0)

def season(month):
    return {12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer'}.get(month, 'Autumn')

daily['season'] = [season(d.month) for d in daily.index]
daily.index = daily.index.strftime('%Y-%m-%d')
daily.index.name = 'date'
daily.to_csv('data_cache/daily_energy.csv')
print(f'  → {len(daily)} days')

# ── 2. Electricity heatmap → data_cache/elec_heatmap.csv ──────────────────────
print('Exporting elec_heatmap.csv...')
df_e['hour'] = df_e.index.hour
df_e['dow']  = df_e.index.dayofweek
heatmap = (
    df_e.groupby(['dow', 'hour'])['delta_kwh']
    .mean()
    .reset_index()
    .rename(columns={'delta_kwh': 'mean_kwh'})
)
heatmap.to_csv('data_cache/elec_heatmap.csv', index=False)
print(f'  → {len(heatmap)} cells')

# ── 3. Occupancy clusters → data_cache/hourly_clusters.csv ────────────────────
print('Loading SmartThings motion events...')
with db._engine.connect() as conn:
    motion_raw = pd.read_sql(
        text("""
            SELECT epoch, name
            FROM smartthings_messages
            WHERE capability = 'motionSensor'
              AND attribute  = 'motion'
              AND value      = 'active'
              AND epoch BETWEEN :s AND :e
        """),
        conn, params={'s': EPOCH_START, 'e': EPOCH_END}
    )

# UTC integer division — avoids DST ambiguity
motion_raw['hour_bin'] = pd.to_datetime(
    (motion_raw['epoch'] // 3600) * 3600, unit='s', utc=True
)
motion_main = motion_raw[motion_raw['name'].isin(MOTION_DEVICES)]

sensor_counts = (
    motion_main.groupby(['hour_bin', 'name']).size()
    .unstack('name', fill_value=0)
)
col_map = {
    'Bathroom (sensor eye)' : 'bathroom',
    'Blue room (move aeotec)': 'blue_room',
    'Kitchen (move)'         : 'kitchen',
    'Kitchen (stairs)'       : 'stairs',
    'Living Room (move)'     : 'living_room',
}
sensor_counts = sensor_counts.rename(columns=col_map)
sensor_counts['total'] = sensor_counts.sum(axis=1)

full_index = pd.date_range(
    start=sensor_counts.index.min(),
    end=sensor_counts.index.max(),
    freq='h', tz='UTC'
)
sensor_counts = sensor_counts.reindex(full_index, fill_value=0)

features  = ['bathroom', 'blue_room', 'kitchen', 'stairs', 'living_room']
X         = sensor_counts[features].values
X_scaled  = StandardScaler().fit_transform(X)

print(f'Running K-means (K={K})...')
km = KMeans(n_clusters=K, random_state=42, n_init='auto')
sensor_counts['cluster'] = km.fit_predict(X_scaled)

# Join with hourly electricity (UTC to match sensor index)
elec_hourly = df_e['delta_kwh'].tz_convert('UTC').resample('h').sum(min_count=4)

hourly = pd.concat([
    elec_hourly.rename('kwh'),
    sensor_counts['cluster'],
    sensor_counts['total'].rename('motion_total'),
], axis=1).dropna(subset=['kwh', 'cluster'])
hourly['cluster'] = hourly['cluster'].astype(int)

hourly_out = hourly.copy()
hourly_out.index = hourly_out.index.strftime('%Y-%m-%dT%H:%M:%SZ')
hourly_out.index.name = 'hour_utc'
hourly_out.to_csv('data_cache/hourly_clusters.csv')
print(f'  → {len(hourly_out):,} hourly rows')

# Cluster summary (sorted by activity, high → low)
clust_sum = (
    hourly.groupby('cluster')
    .agg(mean_kwh=('kwh', 'mean'), median_kwh=('kwh', 'median'),
         count=('kwh', 'count'), mean_motion=('motion_total', 'mean'))
    .round(4)
    .sort_values('mean_motion', ascending=False)
    .reset_index()
)
clust_sum.to_csv('data_cache/cluster_summary.csv', index=False)
print(f'  → cluster_summary.csv ({len(clust_sum)} clusters)')

print('\nAll exports complete. Files in data_cache/:')
for f in sorted(Path('data_cache').iterdir()):
    print(f'  {f.name}  ({f.stat().st_size / 1024:.1f} KB)')
