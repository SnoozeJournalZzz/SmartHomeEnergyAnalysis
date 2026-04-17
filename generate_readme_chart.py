#!/usr/bin/env python3
"""
generate_readme_chart.py
Generates docs/readme_chart.png — the hero figure for the GitHub README.

Run:  python generate_readme_chart.py
Requires: data_cache/ files (run export_dashboard_data.py first)
"""

import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# ── Style ──────────────────────────────────────────────────────────────────────
plt.rcParams.update({
    'font.family':        'DejaVu Sans',
    'font.size':          11,
    'axes.spines.top':    False,
    'axes.spines.right':  False,
    'axes.linewidth':     0.8,
    'axes.grid':          True,
    'grid.alpha':         0.25,
    'grid.linewidth':     0.6,
    'grid.color':         '#bbbbbb',
    'figure.facecolor':   'white',
    'axes.facecolor':     '#fafafa',
    'xtick.labelsize':    10,
    'ytick.labelsize':    10,
})

SEASON_COLORS = {
    'Winter': '#2166ac',
    'Spring': '#4dac26',
    'Summer': '#e08214',
    'Autumn': '#d6604d',
}

# ── Load data ──────────────────────────────────────────────────────────────────
daily = pd.read_csv('data_cache/daily_energy.csv', index_col='date', parse_dates=True)
cluster_sum = pd.read_csv('data_cache/cluster_summary.csv')

with open('data_cache/hdd_model.json') as fh:
    hdd = json.load(fh)

# ── Figure layout ──────────────────────────────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.2))
fig.suptitle(
    "What drives this household's energy consumption?",
    fontsize=14, fontweight='bold', y=1.02,
)

# ══════════════════════════════════════════════════════════════════════════════
# Panel 1 — Daily gas consumption vs. Heating Degree Days
# Story: weather alone explains 80.6% of gas variance
# ══════════════════════════════════════════════════════════════════════════════
df = daily.dropna(subset=['gas_m3', 'hdd'])

for season in ['Winter', 'Autumn', 'Spring', 'Summer']:
    grp = df[df['season'] == season]
    ax1.scatter(
        grp['hdd'], grp['gas_m3'],
        color=SEASON_COLORS[season],
        alpha=0.5, s=20, label=season, zorder=3,
    )

# Regression line — params from hdd_model.json (statsmodels smf.ols)
x_line = np.linspace(df['hdd'].min(), df['hdd'].max(), 300)
y_line = hdd['slope'] * x_line + hdd['intercept']
ax1.plot(x_line, y_line, color='#111111', lw=1.8, zorder=5)

ax1.set_xlabel('Heating Degree Days  (base 15.5 °C)', fontsize=11)
ax1.set_ylabel('Daily gas consumption  (m³)', fontsize=11)
ax1.set_title('Gas consumption vs. outdoor temperature', fontsize=12, pad=10)
ax1.legend(
    title='Season', fontsize=9, title_fontsize=9,
    framealpha=0.85, edgecolor='#cccccc',
)

# Annotation box — key numbers a hiring manager reads in 5 seconds
ann_text = (
    f"R² = {hdd['r2']:.3f}\n"
    f"Slope = {hdd['slope']:.2f} m³ per HDD\n"
    f"n = {hdd['n_days']} days"
)
ax1.text(
    0.97, 0.06, ann_text,
    transform=ax1.transAxes,
    ha='right', va='bottom', fontsize=9.5,
    bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
              edgecolor='#cccccc', alpha=0.95),
)

# ══════════════════════════════════════════════════════════════════════════════
# Panel 2 — Mean hourly electricity by occupancy cluster
# Story: occupancy creates a 2.3× electricity gap
# ══════════════════════════════════════════════════════════════════════════════
# Sort ascending by activity so quiet is at bottom, busy at top
cs = cluster_sum.sort_values('mean_motion').reset_index(drop=True)

# Human-readable y labels
def activity_label(motion, count):
    hours_pct = 100 * count / cluster_sum['count'].sum()
    return f"{motion:.0f} events/h  ({hours_pct:.0f}% of hours)"

cs['label'] = [
    activity_label(row.mean_motion, row['count'])
    for _, row in cs.iterrows()
]

# Color: cool blue (quiet) → warm red (busy), matching the scatter palette
n = len(cs)
bar_colors = [plt.cm.RdYlBu_r(v) for v in np.linspace(0.1, 0.85, n)]

bars = ax2.barh(
    cs['label'], cs['mean_kwh'],
    color=bar_colors, edgecolor='white', linewidth=0.8, height=0.6,
)

# Value labels on each bar
for bar, val in zip(bars, cs['mean_kwh']):
    ax2.text(
        val + 0.006,
        bar.get_y() + bar.get_height() / 2,
        f'{val:.2f} kWh/h',
        va='center', ha='left', fontsize=9.5,
    )

# 2.3× gap annotation between quietest and busiest bar
min_val = cs['mean_kwh'].iloc[0]   # quietest (bottom)
max_val = cs['mean_kwh'].iloc[-1]  # busiest (top)
mid_y   = (n - 1) / 2.0

ax2.annotate(
    '',
    xy=(max_val, n - 1), xytext=(min_val, 0),
    arrowprops=dict(
        arrowstyle='<->', color='#444444',
        lw=1.4, shrinkA=4, shrinkB=4,
    ),
    annotation_clip=False,
)
ax2.text(
    (min_val + max_val) / 2 - 0.01, mid_y,
    f'{max_val / min_val:.1f}×\ngap',
    ha='right', va='center', fontsize=10.5,
    fontweight='bold', color='#333333',
)

ax2.set_xlabel('Mean hourly electricity consumption  (kWh/h)', fontsize=11)
ax2.set_title('Electricity consumption by occupancy state', fontsize=12, pad=10)
ax2.set_xlim(0, max_val * 1.38)
ax2.xaxis.set_major_formatter(mticker.FormatStrFormatter('%.2f'))

# Caption below each panel
fig.text(
    0.26, -0.04,
    'Each point = one day.  Line = OLS regression (statsmodels, n=907 days).',
    ha='center', fontsize=9, color='#666666',
)
fig.text(
    0.74, -0.04,
    'Each row = one K-means cluster (K=5, silhouette-optimised).  '
    'Bars show cluster-mean electricity.',
    ha='center', fontsize=9, color='#666666',
)

# ── Save ───────────────────────────────────────────────────────────────────────
Path('docs').mkdir(exist_ok=True)
out = Path('docs/readme_chart.png')
fig.tight_layout()
fig.savefig(out, dpi=160, bbox_inches='tight', facecolor='white')
print(f'Saved → {out}  ({out.stat().st_size / 1024:.0f} KB)')
plt.close()
