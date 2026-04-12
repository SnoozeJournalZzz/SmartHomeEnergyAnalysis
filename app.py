"""
app.py — Smart Home Energy Analysis Dashboard
Dash + Plotly, reads pre-computed CSVs from data_cache/.

Local:  python app.py
Deploy: gunicorn app:server

Data contract
-------------
All files in data_cache/ are produced by export_dashboard_data.py.
Run that script once locally (or in CI) before running or deploying this app.
The app never connects to the database — it only reads pre-computed files.
"""

import json
from pathlib import Path
import pandas as pd
from dash import Dash, dcc, html, Input, Output

from charts import (
    make_timeseries, make_heatmap, make_regression,
    make_violin, make_motion_scatter,
    CLUSTER_PALETTE,
)

# ── Load data ──────────────────────────────────────────────────────────────────
BASE = Path(__file__).parent / 'data_cache'
daily      = pd.read_csv(BASE / 'daily_energy.csv', parse_dates=['date'])
heatmap_df = pd.read_csv(BASE / 'elec_heatmap.csv')
hourly     = pd.read_csv(BASE / 'hourly_clusters.csv', parse_dates=['hour_utc'])
clust_sum  = pd.read_csv(BASE / 'cluster_summary.csv')

# ── HDD regression parameters — loaded from pre-computed JSON ─────────────────
# WHY load from JSON instead of re-computing here?
#
# Previously this block used np.polyfit on the CSV data, which created two
# separate code paths for "the same" regression:
#   1. report_energy_analysis.ipynb  — statsmodels smf.ols  (the canonical source)
#   2. app.py                         — np.polyfit           (the copy)
#
# The copy introduced a subtle bug: np.polyfit's RMSE used 'n' in the
# denominator, while statsmodels uses 'n-2' (unbiased).  More importantly,
# any future change to the regression (different base temperature, different
# outage exclusion window) would need to be made in TWO places.
#
# The fix: export_dashboard_data.py fits the model with statsmodels (matching
# the notebook exactly) and writes the four scalar results to hdd_model.json.
# This app reads those scalars.  There is now exactly ONE place where the
# regression is computed.
#
# HOW to update: re-run `python export_dashboard_data.py` whenever the data
# changes. The JSON is the single source of truth for all regression numbers
# shown in the Dashboard.
_hdd_model_path = BASE / 'hdd_model.json'
try:
    with open(_hdd_model_path) as _fh:
        _m = json.load(_fh)
    slope     = _m['slope']
    intercept = _m['intercept']
    R2        = _m['r2']
    RMSE      = _m['rmse']
except FileNotFoundError:
    raise FileNotFoundError(
        f"'{_hdd_model_path}' not found.\n"
        "Run  python export_dashboard_data.py  to generate it, then restart the app."
    )

# reg is still needed by fig_regression() for the scatter points and season colouring.
# It is NOT used to recompute slope/intercept/R²/RMSE any more.
reg = daily.dropna(subset=['gas_m3', 'hdd'])

# ── App init ───────────────────────────────────────────────────────────────────
app = Dash(
    __name__,
    title='Smart Home Energy · Nordwijk',
    meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}],
)
server = app.server  # gunicorn entry point

# ── Colour palette ─────────────────────────────────────────────────────────────
C_ELEC  = '#1E88E5'
C_GAS   = '#F4511E'
SEASONS = {'Winter': '#5C6BC0', 'Spring': '#43A047', 'Summer': '#FFA726', 'Autumn': '#8D6E63'}


# ── Helper components ──────────────────────────────────────────────────────────
def kpi_card(value, label, bg, accent):
    return html.Div([
        html.Div(value, style={
            'fontSize': '1.75rem', 'fontWeight': 700,
            'color': accent, 'lineHeight': 1,
        }),
        html.Div(label, style={
            'fontSize': '0.72rem', 'color': '#555',
            'marginTop': '6px', 'lineHeight': 1.4,
        }),
    ], style={
        'background': bg, 'borderRadius': '10px',
        'padding': '16px 18px', 'flex': '1', 'minWidth': '150px',
        'borderLeft': f'4px solid {accent}',
        'boxShadow': '0 1px 4px rgba(0,0,0,0.08)',
    })


def insight_box(children, accent='#1565C0', bg='#E3F2FD'):
    return html.Div(children, style={
        'background': bg, 'borderLeft': f'4px solid {accent}',
        'borderRadius': '0 8px 8px 0', 'padding': '12px 16px',
        'fontSize': '0.88rem', 'color': '#444', 'marginTop': '16px',
        'lineHeight': 1.6,
    })


# ── Layout ─────────────────────────────────────────────────────────────────────
TAB_STYLE = {
    'fontFamily': 'inherit', 'fontWeight': 500,
    'padding': '10px 20px', 'fontSize': '0.92rem',
}
TAB_SELECTED = {**TAB_STYLE, 'borderTop': '3px solid #1E88E5', 'color': '#1E88E5'}

app.layout = html.Div([

    # ── Header ─────────────────────────────────────────────────────────────────
    html.Div([
        html.H1('Smart Home Energy Analysis',
                style={'margin': 0, 'fontSize': '1.6rem', 'fontWeight': 700, 'letterSpacing': '-0.3px'}),
        html.P('Nordwijk, Netherlands · Single-family household · Oct 2022 – Mar 2025',
               style={'margin': '5px 0 0', 'opacity': 0.82, 'fontSize': '0.88rem'}),
    ], style={
        'background': 'linear-gradient(120deg, #1565C0 0%, #1976D2 100%)',
        'color': 'white', 'padding': '22px 40px',
    }),

    # ── KPI strip ──────────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            kpi_card('106K+',  'P1 meter readings',                '#E3F2FD', '#1565C0'),
            kpi_card('1.7M',   'SmartThings IoT events',           '#FFF3E0', '#E65100'),
            kpi_card(f'R²={R2:.2f}', 'Gas variance explained by temperature', '#E8F5E9', '#2E7D32'),
            kpi_card('2.3×',   'Electricity: active home vs quiet','#F3E5F5', '#6A1B9A'),
        ], style={'display': 'flex', 'gap': '14px', 'flexWrap': 'wrap'}),
    ], style={
        'padding': '18px 40px',
        'background': '#F5F5F5',
        'borderBottom': '1px solid #E0E0E0',
    }),

    # ── Tabs ───────────────────────────────────────────────────────────────────
    html.Div([
        dcc.Tabs(
            id='tabs', value='tab-1',
            children=[
                dcc.Tab(label='Consumption Patterns',  value='tab-1',
                        style=TAB_STYLE, selected_style=TAB_SELECTED),
                dcc.Tab(label='Weather & Gas',         value='tab-2',
                        style=TAB_STYLE, selected_style=TAB_SELECTED),
                dcc.Tab(label='Occupancy Detection',   value='tab-3',
                        style=TAB_STYLE, selected_style=TAB_SELECTED),
            ],
            colors={'border': '#E0E0E0', 'primary': '#1E88E5', 'background': '#FAFAFA'},
        ),
        html.Div(id='tab-content', style={'paddingTop': '24px', 'minHeight': '500px'}),
    ], style={'padding': '16px 40px 40px', 'maxWidth': '1280px', 'margin': '0 auto'}),

], style={
    'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    'background': '#FAFAFA', 'minHeight': '100vh',
})


# ── Callback ───────────────────────────────────────────────────────────────────
@app.callback(Output('tab-content', 'children'), Input('tabs', 'value'))
def render_tab(tab):

    if tab == 'tab-1':
        return html.Div([
            dcc.Graph(figure=make_timeseries(daily, C_ELEC, C_GAS), config={'displayModeBar': False}),
            html.Hr(style={'border': 'none', 'borderTop': '1px solid #E0E0E0', 'margin': '24px 0 20px'}),
            dcc.Graph(figure=make_heatmap(heatmap_df), config={'displayModeBar': False}),
            insight_box([
                'Electricity median ',
                html.B(f"{daily['elec_kwh'].median():.1f} kWh/day"),
                f" (P5–P95: {daily['elec_kwh'].quantile(.05):.1f}–{daily['elec_kwh'].quantile(.95):.1f} kWh). ",
                'Gas is near-zero in summer — this household heats entirely with gas. '
                'The heatmap reveals a stable two-peak daily routine: '
                'morning ramp 07:00–09:00 and evening peak 18:00–22:00, consistent across all weekdays.',
            ], accent='#1565C0', bg='#E3F2FD'),
        ])

    elif tab == 'tab-2':
        return html.Div([
            dcc.Graph(figure=make_regression(reg, slope, intercept, R2, RMSE, SEASONS), config={'displayModeBar': False}),
            insight_box([
                f'Temperature explains ',
                html.B(f'{R2*100:.1f}%'),
                ' of daily gas variance (HDD model, base 15.5°C). '
                f'Slope: {slope:.3f} m³ per degree-day — each 1°C colder day adds ~{slope:.2f} m³ gas. ',
                'Annual consumption ~1,196 m³, below the Dutch residential average (1,500–2,000 m³/yr). '
                'Residuals show no day-of-week pattern: the boiler responds to temperature, not the calendar.',
            ], accent='#2E7D32', bg='#E8F5E9'),
        ])

    elif tab == 'tab-3':
        lo_kwh = clust_sum.iloc[-1]['mean_kwh']   # last row = lowest activity
        hi_kwh = clust_sum.iloc[0]['mean_kwh']    # first row = highest activity
        return html.Div([
            html.Div([
                html.Div([
                    dcc.Graph(figure=make_violin(hourly, clust_sum, CLUSTER_PALETTE), config={'displayModeBar': False}),
                ], style={'flex': 1}),
                html.Div([
                    dcc.Graph(figure=make_motion_scatter(hourly), config={'displayModeBar': False}),
                ], style={'flex': 1}),
            ], style={'display': 'flex', 'gap': '20px'}),
            insight_box([
                'K-means (K=6) on 5-sensor hourly motion counts identifies six occupancy states. '
                'Lowest-activity cluster: ',
                html.B(f'{lo_kwh:.2f} kWh/h'),
                ' — highest-activity cluster: ',
                html.B(f'{hi_kwh:.2f} kWh/h'),
                '. The ',
                html.B(f'{hi_kwh/lo_kwh:.1f}×'),
                ' electricity difference is validated independently by the P1 meter — '
                'the sensor-derived labels capture a real energy signal.',
            ], accent='#6A1B9A', bg='#F3E5F5'),
        ])


if __name__ == '__main__':
    app.run(debug=True, port=8050)
