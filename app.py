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
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, Input, Output

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
CLUSTER_PALETTE = px.colors.qualitative.Safe

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


# ── Figure builders ────────────────────────────────────────────────────────────
def fig_timeseries():
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily['date'], y=daily['elec_kwh'],
        name='Electricity (kWh/day)',
        line=dict(color=C_ELEC, width=1.5),
        fill='tozeroy', fillcolor='rgba(30,136,229,0.10)',
    ))
    fig.add_trace(go.Scatter(
        x=daily['date'], y=daily['gas_m3'],
        name='Gas (m³/day)',
        line=dict(color=C_GAS, width=1.5),
        fill='tozeroy', fillcolor='rgba(244,81,30,0.10)',
        yaxis='y2',
    ))
    fig.update_layout(
        template='plotly_white',
        height=320,
        margin=dict(l=60, r=70, t=40, b=50),
        title='Daily Electricity and Gas Consumption',
        legend=dict(orientation='h', y=1.12, x=0),
        xaxis=dict(
            rangeslider=dict(visible=True, thickness=0.06),
            rangeselector=dict(buttons=[
                dict(count=3,  label='3M',  step='month', stepmode='backward'),
                dict(count=6,  label='6M',  step='month', stepmode='backward'),
                dict(count=1,  label='1Y',  step='year',  stepmode='backward'),
                dict(step='all', label='All'),
            ], bgcolor='#F5F5F5', activecolor='#1E88E5', font=dict(size=11)),
        ),
        yaxis=dict(title='kWh/day', color=C_ELEC, titlefont_color=C_ELEC),
        yaxis2=dict(title='m³/day', color=C_GAS, titlefont_color=C_GAS,
                    overlaying='y', side='right'),
    )
    return fig


def fig_heatmap():
    pivot = heatmap_df.pivot_table(index='dow', columns='hour', values='mean_kwh')
    days  = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f'{h:02d}:00' for h in range(24)],
        y=days,
        colorscale='YlOrRd',
        colorbar=dict(title='kWh', thickness=12, len=0.85),
        hoverongaps=False,
        hovertemplate='%{y}, %{x}: <b>%{z:.4f} kWh</b><extra></extra>',
    ))
    fig.update_layout(
        template='plotly_white',
        title='Mean Electricity — Hour of Day × Day of Week',
        xaxis=dict(title='Hour of day', tickangle=0, tickfont_size=10),
        margin=dict(l=60, r=80, t=50, b=40),
        height=270,
    )
    return fig


def fig_regression():
    hdd_x   = np.linspace(0, reg['hdd'].max(), 200)
    fig = go.Figure()

    for s, grp in reg.groupby('season'):
        g = grp.dropna(subset=['gas_m3', 'hdd'])
        fig.add_trace(go.Scatter(
            x=g['hdd'], y=g['gas_m3'],
            mode='markers', name=s,
            marker=dict(color=SEASONS[s], size=5, opacity=0.65,
                        line=dict(color='white', width=0.3)),
            hovertemplate=f'<b>{s}</b><br>HDD: %{{x:.1f}}<br>Gas: %{{y:.2f}} m³<extra></extra>',
        ))

    fig.add_trace(go.Scatter(
        x=hdd_x, y=intercept + slope * hdd_x,
        mode='lines', name='OLS fit',
        line=dict(color='#1A237E', width=2.5),
    ))
    fig.add_annotation(
        xref='paper', yref='paper', x=0.99, y=0.97, showarrow=False,
        align='right', bgcolor='rgba(255,255,255,0.9)',
        bordercolor='#BDBDBD', borderwidth=1,
        text=(
            f'<b>R² = {R2:.3f}</b><br>'
            f'slope = {slope:.3f} m³ / degree-day<br>'
            f'RMSE = {RMSE:.2f} m³/day<br>'
            f'n = {len(reg)} days'
        ),
        font=dict(size=12, family='monospace'),
    )
    fig.update_layout(
        template='plotly_white',
        title='Gas Consumption vs Heating Degree Days (base 15.5 °C)',
        xaxis_title='Heating Degree Days (HDD)',
        yaxis_title='Daily gas (m³)',
        legend=dict(orientation='h', y=-0.18, x=0),
        margin=dict(l=60, r=30, t=60, b=90),
        height=440,
    )
    return fig


def fig_violin():
    order   = clust_sum['cluster'].tolist()   # already sorted high→low motion
    fig = go.Figure()
    for i, cid in enumerate(order):
        kwh    = hourly[hourly['cluster'] == cid]['kwh']
        motion = clust_sum.loc[clust_sum['cluster'] == cid, 'mean_motion'].values[0]
        kwh_c  = kwh.clip(upper=kwh.quantile(0.98))
        fig.add_trace(go.Violin(
            y=kwh_c, name=f'{motion:.0f} ev/h',
            box_visible=True, meanline_visible=True,
            fillcolor=CLUSTER_PALETTE[i % len(CLUSTER_PALETTE)],
            opacity=0.8, line_color='rgba(0,0,0,0.25)',
            hoverinfo='y+name',
        ))
    fig.update_layout(
        template='plotly_white',
        title='Electricity by Occupancy State (sorted: active → quiet)',
        yaxis_title='kWh per hour',
        xaxis_title='Cluster label = mean motion events/hour',
        showlegend=False,
        margin=dict(l=60, r=30, t=60, b=70),
        height=380,
    )
    return fig


def fig_motion_scatter():
    sample = hourly.sample(min(5000, len(hourly)), random_state=42)
    fig = px.scatter(
        sample, x='motion_total', y='kwh', color='cluster',
        color_continuous_scale='Turbo',
        opacity=0.25,
        labels={'motion_total': 'Motion events in hour',
                'kwh': 'Electricity (kWh/hour)',
                'cluster': 'Cluster'},
        title='Motion Activity vs Electricity — each point is one hour',
        template='plotly_white',
        height=380,
    )
    fig.update_traces(marker_size=4)
    fig.update_layout(margin=dict(l=60, r=30, t=60, b=60))
    return fig


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
            dcc.Graph(figure=fig_timeseries(), config={'displayModeBar': False}),
            html.Hr(style={'border': 'none', 'borderTop': '1px solid #E0E0E0', 'margin': '24px 0 20px'}),
            dcc.Graph(figure=fig_heatmap(), config={'displayModeBar': False}),
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
            dcc.Graph(figure=fig_regression(), config={'displayModeBar': False}),
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
                    dcc.Graph(figure=fig_violin(), config={'displayModeBar': False}),
                ], style={'flex': 1}),
                html.Div([
                    dcc.Graph(figure=fig_motion_scatter(), config={'displayModeBar': False}),
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
