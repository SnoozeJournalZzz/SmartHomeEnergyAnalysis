"""
charts.py — Pure figure-builder functions for the Smart Home Energy dashboard.

Each function accepts its data as explicit parameters and returns a Plotly Figure.
This makes them independently testable without needing a running Dash app or
loaded CSV files.

Consumed by app.py via:
    from charts import make_timeseries, make_heatmap, make_regression, \
                       make_violin, make_motion_scatter
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# Default colour palette for K-means cluster traces.
# Exported so app.py can pass it back in without a direct plotly.express import there.
CLUSTER_PALETTE = px.colors.qualitative.Safe


def make_timeseries(
    daily: pd.DataFrame,
    c_elec: str = '#1E88E5',
    c_gas: str = '#F4511E',
) -> go.Figure:
    """Dual-axis time series: daily electricity (left) and gas (right).

    Parameters
    ----------
    daily : DataFrame with columns ['date', 'elec_kwh', 'gas_m3']
    c_elec : hex colour for electricity traces
    c_gas  : hex colour for gas traces
    """
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily['date'], y=daily['elec_kwh'],
        name='Electricity (kWh/day)',
        line=dict(color=c_elec, width=1.5),
        fill='tozeroy', fillcolor='rgba(30,136,229,0.10)',
    ))
    fig.add_trace(go.Scatter(
        x=daily['date'], y=daily['gas_m3'],
        name='Gas (m³/day)',
        line=dict(color=c_gas, width=1.5),
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
        yaxis=dict(title='kWh/day', color=c_elec, titlefont_color=c_elec),
        yaxis2=dict(title='m³/day', color=c_gas, titlefont_color=c_gas,
                    overlaying='y', side='right'),
    )
    return fig


def make_heatmap(heatmap_df: pd.DataFrame) -> go.Figure:
    """Mean electricity by hour-of-day × day-of-week.

    Parameters
    ----------
    heatmap_df : DataFrame with columns ['dow', 'hour', 'mean_kwh']
    """
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


def make_regression(
    reg: pd.DataFrame,
    slope: float,
    intercept: float,
    r2: float,
    rmse: float,
    seasons: dict,
) -> go.Figure:
    """HDD regression scatter + OLS line, coloured by season.

    Parameters
    ----------
    reg       : DataFrame with columns ['hdd', 'gas_m3', 'season']
    slope     : OLS slope (m³ per degree-day)
    intercept : OLS intercept
    r2        : Coefficient of determination
    rmse      : Root mean squared error (m³/day)
    seasons   : dict mapping season name → hex colour
    """
    hdd_x = np.linspace(0, reg['hdd'].max(), 200)
    fig = go.Figure()

    for s, grp in reg.groupby('season'):
        g = grp.dropna(subset=['gas_m3', 'hdd'])
        fig.add_trace(go.Scatter(
            x=g['hdd'], y=g['gas_m3'],
            mode='markers', name=s,
            marker=dict(color=seasons[s], size=5, opacity=0.65,
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
            f'<b>R² = {r2:.3f}</b><br>'
            f'slope = {slope:.3f} m³ / degree-day<br>'
            f'RMSE = {rmse:.2f} m³/day<br>'
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


def make_violin(
    hourly: pd.DataFrame,
    clust_sum: pd.DataFrame,
    cluster_palette: list,
) -> go.Figure:
    """Violin plots of electricity by K-means occupancy cluster.

    Parameters
    ----------
    hourly          : DataFrame with columns ['cluster', 'kwh']
    clust_sum       : DataFrame with columns ['cluster', 'mean_motion', 'mean_kwh'],
                      sorted by mean_motion descending (busiest first)
    cluster_palette : list of hex colours (one per cluster)
    """
    order = clust_sum['cluster'].tolist()
    fig = go.Figure()
    for i, cid in enumerate(order):
        kwh    = hourly[hourly['cluster'] == cid]['kwh']
        motion = clust_sum.loc[clust_sum['cluster'] == cid, 'mean_motion'].values[0]
        kwh_c  = kwh.clip(upper=kwh.quantile(0.98))
        fig.add_trace(go.Violin(
            y=kwh_c, name=f'{motion:.0f} ev/h',
            box_visible=True, meanline_visible=True,
            fillcolor=cluster_palette[i % len(cluster_palette)],
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


def make_motion_scatter(hourly: pd.DataFrame) -> go.Figure:
    """Scatter of hourly motion events vs electricity, coloured by cluster.

    Parameters
    ----------
    hourly : DataFrame with columns ['motion_total', 'kwh', 'cluster']
    """
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
