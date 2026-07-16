"""
Mini BFCM Live Dashboard

Streamlit app that polls the DuckDB aggregate tables every 2 seconds
and renders orders/min, revenue/min, and consumer lag as live charts --
the public-facing view of "is the pipeline keeping up with the spike."

Why this only ever reads a handful of aggregated rows, not raw events:
window_stats has one row per 10-second window, so the query cost stays
flat regardless of how many individual orders were produced in that
window, even during a 100x spike. That's the payoff of doing the
aggregation work in the consumer (see consumer/store.py) instead of
here -- the dashboard would fall over trying to read raw events at
spike volume if it had to do that aggregation itself on every poll.

LAG_HEALTHY / LAG_ELEVATED are calibrated to this project's demo scale
(a spike here peaks around a few hundred messages of backlog), not a
universal constant -- a production system would size these to its own
expected backlog under load.
"""

import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from db import DB_PATH, get_lag_series, get_window_stats

POLL_SECONDS = 2
WINDOW_SECONDS = 10

# Status thresholds for the lag indicator: chosen for this project's demo
# scale (a spike peaks around a few hundred messages of backlog), not a
# universal constant -- a production system would size these to its own
# expected backlog under load.
LAG_HEALTHY = 50
LAG_ELEVATED = 300

st.set_page_config(page_title="Mini BFCM", layout="wide")
st.title("Mini BFCM — Live Flash Sale Dashboard")
st.caption("Polls the local DuckDB aggregate tables every 2s. Independent of the Kafka pipeline underneath it.")

if not DB_PATH.exists():
    st.info("Waiting for the consumer to create the database — start producer.py and consumer.py first.")
    time.sleep(POLL_SECONDS)
    st.rerun()

windows = get_window_stats()
lag = get_lag_series()

if windows.empty:
    st.info("No orders processed yet.")
    time.sleep(POLL_SECONDS)
    st.rerun()

windows["window_time"] = pd.to_datetime(windows["window_start"], unit="s")
# Each row is a 10-second window; scale up to a per-minute rate so the
# numbers read the way "orders/min" and "revenue/min" normally do.
per_minute_factor = 60 / WINDOW_SECONDS
windows["orders_per_min"] = windows["order_count"] * per_minute_factor
windows["revenue_per_min"] = windows["revenue"] * per_minute_factor

# The consumer flushes every 1s but a window only closes every 10s, so the
# newest row here is often still accumulating (a partial count), not a
# finished window. Showing that as "the latest window" makes the number
# climb through each 10s window then drop back down -- a sawtooth that
# reflects flush timing, not real traffic. Only treat a window as reportable
# once wall-clock time has actually passed its end.
now = time.time()
completed = windows[windows["window_start"] + WINDOW_SECONDS <= now]

if completed.empty:
    st.info("Waiting for the first window to complete...")
    time.sleep(POLL_SECONDS)
    st.rerun()

latest = completed.iloc[-1]
current_lag = int(lag.iloc[-1]["lag"]) if not lag.empty else 0

if current_lag < LAG_HEALTHY:
    lag_status, lag_color = "caught up", "#2E7D32"
elif current_lag < LAG_ELEVATED:
    lag_status, lag_color = "elevated", "#B26A00"
else:
    lag_status, lag_color = "falling behind", "#C62828"

col1, col2, col3 = st.columns(3)
col1.metric("Orders / min (latest window)", f"{latest['orders_per_min']:.0f}")
col2.metric("Revenue / min (latest window)", f"${latest['revenue_per_min']:,.0f}")
with col3:
    st.markdown("**Consumer lag**")
    st.markdown(
        f"<span style='font-size:2rem; color:{lag_color}'>{current_lag}</span> "
        f"<span style='color:{lag_color}'>({lag_status})</span>",
        unsafe_allow_html=True,
    )

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    fig_orders = go.Figure()
    fig_orders.add_trace(
        go.Scatter(x=completed["window_time"], y=completed["orders_per_min"], mode="lines", line=dict(width=2, color="#3B6FD4"))
    )
    fig_orders.update_layout(title="Orders / min", margin=dict(t=40, b=20), height=300)
    st.plotly_chart(fig_orders, use_container_width=True)

with chart_col2:
    fig_revenue = go.Figure()
    fig_revenue.add_trace(
        go.Scatter(x=completed["window_time"], y=completed["revenue_per_min"], mode="lines", line=dict(width=2, color="#3B6FD4"))
    )
    fig_revenue.update_layout(title="Revenue / min", margin=dict(t=40, b=20), height=300)
    st.plotly_chart(fig_revenue, use_container_width=True)

if not lag.empty:
    lag["time"] = pd.to_datetime(lag["recorded_at"], unit="s")
    fig_lag = go.Figure()
    fig_lag.add_trace(go.Scatter(x=lag["time"], y=lag["lag"], mode="lines", line=dict(width=2, color="#C62828")))
    fig_lag.update_layout(title="Consumer lag over time", margin=dict(t=40, b=20), height=250)
    st.plotly_chart(fig_lag, use_container_width=True)

time.sleep(POLL_SECONDS)
st.rerun()
