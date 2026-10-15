"""Streamlit dashboard for the daily crypto pipeline.

Run locally:   streamlit run dashboard/app.py
Deployed on Streamlit Community Cloud, reading from Neon with a read-only login.
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st

import queries

st.set_page_config(page_title="Crypto Pipeline Dashboard", page_icon="📈", layout="wide")

VOL = "Volatility (30d, annualised)"
STATUS = {"success": "✅ success", "failed": "❌ failed", "running": "⏳ running"}


def database_url() -> str:
    # On Streamlit Cloud the URL is a secret; locally it comes from the environment.
    try:
        if "DATABASE_URL" in st.secrets:
            os.environ.setdefault("DATABASE_URL", st.secrets["DATABASE_URL"])
    except FileNotFoundError:  # no secrets file locally: that's fine
        pass
    return queries.database_url()


@st.cache_resource
def engine():
    return queries.get_engine(database_url())


@st.cache_data(ttl=600)  # re-query at most every 10 minutes
def load() -> dict:
    eng = engine()
    return {
        "summary": queries.coin_summary(eng),
        "metrics": queries.daily_metrics(eng),
        "runs": queries.recent_runs(eng),
        "checks": queries.latest_checks(eng),
        "rows": queries.total_rows(eng),
    }


def gbp(value: float) -> str:
    """£61,682.24 for big prices, £0.1712 for small ones."""
    return f"£{value:,.2f}" if value >= 1 else f"£{value:,.4f}"


def line_chart(df, y, title, y_label, percent=False):
    fig = px.line(
        df,
        x="price_date",
        y=y,
        color="symbol",
        color_discrete_map=queries.COIN_COLOURS,
        category_orders={"symbol": queries.COIN_ORDER},
        labels={"price_date": "", "symbol": "Coin", y if isinstance(y, str) else "value": y_label},
        title=title,
    )
    fig.update_traces(line={"width": 2})
    fig.update_layout(hovermode="x unified", legend_title_text="", margin={"t": 50, "b": 10})
    if percent:
        fig.update_yaxes(tickformat=".0%")
    return fig


try:
    d = load()
except Exception as exc:  # show a readable message instead of a traceback
    st.error(f"Could not read the database: {type(exc).__name__}: {exc}")
    st.stop()

summary, metrics, runs = d["summary"], d["metrics"], d["runs"]
latest_day = metrics["price_date"].max()

st.title("📈 Daily Crypto Pipeline")
st.caption(
    f"Prices in GBP from CoinGecko, loaded every morning at 06:17 UTC by GitHub Actions, "
    f"checked, and transformed with dbt. Latest data: **{latest_day:%d %b %Y}** (00:00 UTC)."
)

overview, detail, pipeline = st.tabs(["Overview", "Coin detail", "Pipeline health"])

# ---------------------------------------------------------------- Overview
with overview:
    cols = st.columns(len(summary))
    for col, row in zip(cols, summary.itertuples()):
        col.metric(
            row.symbol,
            gbp(row.latest_price_gbp),
            f"{row.return_7d:+.1%} over 7 days" if pd.notna(row.return_7d) else None,
        )

    days = st.radio(
        "Period", [30, 90, 365], index=1, horizontal=True, format_func=lambda n: f"{n} days"
    )
    fig = line_chart(
        queries.rebased(metrics, days),
        "index_100",
        f"Performance over the last {days} days (each coin starts at 100)",
        "Index (start = 100)",
    )
    fig.add_hline(y=100, line_dash="dot", line_color="grey", opacity=0.6)
    st.plotly_chart(fig, width="stretch")

    table = summary[
        [
            "symbol",
            "latest_price_gbp",
            "return_7d",
            "return_30d",
            "volatility_30d_annualised",
            "current_drawdown",
            "max_drawdown",
        ]
    ].rename(
        columns={
            "symbol": "Coin",
            "latest_price_gbp": "Price",
            "return_7d": "7-day return",
            "return_30d": "30-day return",
            "volatility_30d_annualised": VOL,
            "current_drawdown": "Below peak now",
            "max_drawdown": "Worst drop from peak",
        }
    )
    signed = ["7-day return", "30-day return", "Below peak now", "Worst drop from peak"]
    st.dataframe(
        table.style.format({"Price": gbp, VOL: "{:.1%}", **{c: "{:+.1%}" for c in signed}}),
        hide_index=True,
        width="stretch",
    )
    st.caption(
        "Volatility = how much daily returns jump around (standard deviation of the last 30 "
        "daily log returns × √365). Drop from peak = how far below its highest price "
        "since the data starts."
    )

# ---------------------------------------------------------------- Coin detail
with detail:
    symbol = st.selectbox("Coin", queries.COIN_ORDER)
    coin = metrics[metrics["symbol"] == symbol]
    colour = queries.COIN_COLOURS[symbol]

    price = px.line(
        coin.melt(
            id_vars="price_date",
            value_vars=["price_gbp", "ma_7d", "ma_30d"],
            var_name="series",
        ).replace({"price_gbp": "Price", "ma_7d": "7-day average", "ma_30d": "30-day average"}),
        x="price_date",
        y="value",
        color="series",
        color_discrete_map={
            "Price": colour,
            "7-day average": "#52514e",
            "30-day average": "#9a9890",
        },
        labels={"price_date": "", "value": "£", "series": ""},
        title=f"{symbol} price and moving averages",
    )
    price.update_traces(line={"width": 2})
    price.update_layout(hovermode="x unified", margin={"t": 50, "b": 10})
    st.plotly_chart(price, width="stretch")

    left, right = st.columns(2)
    vol = line_chart(coin, "volatility_30d_annualised", "Volatility (30-day, annualised)", "", True)
    vol.update_layout(showlegend=False)
    left.plotly_chart(vol, width="stretch")
    dd = line_chart(coin, "drawdown", "Drop from previous peak", "", True)
    dd.update_traces(fill="tozeroy")
    dd.update_layout(showlegend=False)
    right.plotly_chart(dd, width="stretch")

# ---------------------------------------------------------------- Pipeline health
with pipeline:
    h = queries.health(runs)
    checks = d["checks"]
    a, b, c, e = st.columns(4)
    a.metric("Last run", STATUS.get(h["last_status"], h["last_status"]))
    hours = h["hours_since_success"]
    b.metric("Last success", "never" if hours is None else f"{hours:.0f} h ago")
    passed = int(checks["passed"].sum()) if not checks.empty else 0
    c.metric("Checks passed (latest run)", f"{passed} / {len(checks)}")
    e.metric("Rows stored", f"{d['rows']:,}")

    if hours is not None and hours > 36:
        st.warning("No successful run in the last 36 hours: the daily job may be failing.")

    st.subheader("Recent runs")
    shown = pd.DataFrame(
        {
            "Run": runs["run_id"],
            "Started (UTC)": pd.to_datetime(runs["started_at"], utc=True).dt.strftime(
                "%d %b %H:%M"
            ),
            "Status": runs["status"].map(STATUS).fillna(runs["status"]),
            "Source": runs["source"],
            "Inserted": runs["rows_inserted"],
            "Updated": runs["rows_updated"],
            "Error": runs["error"].fillna("").str.slice(0, 90),
        }
    )
    st.dataframe(shown, hide_index=True, width="stretch")

    st.subheader("Quality checks from the latest run")
    st.dataframe(
        pd.DataFrame(
            {
                "Stage": checks["stage"],
                "Check": checks["check_name"],
                "Result": checks["passed"].map({True: "✅ pass", False: "❌ fail"}),
                "Details": checks["details"],
            }
        ),
        hide_index=True,
        width="stretch",
    )
    st.caption(
        "Batch checks run before loading (bad data never reaches the table), table checks "
        "run after, and dbt's 20 tests run on the transformed tables."
    )
