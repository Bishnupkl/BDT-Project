from pathlib import Path

import pandas as pd
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET_DIR = ROOT_DIR / "data" / "crypto_analytics.db"
st.set_page_config(page_title="Crypto Streaming Analytics", layout="wide")
st.title("Crypto Streaming Analytics")
parquet_dir = Path(st.sidebar.text_input("Parquet directory", str(DEFAULT_PARQUET_DIR)))


@st.cache_data(ttl=10)
def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)
prices = read_parquet(parquet_dir)

if prices.empty:
    st.info("Waiting for Spark to write Parquet data.")
    st.stop()
prices["timestamp"] = pd.to_datetime(prices["timestamp"])
latest = prices.sort_values("timestamp").groupby("symbol", as_index=False).tail(1)
st.subheader("Live Prices")
st.dataframe(
    latest[["symbol", "coin_name", "category", "price", "timestamp"]].sort_values("symbol"),
    use_container_width=True,
    hide_index=True,
)
prices["minute"] = prices["timestamp"].dt.floor("min")
analytics = (
    prices.groupby(["minute", "symbol", "coin_name", "category"], as_index=False)
    .agg(
        average_price=("price", "mean"),
        max_price=("price", "max"),
        min_price=("price", "min"),
        event_count=("price", "count"),
    )
    .sort_values(["symbol", "minute"])
)
st.subheader("1-Minute Aggregations")
st.dataframe(
    analytics[
        [
            "symbol",
            "coin_name",
            "category",
            "minute",
            "average_price",
            "max_price",
            "min_price",
            "event_count",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)
st.subheader("Average Price Trend")
trend = analytics.pivot_table(
    index="minute",
    columns="symbol",
    values="average_price",
    aggfunc="last",
)
st.line_chart(trend)
