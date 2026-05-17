import json
import time
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer, TopicPartition


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET_DIR = ROOT_DIR / "data" / "crypto_analytics.db"
DEFAULT_METADATA_PATH = ROOT_DIR / "spark-app" / "metadata" / "crypto_metadata.csv"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_KAFKA_TOPIC = "crypto-topic"
PRICE_CHART_BUCKET_SECONDS = 40
PRICE_CHART_HISTORY_MINUTES = 10
PRICE_CHART_LOOKAHEAD_SECONDS = 60


def normalize_prices(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return prices

    prices = prices.copy()
    prices["timestamp"] = pd.to_datetime(prices["timestamp"], errors="coerce")
    prices["price"] = pd.to_numeric(prices["price"], errors="coerce")
    prices = prices.dropna(subset=["symbol", "price", "timestamp"])

    for column in ["coin_name", "category"]:
        if column not in prices:
            prices[column] = ""

    return prices


@st.cache_data(ttl=10)
def read_metadata(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "coin_name", "category"])
    return pd.read_csv(path)[["symbol", "coin_name", "category"]]


@st.cache_data(ttl=10)
def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return normalize_prices(pd.read_parquet(path))


@st.cache_data(ttl=3)
def read_kafka_topic(
    bootstrap_servers: str, topic: str, timeout_ms: int
) -> pd.DataFrame:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return pd.DataFrame()

        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        consumer.assign(topic_partitions)
        consumer.seek_to_beginning(*topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        records = []
        for message in consumer:
            records.append(message.value)
            topic_partition = TopicPartition(message.topic, message.partition)
            if message.offset + 1 >= end_offsets[topic_partition]:
                end_offsets.pop(topic_partition)
                if not end_offsets:
                    break

        return normalize_prices(pd.DataFrame(records))
    finally:
        consumer.close()


def build_analytics(prices: pd.DataFrame) -> pd.DataFrame:
    prices = prices.copy()
    prices["time_bucket"] = prices["timestamp"].dt.floor("2s")
    return (
        prices.groupby(["time_bucket", "symbol", "coin_name", "category"], as_index=False)
        .agg(
            average_price=("price", "mean"),
            max_price=("price", "max"),
            min_price=("price", "min"),
            event_count=("price", "count"),
        )
        .sort_values(["symbol", "time_bucket"])
    )


def enrich_with_metadata(prices: pd.DataFrame, metadata_path: Path) -> pd.DataFrame:
    if prices.empty:
        return prices

    metadata = read_metadata(metadata_path)
    if metadata.empty:
        return prices

    prices = prices.drop(columns=["coin_name", "category"], errors="ignore")
    return normalize_prices(prices.merge(metadata, on="symbol", how="left"))


def build_price_trends(prices: pd.DataFrame) -> pd.DataFrame:
    prices = prices.sort_values("timestamp").copy()
    prices["time_bucket"] = prices["timestamp"].dt.floor(
        f"{PRICE_CHART_BUCKET_SECONDS}s"
    )
    return (
        prices.groupby(["time_bucket", "symbol"], as_index=False)
        .agg(price=("price", "last"))
        .sort_values(["symbol", "time_bucket"])
    )


def render_symbol_price_chart(symbol: str, symbol_prices: pd.DataFrame) -> None:
    latest_bucket = symbol_prices["time_bucket"].max()
    chart_start = latest_bucket - pd.Timedelta(minutes=PRICE_CHART_HISTORY_MINUTES)
    chart_end = latest_bucket + pd.Timedelta(seconds=PRICE_CHART_LOOKAHEAD_SECONDS)
    time_buckets = pd.date_range(
        start=chart_start.floor(f"{PRICE_CHART_BUCKET_SECONDS}s"),
        end=chart_end,
        freq=f"{PRICE_CHART_BUCKET_SECONDS}s",
    )
    chart_data = pd.DataFrame({"time_bucket": time_buckets})
    chart_data = chart_data.merge(
        symbol_prices[["time_bucket", "price"]],
        on="time_bucket",
        how="left",
    )
    chart_data["time_label"] = chart_data["time_bucket"].dt.strftime("%H:%M:%S")
    time_labels = chart_data["time_label"].tolist()
    plotted_data = chart_data.dropna(subset=["price"])
    chart = (
        alt.Chart(plotted_data)
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "time_label:O",
                title="Time",
                axis=alt.Axis(labelAngle=-45),
                scale=alt.Scale(domain=time_labels),
                sort=time_labels,
            ),
            y=alt.Y("price:Q", title="Price", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("time_label:O", title="Time"),
                alt.Tooltip("price:Q", title="Price"),
            ],
        )
        .properties(height=260)
    )
    st.markdown(f"**{symbol} Price**")
    st.altair_chart(chart, use_container_width=True)


def render_price_charts(prices: pd.DataFrame) -> None:
    st.subheader("Real-Time Crypto Prices")
    price_trends = build_price_trends(prices)
    for symbol in sorted(price_trends["symbol"].dropna().unique()):
        symbol_prices = price_trends[price_trends["symbol"] == symbol]
        render_symbol_price_chart(symbol, symbol_prices)


def render_dashboard(prices: pd.DataFrame) -> None:
    latest = prices.sort_values("timestamp").groupby("symbol", as_index=False).tail(1)
    st.subheader("Live Prices")
    st.dataframe(
        latest[["symbol", "coin_name", "category", "price", "timestamp"]].sort_values(
            "symbol"
        ),
        use_container_width=True,
        hide_index=True,
    )

    analytics = build_analytics(prices)

    st.subheader("2-Second Aggregations")
    st.dataframe(
        analytics[
            [
                "symbol",
                "coin_name",
                "category",
                "time_bucket",
                "average_price",
                "max_price",
                "min_price",
                "event_count",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    render_price_charts(prices)


st.set_page_config(page_title="Crypto Streaming Analytics", layout="wide")
st.title("Crypto Streaming Analytics")
data_source = st.sidebar.radio("Data source", ["Kafka topic", "Parquet files"])
refresh_seconds = st.sidebar.number_input(
    "Refresh interval seconds",
    min_value=1,
    max_value=60,
    value=10,
)
auto_refresh = st.sidebar.toggle("Auto refresh", value=True)

if data_source == "Kafka topic":
    kafka_bootstrap_servers = st.sidebar.text_input(
        "Kafka bootstrap servers", DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    )
    kafka_topic = st.sidebar.text_input("Kafka topic", DEFAULT_KAFKA_TOPIC)
    metadata_path = Path(
        st.sidebar.text_input("Metadata CSV", str(DEFAULT_METADATA_PATH))
    )
    kafka_timeout_ms = st.sidebar.number_input(
        "Kafka read timeout milliseconds",
        min_value=250,
        max_value=10000,
        value=1500,
        step=250,
    )
    try:
        prices = read_kafka_topic(kafka_bootstrap_servers, kafka_topic, kafka_timeout_ms)
        prices = enrich_with_metadata(prices, metadata_path)
    except Exception as exc:
        st.error(f"Could not read Kafka topic: {exc}")
        prices = pd.DataFrame()
    empty_message = "Waiting for producer messages in the Kafka topic."
else:
    parquet_dir = Path(st.sidebar.text_input("Parquet directory", str(DEFAULT_PARQUET_DIR)))
    prices = read_parquet(parquet_dir)
    empty_message = "Waiting for Spark to write Parquet data."

st.caption(f"Last refresh: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

if prices.empty:
    st.info(empty_message)
else:
    render_dashboard(prices)

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
