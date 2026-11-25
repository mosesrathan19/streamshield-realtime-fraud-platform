"""StreamShield local operations & performance dashboard.

Launch from repository root:
    streamlit run dashboard/app.py
Read-only against existing local PostgreSQL snapshots and benchmark JSON.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from data import benchmarks, postgres_snapshots, DEFAULT_DSN
from health import dependency_checks, report_checks
from kafka_observability import kafka_topic_offsets
from kafka_groups import kafka_consumer_groups

st.set_page_config(
    page_title="StreamShield | Streaming Analytics",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      .block-container {padding-top: 2rem; padding-bottom: 3rem; max-width: 1500px;}
      [data-testid="stMetric"] {
        background: #132237; border: 1px solid #263d55; padding: 1.1rem;
        border-radius: 14px;
      }
      [data-testid="stMetricLabel"] {color: #a6b8c9;}
      [data-testid="stMetricValue"] {color: #ecf7ff;}
      .eyebrow {font-size: .8rem; letter-spacing: .18rem; color: #48d6bf;
                font-weight: 750; text-transform: uppercase;}
      .subtitle {color: #9cb1c2; font-size: .95rem; margin-bottom: 1.5rem;}
      .status-pill {display: inline-block; padding: .25rem .65rem;
                    background: #153d39; border: 1px solid #286359;
                    border-radius: 999px; color: #90f1d3; font-size:.78rem;}
      .small-note {color:#92aabc; font-size:.83rem;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=30, show_spinner=False)
def read_postgres(dsn: str) -> dict:
    return postgres_snapshots(dsn)


@st.cache_data(ttl=30, show_spinner=False)
def read_benchmarks() -> list[dict]:
    return benchmarks()


with st.sidebar:
    st.markdown("## 🛡️ StreamShield")
    st.caption("STREAMING ANALYTICS · LOCAL DEV")
    page = st.radio("Workspace", ["Overview", "Transactions & Risk", "Performance", "System Health", "Kafka Topics", "Consumer Groups", "Pipeline Architecture"])
    st.divider()
    st.markdown("**Data sources**")
    st.caption("PostgreSQL: historical backfill snapshot")
    st.caption("Benchmarks: saved local JSON reports")
    st.info("This dashboard is read-only. It does not start Kafka or Spark jobs, or load benchmark events into PostgreSQL.")
    if st.button("Refresh data", use_container_width=True):
        read_postgres.clear()
        read_benchmarks.clear()
        st.rerun()

st.markdown('<div class="eyebrow">STREAMSHIELD / OBSERVABILITY</div>', unsafe_allow_html=True)
st.title("Transaction Intelligence")
st.markdown(
    '<div class="subtitle">Synthetic transaction analytics and verified pipeline benchmarks. '
    'Historical demo snapshots — not a live production monitor.</div>',
    unsafe_allow_html=True,
)

dsn = os.environ.get("STREAMSHIELD_PG_DSN", DEFAULT_DSN)
db = None
db_error = None
if page in ("Overview", "Transactions & Risk"):
    try:
        db = read_postgres(dsn)
    except Exception as exc:
        db_error = exc

try:
    runs = read_benchmarks()
except (OSError, ValueError, KeyError, TypeError) as exc:
    runs = []
    st.warning(f"Could not read a benchmark JSON file: {exc}")


def show_metric_row(snapshot: dict):
    total = snapshot["total"]
    flagged = snapshot["flagged"]
    cols = st.columns(4)
    cols[0].metric("Transactions", f"{total:,}")
    cols[1].metric("Synthetic risk flags", f"{flagged:,}")
    cols[2].metric("Risk-flag rate", f"{100 * flagged / total:.2f}%" if total else "0.00%")
    cols[3].metric("Unique accounts", f"{snapshot['accounts']:,}")


def show_risk_charts(snapshot: dict):
    categories = snapshot["categories"]
    st.subheader("Merchant category activity")
    if not categories:
        st.info("No PostgreSQL transaction data found.")
        return
    df = pd.DataFrame(categories)
    left, right = st.columns([3, 2], gap="large")
    with left:
        st.caption("Transactions by category")
        st.bar_chart(df.set_index("category")[["transactions"]], color="#39b6bd", height=310)
    with right:
        st.caption("Synthetic risk-flag rate by category")
        st.bar_chart(df.set_index("category")[["flagged_pct"]],
                     color="#e9a96a", height=310)
    with st.expander("View category metrics"):
        st.dataframe(df, hide_index=True, use_container_width=True)


def show_daily(snapshot: dict):
    st.subheader("Daily transaction volume (UTC)")
    if snapshot["daily"]:
        df = pd.DataFrame(snapshot["daily"])
        st.line_chart(df.set_index("day")[["transactions", "flags"]],
                      color=["#3bd4ba", "#eca76a"], height=300)
        st.caption("Event dates come from synthetic event timestamps, not today's real-time traffic.")
    else:
        st.info("No daily data available.")


def show_performance(data: list[dict]):
    st.subheader("Measured Kafka → Spark throughput")
    if not data:
        st.info("Saved benchmark reports not found. Complete Phase 6 trials first.")
        return
    df = pd.DataFrame(data)
    first, second = st.columns(2, gap="large")
    with first:
        st.caption("Acknowledged Kafka producer rate · events/sec")
        st.bar_chart(df.set_index("run")[["producer_events_per_sec"]],
                     color="#37c8b7", height=300)
    with second:
        st.caption("Spark query persisted rate · events/sec")
        st.bar_chart(df.set_index("run")[["spark_events_per_sec"]],
                     color="#4a95f0", height=300)
    st.subheader("Run details")
    cols = ["run", "transactions", "synthetic_flags", "producer_seconds",
            "spark_seconds", "producer_events_per_sec", "spark_events_per_sec"]
    st.dataframe(df[cols], hide_index=True, use_container_width=True,
                 column_config={
                     "run": "Run",
                     "transactions": st.column_config.NumberColumn("Events", format="%d"),
                     "synthetic_flags": st.column_config.NumberColumn("Risk flags", format="%d"),
                     "producer_seconds": st.column_config.NumberColumn("Kafka seconds", format="%.3f"),
                     "spark_seconds": st.column_config.NumberColumn("Spark seconds", format="%.3f"),
                     "producer_events_per_sec": st.column_config.NumberColumn("Kafka events/sec", format="%.1f"),
                     "spark_events_per_sec": st.column_config.NumberColumn("Spark events/sec", format="%.1f"),
                 })
    st.info(
        "The producer rate measures JSON generation, Kafka publish and acknowledgement. "
        "The Spark rate includes query start, availableNow processing and checkpointed Parquet "
        "completion, but excludes SparkSession startup, publishing and final verification. "
        "These are separate local runs, not simultaneous end-to-end throughput or p95 latency."
    )


if page == "Overview":
    st.markdown('<span class="status-pill">LOCAL · SYNTHETIC DATA</span>', unsafe_allow_html=True)
    if db_error:
        st.warning("PostgreSQL is unavailable. Start the local database or select another page. "
                   "Use the clean UTC demo database for reliable daily dates.")
        st.caption(f"Connection detail: {type(db_error).__name__}: {db_error}")
    elif db is not None:
        st.caption("POSTGRESQL SNAPSHOT · NOT THE 100K/1M STREAMING BENCHMARK DATASET")
        show_metric_row(db)
        st.divider()
        show_risk_charts(db)
    st.divider()
    show_performance(runs)

elif page == "Transactions & Risk":
    if db_error:
        st.error("PostgreSQL snapshot unavailable. Check Docker and STREAMSHIELD_PG_DSN.")
        st.caption(f"Connection detail: {type(db_error).__name__}: {db_error}")
    elif db:
        show_metric_row(db)
        st.divider()
        show_daily(db)
        st.divider()
        show_risk_charts(db)
        st.divider()
        st.subheader("Illustrative risk-rule breakdown")
        risk = db["risk_reasons"]
        c1, c2, c3 = st.columns(3)
        c1.metric("High amount", f"{risk['High amount']:,}")
        c2.metric("Synthetic-country anomaly", f"{risk['Synthetic country']:,}")
        c3.metric("Both (overlap)", f"{risk['Both (overlap)']:,}")
        st.caption("These flags are simple synthetic rules, not predictions of real fraud.")

elif page == "Performance":
    show_performance(runs)

elif page == "Kafka Topics":
    st.subheader("Kafka topic offsets — read-only snapshot")
    st.caption(
        "Broker metadata and earliest/latest retained offsets, queried on page load "
        "or refresh. Offset spans are not exact message counts and are NOT consumer lag."
    )
    try:
        offsets = kafka_topic_offsets()
    except Exception as exc:
        st.warning("Kafka offset inspection unavailable. Check broker health and "
                   "STREAMSHIELD_KAFKA_BOOTSTRAP.")
        st.caption(f"{type(exc).__name__}: {exc}")
    else:
        if not offsets:
            st.info("No matching streamshield.* topics found on this Kafka broker.")
        else:
            df = pd.DataFrame(offsets)
            c1, c2, c3 = st.columns(3)
            c1.metric("Topics inspected", str(df["topic"].nunique()))
            c2.metric("Partitions inspected", str(len(df)))
            c3.metric("Retained offset span", f'{int(df["offset_span"].sum()):,}')
            st.dataframe(df.rename(columns={
                "topic": "Topic", "partition": "Partition",
                "earliest": "Earliest retained offset", "latest": "Latest offset",
                "offset_span": "Retained offset span",
            }), hide_index=True, use_container_width=True)
    st.info(
        "This page does not create a consumer group, consume or commit offsets. "
        "Kafka log offsets alone cannot tell whether Spark has caught up. "
        "Spark checkpoint progress and consumer-group lag require separate instrumentation."
    )

elif page == "Consumer Groups":
    st.subheader("Kafka consumer groups — committed offsets")
    st.caption(
        "Read-only point-in-time broker snapshot of existing consumer groups. "
        "Lag is computed only for partitions with comparable committed offsets. "
        "No consumer is started or registered by this page."
    )
    try:
        groups = kafka_consumer_groups()
    except Exception as exc:
        st.warning("Kafka group inspection unavailable; check the broker and Docker logs.")
        st.caption(f"{type(exc).__name__}: {exc}")
    else:
        a, b, d = st.columns(3)
        a.metric("Existing groups discovered", groups["group_count"])
        b.metric("Groups inspected", groups["inspected_groups"])
        d.metric("Group partitions with commits", sum(x["committed"] is not None for x in groups["rows"]))
        if groups["skipped_groups"]:
            st.warning(f'{groups["skipped_groups"]} groups were not inspected due to the safety cap.')
        if not groups["rows"]:
            st.info(
                "No committed offsets for streamshield.* topics were found in the inspected "
                "groups. This is normal when Spark uses checkpoint-based offset tracking "
                "instead of Kafka group commits. No lag can be calculated."
            )
        else:
            rows = groups["rows"]
            comparable = [x for x in rows if x["lag"] is not None]
            if comparable:
                st.metric("Comparable committed-offset lag (inspected partitions)",
                          f'{sum(x["lag"] for x in comparable):,}')
            else:
                st.info("No comparable committed offsets were found; lag is unavailable.")
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    st.info(
        "Group lag here is latest Kafka offset minus committed group offset, only when "
        "that offset is within the retained broker range. It is not Spark checkpoint "
        "progress, processing latency, or proof that a pipeline is healthy."
    )

elif page == "System Health":
    st.subheader("Local component reachability")
    st.caption("Point-in-time TCP checks. Reachable does not guarantee broker readiness, SQL query health, consumer-group progress, or end-to-end latency.")
    dependencies = dependency_checks()
    for item in dependencies:
        status = "Reachable" if item["reachable"] else "Unreachable"
        st.write(f'**{item["component"]}:** {status} · `{item["endpoint"]}`')
    st.divider()
    st.subheader("Saved benchmark report files")
    files = report_checks()
    if files:
        st.dataframe(
            pd.DataFrame([{"Run": x["run"], "Report": x["report"],
                           "Status": "Present" if x["present"] else "Missing"}
                          for x in files]),
            hide_index=True, use_container_width=True,
        )
    else:
        st.info("No benchmark report paths configured.")
    st.info("No consumer-lag polling, live alert delivery, or continuous Spark driver inspection is implemented. Refresh manually to run these checks again.")

else:
    st.subheader("Architecture and measurement boundaries")
    st.code(
        "Deterministic synthetic generator\n"
        "           |\n"
        "           v\n"
        "     Kafka (6 partitions)\n"
        "           |\n"
        "           v\n"
        "  Spark Structured Streaming\n"
        "  schema validation · 10-minute watermark\n"
        "  event-ID deduplication · risk flags\n"
        "           |\n"
        "           v\n"
        "  Checkpointed Parquet outputs\n"
        "           |\n"
        "           v (separate batch/backfill)\n"
        "    PostgreSQL SQL views\n"
        "           |\n"
        "           v\n"
        "  Streamlit dashboard (read-only)",
        language="text",
    )
    st.markdown(
        "**Data boundaries:** PostgreSQL represents the earlier Phase 4/5 "
        "snapshot (typically 10K–10.1K events depending on selected demo DB). "
        "The 100K and 1M Spark benchmarks remain in Parquet and are displayed "
        "only through the saved aggregate JSON files, not merged into the DB."
    )
    st.markdown(
         "**Not implemented as real-time monitoring yet:** automatic Kafka polling, "
        "consumer-group health, live alert delivery, end-to-end latency percentiles, "
        "automatic incident alerts or continuously streaming PostgreSQL writes."
    )
