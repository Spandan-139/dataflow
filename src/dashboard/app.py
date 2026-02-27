import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
from src.warehouse.db import (
    get_top_repos, get_event_distribution,
    get_hourly_activity, get_top_contributors,
    get_summary_stats
)

st.set_page_config(
    page_title="DataFlow Analytics",
    page_icon="🔄",
    layout="wide"
)

st.title("🔄 DataFlow — GitHub Archive Analytics")
st.caption(f"ELT Pipeline | Bronze → Silver → Gold | DuckDB Warehouse | Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

st.divider()

# ── Summary Stats ─────────────────────────────────────────────────────────────
try:
    stats = get_summary_stats()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Events", f"{stats['total_events']:,}")
    with col2:
        st.metric("Repos Tracked", f"{stats['total_repos_tracked']:,}")
    with col3:
        st.metric("Contributors Tracked", f"{stats['total_contributors_tracked']:,}")
    with col4:
        st.metric("Most Common Event", stats['most_common_event'])
except Exception as e:
    st.error(f"Warehouse not ready — run the ETL pipeline first. Error: {e}")
    st.stop()

st.divider()

# ── Event Distribution ────────────────────────────────────────────────────────
st.subheader("Event Distribution")
col1, col2 = st.columns(2)

events = get_event_distribution()
events_df = pd.DataFrame(events)

with col1:
    fig = px.bar(
        events_df.head(10),
        x="count", y="type",
        orientation="h",
        color="event_category",
        title="Top Event Types",
        color_discrete_map={
            "code": "#2196F3",
            "review": "#4CAF50",
            "issues": "#FF9800",
            "social": "#9C27B0",
            "other": "#607D8B"
        }
    )
    fig.update_layout(height=400, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    category_df = events_df.groupby("event_category")["count"].sum().reset_index()
    fig2 = px.pie(
        category_df,
        values="count",
        names="event_category",
        title="Events by Category",
        color_discrete_map={
            "code": "#2196F3",
            "review": "#4CAF50",
            "issues": "#FF9800",
            "social": "#9C27B0",
            "other": "#607D8B"
        }
    )
    fig2.update_layout(height=400)
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Top Repos ─────────────────────────────────────────────────────────────────
st.subheader("Top Repositories by Activity")
n_repos = st.slider("Number of repos to show", 5, 50, 15)
repos = get_top_repos(n_repos)
repos_df = pd.DataFrame(repos)

fig3 = px.bar(
    repos_df,
    x="total_events",
    y="repo_name",
    orientation="h",
    color="push_count",
    hover_data=["star_count", "fork_count", "pr_count", "unique_contributors"],
    title=f"Top {n_repos} Repos by Total Events",
    color_continuous_scale="Blues"
)
fig3.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Top Contributors ──────────────────────────────────────────────────────────
st.subheader("Top Contributors")
col1, col2 = st.columns(2)

contributors = get_top_contributors(20)
contrib_df = pd.DataFrame(contributors)

with col1:
    fig4 = px.bar(
        contrib_df.head(15),
        x="total_events",
        y="actor_login",
        orientation="h",
        title="Top 15 Contributors by Events",
        color="push_count",
        color_continuous_scale="Greens"
    )
    fig4.update_layout(height=450, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig4, use_container_width=True)

with col2:
    fig5 = px.scatter(
        contrib_df,
        x="unique_repos",
        y="total_events",
        hover_name="actor_login",
        size="push_count",
        title="Contributors: Events vs Unique Repos",
        color="org_events",
        color_continuous_scale="Oranges"
    )
    fig5.update_layout(height=450)
    st.plotly_chart(fig5, use_container_width=True)

st.divider()

# ── Raw Data Tables ───────────────────────────────────────────────────────────
st.subheader("Raw Data Explorer")
tab1, tab2, tab3 = st.tabs(["Top Repos", "Event Distribution", "Top Contributors"])

with tab1:
    st.dataframe(repos_df, use_container_width=True)

with tab2:
    st.dataframe(events_df, use_container_width=True)

with tab3:
    st.dataframe(contrib_df, use_container_width=True)

st.divider()

# ── Pipeline Info ─────────────────────────────────────────────────────────────
st.subheader("Pipeline Architecture")
st.markdown("""
| Layer | Storage | Description |
|---|---|---|
| 🥉 Bronze | Parquet | Raw GitHub Archive events — typed, no transformation |
| 🥈 Silver | Parquet | Cleaned, enriched — event categories, repo parsing, flags |
| 🥇 Gold | Parquet | Aggregated analytics — top repos, contributors, distributions |
| 🏛️ Warehouse | DuckDB | Consolidated SQL-queryable database from Gold tables |
| 🚀 API | FastAPI | REST endpoints serving warehouse queries |
| 📊 Dashboard | Streamlit | This dashboard |

**Orchestration:** Prefect flows with task retries and dependency management  
**Dataset:** GitHub Archive — real GitHub events (pushes, PRs, stars, forks, issues)  
**Source:** [gharchive.org](https://www.gharchive.org)
""")