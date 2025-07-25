"""GitHub Intelligence Dashboard."""
import streamlit as st
import plotly.express as px
import pandas as pd
import snowflake.connector
import os

st.set_page_config(page_title="GitHub Intelligence", page_icon="🐙", layout="wide")

@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"], warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "GITHUB_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "GITHUB_DB"), schema="MARTS",
    )

@st.cache_data(ttl=3600)
def q(sql): return pd.read_sql(sql, get_conn())

st.title("🐙 GitHub Repository Intelligence")

# Top repos
repos = q("SELECT * FROM dim_repositories ORDER BY stars DESC LIMIT 50")
if not repos.empty:
    st.header("Top Repositories")
    fig = px.bar(repos.head(20), x="FULL_NAME", y="STARS", color="LANGUAGE", title="Top 20 by Stars")
    st.plotly_chart(fig, use_container_width=True)

    # Language distribution
    st.header("Language Distribution")
    lang_counts = repos.groupby("LANGUAGE").agg({"FULL_NAME": "count", "STARS": "mean"}).reset_index()
    lang_counts.columns = ["Language", "Count", "Avg Stars"]
    fig2 = px.pie(lang_counts.head(10), names="Language", values="Count", title="Top Languages")
    st.plotly_chart(fig2, use_container_width=True)

# Activity trends
activity = q("SELECT * FROM fct_repo_daily_activity ORDER BY activity_date DESC LIMIT 365")
if not activity.empty:
    st.header("Commit Activity Trends")
    daily_total = activity.groupby("ACTIVITY_DATE")["COMMIT_COUNT"].sum().reset_index()
    fig3 = px.line(daily_total, x="ACTIVITY_DATE", y="COMMIT_COUNT", title="Daily Commits Across All Repos")
    st.plotly_chart(fig3, use_container_width=True)

# Contributors
contribs = q("SELECT * FROM dim_contributors ORDER BY total_commits DESC LIMIT 30")
if not contribs.empty:
    st.header("Top Contributors")
    st.dataframe(contribs[["AUTHOR_NAME", "TOTAL_COMMITS", "REPOS_CONTRIBUTED_TO", "ACTIVE_DAYS"]].head(20))
