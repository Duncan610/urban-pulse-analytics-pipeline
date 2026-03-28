"""
UrbanPulse — City Intelligence Analytics Dashboard
====================================================
Connects to Snowflake MARTS layer and visualises key findings.
Run locally: streamlit run streamlit/app.py
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="UrbanPulse — NYC City Intelligence",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# CUSTOM CSS
# =============================================================================
st.markdown("""
<style>
    .stApp { background-color: #0a0e1a; color: #e8eaf0; }
    [data-testid="stSidebar"] {
        background-color: #0d1220;
        border-right: 1px solid #1e2a3a;
    }
    [data-testid="metric-container"] {
        background-color: #111827;
        border: 1px solid #1e2a3a;
        border-radius: 12px;
        padding: 16px;
    }
    h1, h2, h3 { color: #e8eaf0 !important; font-family: 'Georgia', serif; }
    [data-testid="stMetricValue"] {
        color: #38bdf8 !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
    }
    [data-testid="stMetricLabel"] { color: #94a3b8 !important; }
    hr { border-color: #1e2a3a; }
    .section-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #38bdf8;
        margin-bottom: 0.5rem;
        padding-bottom: 0.3rem;
        border-bottom: 2px solid #1e3a5f;
    }
    .finding-card {
        background: linear-gradient(135deg, #111827, #0d1f35);
        border: 1px solid #1e3a5f;
        border-left: 4px solid #38bdf8;
        border-radius: 8px;
        padding: 16px 20px;
        margin: 8px 0;
    }
    .footer {
        text-align: center;
        color: #475569;
        font-size: 0.8rem;
        padding: 20px;
        border-top: 1px solid #1e2a3a;
        margin-top: 40px;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# SNOWFLAKE CONNECTION
# Works both locally (via .env) and on Streamlit Cloud (via st.secrets)
# =============================================================================
def get_secret(key: str) -> str:
    """
    Get a secret value — checks Streamlit secrets first,
    falls back to environment variables from .env file.
    This makes the app work both locally and on Streamlit Cloud.
    """
    try:
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, "")


@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=get_secret("SNOWFLAKE_ACCOUNT"),
        user=get_secret("SNOWFLAKE_USER"),
        password=get_secret("SNOWFLAKE_PASSWORD"),
        role=get_secret("SNOWFLAKE_ROLE"),
        warehouse="URBANPULSE_TRANSFORM_WH",
        database="URBANPULSE",
        schema="MARTS",
    )


@st.cache_data(ttl=3600)
def run_query(query: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    return pd.read_sql(query, conn)


# =============================================================================
# DATA LOADING
# =============================================================================
@st.cache_data(ttl=3600)
def load_key_metrics():
    return run_query("""
        SELECT
            COUNT(*)                                    AS total_complaints,
            COUNT(DISTINCT borough)                     AS total_boroughs,
            COUNT(DISTINCT complaint_type)              AS unique_complaint_types,
            ROUND(AVG(response_time_hours), 1)          AS avg_response_hours,
            SUM(CASE WHEN is_rainy THEN 1 ELSE 0 END)  AS rainy_day_complaints,
            SUM(CASE WHEN is_closed THEN 1 ELSE 0 END) AS closed_complaints
        FROM URBANPULSE.MARTS.FACT_311
    """)


@st.cache_data(ttl=3600)
def load_borough_summary():
    return run_query("""
        SELECT
            borough,
            COUNT(*)                                AS total_complaints,
            ROUND(AVG(response_time_hours), 1)      AS avg_response_hours,
            SUM(CASE WHEN is_rainy
                THEN 1 ELSE 0 END)                  AS rainy_complaints,
            MAX(borough_median_income)               AS median_income,
            MAX(borough_poverty_rate)                AS poverty_rate,
            MAX(borough_income_bracket)              AS income_bracket
        FROM URBANPULSE.MARTS.FACT_311
        WHERE borough IS NOT NULL
        GROUP BY borough
        ORDER BY total_complaints DESC
    """)


@st.cache_data(ttl=3600)
def load_complaint_types():
    return run_query("""
        SELECT
            complaint_type,
            COUNT(*)                            AS total,
            ROUND(AVG(response_time_hours), 1) AS avg_response_hours,
            SUM(CASE WHEN is_rainy
                THEN 1 ELSE 0 END)             AS rainy_count
        FROM URBANPULSE.MARTS.FACT_311
        WHERE complaint_type IS NOT NULL
        GROUP BY complaint_type
        ORDER BY total DESC
        LIMIT 15
    """)


@st.cache_data(ttl=3600)
def load_weather_impact():
    return run_query("""
        SELECT
            is_rainy,
            COUNT(*)                            AS total_complaints,
            ROUND(AVG(response_time_hours), 1) AS avg_response_hours,
            COUNT(DISTINCT complaint_type)      AS unique_types
        FROM URBANPULSE.MARTS.FACT_311
        WHERE is_rainy IS NOT NULL
        GROUP BY is_rainy
    """)


@st.cache_data(ttl=3600)
def load_daily_trend():
    return run_query("""
        SELECT
            complaint_date,
            borough,
            total_complaints,
            is_rainy,
            temp_celsius,
            avg_response_time_hours
        FROM URBANPULSE.MARTS.FACT_COMPLAINTS
        WHERE complaint_date IS NOT NULL
        ORDER BY complaint_date ASC
    """)


@st.cache_data(ttl=3600)
def load_income_vs_response():
    return run_query("""
        SELECT
            borough,
            borough_income_bracket,
            MAX(borough_median_income)          AS median_income,
            MAX(borough_poverty_rate)           AS poverty_rate,
            ROUND(AVG(response_time_hours), 1) AS avg_response_hours,
            COUNT(*)                            AS total_complaints,
            SUM(CASE WHEN is_slow_response
                THEN 1 ELSE 0 END)             AS slow_responses
        FROM URBANPULSE.MARTS.FACT_311
        WHERE borough IS NOT NULL
          AND borough_median_income IS NOT NULL
        GROUP BY borough, borough_income_bracket
        ORDER BY median_income DESC
    """)


# =============================================================================
# SIDEBAR
# =============================================================================
with st.sidebar:
    st.markdown("## 🏙️ UrbanPulse")
    st.markdown("*NYC City Intelligence Platform*")
    st.markdown("---")
    st.markdown("### Data Sources")
    st.markdown("📋 NYC 311 Service Requests")
    st.markdown("🌤️ OpenWeather API")
    st.markdown("📊 US Census Bureau ACS5")
    st.markdown("---")
    st.markdown("### Tech Stack")
    st.markdown("❄️ Snowflake")
    st.markdown("🔄 dbt Core")
    st.markdown("🐍 Python")
    st.markdown("🌊 Apache Airflow")
    st.markdown("---")
    st.markdown("### Pipeline Stats")
    st.markdown("✅ 57/57 dbt tests passing")
    st.markdown("🏗️ 10 dbt models")
    st.markdown("📐 3-layer medallion")
    st.markdown("---")
    st.markdown(
        "[GitHub Repo](https://github.com/Duncan610/urban-pulse-analytics-pipeline)",
        unsafe_allow_html=True
    )


# =============================================================================
# MAIN DASHBOARD
# =============================================================================
st.markdown("# 🏙️ UrbanPulse")
st.markdown("### NYC City Intelligence — Weather, Demographics & Service Analytics")
st.markdown("---")

with st.spinner("Loading data from Snowflake..."):
    metrics_df        = load_key_metrics()
    borough_df        = load_borough_summary()
    complaint_type_df = load_complaint_types()
    weather_df        = load_weather_impact()
    daily_df          = load_daily_trend()
    income_df         = load_income_vs_response()

# =============================================================================
# SECTION 1 — KEY METRICS
# =============================================================================
st.markdown('<p class="section-header">📊 Key Metrics</p>', unsafe_allow_html=True)

m = metrics_df.iloc[0]
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Complaints", f"{int(m['TOTAL_COMPLAINTS']):,}")
with col2:
    st.metric("Avg Response Time", f"{m['AVG_RESPONSE_HOURS']}h")
with col3:
    st.metric("Complaint Types", f"{int(m['UNIQUE_COMPLAINT_TYPES'])}")
with col4:
    st.metric("Rainy Day Complaints", f"{int(m['RAINY_DAY_COMPLAINTS']):,}")
with col5:
    closed_pct = round(
        int(m['CLOSED_COMPLAINTS']) / int(m['TOTAL_COMPLAINTS']) * 100, 1
    )
    st.metric("Resolution Rate", f"{closed_pct}%")

st.markdown("---")

# =============================================================================
# SECTION 2 — KEY FINDINGS
# =============================================================================
st.markdown('<p class="section-header">🔍 Key Findings</p>', unsafe_allow_html=True)

rainy = weather_df[weather_df['IS_RAINY'] == True]
dry   = weather_df[weather_df['IS_RAINY'] == False]
rainy_count = int(rainy['TOTAL_COMPLAINTS'].values[0]) if not rainy.empty else 0
dry_count   = int(dry['TOTAL_COMPLAINTS'].values[0]) if not dry.empty else 0

if not income_df.empty:
    sorted_income          = income_df.sort_values('MEDIAN_INCOME')
    lowest_income_borough  = sorted_income.iloc[0]
    highest_income_borough = sorted_income.iloc[-1]
else:
    lowest_income_borough  = {
        'BOROUGH': 'N/A', 'MEDIAN_INCOME': 0,
        'AVG_RESPONSE_HOURS': 0, 'POVERTY_RATE': 0
    }
    highest_income_borough = {
        'BOROUGH': 'N/A', 'MEDIAN_INCOME': 0,
        'AVG_RESPONSE_HOURS': 0, 'POVERTY_RATE': 0
    }

col1, col2 = st.columns(2)

with col1:
    st.markdown(f"""
    <div class="finding-card">
        <strong>🌧️ Weather Impact on Complaints</strong><br><br>
        Rainy days: <strong>{rainy_count:,}</strong> complaints
        vs <strong>{dry_count:,}</strong> on dry days.
        Weather is a measurable driver of 311 service demand.
    </div>
    """, unsafe_allow_html=True)

    if not borough_df.empty:
        st.markdown(f"""
        <div class="finding-card">
            <strong>🏘️ Borough with Most Complaints</strong><br><br>
            <strong>{borough_df.iloc[0]['BOROUGH']}</strong> leads with
            <strong>{int(borough_df.iloc[0]['TOTAL_COMPLAINTS']):,}</strong> complaints —
            {borough_df.iloc[0]['INCOME_BRACKET']} income bracket,
            {borough_df.iloc[0]['POVERTY_RATE']}% poverty rate.
        </div>
        """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="finding-card">
        <strong>💰 Income & Response Time Gap</strong><br><br>
        <strong>{highest_income_borough['BOROUGH']}</strong>
        (${int(highest_income_borough['MEDIAN_INCOME']):,} median income)
        avg response: <strong>{highest_income_borough['AVG_RESPONSE_HOURS']}h</strong><br>
        <strong>{lowest_income_borough['BOROUGH']}</strong>
        (${int(lowest_income_borough['MEDIAN_INCOME']):,} median income)
        avg response: <strong>{lowest_income_borough['AVG_RESPONSE_HOURS']}h</strong>
    </div>
    """, unsafe_allow_html=True)

    income_gap = int(
        highest_income_borough['MEDIAN_INCOME'] -
        lowest_income_borough['MEDIAN_INCOME']
    )
    st.markdown(f"""
    <div class="finding-card">
        <strong>🏙️ Income Disparity</strong><br><br>
        A <strong>${income_gap:,}</strong> income gap exists between
        <strong>{highest_income_borough['BOROUGH']}</strong> and
        <strong>{lowest_income_borough['BOROUGH']}</strong>
        — two boroughs in the same city.
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# =============================================================================
# SECTION 3 — BOROUGH ANALYSIS
# =============================================================================
st.markdown(
    '<p class="section-header">🗺️ Borough Analysis</p>',
    unsafe_allow_html=True
)

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(
        borough_df,
        x="BOROUGH",
        y="TOTAL_COMPLAINTS",
        color="INCOME_BRACKET",
        title="Total Complaints by Borough",
        color_discrete_map={
            "LOW": "#ef4444", "MEDIUM": "#f59e0b", "HIGH": "#22c55e"
        },
        template="plotly_dark",
    )
    fig.update_layout(
        paper_bgcolor="#111827", plot_bgcolor="#111827",
        font_color="#e8eaf0", showlegend=True,
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig2 = px.bar(
        borough_df,
        x="BOROUGH",
        y="AVG_RESPONSE_HOURS",
        color="INCOME_BRACKET",
        title="Average Response Time by Borough (Hours)",
        color_discrete_map={
            "LOW": "#ef4444", "MEDIUM": "#f59e0b", "HIGH": "#22c55e"
        },
        template="plotly_dark",
    )
    fig2.update_layout(
        paper_bgcolor="#111827", plot_bgcolor="#111827",
        font_color="#e8eaf0",
    )
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# =============================================================================
# SECTION 4 — INCOME VS RESPONSE TIME
# =============================================================================
st.markdown(
    '<p class="section-header">💰 Income vs Service Response Time</p>',
    unsafe_allow_html=True
)

if not income_df.empty:
    fig3 = px.scatter(
        income_df,
        x="MEDIAN_INCOME",
        y="AVG_RESPONSE_HOURS",
        size="TOTAL_COMPLAINTS",
        color="BOROUGH",
        text="BOROUGH",
        title="Does Borough Income Affect Response Time?",
        labels={
            "MEDIAN_INCOME":      "Median Household Income ($)",
            "AVG_RESPONSE_HOURS": "Avg Response Time (Hours)",
            "TOTAL_COMPLAINTS":   "Total Complaints",
        },
        template="plotly_dark",
    )
    fig3.update_traces(textposition="top center")
    fig3.update_layout(
        paper_bgcolor="#111827", plot_bgcolor="#111827",
        font_color="#e8eaf0", height=450,
    )
    st.plotly_chart(fig3, use_container_width=True)

st.markdown("---")

# =============================================================================
# SECTION 5 — TOP COMPLAINT TYPES
# =============================================================================
st.markdown(
    '<p class="section-header">📋 Top 15 Complaint Types</p>',
    unsafe_allow_html=True
)

fig4 = px.bar(
    complaint_type_df,
    x="TOTAL",
    y="COMPLAINT_TYPE",
    orientation="h",
    title="Most Common 311 Complaint Types",
    color="AVG_RESPONSE_HOURS",
    color_continuous_scale="Blues",
    labels={
        "TOTAL":              "Number of Complaints",
        "COMPLAINT_TYPE":     "Complaint Type",
        "AVG_RESPONSE_HOURS": "Avg Response (hrs)",
    },
    template="plotly_dark",
)
fig4.update_layout(
    paper_bgcolor="#111827", plot_bgcolor="#111827",
    font_color="#e8eaf0", height=500,
    yaxis={"categoryorder": "total ascending"},
)
st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# =============================================================================
# SECTION 6 — DAILY TREND
# =============================================================================
st.markdown(
    '<p class="section-header">📈 Daily Complaint Volume</p>',
    unsafe_allow_html=True
)

if not daily_df.empty:
    daily_total = daily_df.groupby(
        "COMPLAINT_DATE"
    )["TOTAL_COMPLAINTS"].sum().reset_index()

    fig5 = px.line(
        daily_total,
        x="COMPLAINT_DATE",
        y="TOTAL_COMPLAINTS",
        title="Daily 311 Complaint Volume Over Time",
        labels={
            "COMPLAINT_DATE":   "Date",
            "TOTAL_COMPLAINTS": "Total Complaints",
        },
        template="plotly_dark",
    )
    fig5.update_traces(line_color="#38bdf8", line_width=2)
    fig5.update_layout(
        paper_bgcolor="#111827", plot_bgcolor="#111827",
        font_color="#e8eaf0", height=350,
    )
    st.plotly_chart(fig5, use_container_width=True)

# =============================================================================
# FOOTER
# =============================================================================
st.markdown("""
<div class="footer">
    Built by Duncan Otieno &nbsp;|&nbsp;
    Stack: Python · Snowflake · dbt Core · Apache Airflow · Streamlit &nbsp;|&nbsp;
    <a href="https://github.com/Duncan610/urban-pulse-analytics-pipeline"
       style="color: #38bdf8;">GitHub</a> &nbsp;|&nbsp;
    <a href="https://www.linkedin.com/in/duncan-otieno"
       style="color: #38bdf8;">LinkedIn</a>
</div>
""", unsafe_allow_html=True)