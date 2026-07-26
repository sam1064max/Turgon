import os
import sys
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(
    page_title="Turgon | Medallion Pipeline & AI Agents",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from config.settings import settings
    from pipeline.silver import SilverTransformer
    from pipeline.gold import GoldAggregator
    from agents.data_quality_agent import DataQualityAgent
    from agents.semantic_classifier import SemanticClassifierAgent
    from agents.gold_design_agent import GoldDesignAgent
except Exception as err:
    st.error(f"Startup Import Warning: {err}")
    st.exception(err)

# Custom CSS for modern dark-mode aesthetic
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #4F46E5 0%, #7C3AED 50%, #EC4899 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #9CA3AF;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #1F2937;
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #374151;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    .badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 9999px;
        font-size: 0.85rem;
        font-weight: 600;
        margin-right: 8px;
    }
    .badge-bronze { background-color: #78350F; color: #FDE68A; }
    .badge-silver { background-color: #374151; color: #E5E7EB; }
    .badge-gold { background-color: #854D0E; color: #FEF08A; }
    .badge-agent { background-color: #4C1D95; color: #DDD6FE; }
</style>
""", unsafe_allow_html=True)

# Cache data processing
@st.cache_data(ttl=600)
def load_raw_data():
    raw_path = settings.DATA_FILE_PATH
    if not os.path.exists(raw_path):
        raw_path = os.path.join(settings.BASE_DIR, "raw_tickets (4).csv")
    if os.path.exists(raw_path):
        return pd.read_csv(raw_path)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_silver_data(df_raw):
    transformer = SilverTransformer()
    return transformer.transform_dataframe(df_raw)

# Application Header
st.markdown('<div class="main-header">⚡ Turgon Medallion Pipeline & Agentic Suite</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Production Data Engineering Pipeline (Bronze → Silver → Gold) with Autonomous AI Agents</div>', unsafe_allow_html=True)

# Sidebar Controls
st.sidebar.image("https://raw.githubusercontent.com/sam1064max/Turgon/master/docs/architecture.png" if os.path.exists("docs/architecture.png") else "https://img.shields.io/badge/Architecture-Medallion-blueviolet", use_container_width=True)
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Stage:",
    ["📊 Executive Overview", "🥉 Bronze Layer (Raw)", "🥈 Silver Layer (Cleaned)", "🥇 Gold Layer (Analytics)", "🤖 AI Agent Control Room"]
)

# Load data
df_bronze = load_raw_data()
df_silver, summary_silver = get_silver_data(df_bronze) if not df_bronze.empty else (pd.DataFrame(), {})

# --- PAGE 1: EXECUTIVE OVERVIEW ---
if page == "📊 Executive Overview":
    st.header("📊 Pipeline Executive Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Bronze Ingested Rows", f"{len(df_bronze):,}", delta="Schema-on-Read")
    with col2:
        st.metric("Silver Cleansed Rows", f"{len(df_silver):,}", delta=f"-{summary_silver.get('duplicates_removed', 0)} Deduped")
    with col3:
        breach_rate = (df_silver['is_sla_breached'].mean() * 100) if 'is_sla_breached' in df_silver.columns else 0
        st.metric("Overall SLA Breach Rate", f"{breach_rate:.1f}%", delta_color="inverse")
    with col4:
        st.metric("Data Quality Rules Applied", "15 Rules", delta="100% Automated")

    st.markdown("---")
    
    st.subheader("🏗️ Medallion Architecture Workflow")
    st.markdown("""
    ```
    ┌─────────────────────────┐      ┌───────────────────────────┐      ┌───────────────────────────┐
    │      BRONZE LAYER       │ ───► │       SILVER LAYER        │ ───► │        GOLD LAYER         │
    │  Raw CSV Ingestion      │      │  Cleansed & Deduplicated  │      │  Business Aggregations    │
    │  Lineage Metadata & SHA │      │  15 Data Quality Rules    │      │  SLA, Vendor & Volume     │
    └─────────────────────────┘      └───────────────────────────┘      └───────────────────────────┘
                                                   ▲
                                                   │ Autonomous Rule Proposals & Category Mapping
                                     ┌───────────────────────────┐
                                     │     AI AGENTS SUITE       │
                                     │  Data Quality & Semantic  │
                                     └───────────────────────────┘
    ```
    """)
    
    st.subheader("📈 Quick Category & Priority Snapshot")
    col_a, col_b = st.columns(2)
    with col_a:
        if 'category' in df_silver.columns:
            cat_counts = df_silver['category'].value_counts().reset_index()
            cat_counts.columns = ['Category', 'Ticket Count']
            fig_cat = px.bar(cat_counts, x='Ticket Count', y='Category', orientation='h', 
                             title="Tickets by Canonical Category", color='Category',
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig_cat, use_container_width=True)
    with col_b:
        if 'priority' in df_silver.columns:
            prio_counts = df_silver['priority'].value_counts().reset_index()
            prio_counts.columns = ['Priority', 'Count']
            fig_prio = px.pie(prio_counts, values='Count', names='Priority', 
                              title="Priority Distribution", hole=0.4,
                              color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig_prio, use_container_width=True)

# --- PAGE 2: BRONZE LAYER ---
elif page == "🥉 Bronze Layer (Raw)":
    st.header("🥉 Bronze Layer: Raw Schema-on-Read Ingestion")
    st.markdown("The Bronze layer ingests raw operational support tickets without schema loss, capturing source audit metadata and computing SHA-256 row hashes.")

    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**Total Raw Rows:** {len(df_bronze):,}")
    with col2:
        st.info(f"**Raw Columns:** {len(df_bronze.columns)} fields stored as raw text")

    st.subheader("🔍 Raw Data Sample")
    st.dataframe(df_bronze.head(100), use_container_width=True)

    st.subheader("⚠️ Detected Raw Data Quality Anomalies")
    st.markdown("""
    - **Date Format Heterogeneity**: Mix of ISO 8601, `MM/DD/YYYY`, `DD-Mon-YYYY HH:MM`, and **Unix Epoch timestamps**.
    - **Category Chaos**: Over ~60 messy category values + 33 description-category column swaps.
    - **Priority Variants**: 20 distinct strings (`lo`, `med`, `crit`, `urgent!!!`, `???`).
    - **Cost Sentinels**: `$150.00`, `TBD`, `error`, `-1`.
    - **SLA Sentinels**: `-1`, `0`, `999`, `N/A`.
    """)

# --- PAGE 3: SILVER LAYER ---
elif page == "🥈 Silver Layer (Cleaned)":
    st.header("🥈 Silver Layer: Cleansed & Deduplicated Schema")
    st.markdown("The Silver layer applies **15 explicit cleaning rules** to standardize types, resolve sentinels, generate synthetic IDs, and clean submitter names.")

    st.subheader("📋 Rule Execution Summary")
    st.json(summary_silver)

    st.subheader("✨ Canonical Category Normalization Visualizer")
    if 'category' in df_silver.columns and 'category' in df_bronze.columns:
        col_left, col_right = st.columns(2)
        with col_left:
            st.write("**Raw Categories (Sample 20)**")
            st.dataframe(df_bronze['category'].dropna().unique()[:20], use_container_width=True)
        with col_right:
            st.write("**Canonical Categories (Cleaned)**")
            st.dataframe(df_silver['category'].value_counts(), use_container_width=True)

    st.subheader("🔍 Cleansed Silver Data Table")
    st.dataframe(df_silver.head(100), use_container_width=True)

# --- PAGE 4: GOLD LAYER ---
elif page == "🥇 Gold Layer (Analytics)":
    st.header("🥇 Gold Layer: Business Analytics & Aggregations")
    st.markdown("High-value operational metrics aggregated for facility managers, procurement, and operations teams.")

    tab1, tab2, tab3 = st.tabs(["🎯 SLA Performance", "🤝 Vendor Scorecards", "📈 Ticket Volume & Trends"])

    with tab1:
        st.subheader("SLA Breach Performance by Category & Priority")
        if not df_silver.empty and 'is_sla_breached' in df_silver.columns:
            sla_agg = df_silver.groupby(['category', 'priority'])['is_sla_breached'].agg(
                total_tickets='count',
                sla_breaches='sum',
                breach_rate=lambda x: (x.sum() / x.count()) * 100
            ).reset_index()

            fig_sla = px.bar(sla_agg, x='category', y='breach_rate', color='priority', barmode='group',
                             title="SLA Breach Rate (%) by Category and Priority",
                             labels={'breach_rate': 'Breach Rate (%)', 'category': 'Category'},
                             color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig_sla, use_container_width=True)
            st.dataframe(sla_agg, use_container_width=True)

    with tab2:
        st.subheader("Vendor Performance Scorecards")
        if not df_silver.empty and 'assigned_to' in df_silver.columns:
            vendor_agg = df_silver.groupby('assigned_to').agg(
                assigned_tickets=('ticket_id', 'count'),
                avg_resolution_hours=('resolution_hours', 'mean'),
                avg_cost=('cost_cleaned', 'mean'),
                sla_breach_rate=('is_sla_breached', lambda x: (x.sum() / x.count()) * 100)
            ).reset_index()

            fig_vendor = px.scatter(vendor_agg, x='avg_resolution_hours', y='sla_breach_rate', size='assigned_tickets',
                                    color='assigned_to', text='assigned_to', title="Vendor Efficiency: Resolution Time vs SLA Breach Rate",
                                    labels={'avg_resolution_hours': 'Avg Resolution Hours', 'sla_breach_rate': 'SLA Breach Rate (%)'})
            st.plotly_chart(fig_vendor, use_container_width=True)
            st.dataframe(vendor_agg, use_container_width=True)

    with tab3:
        st.subheader("Support Volume & Cost Trends")
        if not df_silver.empty and 'created_at' in df_silver.columns:
            df_trend = df_silver.dropna(subset=['created_at']).copy()
            df_trend['created_month'] = pd.to_datetime(df_trend['created_at']).dt.to_period('M').astype(str)
            trend_agg = df_trend.groupby('created_month').agg(
                monthly_volume=('ticket_id', 'count'),
                monthly_cost=('cost_cleaned', 'sum')
            ).reset_index()

            fig_trend = px.line(trend_agg, x='created_month', y='monthly_volume', title="Monthly Support Ticket Volume Trend",
                                markers=True, line_shape='spline')
            st.plotly_chart(fig_trend, use_container_width=True)

# --- PAGE 5: AI AGENT CONTROL ROOM ---
elif page == "🤖 AI Agent Control Room":
    st.header("🤖 AI Agent Control Room")
    st.markdown("Interact with autonomous agents designed to profile data quality, classify semantic categories, and engineer gold aggregations.")

    agent_choice = st.selectbox("Choose AI Agent:", [
        "Data Quality Profiler Agent",
        "Semantic Category Classifier Agent",
        "Gold Layer Design Agent"
    ])

    if agent_choice == "Data Quality Profiler Agent":
        st.subheader("🔍 Data Quality Profiler Agent")
        st.write("Profiles raw bronze data and formulates structured cleaning rules.")
        if st.button("Run Data Quality Profiler Agent"):
            agent = DataQualityAgent()
            with st.spinner("Analyzing bronze sample with LLM..."):
                proposals = agent.profile_data(df_bronze.head(100) if not df_bronze.empty else pd.DataFrame())
                st.success("Analysis Complete!")
                st.json(proposals)

    elif agent_choice == "Semantic Category Classifier Agent":
        st.subheader("🏷️ Semantic Category Classifier Agent")
        st.write("Test fuzzy category classification or column-swap detection.")
        test_input = st.text_input("Enter raw category string or swapped sentence:", "Aircon broken in room 302, leaking water")
        if st.button("Classify Category"):
            agent = SemanticClassifierAgent()
            with st.spinner("Classifying with LLM..."):
                res = agent.classify_categories([test_input])
                st.success("Classification Result:")
                st.json(res)

    elif agent_choice == "Gold Layer Design Agent":
        st.subheader("📐 Gold Layer Design Agent")
        st.write("Generates proposed gold SQL models given silver schema specifications.")
        if st.button("Propose Gold Models"):
            agent = GoldDesignAgent()
            with st.spinner("Designing Gold Models..."):
                silver_schema_sample = "CREATE TABLE silver.tickets (ticket_id VARCHAR, category VARCHAR, priority VARCHAR, created_at TIMESTAMP, resolved_at TIMESTAMP, cost_cleaned NUMERIC, is_sla_breached BOOLEAN);"
                models = agent.propose_gold_models(silver_schema_sample)
                st.success("Gold Models Proposed:")
                st.json(models)

st.sidebar.markdown("---")
st.sidebar.caption("⚡ Built with Python, Streamlit, PostgreSQL & OpenAI")
