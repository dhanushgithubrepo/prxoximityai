import streamlit as st
import requests
import pandas as pd
from io import BytesIO
import plotly.express as px

st.set_page_config(page_title="Proximity AI - Customer Intelligence", layout="wide")

st.title("🧠 Proximity AI: Customer Intelligence Dashboard")
st.markdown("Upload a CSV/Excel file to analyze customer RFM, churn risk, and get AI-driven insights.")

# Sidebar: File upload and AI toggle
with st.sidebar:
    st.header("⚙️ Settings")
    uploaded_file = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx", "xls"])
    use_ai = st.checkbox("🤖 Enable AI Insights (Gemini)", value=True)

if uploaded_file:
    # Prepare file for FastAPI
    files = {"file": (uploaded_file.name, uploaded_file.read(), uploaded_file.type)}
    params = {"use_ai": use_ai}
    try:
        response = requests.post("http://127.0.0.1:8000/analyze/rfm", files=files, params=params)
        if response.status_code != 200:
            st.error(f"API Error: {response.status_code} — {response.text}")
            st.stop()
        data = response.json()
    except requests.exceptions.ConnectionError:
        st.error("❌ Could not connect to FastAPI backend. Start it with: `uvicorn main:app --reload`")
        st.stop()

    summary = data.get("summary", {})
    customers = pd.DataFrame(data.get("customers", []))
    ai_insights = data.get("ai_insights", {})
    agent = data.get("agent", {})
    agent_actions = agent.get("actions", [])
    discount_codes = agent.get("discount_codes", [])
    agent_metadata = agent.get("metadata", {})
    is_cached = data.get("cached", False)

    # Cache status
    with st.sidebar:
        st.header("💾 Cache Status")
        try:
            cache_stats = requests.get("http://127.0.0.1:8000/cache/stats").json()
            cache_type = cache_stats.get("cache_type", "Unknown")
            st.metric("Cache Type", cache_type)
            st.metric("Total Keys", cache_stats.get("total_keys", 0))
            st.metric("Memory Used", cache_stats.get("used_memory", "0B"))
            if cache_type == "Redis":
                st.metric("Hit Rate", f"{cache_stats.get('keyspace_hits', 0)}/{cache_stats.get('keyspace_hits', 0) + cache_stats.get('keyspace_misses', 1)}")
            if st.button("Clear Cache"):
                requests.delete("http://127.0.0.1:8000/cache/clear")
                st.success("Cache cleared!")
                st.rerun()
        except Exception as e:
            st.warning(f"Cache service unavailable: {e}")

    # Summary Metrics
    st.header("📊 Summary")
    col1, col2, col3, col4 = st.columns(4)
    tier_counts = summary.get("tier_counts", {})
    segment_counts = summary.get("segment_counts", {})

    with col1:
        st.metric("Total Customers", sum(tier_counts.values()))
    with col2:
        st.metric("At-Risk", tier_counts.get("At-Risk", 0))
    with col3:
        st.metric("Watchlist", tier_counts.get("Watchlist", 0))
    with col4:
        st.metric("Healthy", tier_counts.get("Healthy", 0))

    # Agent Metrics
    if agent_metadata:
        st.subheader("🤖 Agent Actions")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Routed Customers", agent_metadata.get("routed_customers", 0))
        with col2:
            st.metric("Generated Discounts", agent_metadata.get("generated_discounts", 0))
        with col3:
            st.metric("Pending Actions", len(agent_actions))
    
    if is_cached:
        st.success("⚡ Results loaded from cache (fast!)")
    else:
        st.info("🔄 Results computed fresh")

    # Charts
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🎯 Tier Distribution")
        fig_tier = px.pie(
            names=list(tier_counts.keys()),
            values=list(tier_counts.values()),
            title="Customer Tiers"
        )
        st.plotly_chart(fig_tier, use_container_width=True)
    with col2:
        st.subheader("📈 Segment Distribution")
        fig_seg = px.pie(
            names=list(segment_counts.keys()),
            values=list(segment_counts.values()),
            title="Customer Segments"
        )
        st.plotly_chart(fig_seg, use_container_width=True)

    # Customer Table with Filters
    st.header("👥 Customers")
    tier_filter = st.multiselect("Filter by Tier", options=customers["tier"].unique().tolist(), default=customers["tier"].unique().tolist())
    segment_filter = st.multiselect("Filter by Segment", options=customers["segment"].unique().tolist(), default=customers["segment"].unique().tolist())
    filtered = customers[(customers["tier"].isin(tier_filter)) & (customers["segment"].isin(segment_filter))]
    st.dataframe(filtered, use_container_width=True)

    # Agent Actions Table
    if agent_actions:
        st.header("🤖 Agent-Generated Actions")
        actions_df = pd.DataFrame(agent_actions)
        # Format datetime
        if "scheduled_at" in actions_df.columns:
            actions_df["scheduled_at"] = pd.to_datetime(actions_df["scheduled_at"]).dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(actions_df[[
            "customer_name", "customer_email", "tier", "segment", "action_type", 
            "discount_code", "priority", "scheduled_at"
        ]], use_container_width=True)

        # Show discount codes
        if discount_codes:
            st.subheader("🎟️ Generated Discount Codes")
            st.code(", ".join(discount_codes))

    # AI Insights
    if use_ai and ai_insights:
        st.header("🤖 AI Insights")
        st.subheader("📝 Executive Summary")
        st.info(ai_insights.get("insights_summary", "No summary available."))

        st.subheader("🎯 Recommended Actions")
        for action in ai_insights.get("recommended_actions", []):
            st.success(f"- {action}")

        st.subheader("📧 Email Templates")
        templates = ai_insights.get("email_templates", [])
        if templates:
            for i, tmpl in enumerate(templates[:3], 1):
                with st.expander(f"Email {i}: {tmpl.get('subject', 'No Subject')}"):
                    st.markdown(f"**To:** {tmpl.get('to', '')}")
                    st.markdown(f"**Subject:** {tmpl.get('subject', '')}")
                    st.text_area("Body", tmpl.get("body", ""), height=150, disabled=True)
        else:
            st.warning("No email templates available.")
else:
    st.info("👆 Upload a CSV/Excel file to begin.")
