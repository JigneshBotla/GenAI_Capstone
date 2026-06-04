import streamlit as st
import pandas as pd
import datetime
import time
import os
from agent_core import AgentCore
from database_manager import MetadataHelper

# Page config
st.set_page_config(
    page_title="Deco - RAG-Powered Data Engineering Assistant",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom premium styling (Glassmorphism & Sleek Dark Theme)
st.markdown(
    """
    <style>
    /* Dark Theme Core Adjustments */
    .stApp {
        background-color: #0d1117;
        color: #c9d1d9;
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #161b22 !important;
        border-right: 1px solid #30363d;
    }
    
    /* Premium Headers */
    h1, h2, h3 {
        color: #58a6ff !important;
        font-family: 'Outfit', 'Inter', sans-serif;
    }
    
    /* Styled Card Containers */
    .premium-card {
        background: rgba(22, 27, 34, 0.6);
        backdrop-filter: blur(12px);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.25);
    }
    
    /* Neon Status Badges */
    .badge-success {
        background-color: rgba(46, 160, 67, 0.15);
        color: #3fb950;
        border: 1px solid rgba(46, 160, 67, 0.4);
        padding: 4px 8px;
        border-radius: 20px;
        font-size: 0.85em;
        font-weight: bold;
    }
    
    .badge-failed {
        background-color: rgba(248, 81, 73, 0.15);
        color: #f85149;
        border: 1px solid rgba(248, 81, 73, 0.4);
        padding: 4px 8px;
        border-radius: 20px;
        font-size: 0.85em;
        font-weight: bold;
    }
    
    .badge-pii {
        background-color: rgba(210, 153, 34, 0.15);
        color: #d29922;
        border: 1px solid rgba(210, 153, 34, 0.4);
        padding: 2px 6px;
        border-radius: 4px;
        font-size: 0.75em;
        font-weight: bold;
    }
    
    /* Lineage Graph Card styling */
    .lineage-node {
        background: #21262d;
        border: 1px solid #58a6ff;
        border-radius: 8px;
        padding: 12px;
        text-align: center;
        margin: 5px 0;
        font-weight: bold;
        color: #ffffff;
    }
    
    .lineage-arrow {
        text-align: center;
        color: #8b949e;
        font-size: 1.5em;
        margin: 2px 0;
    }
    
    /* Custom buttons */
    .stButton>button {
        background-color: #21262d !important;
        color: #c9d1d9 !important;
        border: 1px solid #30363d !important;
        border-radius: 6px !important;
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        border-color: #58a6ff !important;
        color: #58a6ff !important;
        background-color: #30363d !important;
        box-shadow: 0 0 10px rgba(88, 166, 255, 0.2);
    }
    
    /* Decorative glowing accents */
    .glow-border {
        border-left: 4px solid #58a6ff;
        padding-left: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ----------------- SESSION STATE SETUP -----------------
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "👋 **Hello, I am Deco!** Your RAG-Powered Data Engineering Co-pilot.\n\nAsk me about our **medallion architecture**, trace **data lineage**, view **schemas and PII settings**, troubleshoot **recent failures**, or request me to **trigger active data quality checks** on staging/mart tables."
        }
    ]

# Initialize helper classes
metadata_helper = MetadataHelper()

# Helper tools

# Caching utility function for Snowflake metadata
def refresh_snowflake_data(force=False):
    if "snowflake_data" not in st.session_state or force:
        with st.spinner("Fetching latest metadata from Snowflake..."):
            try:
                # 1. Fetch tables (1 query)
                tables = metadata_helper._execute_query("SELECT table_id, schema_name, table_name, description, row_count, size_bytes FROM public.tables")
                if isinstance(tables, dict) and "error" in tables:
                    raise Exception(tables["error"])
                
                # 2. Fetch columns in a single query (1 query)
                columns = metadata_helper._execute_query("SELECT table_id, column_name, data_type, description, is_pii, pii_type, masking_policy FROM public.columns")
                if isinstance(columns, dict) and "error" in columns:
                    raise Exception(columns["error"])
                
                # 3. Fetch lineage in a single query (1 query)
                lineage = metadata_helper._execute_query("SELECT source_table, target_table, lineage_type FROM public.lineage")
                if isinstance(lineage, dict) and "error" in lineage:
                    raise Exception(lineage["error"])
                
                # 4. Fetch pipeline runs (most recent 15)
                runs = metadata_helper.get_pipeline_runs(limit=15)
                if isinstance(runs, dict) and "error" in runs:
                    raise Exception(runs["error"])
                
                # 5. Fetch DQ pipeline run history (dynamic success rates per bronze table)
                try:
                    dq_history = metadata_helper.get_dq_pipeline_history()
                except Exception as dh_err:
                    print(f"Warning: DQ pipeline history fetch failed: {dh_err}")
                    dq_history = {}
                
                # 6. Fetch live DQ summary from Snowflake (runs actual DQ queries)
                try:
                    dq_summary = metadata_helper.get_dq_summary()
                except Exception as dq_err:
                    print(f"Warning: DQ summary fetch failed: {dq_err}")
                    dq_summary = {}
                
                # Assemble columns into details dict
                table_details = {}
                if isinstance(tables, list):
                    for t in tables:
                        table_details[t["table_id"]] = {
                            "table": t,
                            "columns": []
                        }
                if isinstance(columns, list):
                    for col in columns:
                        tid = col["table_id"]
                        if tid in table_details:
                            table_details[tid]["columns"].append(col)
                            
                # Assemble lineage into cache dict
                lineage_cache = {}
                if isinstance(tables, list):
                    for t in tables:
                        lineage_cache[t["table_id"]] = {
                            "table_id": t["table_id"],
                            "upstream": [],
                            "downstream": []
                        }
                if isinstance(lineage, list):
                    for edge in lineage:
                        src = edge["source_table"]
                        tgt = edge["target_table"]
                        l_type = edge["lineage_type"]
                        
                        if tgt in lineage_cache:
                            lineage_cache[tgt]["upstream"].append({
                                "source_table": src,
                                "lineage_type": l_type
                            })
                        if src in lineage_cache:
                            lineage_cache[src]["downstream"].append({
                                "target_table": tgt,
                                "lineage_type": l_type
                            })
                
                st.session_state.snowflake_data = {
                    "tables": tables,
                    "table_details": table_details,
                    "lineage_cache": lineage_cache,
                    "runs": runs,
                    "dq_summary": dq_summary,
                    "dq_history": dq_history,
                    "error": None
                }
            except Exception as e:
                st.session_state.snowflake_data = {
                    "tables": [],
                    "table_details": {},
                    "lineage_cache": {},
                    "runs": [],
                    "dq_summary": {},
                    "dq_history": {},
                    "error": str(e)
                }

# Initialize/Refresh cache
refresh_snowflake_data(force=False)

snowflake_data = st.session_state.snowflake_data


# ----------------- SIDEBAR CONFIG -----------------
with st.sidebar:
    st.image("artificial-intelligence.png", width=75)
    st.title("Deco Engine Control")
    st.caption("Settings & Integration Hub")
    st.markdown("---")
    
    # Available Bedrock models
    BEDROCK_MODELS = {
        "⚡ Nova Lite  (fast, cost-efficient)":    "amazon.nova-lite-v1:0",
        "🚀 Nova Pro  (balanced, recommended)":    "amazon.nova-pro-v1:0",
        "🧠 Nova Micro  (ultra-fast, minimal)":   "amazon.nova-micro-v1:0"
    }

    aws_region = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1"))

    # Model selector
    selected_model_label = st.selectbox(
        "🤖 Select Bedrock Model",
        options=list(BEDROCK_MODELS.keys()),
        index=1,  # default: Nova Pro
        key="selected_model_label",
        help="Choose the foundation model to power Deco's reasoning."
    )
    selected_model_id = BEDROCK_MODELS[selected_model_label]

    # Instantiate the agent using environment credentials and selected model
    agent = AgentCore(region_name=aws_region, model_id=selected_model_id)

    if agent.has_aws:
        st.success("✅ AWS Bedrock Connected")
        st.caption(f"Active model ID: `{selected_model_id}`")
        st.success(f"🤖 AWS Bedrock Active — **{selected_model_label.split('(')[0].strip()}**")
    else:
        st.error("🔌 AWS Bedrock NOT Configured")
        st.info("💡 Please set AWS credentials in your .env file to use Deco.")

        
    st.markdown("---")
    st.subheader("⚙️ Agentic Control Room")
    
    # Active Action Button — only bronze tables are valid DQ ETL targets
    st.markdown("**Manual Trigger Quality Assertions**")
    dq_table = st.selectbox(
        "Select Bronze Source Table:",
        ["bronze.raw_users", "bronze.raw_transactions"],
        help="Triggers data generation + full ETL pipeline on the selected bronze table"
    )
    if st.button("🚀 Trigger Data Quality Suite"):
        with st.spinner("Executing Data Quality validations..."):
            time.sleep(1.5)
            dq_result = agent.tool_trigger_data_quality_check(dq_table)
            
            # Append result directly into the chat log!
            st.session_state.messages.append({
                "role": "user",
                "content": f"Manual action: Trigger data quality check on `{dq_table}`."
            })
            st.session_state.messages.append({
                "role": "assistant",
                "content": dq_result
            })
            # Force cache reload so changes reflect on the control panels
            refresh_snowflake_data(force=True)
            st.success(f"Checks completed for {dq_table}!")
            st.rerun()
            
    st.markdown("---")
    st.subheader("🔄 Refresh Data Catalog")
    if st.button("🔄 Refresh from Snowflake", use_container_width=True):
        refresh_snowflake_data(force=True)
        st.success("Successfully refreshed cache!")
        st.rerun()

# ----------------- MAIN NAVIGATION TABS -----------------
tab_dashboard, tab_chatbot = st.tabs(["📊 DE Platform Control Panel", "🤖 Chat with Deco"])

with tab_dashboard:
    st.title("📊 DE Platform Control Panel")
    st.caption("Live Data Catalog, System Lineage & Operational Observability")
    
    # Create tab structure
    tab_catalog, tab_lineage, tab_operations = st.tabs(["🗂️ Data Catalog", "🔗 Data Lineage", "⚡ Operations & SLOs"])
    
    # TAB 1: DATA CATALOG
    with tab_catalog:
        st.subheader("Registered Data Assets")
        tables = snowflake_data["tables"]
        if snowflake_data["error"]:
            st.error(f"Error loading tables from Snowflake: {snowflake_data['error']}")
        elif not tables:
            st.info("No conformed tables found in the Snowflake catalog. Please check your credentials or run the setup script.")
        else:
            # Display as styled cards
            for t in tables:
                row_count_val = t['row_count'] if t['row_count'] is not None else 0
                size_mb = t['size_bytes'] / (1024*1024) if t['size_bytes'] is not None else 0.0
                st.markdown(
                    f"""
                    <div class="premium-card glow-border">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <h4 style="margin: 0; color: #58a6ff;">{t['table_id']}</h4>
                            <span class="badge-success">{row_count_val:,} rows</span>
                        </div>
                        <p style="margin: 8px 0; font-size: 0.9em; color: #8b949e;">{t['description']}</p>
                        <div style="font-size: 0.85em; color: #c9d1d9;">
                            Warehouse Layer: <code>{t['schema_name']}</code> | Size: <code>{size_mb:.2f} MB</code>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                
                # Collapsible column detail table
                with st.expander(f"Inspect `{t['table_name']}` Column Definitions"):
                    details = snowflake_data["table_details"].get(t['table_id'])
                    if details:
                        cols_df = pd.DataFrame(details["columns"])
                        # Standardize column display
                        cols_df["is_pii"] = cols_df["is_pii"].map(lambda x: "🚨 PII" if x else "✅ No")
                        cols_df["masking_policy"] = cols_df["masking_policy"].fillna("N/A")
                        st.dataframe(
                            cols_df[["column_name", "data_type", "description", "is_pii", "masking_policy"]],
                            hide_index=True,
                            use_container_width=True
                        )

    # TAB 2: DATA LINEAGE
    with tab_lineage:
        st.subheader("Medallion Platform Lineage Map")
        st.caption("Structured data transformations tracing Bronze raw landing databases up to Gold business BI views.")
        
        registered_tables = snowflake_data["tables"]
        if registered_tables:
            table_ids = ["All Tables (Default)"] + [t["table_id"] for t in registered_tables]
            selected_lineage_table = st.selectbox(
                "Select Target Table to Trace Lineage:", 
                table_ids, 
                index=0
            )
            
            def get_node_style(table_id):
                if table_id.startswith("bronze"):
                    return "rgba(248, 81, 73, 0.15)", "#f85149", "🪣 Bronze"
                elif table_id.startswith("staging"):
                    if "quarantine" in table_id:
                        return "rgba(248, 81, 73, 0.1)", "#f85149", "🚨 Quarantine"
                    return "rgba(210, 153, 34, 0.15)", "#d29922", "✨ Silver"
                elif table_id.startswith("marts"):
                    return "rgba(46, 160, 67, 0.15)", "#3fb950", "🏆 Gold"
                else:
                    return "#161b22", "#58a6ff", "📊 Table"

            def get_recursive_lineage(target_table, lineage_cache):
                upstream_edges = []
                downstream_edges = []
                
                # Recursive helper for upstream (finding parents)
                visited_upstream = set()
                def trace_upstream(table):
                    if table in visited_upstream:
                        return
                    visited_upstream.add(table)
                    lin = lineage_cache.get(table)
                    if lin and lin.get("upstream"):
                        for u in lin["upstream"]:
                            src = u["source_table"]
                            upstream_edges.append((src, table, u.get("lineage_type", "Direct")))
                            trace_upstream(src)
                            
                # Recursive helper for downstream (finding children)
                visited_downstream = set()
                def trace_downstream(table):
                    if table in visited_downstream:
                        return
                    visited_downstream.add(table)
                    lin = lineage_cache.get(table)
                    if lin and lin.get("downstream"):
                        for d in lin["downstream"]:
                            tgt = d["target_table"]
                            downstream_edges.append((table, tgt, d.get("lineage_type", "Direct")))
                            trace_downstream(tgt)
                            
                trace_upstream(target_table)
                trace_downstream(target_table)
                
                return upstream_edges, downstream_edges

            # Generate and render HTML diagram
            if selected_lineage_table == "All Tables (Default)":
                # Full Medallion platform flow overview in HTML
                b_u_bg, b_u_border, b_u_badge = get_node_style("bronze.raw_users")
                b_t_bg, b_t_border, b_t_badge = get_node_style("bronze.raw_transactions")
                
                s_u_bg, s_u_border, s_u_badge = get_node_style("staging.stg_users")
                s_uq_bg, s_uq_border, s_uq_badge = get_node_style("staging.stg_users_quarantine")
                s_t_bg, s_t_border, s_t_badge = get_node_style("staging.stg_transactions")
                s_tq_bg, s_tq_border, s_tq_badge = get_node_style("staging.stg_transactions_quarantine")
                
                g_t_bg, g_t_border, g_t_badge = get_node_style("marts.fct_user_transactions")
                g_c_bg, g_c_border, g_c_badge = get_node_style("marts.fct_user_churn")

                st.markdown(
                    f"""<div class="premium-card" style="display: flex; justify-content: space-around; align-items: stretch; gap: 20px; width: 100%;">
<div style="flex: 1; display: flex; flex-direction: column; gap: 15px; align-items: center; justify-content: center; background: rgba(22, 27, 34, 0.4); padding: 15px; border-radius: 8px; border: 1px solid #30363d;">
<div style="font-weight: bold; color: #f85149; font-size: 0.95em; border-bottom: 2px solid #f85149; padding-bottom: 4px; width: 100%; text-align: center;">🪣 BRONZE LAYER (RAW)</div>
<div class="lineage-node" style="background: {b_u_bg}; border: 1px solid {b_u_border}; width: 100%; font-size: 0.9em;">{b_u_badge}: raw_users</div>
<div class="lineage-node" style="background: {b_t_bg}; border: 1px solid {b_t_border}; width: 100%; font-size: 0.9em;">{b_t_badge}: raw_transactions</div>
</div>
<div style="font-size: 1.5em; color: #58a6ff; align-self: center; user-select: none;">➡️</div>
<div style="flex: 1.3; display: flex; flex-direction: column; gap: 12px; align-items: center; justify-content: center; background: rgba(22, 27, 34, 0.4); padding: 15px; border-radius: 8px; border: 1px solid #30363d;">
<div style="font-weight: bold; color: #d29922; font-size: 0.95em; border-bottom: 2px solid #d29922; padding-bottom: 4px; width: 100%; text-align: center;">✨ SILVER LAYER (STAGING)</div>
<div style="display: flex; gap: 8px; width: 100%;">
<div class="lineage-node" style="background: {s_u_bg}; border: 1px solid {s_u_border}; flex: 1.2; font-size: 0.85em;">{s_u_badge}: stg_users</div>
<div class="lineage-node" style="background: {s_uq_bg}; border: 1px solid {s_uq_border}; flex: 0.8; font-size: 0.8em; color: #f85149;">Quarantine</div>
</div>
<div style="display: flex; gap: 8px; width: 100%;">
<div class="lineage-node" style="background: {s_t_bg}; border: 1px solid {s_t_border}; flex: 1.2; font-size: 0.85em;">{s_t_badge}: stg_transactions</div>
<div class="lineage-node" style="background: {s_tq_bg}; border: 1px solid {s_tq_border}; flex: 0.8; font-size: 0.8em; color: #f85149;">Quarantine</div>
</div>
</div>
<div style="font-size: 1.5em; color: #58a6ff; align-self: center; user-select: none;">➡️</div>
<div style="flex: 1.1; display: flex; flex-direction: column; gap: 15px; align-items: center; justify-content: center; background: rgba(22, 27, 34, 0.4); padding: 15px; border-radius: 8px; border: 1px solid #30363d;">
<div style="font-weight: bold; color: #3fb950; font-size: 0.95em; border-bottom: 2px solid #3fb950; padding-bottom: 4px; width: 100%; text-align: center;">🏆 GOLD LAYER (MARTS)</div>
<div class="lineage-node" style="background: {g_t_bg}; border: 1px solid {g_t_border}; width: 100%; font-size: 0.85em;">{g_t_badge}: fct_user_transactions</div>
<div class="lineage-node" style="background: {g_c_bg}; border: 1px solid {g_c_border}; width: 100%; font-size: 0.85em;">{g_c_badge}: fct_user_churn</div>
</div>
</div>""",
                    unsafe_allow_html=True
                )
            else:
                # Recursive tracing for single table
                upstream_edges, downstream_edges = get_recursive_lineage(selected_lineage_table, snowflake_data["lineage_cache"])
                
                upstream_tables = []
                visited_u = set()
                for src, tgt, l_type in upstream_edges:
                    if src not in visited_u and src != selected_lineage_table:
                        visited_u.add(src)
                        upstream_tables.append({"table": src, "type": l_type})
                        
                downstream_tables = []
                visited_d = set()
                for src, tgt, l_type in downstream_edges:
                    if tgt not in visited_d and tgt != selected_lineage_table:
                        visited_d.add(tgt)
                        downstream_tables.append({"table": tgt, "type": l_type})
                
                # 1. Upstream sources HTML
                upstream_html = ""
                if upstream_tables:
                    for u in upstream_tables:
                        src = u["table"]
                        bg, border, badge = get_node_style(src)
                        upstream_html += f'<div class="lineage-node" style="background: {bg}; border: 1px solid {border}; margin-bottom: 10px; width: 100%;">{badge}: {src}<br/><span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(Type: {u["type"]})</span></div>'
                else:
                    upstream_html = "<div class='lineage-node' style='border: 1px dashed #30363d; color: #8b949e; background: transparent; width: 100%;'>None (Ingestion Source)</div>"

                # 2. Target table HTML
                bg, border, badge = get_node_style(selected_lineage_table)
                target_html = f'<div class="lineage-node" style="background: {bg}; border: 2px solid {border}; box-shadow: 0 0 15px {border}40; width: 100%;">{badge}: {selected_lineage_table}<br/><span style="font-size: 0.85em; font-weight: bold; color: #ffffff;">[TARGET]</span></div>'

                # 3. Downstream consumers HTML
                downstream_html = ""
                if downstream_tables:
                    for d in downstream_tables:
                        tgt = d["table"]
                        bg, border, badge = get_node_style(tgt)
                        downstream_html += f'<div class="lineage-node" style="background: {bg}; border: 1px solid {border}; margin-bottom: 10px; width: 100%;">{badge}: {tgt}<br/><span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(Type: {d["type"]})</span></div>'
                else:
                    downstream_html = "<div class='lineage-node' style='border: 1px dashed #30363d; color: #8b949e; background: transparent; width: 100%;'>None (Final Mart Endpoint)</div>"

                # Render styled HTML columns
                st.markdown(
                    f"""<div class="premium-card" style="display: flex; justify-content: space-around; align-items: center; gap: 15px; width: 100%;">
<div style="flex: 1; display: flex; flex-direction: column; align-items: center; text-align: center;">
<div style="font-weight: bold; margin-bottom: 10px; color: #8b949e; font-size: 0.9em;">UPSTREAM SOURCES</div>
{upstream_html}
</div>
<div style="font-size: 1.8em; color: #58a6ff; user-select: none; align-self: center;">➡️</div>
<div style="flex: 1.2; display: flex; flex-direction: column; align-items: center; text-align: center;">
<div style="font-weight: bold; margin-bottom: 10px; color: #58a6ff; font-size: 0.95em;">TARGET TABLE</div>
{target_html}
</div>
<div style="font-size: 1.8em; color: #58a6ff; user-select: none; align-self: center;">➡️</div>
<div style="flex: 1; display: flex; flex-direction: column; align-items: center; text-align: center;">
<div style="font-weight: bold; margin-bottom: 10px; color: #8b949e; font-size: 0.9em;">DOWNSTREAM CONSUMERS</div>
{downstream_html}
</div>
</div>""",
                    unsafe_allow_html=True
                )
        else:
            st.warning("Could not retrieve tables to render lineage. Verify Snowflake connection.")


    # TAB 3: OPERATIONS & SLO COMPLIANCE
    with tab_operations:

        # ── Section 1: Live Data Quality Results ──────────────────────────────
        st.subheader("🔬 Live Data Quality Results")
        st.caption("Metrics are fetched directly from Snowflake and reflect the latest data state. Use the sidebar refresh to update.")
        
        dq = snowflake_data.get("dq_summary", {})
        
        if snowflake_data["error"] and not dq:
            st.error(f"Could not load DQ metrics: {snowflake_data['error']}")
        elif not dq:
            st.info("No DQ results yet. Click **🔄 Refresh from Snowflake** in the sidebar or **🚀 Trigger Data Quality Suite** above.")
        else:
            # ── Bronze: raw_users ────────────────────────────────────────────
            ru = dq.get("bronze.raw_users", {})
            if "error" not in ru and ru:
                total = int(ru.get("total_rows", 0))
                null_id = int(ru.get("null_user_id", 0))
                null_email = int(ru.get("null_email", 0))
                null_cc = int(ru.get("null_country_code", 0))
                ru_overall = "PASS" if null_id == 0 else "CRITICAL"
                ru_color = "#3fb950" if ru_overall == "PASS" else "#f85149"
                ru_emoji = "✅" if ru_overall == "PASS" else "🔴"
                st.markdown(
                    f"""
                    <div class="premium-card" style="border-left: 4px solid {ru_color}; margin-bottom: 14px;">
                      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                        <span style="font-size:1.05em; font-weight:bold; color:#c9d1d9;">🪣 <code>bronze.raw_users</code></span>
                        <span style="padding:3px 12px; border-radius:20px; font-size:0.85em; font-weight:bold;
                              background:{'rgba(46,160,67,0.15)' if ru_overall=='PASS' else 'rgba(248,81,73,0.15)'};
                              color:{ru_color}; border:1px solid {ru_color};">{ru_emoji} {ru_overall}</span>
                      </div>
                      <div style="display:flex; gap:12px; flex-wrap:wrap;">
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Total Rows</div>
                          <div style="font-size:1.5em; font-weight:bold; color:#58a6ff;">{total:,}</div>
                        </div>
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">⚠️ Null user_id</div>
                          <div style="font-size:1.5em; font-weight:bold; color:{'#f85149' if null_id > 0 else '#3fb950'};">{null_id:,}</div>
                          <div style="font-size:0.72em; color:#8b949e;">→ Routed to Quarantine</div>
                        </div>
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">⚠️ Null email</div>
                          <div style="font-size:1.5em; font-weight:bold; color:{'#d29922' if null_email > 0 else '#3fb950'};">{null_email:,}</div>
                          <div style="font-size:0.72em; color:#8b949e;">→ Passed to Staging</div>
                        </div>
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">⚠️ Null country_code</div>
                          <div style="font-size:1.5em; font-weight:bold; color:{'#d29922' if null_cc > 0 else '#3fb950'};">{null_cc:,}</div>
                          <div style="font-size:0.72em; color:#8b949e;">→ Passed to Staging</div>
                        </div>
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            # ── Bronze: raw_transactions ─────────────────────────────────────
            rt = dq.get("bronze.raw_transactions", {})
            if "error" not in rt and rt:
                total_t = int(rt.get("total_rows", 0))
                null_txid = int(rt.get("null_transaction_id", 0))
                neg_amt = int(rt.get("negative_amounts", 0))
                null_uid = int(rt.get("null_user_id", 0))
                min_amt = float(rt.get("min_amount") or 0)
                max_amt = float(rt.get("max_amount") or 0)
                rt_overall = "PASS" if null_txid == 0 else "CRITICAL"
                rt_color = "#3fb950" if rt_overall == "PASS" else "#f85149"
                rt_emoji = "✅" if rt_overall == "PASS" else "🔴"
                st.markdown(
                    f"""
                    <div class="premium-card" style="border-left: 4px solid {rt_color}; margin-bottom: 14px;">
                      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                        <span style="font-size:1.05em; font-weight:bold; color:#c9d1d9;">🪣 <code>bronze.raw_transactions</code></span>
                        <span style="padding:3px 12px; border-radius:20px; font-size:0.85em; font-weight:bold;
                              background:{'rgba(46,160,67,0.15)' if rt_overall=='PASS' else 'rgba(248,81,73,0.15)'};
                              color:{rt_color}; border:1px solid {rt_color};">{rt_emoji} {rt_overall}</span>
                      </div>
                      <div style="display:flex; gap:12px; flex-wrap:wrap;">
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Total Rows</div>
                          <div style="font-size:1.5em; font-weight:bold; color:#58a6ff;">{total_t:,}</div>
                        </div>
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">⚠️ Null transaction_id</div>
                          <div style="font-size:1.5em; font-weight:bold; color:{'#f85149' if null_txid > 0 else '#3fb950'};">{null_txid:,}</div>
                          <div style="font-size:0.72em; color:#8b949e;">→ Routed to Quarantine</div>
                        </div>
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">⚠️ Negative Amounts</div>
                          <div style="font-size:1.5em; font-weight:bold; color:{'#d29922' if neg_amt > 0 else '#3fb950'};">{neg_amt:,}</div>
                          <div style="font-size:0.72em; color:#8b949e;">→ ABS() applied in Staging</div>
                        </div>
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Amount Range (USD)</div>
                          <div style="font-size:1.1em; font-weight:bold; color:#58a6ff;">${min_amt:.2f} → ${max_amt:.2f}</div>
                          <div style="font-size:0.72em; color:#8b949e;">min / max in raw table</div>
                        </div>
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            # ── Quarantine Row Counts ─────────────────────────────────────────
            qu = dq.get("staging.stg_users_quarantine", {})
            qt = dq.get("staging.stg_transactions_quarantine", {})
            qu_count = int(qu.get("row_count", 0))
            qt_count = int(qt.get("row_count", 0))
            st.markdown(
                f"""
                <div class="premium-card" style="border-left: 4px solid #d29922; margin-bottom: 14px;">
                  <div style="font-size:1.05em; font-weight:bold; color:#d29922; margin-bottom:10px;">🚨 Quarantine Tables</div>
                  <div style="display:flex; gap:12px; flex-wrap:wrap;">
                    <div style="flex:1; min-width:160px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                      <div style="font-size:0.78em; color:#8b949e;">staging.stg_users_quarantine</div>
                      <div style="font-size:1.8em; font-weight:bold; color:{'#f85149' if qu_count > 0 else '#3fb950'};">{qu_count:,}</div>
                      <div style="font-size:0.72em; color:#8b949e;">records with null user_id</div>
                    </div>
                    <div style="flex:1; min-width:160px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                      <div style="font-size:0.78em; color:#8b949e;">staging.stg_transactions_quarantine</div>
                      <div style="font-size:1.8em; font-weight:bold; color:{'#f85149' if qt_count > 0 else '#3fb950'};">{qt_count:,}</div>
                      <div style="font-size:0.72em; color:#8b949e;">records with null transaction_id</div>
                    </div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True
            )

            # ── Staging: stg_users post-clean ────────────────────────────────
            su = dq.get("staging.stg_users", {})
            if "error" not in su and su:
                total_su = int(su.get("total_rows", 0))
                null_he = int(su.get("null_hashed_email", 0))
                null_mp = int(su.get("null_masked_phone", 0))
                su_overall = "PASS" if null_he == 0 else "WARNING"
                su_color = "#3fb950" if su_overall == "PASS" else "#d29922"
                su_emoji = "✅" if su_overall == "PASS" else "⚠️"
                st.markdown(
                    f"""
                    <div class="premium-card" style="border-left: 4px solid {su_color}; margin-bottom: 14px;">
                      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                        <span style="font-size:1.05em; font-weight:bold; color:#c9d1d9;">✨ <code>staging.stg_users</code> (Post-ETL Check)</span>
                        <span style="padding:3px 12px; border-radius:20px; font-size:0.85em; font-weight:bold;
                              background:{'rgba(46,160,67,0.15)' if su_overall=='PASS' else 'rgba(210,153,34,0.15)'};
                              color:{su_color}; border:1px solid {su_color};">{su_emoji} {su_overall}</span>
                      </div>
                      <div style="display:flex; gap:12px; flex-wrap:wrap;">
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Clean Rows Loaded</div>
                          <div style="font-size:1.5em; font-weight:bold; color:#58a6ff;">{total_su:,}</div>
                        </div>
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Null hashed_email</div>
                          <div style="font-size:1.5em; font-weight:bold; color:{'#d29922' if null_he > 0 else '#3fb950'};">{null_he:,}</div>
                          <div style="font-size:0.72em; color:#8b949e;">(inherited from raw null email)</div>
                        </div>
                        <div style="flex:1; min-width:120px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Null masked_phone</div>
                          <div style="font-size:1.5em; font-weight:bold; color:{'#d29922' if null_mp > 0 else '#3fb950'};">{null_mp:,}</div>
                          <div style="font-size:0.72em; color:#8b949e;">(inherited from raw null phone)</div>
                        </div>
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        st.markdown("---")

        # ── Section 2: DQ Pipeline Run History (dynamic) ─────────────────────
        st.subheader("🚦 DQ ETL Pipeline Run History")
        st.caption("Success rates are computed live from Snowflake pipeline_runs — updated each time you trigger a DQ ETL above.")

        dq_hist = snowflake_data.get("dq_history", {})

        if not dq_hist or all(v["total_runs"] == 0 for v in dq_hist.values()):
            st.info("No DQ ETL runs recorded yet. Use the **🚀 Trigger Data Quality Suite** button in the sidebar to generate data and run the pipeline.")
        else:
            for table_label, hist in dq_hist.items():
                total  = hist["total_runs"]
                ok     = hist["success_count"]
                fail   = hist["failure_count"]
                rate   = hist["success_rate"]
                last   = hist["last_run"]
                
                rate_color  = "#3fb950" if rate >= 80 else ("#d29922" if rate >= 50 else "#f85149")
                rate_badge  = "HEALTHY" if rate >= 80 else ("AT RISK" if rate >= 50 else "DEGRADED")
                badge_style = "badge-success" if rate >= 80 else "badge-failed"
                last_status = last["status"] if last else "N/A"
                last_time   = last["start_time"] if last else "—"
                last_color  = "#3fb950" if last_status == "SUCCESS" else "#f85149"

                layer_badge = "🪣 Bronze"
                if "users" in table_label:
                    pipeline_label = "bronze.raw_users → staging + marts"
                else:
                    pipeline_label = "bronze.raw_transactions → staging + marts"

                st.markdown(
                    f"""
                    <div class="premium-card" style="border-left: 4px solid {rate_color};">
                      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
                        <div>
                          <span style="font-size:1.05em; font-weight:bold; color:#c9d1d9;">{layer_badge} <code>{table_label}</code></span><br/>
                          <span style="font-size:0.78em; color:#8b949e;">Pipeline: <code>{hist['pipeline_name']}</code></span>
                        </div>
                        <span class="{badge_style}" style="padding:4px 14px; border-radius:20px;">{rate_badge}</span>
                      </div>
                      <div style="display:flex; gap:12px; flex-wrap:wrap;">
                        <div style="flex:1; min-width:110px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Success Rate</div>
                          <div style="font-size:1.8em; font-weight:bold; color:{rate_color};">{rate:.1f}%</div>
                        </div>
                        <div style="flex:1; min-width:110px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Total Runs</div>
                          <div style="font-size:1.8em; font-weight:bold; color:#58a6ff;">{total}</div>
                        </div>
                        <div style="flex:1; min-width:110px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">✅ Succeeded</div>
                          <div style="font-size:1.8em; font-weight:bold; color:#3fb950;">{ok}</div>
                        </div>
                        <div style="flex:1; min-width:110px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">❌ Failed</div>
                          <div style="font-size:1.8em; font-weight:bold; color:#f85149;">{fail}</div>
                        </div>
                        <div style="flex:1; min-width:140px; background:#161b22; border-radius:8px; padding:10px; text-align:center;">
                          <div style="font-size:0.78em; color:#8b949e;">Last Run Status</div>
                          <div style="font-size:1.2em; font-weight:bold; color:{last_color};">{last_status}</div>
                          <div style="font-size:0.72em; color:#8b949e;">{last_time}</div>
                        </div>
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        st.markdown("---")

        # ── Section 3: Job Run History ────────────────────────────────────────
        st.subheader("📋 Job Run History Log")
        runs = snowflake_data["runs"]
        
        def color_status(val):
            color = "rgba(46, 160, 67, 0.15)" if val == "SUCCESS" else "rgba(248, 81, 73, 0.15)"
            text_color = "#3fb950" if val == "SUCCESS" else "#f85149"
            return f"background-color: {color}; color: {text_color}; font-weight: bold;"
            
        if snowflake_data["error"] and not runs:
            st.error(f"Error loading pipeline runs: {snowflake_data['error']}")
        else:
            runs_df = pd.DataFrame(runs)
            if not runs_df.empty and "run_id" in runs_df.columns:
                # Show all relevant columns including error_message
                display_cols = [c for c in ["run_id", "pipeline_name", "status", "start_time", "duration_sec", "error_message"] if c in runs_df.columns]
                styled_df = runs_df[display_cols].style.map(
                    color_status, subset=["status"]
                )
                st.dataframe(styled_df, hide_index=True, use_container_width=True)
            else:
                st.info("No pipeline run records available.")

# ----------------- CONVERSATIONAL INTERFACE -----------------
with tab_chatbot:
    st.title("🤖 Chat with Deco")
    st.caption("Your Agentic Data Engineering Assistant")
    
    # Scrollable chat logs
    chat_container = st.container(height=450)
    with chat_container:
        for m in st.session_state.messages:
            with st.chat_message(m["role"]):
                # Persistent dropdown for thoughts and tool calls (does not affect RAG history)
                if m.get("thoughts") or m.get("tool_calls"):
                    with st.expander("🤖 Deco's Thinking & Tool Execution Process", expanded=False):
                        if m.get("thoughts"):
                            st.markdown("**Thinking & Plan:**")
                            for thought in m["thoughts"]:
                                st.write(thought)
                        if m.get("tool_calls"):
                            st.markdown("---")
                            st.markdown("**Tool Calls:**")
                            for tc in m["tool_calls"]:
                                status_emoji = "⏳" if tc["status"] == "running" else "✅"
                                st.markdown(f"{status_emoji} Executing tool `{tc['name']}` with arguments: `{tc['args']}`")
                                if tc.get("result"):
                                    st.markdown("**Output:**")
                                    st.code(tc["result"])
                st.markdown(m["content"])
                
    # Quick action prompt suggestion pills
    st.markdown("<p style='font-size: 0.85em; color: #8b949e; margin-bottom: 2px;'>⚡ Quick Prompts:</p>", unsafe_allow_html=True)
    qp_cols = st.columns(4)
    quick_prompts = [
        ("🔍 Schema Lookup", "What is the schema of staging.stg_users?"),
        ("🛡️ Check PII Tags", "List all PII columns in our database along with their masking policies."),
        ("🚨 Troubleshoot", "My pipeline broke. Can you check the logs, tell me what failed, and suggest a fix?"),
        ("🔗 Trace Lineage", "Explain the lineage and dependencies of marts.fct_user_churn.")
    ]
    
    prompt = st.chat_input("Ask Deco about data pipelines, catalog details, or operational logs...")
    
    # Process quick prompts
    for col, (label, text) in zip(qp_cols, quick_prompts):
        if col.button(label, use_container_width=True):
            prompt = text
            
    # Process user query
    if prompt:
        # Append User Input
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
            
        # Agent response loader
        with st.chat_message("assistant"):
            status_container = st.empty()
            
            thoughts = []
            tool_calls = []
            
            def agent_callback(event_type, data):
                if event_type == "thought":
                    thoughts.append(data)
                elif event_type == "tool_start":
                    tool_calls.append({
                        "name": data["name"],
                        "args": data["args"],
                        "result": None,
                        "status": "running"
                    })
                elif event_type == "tool_end":
                    if tool_calls:
                        tool_calls[-1]["result"] = data["result"]
                        tool_calls[-1]["status"] = "completed"
                
                with status_container.container():
                    with st.expander("🤖 Deco's Live Thinking & Tool Execution Process", expanded=True):
                        if thoughts:
                            st.markdown("**Thinking & Plan:**")
                            for thought in thoughts:
                                st.write(thought)
                        if tool_calls:
                            st.markdown("---")
                            st.markdown("**Tool Calls:**")
                            for tc in tool_calls:
                                status_emoji = "⏳" if tc["status"] == "running" else "✅"
                                st.markdown(f"{status_emoji} Executing tool `{tc['name']}` with arguments: `{tc['args']}`")
                                if tc["result"]:
                                    st.markdown("**Output:**")
                                    st.code(tc["result"])
            
            with st.spinner("Deco is analyzing metrics and executing tools..."):
                # Clean messages history to avoid passing thoughts/tool_calls context to Bedrock
                clean_history = []
                for msg in st.session_state.messages:
                    clean_history.append({
                        "role": msg["role"],
                        "content": msg["content"]
                    })
                agent_response = agent.run_agent(prompt, chat_history=clean_history, callback=agent_callback)
                
            # Collapse the expander at the end
            with status_container.container():
                with st.expander("🤖 Deco's Live Thinking & Tool Execution Process", expanded=False):
                    if thoughts:
                        st.markdown("**Thinking & Plan:**")
                        for thought in thoughts:
                            st.write(thought)
                    if tool_calls:
                        st.markdown("---")
                        st.markdown("**Tool Calls:**")
                        for tc in tool_calls:
                            status_emoji = "⏳" if tc["status"] == "running" else "✅"
                            st.markdown(f"{status_emoji} Executing tool `{tc['name']}` with arguments: `{tc['args']}`")
                            if tc["result"]:
                                st.markdown("**Output:**")
                                st.code(tc["result"])
                                
            st.markdown(agent_response)
            
        # Append Assistant Response with thoughts & tool calls
        st.session_state.messages.append({
            "role": "assistant",
            "content": agent_response,
            "thoughts": thoughts,
            "tool_calls": tool_calls
        })
        st.rerun()
