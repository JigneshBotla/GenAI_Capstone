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

# ----------------- SIDEBAR CONFIG -----------------
with st.sidebar:
    st.image("https://img.icons8.com/nolan/96/artificial-intelligence.png", width=75)
    st.title("Deco Engine Control")
    st.caption("Settings & Integration Hub")
    st.markdown("---")
    
    # Credentials Config
    st.subheader("🔑 AWS Bedrock Config")
    aws_access_key = st.text_input("AWS Access Key ID", type="password", help="Specify Access Key ID for Bedrock access.")
    aws_secret_key = st.text_input("AWS Secret Access Key", type="password", help="Specify Secret Access Key for Bedrock access.")
    aws_region = st.text_input("AWS Region", value="us-east-1", help="Target Bedrock region.")
    
    # Initialize Agent Core dynamically
    if aws_access_key and aws_secret_key:
        agent = AgentCore(
            region_name=aws_region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        st.success("🤖 AWS Bedrock Active (Nova Pro)")
    else:
        # Fall back to local simulation mode
        agent = AgentCore()
        st.warning("🔌 Offline Simulation Fallback Active")
        st.info("💡 Deco works perfectly offline using structured heuristics and database queries. Enter keys to enable true LLM routing.")
        
    st.markdown("---")
    st.subheader("⚙️ Agentic Control Room")
    
    # Active Action Button
    st.markdown("**Manual Trigger Quality Assertions**")
    registered_tables = metadata_helper.get_all_tables()
    table_options = [t["table_id"] for t in registered_tables] if registered_tables else ["staging.stg_users", "marts.fct_user_churn"]
    dq_table = st.selectbox("Select Target Table:", table_options)
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
            st.success(f"Checks completed for {dq_table}!")
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
        tables = metadata_helper.get_all_tables()
        
        # Display as styled cards
        for t in tables:
            st.markdown(
                f"""
                <div class="premium-card glow-border">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h4 style="margin: 0; color: #58a6ff;">{t['table_id']}</h4>
                        <span class="badge-success">{t['row_count']:,} rows</span>
                    </div>
                    <p style="margin: 8px 0; font-size: 0.9em; color: #8b949e;">{t['description']}</p>
                    <div style="font-size: 0.85em; color: #c9d1d9;">
                        Warehouse Layer: <code>{t['schema_name']}</code> | Size: <code>{t['size_bytes'] / (1024*1024):.2f} MB</code>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            # Collapsible column detail table
            with st.expander(f"Inspect `{t['table_name']}` Column Definitions"):
                details = metadata_helper.get_table_details(t['table_id'])
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
        
        st.markdown(
            """
            <div class="premium-card" style="display: flex; flex-direction: column; align-items: center;">
                <div class="lineage-node" style="width: 250px; background-color: rgba(248, 81, 73, 0.1); border-color: #f85149;">
                    🪣 Bronze: raw_users<br/>
                    <span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(PostgreSQL Landing Replica)</span>
                </div>
                <div class="lineage-arrow">⬇️</div>
                <div class="lineage-node" style="width: 250px; background-color: rgba(210, 153, 34, 0.1); border-color: #d29922;">
                    ✨ Silver: stg_users<br/>
                    <span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(PII Enforced & Standardized)</span>
                </div>
                <div class="lineage-arrow">⬇️</div>
                <div class="lineage-node" style="width: 250px; background-color: rgba(46, 160, 67, 0.1); border-color: #3fb950;">
                    🏆 Gold: fct_user_churn<br/>
                    <span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(Business analytical metrics)</span>
                </div>
            </div>
            
            <div class="premium-card" style="display: flex; flex-direction: column; align-items: center; margin-top: 10px;">
                <div class="lineage-node" style="width: 250px; background-color: rgba(248, 81, 73, 0.1); border-color: #f85149;">
                    💳 Bronze: raw_transactions<br/>
                    <span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(Stripe API replica)</span>
                </div>
                <div class="lineage-arrow">⬇️</div>
                <div class="lineage-node" style="width: 250px; background-color: rgba(210, 153, 34, 0.1); border-color: #d29922;">
                    ✨ Silver: stg_transactions<br/>
                    <span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(Casted decimals & Test exclusions)</span>
                </div>
                <div class="lineage-arrow">⬇️</div>
                <div class="lineage-node" style="width: 250px; background-color: rgba(46, 160, 67, 0.1); border-color: #3fb950;">
                    🏆 Gold: fct_user_transactions<br/>
                    <span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(Daily gross spending rolls)</span>
                </div>
                <div class="lineage-arrow">⬇️</div>
                <div class="lineage-node" style="width: 250px; background-color: rgba(46, 160, 67, 0.1); border-color: #3fb950;">
                    🏆 Gold: fct_user_churn<br/>
                    <span style="font-size: 0.8em; font-weight: normal; color: #8b949e;">(Daily analytics model)</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    # TAB 3: OPERATIONS & SLO COMPLIANCE
    with tab_operations:
        st.subheader("Pipeline Service Level Agreements (SLAs)")
        slos = metadata_helper.get_pipeline_slo_compliance()
        
        # Display as styled status blocks
        for s in slos:
            status_style = "badge-success" if s["slo_adherence_status"] == "HIGHLY COMPLIANT" else "badge-failed"
            status_emoji = "🟢" if s["slo_adherence_status"] == "HIGHLY COMPLIANT" else "🔴"
            
            st.markdown(
                f"""
                <div class="premium-card">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h4 style="margin: 0; color: #c9d1d9;">Pipeline: <code>{s['pipeline_name']}</code></h4>
                        <span class="{status_style}">{status_emoji} {s['slo_adherence_status']}</span>
                    </div>
                    <div style="margin: 12px 0; display: flex; justify-content: space-around; background: #161b22; padding: 10px; border-radius: 6px;">
                        <div style="text-align: center;">
                            <div style="font-size: 0.8em; color: #8b949e;">Recent Success Rate</div>
                            <div style="font-size: 1.3em; font-weight: bold; color: #58a6ff;">{s['success_rate_percent']:.1f}%</div>
                        </div>
                        <div style="text-align: center;">
                            <div style="font-size: 0.8em; color: #8b949e;">Daily Target UTC</div>
                            <div style="font-size: 1.3em; font-weight: bold; color: #58a6ff;">{s['sla_target_completion_time']}</div>
                        </div>
                        <div style="text-align: center;">
                            <div style="font-size: 0.8em; color: #8b949e;">Duration Breaches</div>
                            <div style="font-size: 1.3em; font-weight: bold; color: #f85149;">{s['duration_violations_count']} / {s['recent_runs_evaluated']}</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
            
        st.subheader("Job Run History Log")
        runs = metadata_helper.get_pipeline_runs(limit=8)
        runs_df = pd.DataFrame(runs)
        
        def color_status(val):
            color = "rgba(46, 160, 67, 0.15)" if val == "SUCCESS" else "rgba(248, 81, 73, 0.15)"
            text_color = "#3fb950" if val == "SUCCESS" else "#f85149"
            return f"background-color: {color}; color: {text_color}; font-weight: bold;"
            
        if not runs_df.empty:
            styled_df = runs_df[["run_id", "pipeline_name", "status", "start_time", "duration_sec"]].style.map(
                color_status, subset=["status"]
            )
            st.dataframe(styled_df, hide_index=True, use_container_width=True)

# ----------------- CONVERSATIONAL INTERFACE -----------------
with tab_chatbot:
    st.title("🤖 Chat with Deco")
    st.caption("Your Agentic Data Engineering Assistant")
    
    # Scrollable chat logs
    chat_container = st.container(height=520)
    with chat_container:
        for m in st.session_state.messages:
            with st.chat_message(m["role"]):
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
                agent_response = agent.run_agent(prompt, chat_history=st.session_state.messages, callback=agent_callback)
                
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
            
        # Append Assistant Response
        st.session_state.messages.append({"role": "assistant", "content": agent_response})
        st.rerun()
