"""
Power BI Fabric ↔ Snowflake Semantic Sync
Simplified Streamlit Frontend - Real-Time Dashboard
"""
import streamlit as st
import time
import pandas as pd
from datetime import datetime
from api_client import BackendAPIClient
from styles import inject_styles, COLORS

# Page configuration
st.set_page_config(
    page_title="Semantic Sync Dashboard",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inject custom styles
inject_styles()

# Initialize API client - NO CACHING to ensure fresh instance
def get_api_client():
    return BackendAPIClient()

client = get_api_client()

# Session state initialization
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = True
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()
if 'selected_view' not in st.session_state:
    st.session_state.selected_view = None

# ============================================
# SIDEBAR - Connection Status & Quick Actions
# ============================================
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 20px 0; border-bottom: 1px solid rgba(255,255,255,0.1); margin-bottom: 20px;">
        <div style="font-size: 2.5rem;">🔄</div>
        <h2 style="color: white; margin: 8px 0 0 0; font-size: 1.3rem;">Semantic Sync</h2>
        <p style="color: rgba(255,255,255,0.6); font-size: 0.75rem; margin: 4px 0 0 0;">Fabric ↔ Snowflake</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Test Connections
    st.markdown("##### 🔌 Connection Status")
    
    connections = client.test_connections()
    
    fabric_conn = connections.get("fabric", {})
    snowflake_conn = connections.get("snowflake", {})
    
    fabric_connected = fabric_conn.get("connected", False)
    snowflake_connected = snowflake_conn.get("connected", False)
    
    # Connection indicators
    col1, col2 = st.columns(2)
    with col1:
        if fabric_connected:
            models_count = fabric_conn.get("models_count", 0)
            st.success(f"📊 Fabric\n{models_count} models")
        else:
            st.error(f"📊 Fabric\nOffline")
    with col2:
        if snowflake_connected:
            views_count = snowflake_conn.get("views_count", 0)
            st.success(f"❄️ Snowflake\n{views_count} views")
        else:
            st.error("❄️ Snowflake\nOffline")
    
    st.divider()
    
    # Quick Actions
    st.markdown("##### ⚡ Quick Actions")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    with col2:
        if st.button("🚀 Sync Now", use_container_width=True):
            with st.spinner("Running sync..."):
                result = client.run_sync("bidirectional")
                if result.get("success"):
                    st.success("✅ Sync complete!")
                else:
                    st.error(f"❌ {result.get('error', 'Sync failed')}")
    
    st.divider()
    
    # Auto-refresh toggle
    st.session_state.auto_refresh = st.toggle(
        "🔄 Auto-refresh (5s)",
        value=st.session_state.auto_refresh
    )
    
    # Footer
    st.markdown(f"""
    <div style="margin-top: 20px; text-align: center; color: rgba(255,255,255,0.5); font-size: 0.7rem;">
        Last updated: {datetime.now().strftime('%H:%M:%S')}
    </div>
    """, unsafe_allow_html=True)

# ============================================
# MAIN CONTENT - Tab Navigation
# ============================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Dashboard",
    "❄️ Snowflake Data",
    "📊 Fabric Models",
    "🔄 Sync & Changes"
])

# ============================================
# TAB 1: DASHBOARD - Real-Time Overview
# ============================================
with tab1:
    st.markdown("## 📊 Real-Time Dashboard")
    st.markdown("Live view of your Fabric and Snowflake data")
    
    # Connection Cards
    col1, col2 = st.columns(2)
    
    with col1:
        status_color = "#10b981" if fabric_connected else "#ef4444"
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {COLORS['primary']}20, {COLORS['secondary']}20); 
                    padding: 24px; border-radius: 16px; border-left: 4px solid {status_color};">
            <div style="display: flex; align-items: center; gap: 16px;">
                <span style="font-size: 2.5rem;">📊</span>
                <div>
                    <h3 style="margin: 0; color: #1e293b;">Microsoft Fabric</h3>
                    <p style="margin: 4px 0 0 0; color: {status_color}; font-weight: 600;">
                        {"● Connected" if fabric_connected else "○ Disconnected"}
                    </p>
                    <p style="margin: 4px 0 0 0; color: #64748b; font-size: 0.85rem;">
                        {fabric_conn.get('message', 'No connection info')}
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        status_color = "#10b981" if snowflake_connected else "#ef4444"
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #29b5e820, #0ea5e920); 
                    padding: 24px; border-radius: 16px; border-left: 4px solid {status_color};">
            <div style="display: flex; align-items: center; gap: 16px;">
                <span style="font-size: 2.5rem;">❄️</span>
                <div>
                    <h3 style="margin: 0; color: #1e293b;">Snowflake</h3>
                    <p style="margin: 4px 0 0 0; color: {status_color}; font-weight: 600;">
                        {"● Connected" if snowflake_connected else "○ Disconnected"}
                    </p>
                    <p style="margin: 4px 0 0 0; color: #64748b; font-size: 0.85rem;">
                        {snowflake_conn.get('message', 'No connection info')}
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Detect and show changes
    st.markdown("### 🔍 Detected Changes")
    
    changes_result = client.detect_changes()
    
    if changes_result.get("success"):
        snapshots = changes_result.get("snapshots", [])
        fabric_models = changes_result.get("fabric_models", 0)
        snowflake_views = changes_result.get("snowflake_views", 0)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Fabric Models", fabric_models)
        with col2:
            st.metric("Snowflake Views", snowflake_views)
        with col3:
            st.metric("Total Snapshots", len(snapshots))
        
        if snapshots:
            st.markdown("#### 📋 Current Snapshots")
            
            for snap in snapshots:
                source_icon = "📊" if snap.get("source") == "fabric" else "❄️"
                with st.expander(f"{source_icon} {snap.get('name', 'Unknown')} ({snap.get('source', 'unknown')})"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Tables", snap.get("tables", 0))
                    with col2:
                        st.metric("Columns", snap.get("columns", 0))
                    with col3:
                        st.metric("Measures", snap.get("measures", 0))
                    
                    if snap.get("id"):
                        st.text(f"Model ID: {snap.get('id')}")
        else:
            st.info("No snapshots detected yet. Run a sync to capture changes.")
    else:
        st.warning(f"Could not detect changes: {changes_result.get('error', 'Unknown error')}")

# ============================================
# TAB 2: SNOWFLAKE DATA - Real-Time View
# ============================================
with tab2:
    st.markdown("## ❄️ Snowflake Views - Live Data")
    st.markdown("Real-time data from your Snowflake semantic layer")
    
    if snowflake_connected:
        views_result = client.get_snowflake_views()
        
        if views_result.get("success"):
            views = views_result.get("views", [])
            
            if views:
                st.success(f"✅ Found {len(views)} views in Snowflake")
                
                for view in views:
                    view_name = view.get("name", "Unknown")
                    row_count = view.get("row_count", 0)
                    sample_data = view.get("sample_data", [])
                    
                    with st.expander(f"📋 **{view_name}** ({row_count} rows)", expanded=True):
                        # Show sample data as table
                        if sample_data:
                            st.dataframe(pd.DataFrame(sample_data), use_container_width=True)
                        else:
                            st.info("No data in this view")
                        
                        # Button to load full data
                        if st.button(f"Load Full Data", key=f"load_{view_name}"):
                            with st.spinner(f"Loading {view_name}..."):
                                full_data = client.get_snowflake_view_data(view_name)
                                if full_data.get("success"):
                                    data = full_data.get("data", [])
                                    st.success(f"Loaded {len(data)} rows")
                                    if data:
                                        st.dataframe(pd.DataFrame(data), use_container_width=True)
                                else:
                                    st.error(f"Error: {full_data.get('error')}")
            else:
                st.info("No views found in Snowflake SEMANTIC_LAYER schema")
        else:
            st.error(f"Error fetching views: {views_result.get('error')}")
    else:
        st.warning("❌ Snowflake not connected. Check your credentials in .env file.")
        st.code("""
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=ANALYTICS_DB
SNOWFLAKE_SCHEMA=SEMANTIC_LAYER
        """)

# ============================================
# TAB 3: FABRIC MODELS - Real-Time View
# ============================================
with tab3:
    st.markdown("## 📊 Fabric Semantic Models")
    st.markdown("Live data from your Power BI Fabric workspace")
    
    if fabric_connected:
        models_result = client.get_fabric_models()
        
        if models_result.get("success"):
            models = models_result.get("models", [])
            
            if models:
                st.success(f"✅ Found {len(models)} semantic models in Fabric")
                
                for model in models:
                    model_name = model.get("displayName", model.get("name", "Unknown"))
                    model_id = model.get("id", "")
                    description = model.get("description", "No description")
                    
                    with st.expander(f"📊 **{model_name}**", expanded=True):
                        st.write(f"**ID:** `{model_id}`")
                        st.write(f"**Description:** {description}")
                        
                        # Show any additional properties
                        if model.get("configuredBy"):
                            st.write(f"**Configured By:** {model.get('configuredBy')}")
                        if model.get("lastRefreshTime"):
                            st.write(f"**Last Refresh:** {model.get('lastRefreshTime')}")
            else:
                st.info("No semantic models found in your Fabric workspace")
        else:
            st.error(f"Error fetching models: {models_result.get('error')}")
    else:
        st.warning("❌ Fabric not connected. Check your credentials in .env file.")
        st.code("""
FABRIC_TENANT_ID=your_tenant_id
FABRIC_CLIENT_ID=your_client_id
FABRIC_CLIENT_SECRET=your_secret
FABRIC_WORKSPACE_ID=your_workspace_id
        """)

# ============================================
# TAB 4: SYNC & CHANGES
# ============================================
with tab4:
    st.markdown("## 🔄 Sync Operations")
    st.markdown("Run bidirectional synchronization between Fabric and Snowflake")
    
    # Sync Configuration
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        direction = st.selectbox(
            "Sync Direction",
            options=["bidirectional", "fabric_to_snowflake", "snowflake_to_fabric"],
            format_func=lambda x: {
                "bidirectional": "↔️ Bidirectional (Full Merge)",
                "fabric_to_snowflake": "➡️ Fabric → Snowflake",
                "snowflake_to_fabric": "⬅️ Snowflake → Fabric"
            }.get(x, x)
        )
    
    with col2:
        st.write("")
    
    with col3:
        st.write("")
        if st.button("🚀 Run Sync", type="primary", use_container_width=True):
            with st.spinner("Running synchronization..."):
                result = client.run_sync(direction)
                
                if result.get("success"):
                    st.success("✅ Synchronization completed!")
                    st.balloons()
                    
                    # Show results
                    results = result.get("results", {})
                    if results:
                        st.json(results)
                else:
                    st.error(f"❌ Sync failed: {result.get('error', 'Unknown error')}")
    
    st.divider()
    
    # File Upload for Sync Test
    st.markdown("### 📁 Upload Test File")
    st.markdown("Upload a CSV, Excel, or JSON file to test synchronization")
    
    uploaded_file = st.file_uploader(
        "Choose a file",
        type=["csv", "xlsx", "json"],
        help="Upload a file to validate with both systems"
    )
    
    if uploaded_file:
        st.success(f"📁 **{uploaded_file.name}** uploaded ({uploaded_file.size} bytes)")
        
        # Preview file
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
                st.dataframe(df.head(10), use_container_width=True)
                uploaded_file.seek(0)  # Reset file pointer
            elif uploaded_file.name.endswith('.json'):
                import json
                data = json.load(uploaded_file)
                st.json(data)
                uploaded_file.seek(0)
        except Exception as e:
            st.warning(f"Could not preview file: {e}")
        
        if st.button("🔬 Validate with Systems", type="primary"):
            with st.spinner("Validating..."):
                result = client.upload_test_file(uploaded_file)
                if "error" not in result:
                    st.success("✅ Validation complete!")
                    st.json(result)
                else:
                    st.error(f"Validation failed: {result.get('error')}")

# ============================================
# AUTO-REFRESH LOGIC
# ============================================
if st.session_state.auto_refresh:
    if time.time() - st.session_state.last_refresh > 5:
        st.session_state.last_refresh = time.time()
        time.sleep(0.1)
        st.rerun()
