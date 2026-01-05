"""
Power BI Fabric ↔ Snowflake Semantic Sync
Simplified Streamlit Frontend - Real-Time Dashboard
"""
import streamlit as st
import time
import pandas as pd
from datetime import datetime
from api_client import BackendAPIClient
from styles import inject_styles, COLORS, get_status_color, get_card_style, get_heading_style, get_text_style

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

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Dashboard",
    "❄️ Snowflake Data",
    "📊 Fabric Models",
    "🔄 Sync & Changes",
    "⚙️ Infrastructure"
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
        status_color = get_status_color(fabric_connected)
        st.markdown(f"""
        <div style="{get_card_style('fabric')}">
            <div style="display: flex; align-items: center; gap: 16px;">
                <span style="font-size: 2.5rem;">📊</span>
                <div>
                    <h3 style="{get_heading_style(3)}">Microsoft Fabric</h3>
                    <p style="margin: 4px 0 0 0; color: {status_color}; font-weight: 600;">
                        {"● Connected" if fabric_connected else "○ Disconnected"}
                    </p>
                    <p style="margin: 4px 0 0 0; {get_text_style('muted')} font-size: 0.85rem;">
                        {fabric_conn.get('message', 'No connection info')}
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        status_color = get_status_color(snowflake_connected)
        st.markdown(f"""
        <div style="{get_card_style('snowflake')}">
            <div style="display: flex; align-items: center; gap: 16px;">
                <span style="font-size: 2.5rem;">❄️</span>
                <div>
                    <h3 style="{get_heading_style(3)}">Snowflake</h3>
                    <p style="margin: 4px 0 0 0; color: {status_color}; font-weight: 600;">
                        {"● Connected" if snowflake_connected else "○ Disconnected"}
                    </p>
                    <p style="margin: 4px 0 0 0; {get_text_style('muted')} font-size: 0.85rem;">
                        {snowflake_conn.get('message', 'No connection info')}
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # ============================================
    # SYNC STATUS PANEL - Key feature for detecting issues
    # ============================================
    st.markdown("### 🔄 Sync Status Panel")
    
    changes_result = client.detect_changes()
    
    if changes_result.get("success"):
        snapshots = changes_result.get("snapshots", [])
        fabric_models = changes_result.get("fabric_models", 0)
        snowflake_views = changes_result.get("snowflake_views", 0)
        
        # Calculate sync health
        sync_healthy = fabric_models == snowflake_views and fabric_models > 0
        missing_views = fabric_models - snowflake_views if fabric_models > snowflake_views else 0
        sync_percentage = int((snowflake_views / fabric_models * 100) if fabric_models > 0 else 0)
        
        # Display status metrics with comparison
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Fabric Models", fabric_models, help="Semantic models in Power BI Fabric")
        with col2:
            delta_color = "normal" if snowflake_views >= fabric_models else "inverse"
            delta = f"{snowflake_views - fabric_models}" if snowflake_views != fabric_models else None
            st.metric("Snowflake Views", snowflake_views, delta=delta, delta_color=delta_color,
                     help="Views created in Snowflake")
        with col3:
            status_emoji = "✅" if sync_healthy else "⚠️"
            st.metric("Sync Status", f"{sync_percentage}% {status_emoji}",
                     help="Percentage of models synced to Snowflake")
        with col4:
            if missing_views > 0:
                st.metric("Missing Views", missing_views, delta=f"-{missing_views}", delta_color="inverse")
            else:
                st.metric("Sync Health", "🟢 Healthy")
        
        # Show warning when sync is not healthy
        if missing_views > 0:
            st.markdown(f"""
            <div style="background: rgba(255, 193, 7, 0.15); border: 1px solid rgba(255, 193, 7, 0.5); 
                        padding: 16px; border-radius: 8px; margin: 16px 0;">
                <h4 style="color: #FFC107; margin: 0 0 8px 0;">
                    ⚠️ Sync Mismatch Detected
                </h4>
                <p style="color: {COLORS.get('text_on_dark', '#E0E0E0')}; margin: 0 0 12px 0;">
                    <strong>{fabric_models}</strong> models in Fabric → <strong>{snowflake_views}</strong> views in Snowflake 
                    <br>
                    <strong>{missing_views}</strong> view(s) are missing. This may indicate a change detection issue.
                </p>
                <p style="color: {COLORS.get('text_muted', '#888')}; margin: 0; font-size: 0.85rem;">
                    💡 <strong>Solution:</strong> Click "Force Full Sync" to recreate all views, or "Reconcile" to create only missing views.
                </p>
            </div>
            """, unsafe_allow_html=True)
        elif fabric_models == 0:
            st.info("No Fabric models found. Connect to your Fabric workspace to see models.")
        else:
            st.success(f"✅ All {fabric_models} Fabric models are synced to Snowflake!")
        
        # Action buttons row
        st.markdown("<br>", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔥 Force Full Sync", type="primary", use_container_width=True,
                        help="Recreate ALL views regardless of current state"):
                with st.spinner("Running force sync..."):
                    result = client.run_sync("bidirectional", force=True)
                    if result.get("success"):
                        views_created = result.get("result", {}).get("views_created", 0)
                        st.success(f"✅ Force sync complete! Created {views_created} views.")
                        st.balloons()
                    else:
                        st.error(f"❌ {result.get('error', 'Force sync failed')}")
                st.rerun()
        
        with col2:
            if st.button("🔄 Reconcile", use_container_width=True,
                        help="Detect and create only missing views"):
                with st.spinner("Reconciling sync state..."):
                    result = client.reconcile_sync()
                    if result.get("success"):
                        views_created = result.get("result", {}).get("views_created", 0)
                        missing = result.get("result", {}).get("missing_views", [])
                        if views_created > 0:
                            st.success(f"✅ Reconciliation complete! Created {views_created} missing views.")
                        elif len(missing) == 0:
                            st.success("✅ All views are already synced!")
                        else:
                            st.warning(f"⚠️ Found {len(missing)} missing views but could not create them.")
                    else:
                        st.error(f"❌ {result.get('error', 'Reconciliation failed')}")
                st.rerun()
        
        with col3:
            if st.button("🗑️ Reset State", use_container_width=True,
                        help="Clear all sync state files and start fresh"):
                with st.spinner("Resetting sync state..."):
                    result = client.reset_sync_state()
                    if result.get("success"):
                        st.success("✅ Sync state reset. Run a sync to rebuild state.")
                    else:
                        st.error(f"❌ {result.get('error', 'Reset failed')}")
                st.rerun()
        
        with col4:
            if st.button("📊 Populate Tables", type="primary", use_container_width=True,
                        help="Fill Snowflake tables with sample data"):
                with st.spinner("Populating tables with sample data..."):
                    result = client.populate_tables(force=True)
                    if result.get("success"):
                        st.success(f"✅ Populated {result.get('tables_populated', 0)} tables with {result.get('total_rows_inserted', 0)} rows!")
                        st.balloons()
                    else:
                        st.error(f"❌ {result.get('error', 'Population failed')}")
                st.rerun()
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Snapshots section
        if snapshots:
            st.markdown("#### 📋 Data Snapshots")
            
            for snap in snapshots:
                source = snap.get("source", "unknown")
                source_icon = "📊" if source == "fabric" else "❄️"
                source_label = "Fabric" if source == "fabric" else "Snowflake"
                
                with st.expander(f"{source_icon} {snap.get('name', 'Unknown')} ({source_label})"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Tables", snap.get("tables", 0))
                    with col2:
                        st.metric("Columns", snap.get("columns", 0))
                    with col3:
                        # Show different label based on source
                        if source == "fabric":
                            st.metric("Measures", snap.get("measures", 0))
                        else:
                            st.metric("Rows", snap.get("measures", 0))
                    
                    if snap.get("id"):
                        st.caption(f"Model ID: {snap.get('id')}")
        else:
            st.info("No snapshots detected yet. Upload a file or run a sync to see data.")
    else:
        st.warning(f"Could not detect changes: {changes_result.get('error', 'Unknown error')}")

# ============================================
# TAB 2: SNOWFLAKE DATA - Real-Time View
# ============================================
with tab2:
    st.markdown("## ❄️ Snowflake Data - Live View")
    st.markdown("Real-time data from your Snowflake database (Views & Tables)")
    
    if snowflake_connected:
        views_result = client.get_snowflake_views()
        
        if views_result.get("success"):
            views = views_result.get("views", [])
            
            if views:
                # Count types
                view_count = sum(1 for v in views if v.get("type") == "VIEW")
                table_count = sum(1 for v in views if v.get("type") == "TABLE")
                
                st.success(f"✅ Found {len(views)} objects in Snowflake ({view_count} views, {table_count} tables)")
                
                for view in views:
                    view_name = view.get("name", "Unknown")
                    view_type = view.get("type", "VIEW")
                    row_count = view.get("row_count", 0)
                    sample_data = view.get("sample_data", [])
                    
                    # Icon based on type
                    type_icon = "📋" if view_type == "VIEW" else "📊"
                    
                    with st.expander(f"{type_icon} **{view_name}** ({view_type}: {row_count} rows)", expanded=False):
                        # Show sample data as table
                        if sample_data:
                            st.markdown(f"**Sample Data (first 5 rows):**")
                            st.dataframe(pd.DataFrame(sample_data), use_container_width=True)
                        else:
                            st.info("No data in this object")
                        
                        # Button to load full data
                        if st.button(f"📥 Load Full Data", key=f"load_{view_name}"):
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
                st.info("No views or tables found in Snowflake. Upload a file to sync data!")
        else:
            st.error(f"Error fetching data: {views_result.get('error')}")
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
                
                # Add a button to load all data
                if st.button("📥 **Load All Fabric Data to Snowflake**", type="primary", key="load_all_fabric_data"):
                    with st.spinner("Loading data from all Fabric models to Snowflake..."):
                        result = client.load_data(force=True, sync_mode="full_refresh")
                        if result.get("success"):
                            st.success(f"✅ Loaded {result.get('rows_loaded', 0)} rows across {result.get('tables_processed', 0)} tables!")
                            st.balloons()
                        else:
                            st.error(f"❌ {result.get('error', 'Failed to load data')}")
                
                st.divider()
                
                for model in models:
                    model_name = model.get("displayName", model.get("name", "Unknown"))
                    model_id = model.get("id", "")
                    description = model.get("description", "No description")
                    
                    with st.expander(f"📊 **{model_name}**", expanded=False):
                        st.write(f"**ID:** `{model_id}`")
                        st.write(f"**Description:** {description}")
                        
                        # Show any additional properties
                        if model.get("configuredBy"):
                            st.write(f"**Configured By:** {model.get('configuredBy')}")
                        if model.get("lastRefreshTime"):
                            st.write(f"**Last Refresh:** {model.get('lastRefreshTime')}")
                        
                        st.divider()
                        
                        # Get table info and data for this model
                        if st.button(f"📋 Load Table Data", key=f"load_model_{model_id}"):
                            with st.spinner(f"Fetching data from {model_name}..."):
                                model_data = client.get_fabric_model_data(model_id)
                                
                                if model_data.get("success"):
                                    tables = model_data.get("tables", [])
                                    if tables:
                                        st.success(f"Found {len(tables)} tables in this model")
                                        
                                        for table in tables:
                                            table_name = table.get("name", "Unknown")
                                            row_count = table.get("row_count", 0)
                                            columns = table.get("columns", [])
                                            
                                            st.markdown(f"**📋 {table_name}** - {row_count} rows")
                                            if columns:
                                                st.caption(f"Columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                                            
                                            # Button to view sample data
                                            if st.button(f"👁️ View Sample", key=f"view_{model_id}_{table_name}"):
                                                with st.spinner(f"Loading {table_name}..."):
                                                    sample = client.get_fabric_model_data(model_id, table_name=table_name, limit=10)
                                                    if sample.get("success"):
                                                        data = sample.get("data", [])
                                                        if data:
                                                            st.dataframe(pd.DataFrame(data), use_container_width=True)
                                                        else:
                                                            st.info("No data in this table")
                                                    else:
                                                        st.error(f"Error: {sample.get('error')}")
                                    else:
                                        st.info("No tables found in this model")
                                else:
                                    st.error(f"Error: {model_data.get('error')}")
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
    
    # ============================================
    # DATA COMPARISON - What's in each system
    # ============================================
    st.markdown("### 🔍 Data Comparison")
    st.markdown("See what data exists in each system and what needs syncing")
    
    if st.button("📊 Compare Fabric ↔ Snowflake Data", use_container_width=True):
        with st.spinner("Comparing data between systems..."):
            comparison = client.compare_data()
            
            if comparison.get("success"):
                comp = comparison.get("comparison", {})
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Fabric Datasets", len(comp.get("fabric_datasets", [])))
                with col2:
                    st.metric("Snowflake Tables", len(comp.get("snowflake_tables", [])))
                with col3:
                    st.metric("Already Synced", len(comp.get("synced", [])))
                
                # Show what's missing
                col1, col2 = st.columns(2)
                
                with col1:
                    missing_sf = comp.get("missing_in_snowflake", [])
                    if missing_sf:
                        st.warning(f"⚠️ **Missing in Snowflake** ({len(missing_sf)}):")
                        for name in missing_sf[:10]:
                            st.text(f"  • {name}")
                        if len(missing_sf) > 10:
                            st.text(f"  ... and {len(missing_sf) - 10} more")
                    else:
                        st.success("✅ All Fabric data is in Snowflake")
                
                with col2:
                    missing_fab = comp.get("missing_in_fabric", [])
                    if missing_fab:
                        st.warning(f"⚠️ **Missing in Fabric** ({len(missing_fab)}):")
                        for name in missing_fab[:10]:
                            st.text(f"  • {name}")
                        if len(missing_fab) > 10:
                            st.text(f"  ... and {len(missing_fab) - 10} more")
                    else:
                        st.success("✅ All Snowflake data is available for Fabric")
            else:
                st.error(f"Error comparing: {comparison.get('error', 'Unknown')}")
    
    st.divider()
    
    # ============================================
    # REAL-TIME SYNC CONTROLS
    # ============================================
    st.markdown("### 🔄 Real-Time Sync Controls")
    st.markdown("Control bidirectional synchronization between Fabric and Snowflake in real-time")
    
    # Real-time sync status
    realtime_status = client.get_realtime_sync_status()
    
    if realtime_status.get("success"):
        status = realtime_status.get("status", {})
        is_running = status.get("running", False)
        interval = status.get("interval", 60)
        last_sync = status.get("last_full_sync", "Never")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Status", "🟢 Running" if is_running else "🔴 Stopped")
        with col2:
            st.metric("Sync Interval", f"{interval}s")
        with col3:
            st.metric("Files Synced", status.get("synced_files_count", 0))
        with col4:
            if last_sync and last_sync != "Never":
                try:
                    from datetime import datetime as dt
                    sync_time = dt.fromisoformat(last_sync.replace('Z', '+00:00'))
                    st.metric("Last Sync", sync_time.strftime("%H:%M:%S"))
                except:
                    st.metric("Last Sync", "N/A")
            else:
                st.metric("Last Sync", "Never")
    else:
        st.warning("⚠️ Could not get real-time sync status. Backend may need to be restarted.")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Real-time sync control buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("▶️ Start Real-Time Sync", type="primary" if not realtime_status.get("status", {}).get("running", False) else "secondary", use_container_width=True):
            with st.spinner("Starting real-time sync..."):
                result = client.start_realtime_sync(interval=60)
                if result.get("success"):
                    st.success("✅ Real-time sync started!")
                    st.rerun()
                else:
                    st.error(f"❌ {result.get('error', 'Failed')}")
    
    with col2:
        if st.button("⏹️ Stop Sync", use_container_width=True):
            with st.spinner("Stopping sync..."):
                result = client.stop_realtime_sync()
                if result.get("success"):
                    st.info("ℹ️ Real-time sync stopped")
                    st.rerun()
                else:
                    st.error(f"❌ {result.get('error', 'Failed')}")
    
    with col3:
        if st.button("🚀 **Sync Now**", type="primary", use_container_width=True):
            with st.spinner("Running full bidirectional sync..."):
                result = client.run_realtime_sync_now()
                
                if result.get("success"):
                    st.success("✅ Full bidirectional sync completed!")
                    st.balloons()
                    
                    res = result.get("result", {})
                    
                    # Staged files results
                    staged = res.get("staged_files", {})
                    st.info(f"📁 Staged Files: {staged.get('synced', 0)}/{staged.get('total', 0)} synced")
                    
                    # Fabric -> Snowflake results
                    f2s = res.get("fabric_to_snowflake", {})
                    st.info(f"📊→❄️ Fabric to Snowflake: {f2s.get('synced', 0)}/{f2s.get('total', 0)} synced")
                    
                    # Snowflake -> Fabric results
                    s2f = res.get("snowflake_to_fabric", {})
                    st.info(f"❄️→📊 Snowflake to Fabric: {s2f.get('synced', 0)}/{s2f.get('total', 0)} synced")
                else:
                    st.error(f"❌ Sync failed: {result.get('error', 'Unknown')}")
    
    with col4:
        # Sync interval selector
        new_interval = st.selectbox(
            "⏱️ Interval",
            options=[10, 30, 60, 120, 300, 600, 900],
            format_func=lambda x: f"{x}s" if x < 60 else f"{x//60}m",
            index=2,  # Default 60s
            key="sync_interval_select"
        )
        if st.button("Set", use_container_width=True, key="set_interval_btn"):
            result = client.set_sync_interval(new_interval)
            if result.get("success"):
                st.success(f"Interval set to {new_interval}s")
            else:
                st.error(f"Error: {result.get('error', 'Failed')}")
    
    st.divider()
    
    # Direction-specific sync buttons
    st.markdown("### ↔️ Directional Sync")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 **Full Bidirectional Sync**", type="primary", use_container_width=True, key="full_bidir_sync"):
            with st.spinner("Running TRUE bidirectional sync..."):
                result = client.run_full_data_sync()
                
                if result.get("success"):
                    st.success("✅ Full bidirectional sync completed!")
                    st.balloons()
                    
                    res = result.get("result", {})
                    
                    # Fabric -> Snowflake results
                    f2s = res.get("fabric_to_snowflake", {})
                    st.info(f"📊→❄️ Fabric to Snowflake: {f2s.get('synced', 0)}/{f2s.get('total', 0)} synced")
                    
                    # Snowflake -> Fabric results
                    s2f = res.get("snowflake_to_fabric", {})
                    st.info(f"❄️→📊 Snowflake to Fabric: {s2f.get('synced', 0)}/{s2f.get('total', 0)} synced")
                else:
                    st.error(f"❌ Sync failed: {result.get('error', 'Unknown')}")
    
    with col2:
        if st.button("➡️ Fabric → Snowflake", use_container_width=True, key="fab_to_snow_sync"):
            with st.spinner("Syncing Fabric data to Snowflake..."):
                result = client.realtime_fabric_to_snowflake()
                if result.get("success"):
                    res = result.get("result", {})
                    st.success(f"✅ Synced {res.get('synced', 0)}/{res.get('total', 0)} to Snowflake")
                else:
                    st.error(f"❌ {result.get('error', 'Failed')}")
    
    with col3:
        if st.button("⬅️ Snowflake → Fabric", use_container_width=True, key="snow_to_fab_sync"):
            with st.spinner("Syncing Snowflake data to Fabric..."):
                result = client.realtime_snowflake_to_fabric()
                if result.get("success"):
                    res = result.get("result", {})
                    st.success(f"✅ Synced {res.get('synced', 0)}/{res.get('total', 0)} to Fabric")
                else:
                    st.error(f"❌ {result.get('error', 'Failed')}")
    
    st.divider()
    
    # ============================================
    # LOAD DATA - Critical for populating tables
    # ============================================
    st.markdown("### 📥 Load Actual Data to Snowflake")
    st.markdown("""
    **⚠️ Important:** This extracts actual row-level data from Fabric semantic models and loads it into Snowflake tables.
    
    If your Snowflake tables are empty (only metadata/schema was synced), use this button to populate them with real data.
    """)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        sync_mode = st.selectbox(
            "Sync Mode",
            options=["full_refresh", "incremental", "append"],
            format_func=lambda x: {
                "full_refresh": "🔄 Full Refresh (Replace all data)",
                "incremental": "📈 Incremental (Merge new/changed records)",
                "append": "➕ Append Only (Add new records)"
            }.get(x, x),
            help="Choose how data should be loaded into Snowflake tables"
        )
    
    with col2:
        force_load = st.checkbox("Force reload", value=True, help="Reload data even if tables have data")
    
    if st.button("📥 **Load Data from Fabric to Snowflake**", type="primary", use_container_width=True, key="load_data_btn"):
        with st.spinner("🔄 Extracting data from Fabric and loading to Snowflake... This may take a few minutes."):
            result = client.load_data(force=force_load, sync_mode=sync_mode)
            
            if result.get("success"):
                st.success("✅ Data loaded successfully!")
                st.balloons()
                
                # Show results
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Models Processed", result.get("models_processed", 0))
                with col2:
                    st.metric("Tables Processed", result.get("tables_processed", 0))
                with col3:
                    st.metric("Rows Extracted", result.get("rows_extracted", 0))
                with col4:
                    st.metric("Rows Loaded", result.get("rows_loaded", 0))
                
                if result.get("failures", 0) > 0:
                    st.warning(f"⚠️ {result.get('failures')} table(s) had loading failures. Check logs for details.")
                
                # Show details in expander
                with st.expander("📋 View Full Results"):
                    st.json(result.get("details", result))
            else:
                st.error(f"❌ Data load failed: {result.get('error', 'Unknown error')}")
                
                # Show what we have
                if result.get("details"):
                    with st.expander("🔍 View Error Details"):
                        st.json(result.get("details"))
    
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
        
        if st.button("� Sync to Both Systems", type="primary"):
            with st.spinner("Syncing to Snowflake and Fabric..."):
                result = client.upload_test_file(uploaded_file)
                
                if "error" not in result:
                    st.success("✅ Sync operation completed!")
                    
                    # Show table name
                    table_name = result.get("table_name", "Unknown")
                    st.info(f"📋 **Table Name:** `{table_name}` ({result.get('records_count', 0)} records)")
                    
                    # Show sync results in columns
                    col1, col2 = st.columns(2)
                    
                    sync_results = result.get("sync_results", {})
                    
                    # Snowflake Result
                    with col1:
                        sf_result = sync_results.get("snowflake", {})
                        sf_status = sf_result.get("status", "unknown")
                        
                        if sf_status == "success":
                            st.markdown(f"""
                            <div style="{get_card_style('success')}">
                                <h4 style="margin: 0; color: {COLORS['success']};">❄️ Snowflake ✓</h4>
                                <p style="margin: 8px 0 0 0; {get_text_style('on_dark')}">{sf_result.get('message', '')}</p>
                                <p style="margin: 4px 0 0 0; {get_text_style('muted')} font-size: 0.85rem;">
                                    Rows inserted: {sf_result.get('rows_inserted', 0)}
                                </p>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div style="{get_card_style('error')}">
                                <h4 style="margin: 0; color: {COLORS['error']};">❄️ Snowflake ✗</h4>
                                <p style="margin: 8px 0 0 0; {get_text_style('on_dark')}">{sf_result.get('message', 'Error')}</p>
                            </div>
                            """, unsafe_allow_html=True)
                    
                    # Fabric Result
                    with col2:
                        fab_result = sync_results.get("fabric", {})
                        fab_status = fab_result.get("status", "unknown")
                        
                        if fab_status == "success":
                            st.markdown(f"""
                            <div style="{get_card_style('success')}">
                                <h4 style="margin: 0; color: {COLORS['success']};">📊 Fabric ✓</h4>
                                <p style="margin: 8px 0 0 0; {get_text_style('on_dark')}">{fab_result.get('message', '')}</p>
                                <p style="margin: 4px 0 0 0; {get_text_style('muted')} font-size: 0.85rem;">
                                    Columns: {fab_result.get('columns_count', 0)}
                                </p>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div style="{get_card_style('error')}">
                                <h4 style="margin: 0; color: {COLORS['error']};">📊 Fabric ✗</h4>
                                <p style="margin: 8px 0 0 0; {get_text_style('on_dark')}">{fab_result.get('message', 'Error')}</p>
                            </div>
                            """, unsafe_allow_html=True)
                    
                    # Show overall status
                    st.markdown("<br>", unsafe_allow_html=True)
                    overall = result.get("overall_status", "unknown")
                    if overall == "success":
                        st.balloons()
                        st.success("🎉 Data successfully synced to both Snowflake and Fabric!")
                    elif overall == "partial":
                        st.warning("⚠️ Partial sync - some operations may have failed. Check individual results above.")
                    
                    # Show sample data
                    with st.expander("📋 View Sample Data"):
                        sample = result.get("sample_data", [])
                        if sample:
                            st.dataframe(pd.DataFrame(sample), use_container_width=True)
                    
                    # Show full JSON response
                    with st.expander("🔍 View Full Response"):
                        st.json(result)
                else:
                    st.error(f"Sync failed: {result.get('error')}")

# ============================================
# TAB 5: INFRASTRUCTURE STATUS
# ============================================
with tab5:
    st.markdown("## ⚙️ Infrastructure Status")
    st.markdown("View configured background automation, scheduled tasks, and monitoring systems")
    
    # Infrastructure status overview
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### ❄️ Snowflake Scheduled Tasks")
        st.markdown(f"""
        <div style="{get_card_style('snowflake')}">
            <p style="color: {COLORS['success']}; font-weight: 600; margin: 0 0 8px 0;">✅ Configured (7 Tasks)</p>
            <table style="width: 100%; font-size: 0.85rem; color: {COLORS['text_on_dark']};">
                <tr><td><strong>TASK_BIDIRECTIONAL_SYNC</strong></td><td>Every 1 hour</td></tr>
                <tr><td><strong>TASK_FABRIC_TO_SNOWFLAKE</strong></td><td>Daily 2 AM UTC</td></tr>
                <tr><td><strong>TASK_SNOWFLAKE_TO_FABRIC</strong></td><td>Daily 3 AM UTC</td></tr>
                <tr><td><strong>TASK_SYNC_HEALTH_CHECK</strong></td><td>Every 15 min</td></tr>
                <tr><td><strong>TASK_CHANGE_DETECTION</strong></td><td>Every 1 hour (:30)</td></tr>
                <tr><td><strong>TASK_CLEANUP</strong></td><td>Daily 4 AM UTC</td></tr>
                <tr><td><strong>TASK_AUTO_RETRY</strong></td><td>Every 15 min</td></tr>
            </table>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### ☁️ Azure Functions")
        st.markdown(f"""
        <div style="{get_card_style('fabric')}">
            <p style="color: {COLORS['success']}; font-weight: 600; margin: 0 0 8px 0;">✅ Configured (4 Functions)</p>
            <table style="width: 100%; font-size: 0.85rem; color: {COLORS['text_on_dark']};">
                <tr><td><strong>timer_bidirectional_sync</strong></td><td>Timer (hourly)</td></tr>
                <tr><td><strong>timer_health_check</strong></td><td>Timer (15 min)</td></tr>
                <tr><td><strong>http_trigger_sync</strong></td><td>HTTP POST</td></tr>
                <tr><td><strong>http_detect_changes</strong></td><td>HTTP POST</td></tr>
            </table>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Monitoring & Alerting
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 📊 Monitoring")
        st.markdown(f"""
        <div style="background: {COLORS['bg_card']}; padding: 12px; border-radius: 8px; border-left: 3px solid {COLORS['success']};">
            <p style="margin: 0; color: {COLORS['text_on_dark']};"><strong>Alert Manager</strong></p>
            <p style="margin: 4px 0 0 0; color: {COLORS['text_muted']}; font-size: 0.8rem;">Slack/Email notifications</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### 📈 Dashboard")
        st.markdown(f"""
        <div style="background: {COLORS['bg_card']}; padding: 12px; border-radius: 8px; border-left: 3px solid {COLORS['info']};">
            <p style="margin: 0; color: {COLORS['text_on_dark']};"><strong>Monitoring Dashboard</strong></p>
            <p style="margin: 4px 0 0 0; color: {COLORS['text_muted']}; font-size: 0.8rem;">Streamlit-based</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("### 📝 Audit Logging")
        st.markdown(f"""
        <div style="background: {COLORS['bg_card']}; padding: 12px; border-radius: 8px; border-left: 3px solid {COLORS['secondary']};">
            <p style="margin: 0; color: {COLORS['text_on_dark']};"><strong>SYNC_AUDIT_LOG</strong></p>
            <p style="margin: 4px 0 0 0; color: {COLORS['text_muted']}; font-size: 0.8rem;">Snowflake tables</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Staged Datasets (pending uploads)
    st.markdown("### 📦 Staged Datasets (Pending Sync)")
    st.markdown("Uploaded files waiting to be synced to Fabric/Snowflake")
    
    try:
        staged_result = client._request("GET", "/api/staged-datasets")
        if staged_result.get("success"):
            datasets = staged_result.get("datasets", [])
            if datasets:
                for ds in datasets:
                    with st.expander(f"📋 {ds.get('table_name', 'Unknown')} ({ds.get('row_count', 0)} rows)"):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Columns", ds.get('columns_count', 0))
                        with col2:
                            st.metric("Rows", ds.get('row_count', 0))
                        with col3:
                            st.text(f"Source: {ds.get('source_file', '')}")
                        st.text(f"Uploaded: {ds.get('uploaded_at', '')}")
            else:
                st.info("No staged datasets. Upload a file in the 'Sync & Changes' tab.")
        else:
            st.warning("Could not load staged datasets")
    except Exception as e:
        st.info("No staged datasets found")
    
    st.divider()
    
    # Configuration Summary
    st.markdown("### 🔧 Configuration Summary")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Snowflake Infrastructure Files:**
        - `infrastructure/snowflake/01_create_audit_tables.sql`
        - `infrastructure/snowflake/02_create_stored_procedures.sql`
        - `infrastructure/snowflake/03_create_scheduled_tasks.sql`
        """)
    
    with col2:
        st.markdown("""
        **Azure Infrastructure Files:**
        - `infrastructure/azure_functions/function_app.py`
        - `infrastructure/azure_functions/host.json`
        - `infrastructure/monitoring/alert_manager.py`
        - `infrastructure/monitoring/monitoring_dashboard.py`
        """)
    
    # Deployment checklist
    with st.expander("📋 Deployment Checklist"):
        st.markdown("""
        **To deploy the full automation system:**
        
        1. **Snowflake Setup:**
           ```sql
           -- Run in order in Snowflake
           @01_create_audit_tables.sql
           @02_create_stored_procedures.sql
           @03_create_scheduled_tasks.sql
           ```
        
        2. **Azure Functions Setup:**
           ```bash
           cd infrastructure/azure_functions
           func azure functionapp publish YOUR_FUNCTION_APP
           ```
        
        3. **Configure Environment Variables:**
           - Set all variables in `.env` or Azure Function App Settings
           - Configure Redis connection if using shared state
        
        4. **Enable Notifications:**
           - Set `SLACK_WEBHOOK_URL` for Slack alerts
           - Configure email recipients for error notifications
        
        5. **Resume Snowflake Tasks:**
           ```sql
           ALTER TASK TASK_BIDIRECTIONAL_SYNC RESUME;
           -- Resume other tasks as needed
           ```
        """)
    
    st.divider()
    
    # Real-Time Sync Logs
    st.markdown("### 📜 Real-Time Sync Logs")
    st.markdown("Recent synchronization events and activity")
    
    try:
        logs_result = client.get_sync_logs(limit=20)
        if logs_result.get("success"):
            logs = logs_result.get("logs", [])
            if logs:
                # Display logs in reverse order (newest first)
                for log in reversed(logs):
                    log_type = log.get("type", "INFO")
                    log_status = log.get("status", "INFO")
                    log_message = log.get("message", "")
                    log_time = log.get("timestamp", "")
                    
                    # Format timestamp
                    time_display = ""
                    if log_time:
                        try:
                            from datetime import datetime as dt
                            log_dt = dt.fromisoformat(log_time.replace('Z', '+00:00'))
                            time_display = log_dt.strftime("%H:%M:%S")
                        except:
                            time_display = log_time[:19] if len(log_time) >= 19 else log_time
                    
                    # Color based on status
                    if log_status == "ERROR":
                        icon = "❌"
                        color = COLORS.get('error', '#FF6B6B')
                    elif "COMPLETE" in log_type or "SUCCESS" in log_type:
                        icon = "✅"
                        color = COLORS.get('success', '#4CAF50')
                    elif "START" in log_type:
                        icon = "🚀"
                        color = COLORS.get('info', '#60A5FA')
                    else:
                        icon = "📝"
                        color = COLORS.get('text_muted', '#888888')
                    
                    st.markdown(f"""
                    <div style="padding: 8px; margin: 4px 0; border-left: 3px solid {color}; background: rgba(255,255,255,0.02); border-radius: 4px;">
                        <span style="color: {color}; font-weight: 600;">{icon} [{log_type}]</span>
                        <span style="color: {COLORS.get('text_muted', '#888')}; font-size: 0.8rem; margin-left: 8px;">{time_display}</span>
                        <br>
                        <span style="color: {COLORS.get('text_on_dark', '#E0E0E0')};">{log_message}</span>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("No sync logs yet. Run a sync to see activity.")
        else:
            st.warning(f"Could not load sync logs: {logs_result.get('error', 'Unknown')}")
    except Exception as e:
        st.info("Sync logs not available. Start the backend server to enable logging.")


# ============================================
# AUTO-REFRESH AND AUTO-SYNC LOGIC (15 MINUTES)
# ============================================

# Initialize session state for auto-sync
if 'last_auto_sync' not in st.session_state:
    st.session_state.last_auto_sync = time.time()
if 'auto_sync_enabled' not in st.session_state:
    st.session_state.auto_sync_enabled = True

# Display auto-sync status in sidebar
with st.sidebar:
    st.divider()
    st.markdown("##### 🤖 Auto-Sync (15 min)")
    st.caption("Syncs ALL data between Fabric ↔ Snowflake")
    
    st.session_state.auto_sync_enabled = st.toggle(
        "Enable Auto-Sync",
        value=st.session_state.auto_sync_enabled
    )
    
    if st.session_state.auto_sync_enabled:
        time_since_sync = time.time() - st.session_state.last_auto_sync
        minutes_since = int(time_since_sync // 60)
        seconds_since = int(time_since_sync % 60)
        st.caption(f"Last sync: {minutes_since}m {seconds_since}s ago")
        
        # Calculate next sync
        next_sync_in = max(0, 900 - time_since_sync)
        next_minutes = int(next_sync_in // 60)
        next_seconds = int(next_sync_in % 60)
        st.caption(f"Next sync: {next_minutes}m {next_seconds}s")
    
    # Manual sync trigger button - uses TRUE bidirectional sync
    if st.button("🔄 Force Full Sync Now", use_container_width=True):
        with st.spinner("Running TRUE bidirectional data sync..."):
            # Use the NEW data sync service for real sync
            result = client.run_full_data_sync()
            st.session_state.last_auto_sync = time.time()
            
            if result.get("success"):
                res = result.get("result", {})
                f2s = res.get("fabric_to_snowflake", {})
                s2f = res.get("snowflake_to_fabric", {})
                st.success("✅ Full sync completed!")
                st.info(f"📊→❄️ {f2s.get('synced', 0)} synced to Snowflake")
                st.info(f"❄️→📊 {s2f.get('synced', 0)} synced to Fabric")
            else:
                st.error(f"❌ {result.get('error', 'Sync failed')}")
        st.rerun()

# Auto-sync logic - runs every 15 minutes (900 seconds)
AUTO_SYNC_INTERVAL = 10  # 10 seconds

if st.session_state.auto_sync_enabled:
    time_since_sync = time.time() - st.session_state.last_auto_sync
    
    if time_since_sync >= AUTO_SYNC_INTERVAL:
        # Time for auto-sync - use TRUE bidirectional sync
        try:
            # Run TRUE bidirectional sync to ensure ALL Fabric and Snowflake data stays in sync
            result = client.run_full_data_sync()
            st.session_state.last_auto_sync = time.time()
            
            if result.get("success"):
                res = result.get("result", {})
                f2s = res.get("fabric_to_snowflake", {})
                s2f = res.get("snowflake_to_fabric", {})
                st.toast(f"✅ Auto-sync: {f2s.get('synced', 0)}→SF, {s2f.get('synced', 0)}→Fab", icon="🔄")
            else:
                st.toast(f"⚠️ Auto-sync issue: {result.get('error', 'Unknown')}", icon="⚠️")
        except Exception as e:
            st.toast(f"❌ Auto-sync error: {str(e)}", icon="❌")
        
        st.rerun()

# Regular auto-refresh for UI updates (every 60 seconds for less overhead)
if st.session_state.auto_refresh:
    if time.time() - st.session_state.last_refresh > 60:  # 60 second UI refresh
        st.session_state.last_refresh = time.time()
        time.sleep(0.1)
        st.rerun()
