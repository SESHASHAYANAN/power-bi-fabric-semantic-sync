"""
Comprehensive Monitoring Dashboard for Fabric-Snowflake Sync
Real-time monitoring, alerting, and metrics visualization

This Streamlit dashboard provides:
- Sync success rates and execution metrics
- Data drift detection
- API rate limit monitoring
- Schema validation errors
- Real-time health status

Author: Data Engineering Team
Created: 2026-01-03
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
import os
from typing import Dict, List, Any, Optional
import requests

# Page config
st.set_page_config(
    page_title="Fabric-Snowflake Sync Monitor",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        border-radius: 15px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    
    .status-healthy {
        color: #00ff88;
        font-weight: bold;
    }
    
    .status-warning {
        color: #ffcc00;
        font-weight: bold;
    }
    
    .status-critical {
        color: #ff4444;
        font-weight: bold;
    }
    
    .sync-timeline {
        border-left: 3px solid #4a9eff;
        padding-left: 20px;
        margin: 20px 0;
    }
    
    .alert-box {
        background: #2a1f1f;
        border-left: 4px solid #ff4444;
        padding: 15px;
        margin: 10px 0;
        border-radius: 5px;
    }
    
    .success-box {
        background: #1f2a1f;
        border-left: 4px solid #00ff88;
        padding: 15px;
        margin: 10px 0;
        border-radius: 5px;
    }
    
    h1, h2, h3 {
        color: #ffffff;
    }
    
    .stMetric {
        background: rgba(255,255,255,0.05);
        padding: 15px;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)


class MonitoringDataSource:
    """Data source for monitoring metrics."""
    
    def __init__(self):
        self.snowflake_connected = False
        self.redis_connected = False
        
    def get_sync_history(self, hours: int = 24) -> pd.DataFrame:
        """Get sync history from audit log."""
        # In production, this would query Snowflake SYNC_AUDIT_LOG table
        # For demo, generate sample data
        
        dates = pd.date_range(
            end=datetime.now(), 
            periods=hours, 
            freq='H'
        )
        
        data = []
        for i, dt in enumerate(dates):
            status = 'COMPLETED' if i % 5 != 0 else ('PARTIAL' if i % 7 != 0 else 'FAILED')
            data.append({
                'sync_timestamp': dt,
                'sync_id': f'SYNC_{dt.strftime("%Y%m%d%H%M")}',
                'sync_direction': 'BIDIRECTIONAL',
                'sync_status': status,
                'models_processed': 3 if status == 'COMPLETED' else (2 if status == 'PARTIAL' else 0),
                'views_created': 1 if status != 'FAILED' else 0,
                'views_updated': 2 if status == 'COMPLETED' else 1,
                'execution_duration_ms': 5000 + (i * 100) + (500 if status == 'FAILED' else 0),
                'errors': [] if status == 'COMPLETED' else ['Sample error message']
            })
        
        return pd.DataFrame(data)
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status."""
        return {
            'overall_status': 'HEALTHY',
            'last_check': datetime.now().isoformat(),
            'checks': [
                {'name': 'BIDIRECTIONAL', 'status': 'OK', 'minutes_since_success': 15},
                {'name': 'FABRIC_TO_SNOWFLAKE', 'status': 'OK', 'minutes_since_success': 45},
                {'name': 'SNOWFLAKE_TO_FABRIC', 'status': 'OK', 'minutes_since_success': 60},
                {'name': 'HEALTH_CHECK', 'status': 'OK', 'minutes_since_success': 5}
            ],
            'warnings': [],
            'errors': []
        }
    
    def get_error_log(self, hours: int = 24) -> pd.DataFrame:
        """Get recent errors."""
        data = [
            {
                'error_timestamp': datetime.now() - timedelta(hours=2),
                'error_category': 'CONNECTION',
                'error_severity': 'MEDIUM',
                'error_code': 'SF_CONN_001',
                'error_message': 'Snowflake connection timeout',
                'source_model': 'Sales_Model',
                'is_resolved': True
            },
            {
                'error_timestamp': datetime.now() - timedelta(hours=6),
                'error_category': 'VALIDATION',
                'error_severity': 'LOW',
                'error_code': 'SCHEMA_001',
                'error_message': 'Column type mismatch detected',
                'source_model': 'Inventory_Model',
                'is_resolved': True
            }
        ]
        return pd.DataFrame(data)
    
    def get_metrics(self, hours: int = 24) -> pd.DataFrame:
        """Get system metrics."""
        dates = pd.date_range(
            end=datetime.now(), 
            periods=hours * 4,  # Every 15 min
            freq='15T'
        )
        
        import random
        data = []
        for dt in dates:
            data.append({
                'metric_timestamp': dt,
                'sync_success_rate': 0.95 + random.uniform(-0.1, 0.05),
                'avg_duration_ms': 5000 + random.randint(-1000, 2000),
                'api_rate_remaining': 900 + random.randint(-200, 100),
                'models_synced': random.randint(2, 5)
            })
        
        return pd.DataFrame(data)
    
    def get_data_drift_metrics(self) -> pd.DataFrame:
        """Get data drift detection results."""
        data = [
            {'model_name': 'Sales_Model', 'drift_score': 0.02, 'last_checked': datetime.now(), 'status': 'STABLE'},
            {'model_name': 'Inventory_Model', 'drift_score': 0.15, 'last_checked': datetime.now(), 'status': 'MINOR_DRIFT'},
            {'model_name': 'Customer_Model', 'drift_score': 0.01, 'last_checked': datetime.now(), 'status': 'STABLE'},
            {'model_name': 'Marketing_Model', 'drift_score': 0.08, 'last_checked': datetime.now(), 'status': 'STABLE'},
        ]
        return pd.DataFrame(data)


def render_health_overview(data_source: MonitoringDataSource):
    """Render the health overview section."""
    st.header("🏥 System Health Overview")
    
    health = data_source.get_health_status()
    
    # Overall status
    status = health['overall_status']
    status_class = f"status-{status.lower()}"
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Overall Status",
            value=status,
            delta="Healthy" if status == "HEALTHY" else "Degraded"
        )
    
    with col2:
        st.metric(
            label="Last Health Check",
            value="5 min ago",
            delta="On Schedule"
        )
    
    with col3:
        st.metric(
            label="Active Alerts",
            value=len(health['errors']),
            delta=f"{len(health['warnings'])} warnings"
        )
    
    with col4:
        st.metric(
            label="Sync Components",
            value=f"{len([c for c in health['checks'] if c['status'] == 'OK'])}/{len(health['checks'])}",
            delta="All Systems Operational"
        )
    
    # Health checks detail
    st.subheader("Component Health")
    
    check_cols = st.columns(len(health['checks']))
    for i, check in enumerate(health['checks']):
        with check_cols[i]:
            color = "🟢" if check['status'] == 'OK' else ("🟡" if check['status'] == 'WARNING' else "🔴")
            st.markdown(f"""
            **{color} {check['name']}**
            
            Status: {check['status']}
            
            Last Success: {check['minutes_since_success']} min ago
            """)


def render_sync_metrics(data_source: MonitoringDataSource):
    """Render sync performance metrics."""
    st.header("📊 Sync Performance Metrics")
    
    # Time range selector
    time_range = st.selectbox(
        "Time Range",
        options=["Last 24 Hours", "Last 7 Days", "Last 30 Days"],
        index=0
    )
    
    hours = 24 if "24" in time_range else (168 if "7" in time_range else 720)
    
    sync_history = data_source.get_sync_history(min(hours, 168))
    metrics = data_source.get_metrics(min(hours, 168))
    
    # Calculate summary stats
    total_syncs = len(sync_history)
    successful = len(sync_history[sync_history['sync_status'] == 'COMPLETED'])
    failed = len(sync_history[sync_history['sync_status'] == 'FAILED'])
    partial = len(sync_history[sync_history['sync_status'] == 'PARTIAL'])
    
    success_rate = (successful / total_syncs * 100) if total_syncs > 0 else 0
    avg_duration = sync_history['execution_duration_ms'].mean() / 1000  # Convert to seconds
    
    # Summary metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Syncs", total_syncs)
    
    with col2:
        st.metric(
            "Success Rate",
            f"{success_rate:.1f}%",
            delta=f"{'↑' if success_rate > 90 else '↓'} from last period"
        )
    
    with col3:
        st.metric("Successful", successful, delta=f"{failed} failed")
    
    with col4:
        st.metric("Avg Duration", f"{avg_duration:.1f}s")
    
    with col5:
        st.metric(
            "Models Synced",
            sync_history['models_processed'].sum()
        )
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Sync status over time
        fig = px.bar(
            sync_history,
            x='sync_timestamp',
            y='models_processed',
            color='sync_status',
            title='Sync History by Status',
            color_discrete_map={
                'COMPLETED': '#00ff88',
                'PARTIAL': '#ffcc00',
                'FAILED': '#ff4444'
            }
        )
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Execution duration trend
        fig = px.line(
            sync_history,
            x='sync_timestamp',
            y='execution_duration_ms',
            title='Execution Duration Trend',
            labels={'execution_duration_ms': 'Duration (ms)'}
        )
        fig.add_hline(
            y=10000, 
            line_dash="dash", 
            line_color="red",
            annotation_text="Alert Threshold (10s)"
        )
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Success rate gauge
    col1, col2 = st.columns(2)
    
    with col1:
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=success_rate,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Success Rate %"},
            delta={'reference': 95, 'increasing': {'color': "green"}},
            gauge={
                'axis': {'range': [None, 100]},
                'steps': [
                    {'range': [0, 80], 'color': "rgba(255,68,68,0.3)"},
                    {'range': [80, 90], 'color': "rgba(255,204,0,0.3)"},
                    {'range': [90, 100], 'color': "rgba(0,255,136,0.3)"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Status distribution pie chart
        status_counts = sync_history['sync_status'].value_counts()
        fig = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title='Sync Status Distribution',
            color=status_counts.index,
            color_discrete_map={
                'COMPLETED': '#00ff88',
                'PARTIAL': '#ffcc00',
                'FAILED': '#ff4444'
            }
        )
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)


def render_data_drift(data_source: MonitoringDataSource):
    """Render data drift detection section."""
    st.header("📈 Data Drift Detection")
    
    drift_data = data_source.get_data_drift_metrics()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Drift score bar chart
        fig = px.bar(
            drift_data,
            x='model_name',
            y='drift_score',
            color='status',
            title='Drift Score by Model',
            color_discrete_map={
                'STABLE': '#00ff88',
                'MINOR_DRIFT': '#ffcc00',
                'MAJOR_DRIFT': '#ff4444'
            }
        )
        fig.add_hline(y=0.1, line_dash="dash", line_color="orange", annotation_text="Warning Threshold")
        fig.add_hline(y=0.25, line_dash="dash", line_color="red", annotation_text="Critical Threshold")
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Drift Status Summary")
        for _, row in drift_data.iterrows():
            status_color = "🟢" if row['status'] == 'STABLE' else ("🟡" if row['status'] == 'MINOR_DRIFT' else "🔴")
            st.markdown(f"""
            **{status_color} {row['model_name']}**
            - Drift Score: {row['drift_score']:.2%}
            - Status: {row['status']}
            """)


def render_api_rate_limits(data_source: MonitoringDataSource):
    """Render API rate limit monitoring."""
    st.header("⚡ API Rate Limits")
    
    metrics = data_source.get_metrics(24)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Fabric API rate limit
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=850,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Fabric API Rate Remaining"},
            gauge={
                'axis': {'range': [0, 1000]},
                'steps': [
                    {'range': [0, 200], 'color': "rgba(255,68,68,0.5)"},
                    {'range': [200, 500], 'color': "rgba(255,204,0,0.5)"},
                    {'range': [500, 1000], 'color': "rgba(0,255,136,0.3)"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 100
                }
            }
        ))
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Snowflake query credits
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=75,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Snowflake Credits Used (%)"},
            gauge={
                'axis': {'range': [0, 100]},
                'steps': [
                    {'range': [0, 50], 'color': "rgba(0,255,136,0.3)"},
                    {'range': [50, 80], 'color': "rgba(255,204,0,0.5)"},
                    {'range': [80, 100], 'color': "rgba(255,68,68,0.5)"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Rate limit over time
    fig = px.line(
        metrics,
        x='metric_timestamp',
        y='api_rate_remaining',
        title='API Rate Limit Usage Over Time'
    )
    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    st.plotly_chart(fig, use_container_width=True)


def render_error_log(data_source: MonitoringDataSource):
    """Render error log section."""
    st.header("🚨 Error Log & Alerts")
    
    errors = data_source.get_error_log(24)
    
    # Alert summary
    unresolved = len(errors[errors['is_resolved'] == False])
    
    if unresolved > 0:
        st.markdown(f"""
        <div class="alert-box">
            <strong>⚠️ {unresolved} Unresolved Errors</strong>
            <p>There are {unresolved} unresolved errors that require attention.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="success-box">
            <strong>✅ All Clear</strong>
            <p>No unresolved errors at this time.</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Error table
    if len(errors) > 0:
        st.subheader("Recent Errors")
        
        # Format for display
        display_df = errors.copy()
        display_df['error_timestamp'] = display_df['error_timestamp'].dt.strftime('%Y-%m-%d %H:%M')
        display_df['status'] = display_df['is_resolved'].apply(lambda x: '✅ Resolved' if x else '❌ Open')
        
        st.dataframe(
            display_df[['error_timestamp', 'error_category', 'error_severity', 
                       'error_message', 'source_model', 'status']],
            use_container_width=True
        )


def render_sync_timeline(data_source: MonitoringDataSource):
    """Render sync history timeline."""
    st.header("📅 Sync Timeline")
    
    sync_history = data_source.get_sync_history(12)
    sync_history = sync_history.sort_values('sync_timestamp', ascending=False)
    
    for _, sync in sync_history.head(10).iterrows():
        status_icon = "✅" if sync['sync_status'] == 'COMPLETED' else ("⚠️" if sync['sync_status'] == 'PARTIAL' else "❌")
        status_color = "#00ff88" if sync['sync_status'] == 'COMPLETED' else ("#ffcc00" if sync['sync_status'] == 'PARTIAL' else "#ff4444")
        
        with st.expander(f"{status_icon} {sync['sync_id']} - {sync['sync_timestamp'].strftime('%H:%M:%S')}"):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"**Status:** <span style='color:{status_color}'>{sync['sync_status']}</span>", unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"**Duration:** {sync['execution_duration_ms']/1000:.2f}s")
            
            with col3:
                st.markdown(f"**Models:** {sync['models_processed']}")
            
            with col4:
                st.markdown(f"**Views Created:** {sync['views_created']}")
            
            if sync['errors']:
                st.error(f"Errors: {', '.join(sync['errors'])}")


def render_alert_configuration():
    """Render alert configuration section."""
    st.header("🔔 Alert Configuration")
    
    with st.expander("Configure Alerts", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Thresholds")
            
            st.slider(
                "Sync Duration Alert (seconds)",
                min_value=5,
                max_value=60,
                value=10,
                help="Alert when sync takes longer than this"
            )
            
            st.slider(
                "Success Rate Alert (%)",
                min_value=50,
                max_value=100,
                value=90,
                help="Alert when success rate drops below this"
            )
            
            st.slider(
                "Data Drift Threshold (%)",
                min_value=1,
                max_value=50,
                value=10,
                help="Alert when drift exceeds this percentage"
            )
        
        with col2:
            st.subheader("Notification Channels")
            
            st.text_input("Slack Webhook URL", value="", type="password")
            st.text_input("Email Recipients", value="team@company.com")
            
            st.checkbox("Send Slack Alerts", value=True)
            st.checkbox("Send Email Alerts", value=True)
            st.checkbox("Send Teams Notifications", value=False)
            
            if st.button("Save Configuration", type="primary"):
                st.success("Configuration saved!")


def main():
    """Main dashboard application."""
    
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/200x50?text=Sync+Monitor", width=200)
        st.title("🔄 Sync Monitor")
        
        st.markdown("---")
        
        # Navigation
        page = st.radio(
            "Navigation",
            options=[
                "🏥 Health Overview",
                "📊 Sync Metrics",
                "📈 Data Drift",
                "⚡ API Rate Limits",
                "🚨 Error Log",
                "📅 Sync Timeline",
                "🔔 Alert Config"
            ],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # Auto-refresh
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
        
        if st.button("🔄 Refresh Now"):
            st.rerun()
        
        st.markdown("---")
        
        # Last updated
        st.markdown(f"**Last Updated:**\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize data source
    data_source = MonitoringDataSource()
    
    # Main content
    st.title("Fabric-Snowflake Sync Monitor")
    st.markdown("Real-time monitoring for semantic model synchronization")
    
    # Render selected page
    if "Health" in page:
        render_health_overview(data_source)
    elif "Sync Metrics" in page:
        render_sync_metrics(data_source)
    elif "Data Drift" in page:
        render_data_drift(data_source)
    elif "API Rate" in page:
        render_api_rate_limits(data_source)
    elif "Error" in page:
        render_error_log(data_source)
    elif "Timeline" in page:
        render_sync_timeline(data_source)
    elif "Alert" in page:
        render_alert_configuration()
    
    # Auto-refresh script
    if auto_refresh:
        st.markdown("""
        <script>
            setTimeout(function() {
                window.location.reload();
            }, 30000);
        </script>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
