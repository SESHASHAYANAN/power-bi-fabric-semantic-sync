"""
Styles Module for Streamlit Frontend
Custom CSS injection for premium look and feel
"""
import streamlit as st

# Color palette
COLORS = {
    "primary": "#667eea",
    "secondary": "#764ba2",
    "success": "#10b981",
    "warning": "#f59e0b",
    "error": "#ef4444",
    "text": "#1e293b",
    "text_light": "#64748b",
    "bg_light": "#f8fafc",
    "bg_card": "#ffffff",
}


def inject_styles():
    """Inject custom CSS styles into Streamlit app."""
    st.markdown(f"""
    <style>
        /* Global Styles */
        .stApp {{
            background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
        }}
        
        /* Sidebar Styling */
        section[data-testid="stSidebar"] {{
            background: linear-gradient(180deg, {COLORS['primary']} 0%, {COLORS['secondary']} 100%);
        }}
        
        section[data-testid="stSidebar"] .stMarkdown {{
            color: white;
        }}
        
        section[data-testid="stSidebar"] label {{
            color: rgba(255, 255, 255, 0.9) !important;
        }}
        
        /* Headers */
        h1, h2, h3 {{
            color: {COLORS['text']};
        }}
        
        /* Cards */
        .glass-card {{
            background: white;
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }}
        
        /* Status Dots */
        .status-dot {{
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 8px;
        }}
        
        .status-active {{
            background: {COLORS['success']};
            box-shadow: 0 0 8px {COLORS['success']};
            animation: pulse 2s infinite;
        }}
        
        .status-inactive {{
            background: {COLORS['error']};
        }}
        
        @keyframes pulse {{
            0% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
            100% {{ opacity: 1; }}
        }}
        
        /* Buttons */
        .stButton > button {{
            border-radius: 10px;
            font-weight: 600;
            transition: all 0.3s ease;
        }}
        
        .stButton > button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
        }}
        
        .stButton > button[kind="primary"] {{
            background: linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['secondary']} 100%);
            border: none;
        }}
        
        /* Metrics */
        [data-testid="stMetricValue"] {{
            font-size: 2rem;
            font-weight: 700;
            color: {COLORS['primary']};
        }}
        
        [data-testid="stMetricLabel"] {{
            font-size: 0.9rem;
            color: {COLORS['text_light']};
        }}
        
        /* Expanders */
        .streamlit-expanderHeader {{
            background: {COLORS['bg_light']};
            border-radius: 10px;
        }}
        
        /* Progress Bar */
        .stProgress > div > div {{
            background: linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['secondary']} 100%);
        }}
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 8px;
        }}
        
        .stTabs [data-baseweb="tab"] {{
            background: white;
            border-radius: 10px;
            padding: 10px 20px;
            font-weight: 600;
        }}
        
        .stTabs [aria-selected="true"] {{
            background: linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['secondary']} 100%);
            color: white;
        }}
        
        /* Divider */
        hr {{
            border-color: rgba(0, 0, 0, 0.05);
        }}
        
        /* File Uploader */
        [data-testid="stFileUploader"] {{
            border: 2px dashed {COLORS['primary']};
            border-radius: 16px;
            padding: 20px;
            background: rgba(102, 126, 234, 0.05);
        }}
        
        /* Toast Messages */
        .stToast {{
            background: {COLORS['primary']};
        }}
        
        /* Hide Streamlit Branding */
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
        
        /* Custom Scrollbar */
        ::-webkit-scrollbar {{
            width: 8px;
            height: 8px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: #f1f1f1;
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: {COLORS['primary']};
            border-radius: 4px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: {COLORS['secondary']};
        }}
    </style>
    """, unsafe_allow_html=True)
