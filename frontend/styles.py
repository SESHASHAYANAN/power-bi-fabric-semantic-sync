"""
Styles Module for Streamlit Frontend
Centralized Design Token System - Color Palette Lock-Down

This module provides a complete design token system to prevent
UI color inconsistencies and ensure visual stability.
"""
import streamlit as st

# ============================================
# DESIGN TOKENS - CENTRALIZED COLOR SYSTEM
# ============================================
# This is the SINGLE SOURCE OF TRUTH for all colors.
# DO NOT use hardcoded colors anywhere else in the application.

COLORS = {
    # Primary Brand Colors
    "primary": "#667eea",
    "primary_hover": "#5a6fd6",
    "primary_light": "#8b9ff5",
    "primary_dark": "#4c5dc2",
    
    # Secondary Brand Colors
    "secondary": "#764ba2",
    "secondary_hover": "#6a4392",
    "secondary_light": "#9067b5",
    "secondary_dark": "#5c3a82",
    
    # Accent Colors - Snowflake Theme
    "accent_snowflake": "#29b5e8",
    "accent_snowflake_light": "#4fc7f0",
    "accent_snowflake_dark": "#1a9dc8",
    
    # Semantic Status Colors
    "success": "#10b981",
    "success_light": "#34d399",
    "success_dark": "#059669",
    "success_bg": "rgba(16, 185, 129, 0.12)",
    
    "warning": "#f59e0b",
    "warning_light": "#fbbf24",
    "warning_dark": "#d97706",
    "warning_bg": "rgba(245, 158, 11, 0.12)",
    
    "error": "#ef4444",
    "error_light": "#f87171",
    "error_dark": "#dc2626",
    "error_bg": "rgba(239, 68, 68, 0.12)",
    
    "info": "#3b82f6",
    "info_light": "#60a5fa",
    "info_dark": "#2563eb",
    "info_bg": "rgba(59, 130, 246, 0.12)",
    
    # Text Colors - WHITE/LIGHT FOR DARK BACKGROUNDS
    "text_primary": "#ffffff",       # Pure white for primary text
    "text_secondary": "#f1f5f9",     # Very light gray for secondary text
    "text_muted": "#cbd5e1",         # Light gray for muted text
    "text_light": "#e2e8f0",         # Light text
    "text_on_primary": "#ffffff",    # White text on primary backgrounds
    "text_on_dark": "#ffffff",       # White text on dark backgrounds
    
    # Background Colors - CONTROLLED PALETTE
    "bg_app": "#0f0f23",           # Dark app background
    "bg_app_gradient_start": "#0f0f23",
    "bg_app_gradient_end": "#1a1a3e",
    
    "bg_card": "#1a1a2e",          # Card backgrounds - NO WHITE
    "bg_card_hover": "#252542",
    "bg_card_gradient_start": "rgba(102, 126, 234, 0.08)",
    "bg_card_gradient_end": "rgba(118, 75, 162, 0.08)",
    
    "bg_sidebar": "#667eea",       # Sidebar gradient start
    "bg_sidebar_end": "#764ba2",   # Sidebar gradient end
    
    "bg_input": "#252542",         # Input/form backgrounds
    "bg_input_focus": "#2d2d4f",
    
    "bg_elevated": "#222240",      # Elevated surfaces
    "bg_overlay": "rgba(15, 15, 35, 0.8)",
    
    # Border Colors
    "border_default": "rgba(102, 126, 234, 0.2)",
    "border_light": "rgba(255, 255, 255, 0.08)",
    "border_focus": "#667eea",
    "border_card": "rgba(102, 126, 234, 0.15)",
    
    # Shadow Colors
    "shadow_sm": "rgba(0, 0, 0, 0.15)",
    "shadow_md": "rgba(0, 0, 0, 0.25)",
    "shadow_lg": "rgba(0, 0, 0, 0.35)",
    "shadow_glow": "rgba(102, 126, 234, 0.3)",
    
    # Glass Effect Colors
    "glass_bg": "rgba(26, 26, 46, 0.85)",
    "glass_border": "rgba(255, 255, 255, 0.1)",
    
    # Tab Colors
    "tab_bg": "#1a1a2e",
    "tab_bg_active": "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
    "tab_text": "#94a3b8",
    "tab_text_active": "#f8fafc",
}

# ============================================
# CSS CUSTOM PROPERTIES (VARIABLES)
# ============================================
def get_css_variables():
    """Generate CSS custom properties from COLORS dict."""
    css_vars = ":root {\n"
    for key, value in COLORS.items():
        css_var_name = f"--color-{key.replace('_', '-')}"
        css_vars += f"    {css_var_name}: {value};\n"
    css_vars += "}\n"
    return css_vars


def inject_styles():
    """Inject custom CSS styles into Streamlit app with locked-down color palette."""
    
    # Generate CSS variables
    css_variables = get_css_variables()
    
    st.markdown(f"""
    <style>
        /* ============================================
           CSS CUSTOM PROPERTIES - DESIGN TOKENS
           ============================================ */
        {css_variables}
        
        /* ============================================
           GLOBAL RESET & BASE STYLES
           ============================================ */
        
        /* Force dark theme consistency */
        .stApp {{
            background: linear-gradient(135deg, 
                var(--color-bg-app-gradient-start) 0%, 
                var(--color-bg-app-gradient-end) 100%) !important;
            color: var(--color-text-secondary) !important;
        }}
        
        /* Override any white backgrounds */
        .stApp, 
        .stApp > div,
        .stApp [data-testid="stAppViewContainer"],
        .stApp [data-testid="stHeader"],
        .main,
        .block-container {{
            background: transparent !important;
            color: var(--color-text-secondary) !important;
        }}
        
        /* ============================================
           SIDEBAR STYLING
           ============================================ */
        section[data-testid="stSidebar"] {{
            background: linear-gradient(180deg, 
                var(--color-bg-sidebar) 0%, 
                var(--color-bg-sidebar-end) 100%) !important;
        }}
        
        section[data-testid="stSidebar"] > div {{
            background: transparent !important;
        }}
        
        section[data-testid="stSidebar"] .stMarkdown,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] label {{
            color: var(--color-text-on-primary) !important;
        }}
        
        section[data-testid="stSidebar"] label {{
            color: rgba(255, 255, 255, 0.9) !important;
        }}
        
        section[data-testid="stSidebar"] hr {{
            border-color: rgba(255, 255, 255, 0.15) !important;
        }}
        
        /* ============================================
           TYPOGRAPHY
           ============================================ */
        h1, h2, h3, h4, h5, h6 {{
            color: var(--color-text-on-dark) !important;
        }}
        
        h1 {{
            background: linear-gradient(135deg, var(--color-primary) 0%, var(--color-secondary) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        p, span, li, td, th {{
            color: #ffffff !important;
        }}
        
        /* Force white text in all text elements */
        .stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown li {{
            color: #ffffff !important;
        }}
        
        /* Tables */
        td, th, tr {{
            color: #ffffff !important;
        }}
        
        a {{
            color: var(--color-primary-light) !important;
        }}
        
        a:hover {{
            color: var(--color-primary) !important;
        }}
        
        /* Markdown text */
        .stMarkdown {{
            color: #ffffff !important;
        }}
        
        /* All label and text elements */
        label, .stTextInput label, .stSelectbox label {{
            color: #ffffff !important;
        }}
        
        /* ============================================
           BUTTONS - LOCKED STYLING
           ============================================ */
        .stButton > button {{
            background: var(--color-bg-card) !important;
            color: var(--color-text-on-dark) !important;
            border: 1px solid var(--color-border-default) !important;
            border-radius: 10px !important;
            font-weight: 600 !important;
            transition: all 0.3s ease !important;
            padding: 0.5rem 1rem !important;
        }}
        
        .stButton > button:hover {{
            background: var(--color-bg-card-hover) !important;
            border-color: var(--color-primary) !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 4px 12px var(--color-shadow-glow) !important;
        }}
        
        .stButton > button:focus {{
            outline: 2px solid var(--color-primary) !important;
            outline-offset: 2px !important;
        }}
        
        /* Primary buttons */
        .stButton > button[kind="primary"],
        .stButton > button[data-testid="baseButton-primary"] {{
            background: linear-gradient(135deg, var(--color-primary) 0%, var(--color-secondary) 100%) !important;
            border: none !important;
            color: var(--color-text-on-primary) !important;
        }}
        
        .stButton > button[kind="primary"]:hover,
        .stButton > button[data-testid="baseButton-primary"]:hover {{
            background: linear-gradient(135deg, var(--color-primary-dark) 0%, var(--color-secondary-dark) 100%) !important;
            box-shadow: 0 6px 20px var(--color-shadow-glow) !important;
        }}
        
        /* ============================================
           TABS - NO WHITE BACKGROUNDS
           ============================================ */
        .stTabs {{
            background: transparent !important;
        }}
        
        .stTabs [data-baseweb="tab-list"] {{
            gap: 8px !important;
            background: transparent !important;
            padding: 4px !important;
        }}
        
        .stTabs [data-baseweb="tab"] {{
            background: var(--color-bg-card) !important;
            color: var(--color-tab-text) !important;
            border-radius: 10px !important;
            padding: 10px 20px !important;
            font-weight: 600 !important;
            border: 1px solid var(--color-border-card) !important;
            transition: all 0.3s ease !important;
        }}
        
        .stTabs [data-baseweb="tab"]:hover {{
            background: var(--color-bg-card-hover) !important;
            border-color: var(--color-primary) !important;
        }}
        
        .stTabs [aria-selected="true"] {{
            background: linear-gradient(135deg, var(--color-primary) 0%, var(--color-secondary) 100%) !important;
            color: var(--color-text-on-primary) !important;
            border: none !important;
        }}
        
        /* Tab panels */
        .stTabs [data-baseweb="tab-panel"] {{
            background: transparent !important;
        }}
        
        /* ============================================
           CARDS & CONTAINERS
           ============================================ */
        .glass-card {{
            background: var(--color-glass-bg) !important;
            backdrop-filter: blur(12px) !important;
            -webkit-backdrop-filter: blur(12px) !important;
            border-radius: 16px !important;
            padding: 24px !important;
            box-shadow: 0 8px 32px var(--color-shadow-md) !important;
            border: 1px solid var(--color-glass-border) !important;
        }}
        
        /* Custom styled divs in markdown */
        div[style*="background"] {{
            border-radius: 12px !important;
        }}
        
        /* ============================================
           METRICS - STYLED
           ============================================ */
        [data-testid="stMetric"] {{
            background: var(--color-bg-card) !important;
            padding: 16px !important;
            border-radius: 12px !important;
            border: 1px solid var(--color-border-card) !important;
        }}
        
        [data-testid="stMetricValue"] {{
            font-size: 2rem !important;
            font-weight: 700 !important;
            color: var(--color-primary-light) !important;
        }}
        
        [data-testid="stMetricLabel"] {{
            font-size: 0.9rem !important;
            color: var(--color-text-muted) !important;
        }}
        
        [data-testid="stMetricDelta"] {{
            color: var(--color-success) !important;
        }}
        
        /* ============================================
           EXPANDERS - DARK THEME
           ============================================ */
        .streamlit-expanderHeader {{
            background: var(--color-bg-card) !important;
            color: var(--color-text-on-dark) !important;
            border-radius: 10px !important;
            border: 1px solid var(--color-border-card) !important;
        }}
        
        .streamlit-expanderHeader:hover {{
            background: var(--color-bg-card-hover) !important;
            border-color: var(--color-primary) !important;
        }}
        
        [data-testid="stExpander"] {{
            background: var(--color-bg-card) !important;
            border: 1px solid var(--color-border-card) !important;
            border-radius: 12px !important;
        }}
        
        [data-testid="stExpander"] > div {{
            background: transparent !important;
        }}
        
        /* ============================================
           INPUTS & FORMS
           ============================================ */
        .stTextInput > div > div,
        .stSelectbox > div > div,
        .stTextArea > div > div,
        .stNumberInput > div > div {{
            background: var(--color-bg-input) !important;
            border: 1px solid var(--color-border-default) !important;
            border-radius: 8px !important;
            color: var(--color-text-on-dark) !important;
        }}
        
        .stTextInput > div > div:focus-within,
        .stSelectbox > div > div:focus-within,
        .stTextArea > div > div:focus-within {{
            border-color: var(--color-primary) !important;
            box-shadow: 0 0 0 3px var(--color-shadow-glow) !important;
        }}
        
        .stTextInput input,
        .stTextArea textarea {{
            background: transparent !important;
            color: var(--color-text-on-dark) !important;
        }}
        
        /* Select box dropdown */
        [data-baseweb="select"] {{
            background: var(--color-bg-input) !important;
        }}
        
        [data-baseweb="popover"] {{
            background: var(--color-bg-elevated) !important;
            border: 1px solid var(--color-border-default) !important;
        }}
        
        [data-baseweb="menu"] {{
            background: var(--color-bg-elevated) !important;
        }}
        
        [data-baseweb="menu"] li {{
            background: var(--color-bg-elevated) !important;
            color: var(--color-text-on-dark) !important;
        }}
        
        [data-baseweb="menu"] li:hover {{
            background: var(--color-bg-card-hover) !important;
        }}
        
        /* ============================================
           FILE UPLOADER - THEMED
           ============================================ */
        [data-testid="stFileUploader"] {{
            background: var(--color-bg-card) !important;
            border: 2px dashed var(--color-primary) !important;
            border-radius: 16px !important;
            padding: 20px !important;
        }}
        
        [data-testid="stFileUploader"]:hover {{
            border-color: var(--color-primary-light) !important;
            background: var(--color-bg-card-hover) !important;
        }}
        
        [data-testid="stFileUploader"] section {{
            background: transparent !important;
        }}
        
        [data-testid="stFileUploader"] button {{
            background: var(--color-primary) !important;
            color: var(--color-text-on-primary) !important;
        }}
        
        /* ============================================
           DATAFRAMES - STYLED
           ============================================ */
        [data-testid="stDataFrame"],
        .stDataFrame {{
            background: var(--color-bg-card) !important;
            border-radius: 12px !important;
            overflow: hidden !important;
        }}
        
        [data-testid="stDataFrame"] div,
        .stDataFrame div {{
            background: var(--color-bg-card) !important;
            color: var(--color-text-secondary) !important;
        }}
        
        .stDataFrame th {{
            background: var(--color-bg-elevated) !important;
            color: var(--color-text-on-dark) !important;
        }}
        
        .stDataFrame td {{
            background: var(--color-bg-card) !important;
            color: var(--color-text-secondary) !important;
        }}
        
        /* ============================================
           ALERTS & MESSAGES
           ============================================ */
        .stSuccess {{
            background: var(--color-success-bg) !important;
            border-left: 4px solid var(--color-success) !important;
            color: var(--color-success-light) !important;
        }}
        
        .stInfo {{
            background: var(--color-info-bg) !important;
            border-left: 4px solid var(--color-info) !important;
            color: var(--color-info-light) !important;
        }}
        
        .stWarning {{
            background: var(--color-warning-bg) !important;
            border-left: 4px solid var(--color-warning) !important;
            color: var(--color-warning-light) !important;
        }}
        
        .stError {{
            background: var(--color-error-bg) !important;
            border-left: 4px solid var(--color-error) !important;
            color: var(--color-error-light) !important;
        }}
        
        /* Alert containers */
        [data-testid="stAlert"] {{
            background: var(--color-bg-card) !important;
            border-radius: 12px !important;
        }}
        
        /* ============================================
           DIVIDERS
           ============================================ */
        hr {{
            border-color: var(--color-border-light) !important;
        }}
        
        /* ============================================
           PROGRESS BARS
           ============================================ */
        .stProgress > div > div {{
            background: linear-gradient(135deg, var(--color-primary) 0%, var(--color-secondary) 100%) !important;
        }}
        
        .stProgress {{
            background: var(--color-bg-input) !important;
        }}
        
        /* ============================================
           SPINNERS & LOADING
           ============================================ */
        .stSpinner > div {{
            border-top-color: var(--color-primary) !important;
        }}
        
        /* ============================================
           TOAST MESSAGES
           ============================================ */
        .stToast {{
            background: var(--color-bg-elevated) !important;
            color: var(--color-text-on-dark) !important;
            border: 1px solid var(--color-border-default) !important;
        }}
        
        /* ============================================
           TOGGLE / CHECKBOX
           ============================================ */
        [data-baseweb="checkbox"] span {{
            background: var(--color-bg-input) !important;
            border-color: var(--color-border-default) !important;
        }}
        
        [data-baseweb="checkbox"] [data-checked="true"] span {{
            background: var(--color-primary) !important;
            border-color: var(--color-primary) !important;
        }}
        
        /* Toggle switch */
        [data-testid="stBaseButton-toggleButton"] {{
            background: var(--color-bg-input) !important;
        }}
        
        /* ============================================
           JSON DISPLAY
           ============================================ */
        [data-testid="stJson"] {{
            background: var(--color-bg-elevated) !important;
            border-radius: 12px !important;
            padding: 16px !important;
        }}
        
        /* ============================================
           CODE BLOCKS
           ============================================ */
        code, pre {{
            background: var(--color-bg-elevated) !important;
            color: var(--color-text-on-dark) !important;
            border-radius: 8px !important;
        }}
        
        .stCodeBlock {{
            background: var(--color-bg-elevated) !important;
        }}
        
        /* ============================================
           SCROLLBAR
           ============================================ */
        ::-webkit-scrollbar {{
            width: 8px;
            height: 8px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: var(--color-bg-app);
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: var(--color-primary);
            border-radius: 4px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: var(--color-secondary);
        }}
        
        /* ============================================
           STATUS INDICATORS - NO FADE/PULSE ANIMATIONS
           ============================================ */
        .status-dot {{
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 8px;
            opacity: 1 !important;
        }}
        
        .status-active {{
            background: var(--color-success);
            box-shadow: 0 0 8px var(--color-success);
            /* REMOVED pulse animation that causes fade */
        }}
        
        .status-inactive {{
            background: var(--color-error);
        }}
        
        /* ============================================
           HIDE STREAMLIT BRANDING
           ============================================ */
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
        
        /* ============================================
           CRITICAL: FORCE WHITE TEXT EVERYWHERE
           ============================================ */
        
        /* Force white text on all common elements */
        *, *::before, *::after {{
            color: #ffffff !important;
        }}
        
        /* Specific overrides for Streamlit elements */
        .stMarkdown, .stMarkdown *, 
        .stText, .stText *,
        p, span, div, label, td, th, tr, li, a,
        h1, h2, h3, h4, h5, h6,
        .element-container, .element-container *,
        [data-testid="stMarkdownContainer"], 
        [data-testid="stMarkdownContainer"] * {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* Expander text */
        [data-testid="stExpander"] *,
        .streamlit-expanderHeader, 
        .streamlit-expanderHeader * {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* Metric text */
        [data-testid="stMetric"] *,
        [data-testid="stMetricValue"],
        [data-testid="stMetricLabel"],
        [data-testid="stMetricDelta"] {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* Button text */
        .stButton button, .stButton button * {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* Tab text */
        .stTabs [data-baseweb="tab"],
        .stTabs [data-baseweb="tab"] * {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* DataFrame/Table text */
        .stDataFrame *, [data-testid="stDataFrame"] *,
        table, table *, tbody, tbody *, thead, thead * {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* Form/Input labels */
        .stTextInput label, .stSelectbox label, 
        .stTextArea label, .stNumberInput label,
        [data-testid="stWidgetLabel"],
        [data-testid="stWidgetLabel"] * {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* Alert/Info/Warning/Error boxes */
        .stAlert, .stAlert *,
        [data-testid="stAlert"], [data-testid="stAlert"] * {{
            opacity: 1 !important;
        }}
        
        /* JSON display */
        [data-testid="stJson"], [data-testid="stJson"] * {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* Code blocks */
        code, pre, code *, pre * {{
            color: #ffffff !important;
            opacity: 1 !important;
        }}
        
        /* ============================================
           DISABLE ALL FADE/OPACITY ANIMATIONS
           ============================================ */
        @keyframes none {{}}
        
        /* Override any fade animations */
        [class*="fade"], [class*="Fade"],
        [style*="opacity: 0"], [style*="opacity:0"] {{
            opacity: 1 !important;
            animation: none !important;
            transition: none !important;
        }}
        
        /* ============================================
           OVERRIDE ANY REMAINING WHITE BACKGROUNDS
           ============================================ */
        
        /* Catch-all for white backgrounds */
        [style*="background: white"],
        [style*="background:#fff"],
        [style*="background: #fff"],
        [style*="background:#ffffff"],
        [style*="background: #ffffff"],
        [style*="background-color: white"],
        [style*="background-color:#fff"],
        [style*="background-color: #fff"],
        [style*="background-color:#ffffff"],
        [style*="background-color: #ffffff"],
        [style*="background: rgb(255, 255, 255)"],
        [style*="background-color: rgb(255, 255, 255)"] {{
            background: var(--color-bg-card) !important;
        }}
        
    </style>
    """, unsafe_allow_html=True)


# ============================================
# HELPER FUNCTIONS FOR CONSISTENT STYLING
# ============================================

def get_status_color(is_active: bool) -> str:
    """Get the appropriate status color."""
    return COLORS["success"] if is_active else COLORS["error"]


def get_card_style(variant: str = "default") -> str:
    """
    Get inline style for cards.
    
    Args:
        variant: One of 'default', 'fabric', 'snowflake', 'success', 'error'
    """
    base_style = f"""
        background: {COLORS['bg_card']};
        border-radius: 16px;
        padding: 24px;
        border: 1px solid {COLORS['border_card']};
        box-shadow: 0 4px 20px {COLORS['shadow_sm']};
    """
    
    variants = {
        "fabric": f"""
            background: linear-gradient(135deg, 
                {COLORS['bg_card_gradient_start']}, 
                {COLORS['bg_card_gradient_end']});
            border-left: 4px solid {COLORS['primary']};
        """,
        "snowflake": f"""
            background: linear-gradient(135deg, 
                rgba(41, 181, 232, 0.08), 
                rgba(14, 165, 233, 0.08));
            border-left: 4px solid {COLORS['accent_snowflake']};
        """,
        "success": f"""
            background: linear-gradient(135deg, 
                {COLORS['success_bg']}, 
                rgba(16, 185, 129, 0.2));
            border-left: 4px solid {COLORS['success']};
        """,
        "error": f"""
            background: linear-gradient(135deg, 
                {COLORS['error_bg']}, 
                rgba(239, 68, 68, 0.2));
            border-left: 4px solid {COLORS['error']};
        """,
    }
    
    return base_style + variants.get(variant, "")


def get_heading_style(level: int = 3) -> str:
    """Get inline style for headings."""
    return f"margin: 0; color: {COLORS['text_on_dark']};"


def get_text_style(variant: str = "default") -> str:
    """
    Get inline style for text.
    
    Args:
        variant: One of 'default', 'muted', 'success', 'error', 'on_dark'
    """
    variants = {
        "default": COLORS["text_secondary"],
        "muted": COLORS["text_muted"],
        "success": COLORS["success"],
        "error": COLORS["error"],
        "on_dark": COLORS["text_on_dark"],
        "primary": COLORS["primary"],
    }
    color = variants.get(variant, COLORS["text_secondary"])
    return f"color: {color};"


# ============================================
# COLOR DOCUMENTATION
# ============================================
COLOR_USAGE_GUIDE = """
# Color Palette Usage Guidelines

## Primary Brand Colors
- `primary` (#667eea): Main brand color, CTAs, links, highlights
- `secondary` (#764ba2): Secondary actions, gradients with primary

## Status Colors
- `success` (#10b981): Connected states, successful operations
- `warning` (#f59e0b): Warnings, pending states
- `error` (#ef4444): Errors, disconnected states
- `info` (#3b82f6): Informational messages

## Text Colors
- `text_primary` (#1e293b): Main headings (light mode only)
- `text_secondary` (#475569): Body text
- `text_muted` (#64748b): Secondary/helper text
- `text_on_dark` (#f1f5f9): Text on dark backgrounds

## Background Colors
- `bg_app`: Main app background (dark gradient)
- `bg_card`: Card/container backgrounds
- `bg_input`: Form element backgrounds
- `bg_elevated`: Elevated surfaces (dropdowns, modals)

## NEVER USE
- #FFFFFF or white for backgrounds
- rgb(255, 255, 255) anywhere
- Hardcoded colors not in this palette
"""
