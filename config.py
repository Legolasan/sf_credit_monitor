"""
Configuration settings for Snowflake Credit Monitor
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": "SNOWFLAKE",
    "schema": "ACCOUNT_USAGE",
}

# Application settings
FIVETRAN_WAREHOUSE = "FIVETRAN_WAREHOUSE"
CREDIT_RATE = 3.00  # USD per credit
DEFAULT_DAYS = 7
CACHE_TTL = 300  # Cache for 5 minutes

# Warehouse size credit multipliers (credits per hour)
WAREHOUSE_SIZE_MULTIPLIERS = {
    'X-Small': 1,
    'Small': 2,
    'Medium': 4,
    'Large': 8,
    'X-Large': 16,
    '2X-Large': 32,
    '3X-Large': 64,
    '4X-Large': 128
}

# Page configuration
PAGE_CONFIG = {
    "page_title": "Snowflake Credit Monitor",
    "page_icon": "❄️",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Custom CSS styling
CUSTOM_CSS = """
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #0d1b2a 100%);
        border-radius: 12px;
        padding: 20px;
        margin: 10px 0;
    }
    .stMetric {
        background: linear-gradient(135deg, #1e3a5f 0%, #0d1b2a 100%);
        border-radius: 12px;
        padding: 15px;
    }
    h1 {
        color: #29b5e8;
    }
    .stDataFrame {
        border-radius: 8px;
    }
</style>
"""
