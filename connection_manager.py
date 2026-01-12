"""
Connection Manager for Snowflake Credit Monitor
Handles multiple Snowflake connection configurations
"""

import json
import os
from pathlib import Path
import snowflake.connector
from config import SNOWFLAKE_CONFIG

# Path for storing connections
CONNECTIONS_FILE = Path(__file__).parent / "connections.json"


def load_connections() -> dict:
    """
    Load saved connections from JSON file.
    
    Returns:
        dict: Dictionary with 'connections' and 'active' keys
    """
    if not CONNECTIONS_FILE.exists():
        return {"connections": {}, "active": None}
    
    try:
        with open(CONNECTIONS_FILE, 'r') as f:
            data = json.load(f)
            return {
                "connections": data.get("connections", {}),
                "active": data.get("active", None)
            }
    except (json.JSONDecodeError, IOError):
        return {"connections": {}, "active": None}


def save_connections(data: dict) -> bool:
    """
    Save connections to JSON file.
    
    Args:
        data: Dictionary with 'connections' and 'active' keys
        
    Returns:
        bool: True if successful
    """
    try:
        with open(CONNECTIONS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except IOError:
        return False


def save_connection(name: str, config: dict) -> bool:
    """
    Save a new connection or update existing one.
    
    Args:
        name: Connection name
        config: Connection configuration dict with account, user, password, warehouse
        
    Returns:
        bool: True if successful
    """
    data = load_connections()
    data["connections"][name] = {
        "account": config.get("account", ""),
        "user": config.get("user", ""),
        "password": config.get("password", ""),
        "warehouse": config.get("warehouse", "COMPUTE_WH")
    }
    
    # Set as active if it's the first connection
    if data["active"] is None:
        data["active"] = name
    
    return save_connections(data)


def delete_connection(name: str) -> bool:
    """
    Delete a saved connection.
    
    Args:
        name: Connection name to delete
        
    Returns:
        bool: True if successful
    """
    data = load_connections()
    
    if name not in data["connections"]:
        return False
    
    del data["connections"][name]
    
    # Update active connection if deleted
    if data["active"] == name:
        remaining = list(data["connections"].keys())
        data["active"] = remaining[0] if remaining else None
    
    return save_connections(data)


def get_active_connection() -> tuple:
    """
    Get the currently active connection configuration.
    
    Returns:
        tuple: (connection_name, config_dict) or (None, None) if no active connection
    """
    data = load_connections()
    active_name = data["active"]
    
    if active_name and active_name in data["connections"]:
        return active_name, data["connections"][active_name]
    
    # Fall back to environment variables
    if SNOWFLAKE_CONFIG.get("account") and SNOWFLAKE_CONFIG.get("user"):
        return "Environment", SNOWFLAKE_CONFIG
    
    return None, None


def set_active_connection(name: str) -> bool:
    """
    Set the active connection.
    
    Args:
        name: Connection name to set as active
        
    Returns:
        bool: True if successful
    """
    data = load_connections()
    
    if name not in data["connections"]:
        return False
    
    data["active"] = name
    return save_connections(data)


def get_connection_names() -> list:
    """
    Get list of all saved connection names.
    
    Returns:
        list: List of connection names
    """
    data = load_connections()
    names = list(data["connections"].keys())
    
    # Add environment option if configured
    if SNOWFLAKE_CONFIG.get("account") and SNOWFLAKE_CONFIG.get("user"):
        if "Environment (.env)" not in names:
            names.insert(0, "Environment (.env)")
    
    return names


def get_snowflake_config() -> dict:
    """
    Get the current Snowflake configuration for connection.
    
    Returns:
        dict: Snowflake connection config
    """
    name, config = get_active_connection()
    
    if config:
        return {
            "account": config.get("account", ""),
            "user": config.get("user", ""),
            "password": config.get("password", ""),
            "warehouse": config.get("warehouse", "COMPUTE_WH"),
            "database": "SNOWFLAKE",
            "schema": "ACCOUNT_USAGE"
        }
    
    return SNOWFLAKE_CONFIG


def test_connection(config: dict) -> tuple:
    """
    Test a Snowflake connection.
    
    Args:
        config: Connection configuration dict
        
    Returns:
        tuple: (success: bool, message: str)
    """
    test_config = {
        "account": config.get("account", ""),
        "user": config.get("user", ""),
        "password": config.get("password", ""),
        "warehouse": config.get("warehouse", "COMPUTE_WH"),
        "database": "SNOWFLAKE",
        "schema": "ACCOUNT_USAGE"
    }
    
    try:
        conn = snowflake.connector.connect(**test_config)
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT()")
        result = cur.fetchone()
        conn.close()
        return True, f"Connected as {result[0]} to {result[1]}"
    except Exception as e:
        error_msg = str(e)
        if "Incorrect username or password" in error_msg:
            return False, "Invalid username or password"
        elif "Account" in error_msg and "not found" in error_msg:
            return False, "Account identifier not found"
        else:
            return False, f"Connection failed: {error_msg[:100]}"
