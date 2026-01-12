# Snowflake Credit Monitor

A Streamlit-based dashboard to monitor and analyze Snowflake credit consumption, with a focus on tracking Fivetran ETL loads.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-red.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## Features

- **Multi-Warehouse Selection** - Analyze one or multiple warehouses simultaneously
- **Cost Breakdown** - Per-warehouse credit and cost distribution
- **Daily Trends** - Visual charts showing credit consumption over time
- **Hourly Patterns** - Identify peak usage hours for optimization
- **Query Analysis** - Breakdown by query type (INSERT, COPY, MERGE, etc.)
- **Warehouse Efficiency** - Utilization metrics, queue times, and optimization recommendations
- **Expensive Queries** - Identify costliest queries with estimated credit usage
- **Auto-Refresh** - Configurable refresh intervals (30s, 1min, 5min)
- **Real-time Metrics** - Total credits, estimated costs, query counts

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Legolasan/sf_credit_monitor.git
cd sf_credit_monitor
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Credentials

Create a `.env` file in the project root:

```bash
# Snowflake credentials
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password

# PostgreSQL (optional, for bulk operations scripts)
POSTGRES_URL=postgres://user:password@host:port/database?sslmode=require
```

> **Note:** The `.env` file is gitignored and will not be committed to the repository.

### 5. Run the Dashboard

```bash
streamlit run app.py
```

> **Note:** The legacy `snowflake_credit_monitor.py` is still available but the new modular `app.py` is recommended.

### 6. Access the App

Open your browser and navigate to:

```
http://localhost:8501
```

## Dashboard Overview

### Sidebar Controls

| Control | Description |
|---------|-------------|
| **Select Warehouses** | Multi-select dropdown to choose warehouses |
| **Days to analyze** | Slider to set the time range (1-30 days) |
| **Auto-refresh** | Toggle automatic data refresh |
| **Refresh Data** | Manual refresh button |
| **Reload Warehouses** | Refresh the warehouse list |

### Metrics Displayed

- **Total Credits** - Sum of credits used across selected warehouses
- **Estimated Cost** - Cost calculation at $3.00/credit (configurable)
- **Total Queries** - Number of queries executed
- **Avg Cost/Query** - Average cost per query

### Charts & Tables

1. **Cost Breakdown by Warehouse** - Table and pie chart (when multiple warehouses selected)
2. **Daily Credit Consumption** - Bar chart with daily trends
3. **Hourly Usage Pattern** - Identifies peak hours
4. **Query Type Breakdown** - Pie chart showing INSERT/COPY/MERGE distribution
5. **Warehouse Efficiency** - Utilization metrics with smart recommendations
6. **Expensive Queries** - Top N queries by execution time with cost estimates
7. **Daily Breakdown Table** - Detailed daily statistics

### Warehouse Efficiency Metrics

| Metric | Description |
|--------|-------------|
| **Total Queries** | Number of queries executed on the warehouse |
| **Active Hours** | Hours the warehouse was actively running |
| **Credits/Query** | Average credit cost per query |
| **Avg Queue Time** | Average time queries waited for resources |
| **Efficiency Score** | Relative efficiency rating (higher is better) |

**Smart Recommendations:**
- 🔴 High queue time → Suggests scaling up warehouse size
- 🔵 Low utilization → Suggests reducing auto-suspend timeout
- 🟢 Healthy → Shows efficiency confirmation

### Expensive Queries Analysis

| Column | Description |
|--------|-------------|
| **User** | Who executed the query |
| **Warehouse** | Which warehouse was used |
| **Type** | Query type (SELECT, INSERT, COPY, MERGE, etc.) |
| **Exec Time** | Execution duration in seconds |
| **GB Scanned** | Amount of data scanned |
| **Est. Cost** | Estimated credit cost based on warehouse size |

**Features:**
- Adjustable limit (10, 15, 25, 50 queries)
- Cost distribution by query type (pie chart)
- Expandable SQL preview for top 5 queries

## Project Structure

```
sf_credit_monitor/
├── app.py                    # Main Streamlit entry point
├── config.py                 # Configuration & constants
├── database.py               # Snowflake connection handling
├── queries.py                # All data fetching functions
├── components/
│   ├── __init__.py
│   ├── sidebar.py            # Sidebar controls
│   ├── metrics.py            # Summary metrics section
│   ├── charts.py             # Daily/hourly charts
│   ├── efficiency.py         # Warehouse efficiency section
│   ├── expensive_queries.py  # Expensive queries analysis
│   └── warehouse_breakdown.py # Per-warehouse breakdown
├── snowflake_credit_monitor.py  # Legacy monolithic version
├── requirements.txt
├── .env                      # Credentials (not in repo)
└── README.md
```

## Configuration

### Snowflake Account Identifier

Find your account identifier in your Snowflake URL:
- Format: `account_identifier` (e.g., `xy12345.us-east-1` or `myorg-myaccount`)

### Credit Rate

The default credit rate is $3.00/credit. To change this, modify the `CREDIT_RATE` variable in `snowflake_credit_monitor.py`:

```python
CREDIT_RATE = 3.00  # USD per credit
```

### Required Snowflake Permissions

The user needs access to:
- `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` - Credit consumption data
- `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY` - Warehouse utilization metrics
- `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` - Query execution details
- `SHOW WAREHOUSES` command - List active warehouses

## Additional Scripts

This repository also includes PostgreSQL bulk operation scripts for testing:

| Script | Description |
|--------|-------------|
| `bulk_insert.py` | Insert 1M records using COPY command |
| `bulk_insert_append.py` | Append records without dropping table |
| `bulk_update.py` | Bulk update with multiple methods |
| `bulk_update_50k.py` | Smaller update script (50K records) |
| `run_insert_then_update.py` | Combined insert + update pipeline |

## Troubleshooting

### Connection Issues

```
Connection failed: could not translate host name...
```
- Verify your `SNOWFLAKE_ACCOUNT` is correct
- Check network connectivity to Snowflake

### Permission Errors

```
Query failed: Insufficient privileges...
```
- Ensure your user has access to `ACCOUNT_USAGE` views
- Contact your Snowflake admin for permissions

### Decimal Type Errors

If you see `TypeError: unsupported operand type(s) for *: 'decimal.Decimal' and 'float'`, the app should handle this automatically. If not, ensure you have the latest version.

## License

MIT License - feel free to use and modify as needed.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

**Built with ❄️ Snowflake + 🎈 Streamlit**
