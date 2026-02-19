import os
import streamlit as st
from databricks import sql
import pandas as pd

# ----------------------------
# Page Title
# ----------------------------
st.title("✅ GoToMeeting Attendance KPI Dashboard")

st.success("Databricks App is Running Successfully 🎉")

# ----------------------------
# Databricks SQL Warehouse Connection
# ----------------------------
HOST = os.environ["DATABRICKS_HOST"]

HTTP_PATH = "/sql/warehouses/bad4090f536f7457"

CLIENT_ID = os.environ["DATABRICKS_CLIENT_ID"]
CLIENT_SECRET = os.environ["DATABRICKS_CLIENT_SECRET"]

# ----------------------------
# Load Gold KPI Table
# ----------------------------
st.header("📌 Gold Layer KPI Table: attendance_kpi")

try:
    # Connect to SQL Warehouse
    connection = sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        auth_type="oauth",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    st.success("✅ Connected to Databricks SQL Warehouse")

    # Query Gold Table
    query = "SELECT * FROM gold.attendance_kpi"

    df = pd.read_sql(query, connection)

    # Display Table
    st.dataframe(df)

    # ----------------------------
    # KPI Summary Metrics
    # ----------------------------
    st.subheader("📊 KPI Summary")

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Records", len(df))
    col2.metric("Total Columns", len(df.columns))
    col3.metric("Table Name", "gold.attendance_kpi")

except Exception as e:
    st.error("❌ Failed to Load KPI Table")
    st.code(str(e))
