import streamlit as st
import pandas as pd
import os
from databricks import sql

st.title("📊 GoToMeeting Dashboard")

# -----------------------------
# Get Databricks App Credentials
# -----------------------------
HOST = os.getenv("DATABRICKS_HOST")
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")

# -----------------------------
# ✅ PLACE HTTP PATH HERE
# -----------------------------
HTTP_PATH = "https://dbc-54ac37fe-c0ec.cloud.databricks.com/sql/warehouses/bad4090f536f7457?o=3724687917049729"   # <-- Replace with your real path

# -----------------------------
# Connect to Databricks SQL Warehouse
# -----------------------------
conn = sql.connect(
    server_hostname=HOST,
    http_path=HTTP_PATH,
    auth_type="oauth",
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)

# -----------------------------
# Query Silver Table
# -----------------------------
st.header("Silver Layer: Student Details")

silver_query = """
SELECT * FROM gotomeeting_silver.student_details LIMIT 20
"""

silver_df = pd.read_sql(silver_query, conn)
st.dataframe(silver_df)

# -----------------------------
# Query Gold Table
# -----------------------------
st.header("Gold Layer KPI: Duration per Attendee")

gold_query = """
SELECT * FROM gotomeeting_gold.attendee_duration_kpi
"""

gold_df = pd.read_sql(gold_query, conn)
st.dataframe(gold_df)

