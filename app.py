import streamlit as st
import pandas as pd
from databricks import sql

st.title("📊 GoToMeeting Dashboard")

st.write("Silver + Gold Layer Analytics App")

# -------------------------------
# Connect to Databricks SQL
# -------------------------------
conn = sql.connect(
    server_hostname="dbc-54ac37fe-c0ec.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/<YOUR_WAREHOUSE_ID>",
    access_token="<YOUR_DATABRICKS_TOKEN>"
)

# -------------------------------
# Silver Layer Table
# -------------------------------
st.header("Silver Layer: Student Details")

silver_query = """
SELECT * 
FROM gotomeeting_silver.student_details
LIMIT 20
"""

silver_df = pd.read_sql(silver_query, conn)
st.dataframe(silver_df)

# -------------------------------
# Gold KPI Table
# -------------------------------
st.header("Gold Layer KPI: Duration per Attendee")

gold_query = """
SELECT *
FROM gotomeeting_gold.attendee_duration_kpi
"""

gold_df = pd.read_sql(gold_query, conn)
st.dataframe(gold_df)
