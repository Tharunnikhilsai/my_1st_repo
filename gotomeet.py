import streamlit as st
import pandas as pd
from databricks import sql

st.title("📊 GoToMeeting Dashboard")

# Connect to Databricks SQL Warehouse
conn = sql.connect(
    server_hostname="dbc-54ac37fe-c0ec.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/<your_warehouse_id>",
    access_token="<your_databricks_pat>"
)

# Silver Layer Query
st.header("Silver Layer: Student Details")

query1 = "SELECT * FROM gotomeeting_silver.student_details LIMIT 20"

silver_df = pd.read_sql(query1, conn)
st.dataframe(silver_df)

# Gold KPI Query
st.header("Gold Layer KPI: Duration per Attendee")

query2 = "SELECT * FROM gotomeeting_gold.attendee_duration_kpi"

gold_df = pd.read_sql(query2, conn)
st.dataframe(gold_df)
