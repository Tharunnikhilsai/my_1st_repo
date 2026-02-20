import streamlit as st
import pandas as pd
from databricks import sql
from datetime import date

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(page_title="Candidate Attendance Dashboard", layout="wide")

st.title("📅 Candidate Attendance Duration Dashboard")

# -----------------------------
# CONNECTION FUNCTION
# -----------------------------
@st.cache_resource
def create_connection():
    try:
        connection = sql.connect(
            server_hostname="dbc-54ac37fe-c0ec.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/YOUR_WAREHOUSE_ID",   # 🔴 REPLACE THIS
            access_token="dapi-YOUR_ACCESS_TOKEN"               # 🔴 REPLACE THIS
        )
        return connection
    except Exception as e:
        st.error("❌ Failed to connect to Databricks")
        st.code(str(e))
        st.stop()

connection = create_connection()

# -----------------------------
# DATE FILTERS
# -----------------------------
col1, col2 = st.columns(2)

with col1:
    start_date = st.date_input("Start Date", date(2026, 1, 1))

with col2:
    end_date = st.date_input("End Date", date.today())

# -----------------------------
# LOAD BUTTON
# -----------------------------
if st.button("🔍 Load Attendance Data"):

    if start_date > end_date:
        st.warning("Start date cannot be greater than End date")
        st.stop()

    query = f"""
        SELECT
            attendee_name,
            SUM(total_duration_hours) AS total_hours,
            SUM(classes_attended) AS total_classes_attended,
            AVG(attendance_percentage) AS avg_attendance
        FROM workspace.gotomeeting_gold.final_attendance
        WHERE meeting_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY attendee_name
        ORDER BY total_hours DESC
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

        df = pd.DataFrame(result, columns=columns)

        if df.empty:
            st.info("No data available for selected date range.")
        else:
            st.success("✅ Data Loaded Successfully")

            # KPI Metrics
            total_candidates = df.shape[0]
            total_hours = df["total_hours"].sum()

            kpi1, kpi2 = st.columns(2)
            kpi1.metric("Total Candidates", total_candidates)
            kpi2.metric("Total Duration Hours", round(total_hours, 2))

            st.divider()

            st.subheader("Candidate-wise Attendance Summary")
            st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error("❌ Query Execution Failed")
        st.code(str(e))
        
