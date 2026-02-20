import streamlit as st
from databricks import sql
import pandas as pd

st.title("📅 Candidate Attendance Duration Dashboard")

connection = sql.connect(
    server_hostname="dbc-54ac37fe-c0ec.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/YOUR_REAL_ID",
    access_token="dapi_REAL_TOKEN"
)

# Date range
col1, col2 = st.columns(2)

with col1:
    start_date = st.date_input("Start Date")

with col2:
    end_date = st.date_input("End Date")

# Load candidate list
with connection.cursor() as cursor:
    cursor.execute("""
        SELECT DISTINCT attendee_name
        FROM workspace.gotomeeting_silver.student_details
        ORDER BY attendee_name
    """)
    candidates = [row[0] for row in cursor.fetchall()]

selected_candidate = st.selectbox("Select Candidate", candidates)

if st.button("Load Data"):

    query = f"""
        SELECT 
            attendee_name,
            meeting_date,
            duration_minutes
        FROM workspace.gotomeeting_silver.student_details
        WHERE attendee_name = '{selected_candidate}'
        AND meeting_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY meeting_date
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(result, columns=columns)

    if not df.empty:
        st.success("Data Loaded Successfully")

        total_duration = df["duration_minutes"].sum()

        st.metric("Total Duration (Minutes)", round(total_duration, 2))

        st.line_chart(df.set_index("meeting_date")["duration_minutes"])

        st.dataframe(df, use_container_width=True)

    else:
        st.warning("No data found in selected date range.")
