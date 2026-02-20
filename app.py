import streamlit as st
import pandas as pd
from databricks import sql

st.set_page_config(page_title="GoToMeeting Gold KPI", layout="wide")

st.title("✅ GoToMeeting Gold KPI Dashboard")

# -------------------------
# TABLE SELECTION
# -------------------------

table_name = st.selectbox(
    "Choose KPI Table:",
    [
        "final_attendence",
        "attendee_duration_kpi",
        "total_student_duration"
    ]
)

# -------------------------
# LOAD DATA BUTTON
# -------------------------

if st.button("Load Table Data"):

    try:
        st.info("Running query... please wait")

        # Create connection
        connection = sql.connect(
            server_hostname=st.secrets["DATABRICKS_SERVER_HOSTNAME"],
            http_path=st.secrets["DATABRICKS_HTTP_PATH"],
            access_token=st.secrets["DATABRICKS_TOKEN"]
        )

        # Query
        query = f"""
        SELECT *
        FROM workspace.gotomeeting_gold.{table_name}
        LIMIT 100
        """

        # Execute query
        df = pd.read_sql(query, connection)

        # Display
        st.success("Data Loaded Successfully")
        st.dataframe(df)

        connection.close()

    except Exception as e:
        st.error("Query Failed")
        st.code(str(e))
