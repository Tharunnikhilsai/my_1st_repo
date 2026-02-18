import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

# Start Spark Session
spark = SparkSession.builder.getOrCreate()

# Title
st.title("📊 GoToMeeting Dashboard")

st.markdown("Silver + Gold Layer Analytics App")

# ------------------------------
# Silver Layer Table
# ------------------------------
st.header("🧾 Silver Layer: Student Details")

silver_df = spark.table("gotomeeting_silver.student_details")
st.dataframe(silver_df.toPandas())

# ------------------------------
# Gold Layer KPI Table
# ------------------------------
st.header("🏆 Gold Layer KPI: Duration per Attendee")

gold_df = spark.table("gotomeeting_gold.attendee_duration_kpi")
st.dataframe(gold_df.toPandas())
