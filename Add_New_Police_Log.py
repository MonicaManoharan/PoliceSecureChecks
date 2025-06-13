import streamlit as st
from datetime import datetime
from PoliceDataQueries import insert_log, run_custom_query
from PoliceDataQueries import QUERY_MAP

def run_page():
    
    st.title("🚔 SecureCheck: Police Stop Logging & Analysis")

    # -----------------------
    # Section 1: Add New Police Log
    # -----------------------
    st.header("➕ Add New Police Log & Predict Outcome and Violation")

    with st.form("log_form"):
        col1, col2 = st.columns(2)

        with col1:
            stop_date = st.date_input("Stop Date")
            stop_time = st.time_input("Stop Time")
            country = st.text_input("Country Name")
            driver_gender = st.selectbox("Driver Gender", ["male", "female"])
            driver_age = st.number_input("Driver Age", min_value=18, max_value=100)
            driver_race = st.text_input("Driver Race")

        with col2:
            search_conducted = st.selectbox("Was a Search Conducted?", ["0", "1"])
            search_type = st.text_input("Search Type")
            is_drug_related = st.selectbox("Was it Drug Related?", ["0", "1"])
            stop_duration = st.selectbox("Stop Duration", ["0-15 Min", "16-30 Min", ">30 Min"])
            vehicle_number = st.text_input("Vehicle Number")
            violation = st.text_input("Violation")
            stop_outcome = st.selectbox("Stop Outcome", ["Warning", "Citation", "Arrest"])

        submitted = st.form_submit_button("Predict Stop Outcome & Violation")

        if submitted:
            # Insert into DB
            insert_log(
                stop_date, stop_time, country, driver_gender, driver_age,
                driver_race, search_conducted, search_type, stop_outcome,
                stop_duration, is_drug_related, violation, vehicle_number
            )

            # Generate summary
            summary = (
                f"A {int(driver_age)}-year-old {driver_gender} driver was stopped for {violation} at {stop_time.strftime('%I:%M %p')}. "
                f"{'A search was conducted' if search_conducted == '1' else 'No search was conducted'}, and "
                f"he received a {stop_outcome.lower()}. The stop lasted {stop_duration} and "
                f"{'was drug-related' if is_drug_related == '1' else 'was not drug-related'}."
            )

            st.success("✅ Log Submitted Successfully!")
            st.markdown(f"**Summary:** {summary}")

    # -----------------------
    # Section 2: Run SQL Queries
    # -----------------------
    st.markdown("---")
    st.header("🧠 Advanced Insights")

    query_option = st.selectbox("Select a Query to Run", list(QUERY_MAP.keys()))
    run_query = st.button("Run Query")

    if run_query:
        sql = QUERY_MAP.get(query_option)
        if sql:
            result_df = run_custom_query(sql)
            st.dataframe(result_df)
        else:
            st.warning("Selected query is not defined.")