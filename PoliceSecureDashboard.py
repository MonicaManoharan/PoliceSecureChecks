import streamlit as st

# Set up sidebar navigation
st.set_page_config(page_title="Police Dashboard", layout="wide")
st.sidebar.title("📋 New Logs and Analysis")
page = st.sidebar.radio("Select a page:", ["📝 Add New Police Log", "📊 Critical Insights Visualization"])

# Page routing logic
if page == "📝 Add New Police Log":
    from Add_New_Police_Log import run_page
    run_page()

elif page == "📊 Critical Insights Visualization":
    from Critical_Insights_Visualization import run_Critical
    run_Critical()
