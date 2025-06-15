# dashboard.py

import streamlit as st
import pandas as pd
import plotly.express as px
from sql import CheckPostAnalytics
from datetime import datetime

# --------------------------------------
# Streamlit Page Configuration
# --------------------------------------
st.set_page_config(
    page_title="Check Post Dashboard",
    page_icon="🚓",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://docs.streamlit.io/',
        'Report a Bug': "https://github.com/streamlit/streamlit/issues",
        'About': "This dashboard monitors traffic stops and violations in real-time."
    }
)
# Initialize session state for officer login
if 'is_authenticated' not in st.session_state:
    st.session_state.is_authenticated = False
    st.session_state.officer_id = None
    st.session_state.officer_role = None

st.markdown("""
    <style>
    .stApp {background-color: #030303;}
    </style>
""", unsafe_allow_html=True)

# --------------------------------------
# Database Connection
# --------------------------------------
@st.cache_resource
def get_analytics_instance():
    return CheckPostAnalytics(
        host="localhost",
        port=5432,
        user="postgres",
        password="vGpostgre",
        database="traffic_stops"
    )

analytics = get_analytics_instance()

# --------------------------------------
# Data Loading
# --------------------------------------
def load_data():
    try:
        tables = ['stops', 'drivers', 'violations']
        dataframes = {}
        for table in tables:
            query = f"SELECT * FROM {table}"
            analytics.mediator.execute(query)
            rows = analytics.mediator.fetchall()
            cols = [desc[0] for desc in analytics.mediator.description]
            dataframes[table] = pd.DataFrame(rows, columns=cols)
        return dataframes['stops'], dataframes['drivers'], dataframes['violations']
    except Exception as e:
        st.error(f"❌ Error loading police stop data:\n{e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

stop_data, drivers_data, violations_data = load_data()

# --------------------------------------
# Sidebar Navigation
# --------------------------------------
page = st.sidebar.radio("📌 Navigation", ['🏠 Overview','📈 Deep Dive', '📝 New Entry + Prediction'])

# --------------------------------------
# Overview Page
# --------------------------------------
if page == '🏠 Overview':
    st.title("🚨 SecureCheck")
    st.markdown("#### A Python-SQL Digital Ledger for Police Post Logs")
    st.markdown("_Real-time monitoring & insights for law enforcement agencies._")
    st.markdown("---")

    st.subheader("🗂️ Police Logs Overview")
    st.dataframe(stop_data, use_container_width=True)

    # Key Metrics
    st.header("📊 KEY METRICS")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Police Stops", stop_data.shape[0])
    with col2:
        arrests = stop_data[stop_data['stop_outcome'].str.lower() == 'arrest']
        st.metric("Total Arrests", arrests.shape[0])
    with col3:
        warnings = stop_data[stop_data['stop_outcome'].str.lower() == 'warning']
        st.metric("Total Warnings", warnings.shape[0])
    with col4:
        drug_related = stop_data[stop_data['drugs_related_stop'] == True]
        st.metric("Drug-Related Stops", drug_related.shape[0])

    # Insights Tabs
    st.markdown("## 🔍 Detailed Insights")
    tab1, tab2 = st.tabs(["🚦 Stops by Violation", "🚻 Driver Gender Distribution"])

    with tab1:
        st.subheader("Stops by Violation Type")
        if 'violation' in violations_data.columns:
            vc = violations_data['violation'].value_counts().reset_index()
            vc.columns = ['Violation', 'Count']
            fig = px.bar(vc, x='Violation', y='Count', color='Violation')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No 'violation' column found in dataset.")

    with tab2:
        st.subheader("Driver Gender Distribution")
        if 'driver_gender' in drivers_data.columns:
            gc = drivers_data['driver_gender'].value_counts().reset_index()
            gc.columns = ['Gender', 'Count']
            fig = px.pie(gc, names='Gender', values='Count',
                         color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No 'driver_gender' column found in dataset.")

# --------------------------------------
# Deep Dive Analytics
# --------------------------------------
elif page == '📈 Deep Dive':
    st.title("⚙️ Operations")
    st.info("Feature coming soon: Add/Edit stop data, filter by officer/date, and manage users.")

    st.header("Advanced Insights")
    category = st.sidebar.selectbox("Choose Category", [
        "🚗 Vehicle-Based", "🢍 Demographic-Based", "🕒 Time-Based",
        "⚖️ Violation-Based", "🌍 Location-Based", "🧠 Complex Analytics"
    ])

    query_map = {
        "🚗 Vehicle-Based": {
            "Top 10 Drug-Related Vehicles": analytics.get_top_10_drug_related_vehicles,
            "Most Frequently Searched Vehicles": analytics.get_most_searched_vehicles
        },
        "🢍 Demographic-Based": {
            "Highest Arrest Rate by Age Group": analytics.get_highest_arrest_rate_by_age_group,
            "Gender Distribution by Country": analytics.get_gender_distribution_by_country,
            "Race & Gender with Highest Search Rate": analytics.get_race_gender_highest_search_rate
        },
        "🕒 Time-Based": {
            "Peak Traffic Stop Times": analytics.get_peak_traffic_stop_time,
            "Average Stop Duration by Violation": analytics.get_average_stop_duration_by_violation,
            "Are Night Stops More Arrest-Prone?": analytics.get_arrest_rate_by_time_of_day
        },
        "⚖️ Violation-Based": {
            "Violations with Most Searches/Arrests": analytics.get_violation_search_arrest_stats,
            "Violations Common < Age 25": analytics.get_common_violations_under_25,
            "Violations Rarely Leading to Arrest": analytics.get_rarely_flagged_violations
        },
        "🌍 Location-Based": {
            "Countries with Highest Drug Stop Rates": analytics.get_country_with_highest_drug_related_rate,
            "Arrest Rate by Country & Violation": analytics.get_arrest_rate_by_country_violation,
            "Country with Most Searches": analytics.get_country_with_most_search_stops
        },
        "🧠 Complex Analytics": {
            "Yearly Stops & Arrests by Country": analytics.get_yearly_stops_arrests_by_country,
            "Violation Trends by Age & Race": analytics.get_violation_trends_by_age_race,
            "Time Period Stop Patterns": analytics.get_time_period_analysis_of_stops,
            "High Search+Arrest Violations": analytics.get_high_search_arrest_violations,
            "Driver Demographics by Country": analytics.get_driver_demographics_by_country,
            "Top 5 Arrest-Prone Violations": analytics.get_top_5_highest_arrest_violations
        }
    }

    if category:
        query_label = st.sidebar.selectbox("Choose a Query", list(query_map[category].keys()))
        if query_label:
            st.subheader(query_label)
            try:
                results = query_map[category][query_label]()
                if results:
                    columns = [desc[0] for desc in analytics.mediator.description]
                    df = pd.DataFrame(results, columns=columns)
                    st.dataframe(df, use_container_width=True)
                else:
                    st.info("No data found for this query.")
            except Exception as e:
                st.error(f"Query error: {e}")

# --------------------------------------
# New Entry & Prediction Page
# --------------------------------------
elif page == '📝 New Entry + Prediction':
    st.title("⚙️ NEW ENTRY")
    st.header("Add new police log & predict outcome & violation")
    st.info("Fill details to get a natural language prediction based on existing data!")

    # Officer Login Section
    user_role = st.radio("Select Your Role", ["viewer", "officer"])

    if user_role == "officer" and not st.session_state.is_authenticated:
        st.subheader("👮 Officer Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login"):
            login_info = analytics.validate_officer_credentials(username, password)
            if login_info:
                st.session_state.is_authenticated = True
                st.session_state.officer_id = login_info[0]
                st.session_state.officer_role = login_info[4]
                st.success(f"✅ Login successful (Role: {st.session_state.officer_role})")
            else:
                st.error("❌ Invalid credentials")


    # Form Entry Section
    if user_role == "viewer" or st.session_state.is_authenticated:
        with st.form("traffic_entry_form"):
            st.subheader("📋 Traffic Stop Entry Form")

            # Driver Info
            vehicle_number = st.text_input("Vehicle Number")
            driver_gender = st.selectbox("Driver Gender", ["M", "F"])
            driver_age = st.number_input("Driver Age", min_value=0, max_value=120)
            age_group = st.selectbox("Age Group", ["Teen", "Young Adult", "Adult", "Middle Age", "Senior"])
            driver_race = st.text_input("Driver Race")

            # Stop Info
            stop_date = st.date_input("Stop Date", value=datetime.today())
            stop_time = st.time_input("Stop Time", value=datetime.now().time())
            stop_duration = st.selectbox("Stop Duration", ["0-15 Min", "16-30 Min", "30+ Min"])
            country_name = st.text_input("Country Name")
            search_conducted = st.checkbox("Was a Search Conducted?")
            drugs_related_stop = st.checkbox("Drugs Related Stop?")
            is_arrested = st.checkbox("Was an Arrest Made?")

            violation_raw = st.text_input("Violation Reason")

            submit_button = st.form_submit_button("Submit Entry")

            # Prediction + Insertion
        if submit_button:
            predicted_outcome = stop_data['stop_outcome'].mode()[0] if not stop_data.empty else "Warning"
            predicted_violation = violations_data['violation'].mode()[0] if not violations_data.empty else (violation_raw or "Speeding")

            search_text = "a search was conducted" if search_conducted else "no search was conducted"
            drug_text = "was drug-related" if drugs_related_stop else "was not drug-related"

            if user_role == "officer" and st.session_state.is_authenticated:
                try:
                    analytics.insert_driver_data(vehicle_number, driver_gender, driver_age, age_group, driver_race)
                    analytics.insert_stop_data(vehicle_number, stop_date, stop_time, stop_duration,
                                                country_name, drugs_related_stop, search_conducted,
                                                is_arrested, stop_outcome=predicted_outcome, added_by=st.session_state.officer_id)
                    analytics.insert_violation_data(vehicle_number, violation_raw, violation=predicted_violation)
                    st.success("✅ Data inserted successfully into database.")
                except Exception as e:
                    st.error(f"❌ Error inserting into DB: {e}")
            else:
                st.info("🔍 As a viewer, your data was not saved — prediction only.")

            st.markdown("### 📝 Prediction Summary")
            st.markdown(f"""
            **Prediction Outcome:** {predicted_outcome}  
            **Predicted Violation:** {predicted_violation}  
            A **{driver_age}-year-old {driver_gender}** driver from **{country_name}** was stopped on **{stop_date}** at **{stop_time}**  
            for **{violation_raw or predicted_violation}**. Based on similar past data, the stop outcome is likely **{predicted_outcome}**,  
            and {search_text}. The stop {drug_text}.
            """)