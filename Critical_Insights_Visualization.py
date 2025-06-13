import streamlit as st
import pandas as pd
import plotly.express as px

def run_Critical():
    # Load data
    df = pd.read_csv("traffic_stops.csv", low_memory=False)

    # Preprocess datetime
    df['stop_date'] = pd.to_datetime(df['stop_date'], errors='coerce')
    df['stop_time'] = pd.to_datetime(df['stop_time'], format="%H:%M:%S", errors='coerce').dt.time
    df['year'] = df['stop_date'].dt.year

    df['month'] = df['stop_date'].dt.month
    df['hour'] = pd.to_datetime(df['stop_time'], errors='coerce').dt.hour

    df['violation'] = df['violation'].fillna("Unknown")
    df['driver_gender'] = df['driver_gender'].fillna("Unknown")
    df['driver_race'] = df['driver_race'].fillna("Unknown")
    df['country_name'] = df['country_name'].fillna("Unknown")

    # Custom dark theme line/bar chart renderer
    def render_chart(fig, title):
        fig.update_layout(
            plot_bgcolor="#1e1e1e",
            paper_bgcolor="#1e1e1e",
            font=dict(color="white", size=14),
            xaxis=dict(color="white", gridcolor="gray", linecolor="white"),
            yaxis=dict(color="white", gridcolor="gray", linecolor="white"),
            title=dict(text=title, font=dict(color="white", size=18), x=0.01),
            legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(color="white")),
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)

    # App layout
    st.title("\U0001F441\ufe0f SecureCheck - Critical Analysis")

    # Tabs for each question
    q1, q2, q3, q4, q5 = st.tabs([
        "1. Yearly Stops & Arrests",
        "2. Violation by Age & Race",
        "3. High Search & Arrest Violations",
        "4. Demographics by Country",
        "5. Top Arrest Rate Violations"
    ])

    with q1:
        data = df.groupby(['country_name', 'year']).agg(
            total_stops=('is_arrested', 'count'),
            total_arrests=('is_arrested', 'sum')
        ).reset_index()
        fig = px.line(data, y='year', x='total_stops', color='country_name', markers=True)
        render_chart(fig, "Yearly Stops by Country")

    with q2:
        df['age_group'] = pd.cut(df['driver_age'], bins=[0, 17, 25, 35, 50, 150],
                                labels=["<18", "18-25", "26-35", "36-50", "50+"])
        data = df.groupby(['age_group', 'driver_race']).agg(count=('violation', 'count')).reset_index()
        fig = px.line(data, x='age_group', y='count', color='driver_race', markers=True)
        render_chart(fig, "Violations by Age Group and Race")

    with q3:
        stats = df.groupby('violation').agg(
            total=('search_conducted', 'count'),
            searches=('search_conducted', 'sum'),
            arrests=('is_arrested', 'sum')
        ).reset_index()
        stats['search_rate'] = stats['searches'] / stats['total'] * 100
        stats['arrest_rate'] = stats['arrests'] / stats['total'] * 100
        top_search = stats.sort_values(by='search_rate', ascending=False).head(5)
        top_arrest = stats.sort_values(by='arrest_rate', ascending=False).head(5)
        top_stats = pd.concat([top_search, top_arrest]).drop_duplicates()
        fig = px.line(top_stats, x='violation', y='arrest_rate', markers=True)
        render_chart(fig, "High Search and Arrest Rate Violations")

    with q4:
        demo = df.groupby('country_name').agg(
            male=('driver_gender', lambda x: (x == 'M').sum()),
            female=('driver_gender', lambda x: (x == 'F').sum())
        ).reset_index()
        demo_melt = demo.melt(id_vars='country_name', var_name='gender', value_name='count')
        fig = px.bar(demo_melt, x='country_name', y='count', color='gender', barmode='group')
        render_chart(fig, "Driver Gender Distribution by Country")

    with q5:
        arrest_stats = df.groupby('violation').agg(
            total=('is_arrested', 'count'),
            arrests=('is_arrested', 'sum')
        ).reset_index()
        arrest_stats['arrest_rate'] = arrest_stats['arrests'] / arrest_stats['total'] * 100
        top5 = arrest_stats.sort_values(by='arrest_rate', ascending=False).head(5)
        fig = px.bar(top5, x='arrest_rate', y='violation', orientation='h')
        render_chart(fig, "Top 5 Violations with Highest Arrest Rate")

