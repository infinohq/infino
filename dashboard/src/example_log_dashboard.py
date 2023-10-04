import streamlit as st
import os
import pandas as pd
import numpy as np
import plotly.express as px

from infinopy import InfinoClient

from util.toml_util import get_server_url


def prepare_df(infino_server_url):
    client = InfinoClient(infino_server_url)

    # Sample data
    data = {
        "Date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
        "Value": np.random.randint(1, 100, size=(31,)),
    }

    df = pd.DataFrame(data)
    return df


def display_df(df):
    st.title("Example Log Dashboard - with Log Level and Time Window Selection")

    # Sidebar with slider for time window selection
    st.sidebar.subheader("Select Time Window")
    start_date = st.sidebar.date_input(
        "Start Date",
        min(df["Date"]),
        min_value=min(df["Date"]),
        max_value=max(df["Date"]),
    )
    end_date = st.sidebar.date_input(
        "End Date",
        max(df["Date"]),
        min_value=min(df["Date"]),
        max_value=max(df["Date"]),
    )

    # Filter data based on selected time window
    filtered_data = df[
        (df["Date"] >= pd.to_datetime(start_date))
        & (df["Date"] <= pd.to_datetime(end_date))
    ]

    # Display bar chart
    st.subheader("Bar Chart")
    fig = px.bar(
        filtered_data, x="Date", y="Value", title="Bar Chart for Selected Time Window"
    )
    st.plotly_chart(fig)


if __name__ == "__main__":
    # Read configuration file to get Infino server url
    toml_file_path = os.path.join("config", "default.toml")
    infino_server_url = get_server_url(toml_file_path)

    # Query Infino to create a dataframe to be plotted
    df = prepare_df(infino_server_url)

    # Display the dataframe
    display_df(df)
