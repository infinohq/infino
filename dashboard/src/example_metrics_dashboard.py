import streamlit as st
from st_keyup import st_keyup
import os
import pandas as pd
import plotly.express as px

from infinopy import InfinoClient

from util.toml_util import get_server_url


def get_metric_name():
    """
    Show a selection box to the user and return the metric name selected
    """

    choice = st.selectbox(
        "Select the metric to display: ", ("cpu_usage", "memory_usage", "disk_usage")
    )
    return choice


def search_metrics(infino_server_url, metric_name):
    """
    Search infino logs and returns a dataframe of time and metric values, for the given metric name
    """

    # Search Infino for the given query
    client = InfinoClient(infino_server_url)
    response = client.search_metrics(label_name="__name__", label_value=metric_name)
    if response.status_code != 200:
        st.error("Could not execute time series query")
        return None

    # Convert the response to a dataframe
    results = response.json()
    df = pd.DataFrame(results)

    return df


def display_results_df(df, metric_name):
    """
    Display the error dataframe as a plotly chart
    """

    if not df.empty:
        fig = px.line(
            df,
            x="time",
            y="value",
            title=f"Metric Values for {metric_name}",
        )
        st.plotly_chart(fig)
    else:
        st.text("No metric returned")


if __name__ == "__main__":
    st.title("Example Metrics Dashboard")

    # Read configuration file to get Infino server url
    toml_file_path = os.path.join("config", "default.toml")
    infino_server_url = get_server_url(toml_file_path)

    metric_name = get_metric_name()

    # Get the timeseries metric values as a dataframe
    df = search_metrics(infino_server_url, metric_name)

    # Display metric values
    display_results_df(df, metric_name)
