import json
import streamlit as st
from st_keyup import st_keyup
import os
import pandas as pd
import plotly.express as px

from infinopy import InfinoClient

from util.toml_util import get_server_url


def get_user_query():
    """
    Display a text input and return the user submitted text
    """

    # Enable one of the inputs below, based on whether search-as-you-type is needed.

    # Display text input that auto-refereshes, enabling search-as-you-type
    # user_query = st_keyup(
    #    "Enter search text:", value="directory index forbidden", debounce=500, key="0"
    # )

    # If search-as-you-type functionality is not needed, use a regular text input (instead of st_keyup above)
    user_query = st.text_input(
        "Enter search text below and hit Enter:", "directory index forbidden"
    )
    show_summary = st.checkbox("Show Summary", value=False)

    return user_query, show_summary


def search_logs(infino_server_url, user_query, show_summary):
    """
    Search infino logs and returns a summary (if show_summary is set) and two dataframes:
    - A datafrme with counts of error logs, grouped by date,
    - A dataframe of results returned by Infino
    """

    # Search Infino for the given query
    client = InfinoClient(infino_server_url)
    if show_summary:
        # show_summary is set, execute the summarization query
        response = client.summarize(text=user_query)
        if response.status_code != 200:
            st.error("Could not execute summarization query")
            return None, None, None

        # Convert the response to json, and extract summary and dataframe of log results
        results = response.json()
        summary = results["summary"]

        df = pd.DataFrame(results["results"])
    else:
        # show_summary is set, execute the search_logs query
        response = client.search_log(text=user_query)
        if response.status_code != 200:
            st.error("Could not execute search_log query")
            return None, None, None

        # Convert the response to json, and extract summary and dataframe of log results
        results = response.json()
        summary = None
        df = pd.DataFrame(results)

    if df.empty:
        return summary, df, df

    # Create a dataframe of error counts by date
    df["date_isoformat"] = df["fields"].apply(lambda x: x.get("date_isoformat"))
    df["log_level"] = df["fields"].apply(lambda x: x.get("log_level"))
    error_df = df[df["log_level"] == "error"]
    error_count_df = (
        error_df.groupby("date_isoformat").size().reset_index(name="error_count")
    )

    df["message"] = df["fields"].apply(lambda x: x.get("message"))

    # Convert the numeric 'timestamp' column to datetime objects
    df["timestamp"] = pd.to_datetime(df["time"], unit="s")

    # Convert the 'timestamp' column to ISO datetime format in UTC
    df["date"] = df["timestamp"].dt.tz_localize("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Create a results dataframe with a subset of columns from the df
    columns = ["date", "log_level", "message"]
    results_df = df[columns]

    return summary, error_count_df, results_df


def display_error_df(df):
    """
    Display the error dataframe as a plotly chart
    """
    if not df.empty:
        fig = px.bar(
            df,
            x="date_isoformat",
            y="error_count",
            title="Errror Count by Date",
        )
        st.plotly_chart(fig)
    else:
        st.text("No error logs returned")


def display_results_df(df, num_rows):
    """
    Display the results dataframe as a table
    """
    if not df.empty:
        st.table(df)


if __name__ == "__main__":
    st.title("Example Logs Dashboard")

    # Read configuration file to get Infino server url
    toml_file_path = os.path.join("config", "default.toml")
    infino_server_url = get_server_url(toml_file_path)

    # Get the user search query and the flag whether to show summary
    user_query, show_summary = get_user_query()

    if not user_query:
        st.text("Please enter your search query")
    else:
        # Query Infino to create a dataframe to be plotted
        summary, error_count_df, results_df = search_logs(
            infino_server_url, user_query, show_summary
        )

        # Display the summary
        if summary is not None:
            st.markdown("**Summary** " + summary)

        if error_count_df is not None:
            # Display the error graph
            display_error_df(error_count_df)

        if results_df is not None:
            # Display results - first 100 rows
            display_results_df(results_df, 1000)
