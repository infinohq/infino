
<h1 align="center">
    Infino Dashboard 
</h1>
<p align="center">
    Infino Dashboard - Example of Generating Dashboard from Code
</p>

If you haven't explored Infino yet, please refer to:
 - [Infino git repo](https://github.com/infinohq/infino)

## Quick Start
Infino dashboards can be generated in code using [Streamlit](https://streamlit.io/). 

### Example Dashboards
You can find an example logs dashboard code [here](src/example_logs_dashboard.py) and metrics dashboard [here](src/example_metrics_dashboard.py). 
To get started quickly and experience how it looks like, see the driver script that starts/stops Infino and populates data to be displayed [here](src/driver.py).

To run the driver script - run `make run-driver`

To run only the logs dashboard (after starting Infino and populating data) - run `make logs-dashboard`

To run only the metrics dashboard (after starting Infino and populating data) - run `make metrics-dashboard`

## How does it look?

![Logs Dashboard](https://media.giphy.com/media/uLRbCWdEBQn1OPj8CV/giphy.gif)

![Metrics Dashboard](https://media.giphy.com/media/uslUuVC1eBlG0UL6yi/giphy.gif)
