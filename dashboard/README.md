
<h1 align="center">
    Infino Dashboard-On-Demand 
</h1>
<p align="center">
    Example of generating dashboards-on-demand
</p>

If you haven't explored Infino yet, please refer to:
 - [Infino git repo](https://github.com/infinohq/infino)

## Quick Start
Infino charts are generated dynamically using [Streamlit](https://streamlit.io/). While it can be used standlone, this is particularly useful for multi-modal chat tools like Slack. 

### Example Dashboards
You can find an example logs dashboard code [here](src/example_logs_dashboard.py) and metrics dashboard [here](src/example_metrics_dashboard.py). 
To get started quickly and experience how it looks like, see the driver script that starts/stops Infino and populates data to be displayed [here](src/driver.py).

#### Use a driver script

To make it easy to experience Infino, the driver script sets up the Apache logs data in Infino and runs the dashboards. To run the driver script:

* Set the `OPENAI_API_KEY` environment variable (and prepay for API credits with OPEN AI),
* From the `dashboard` directory, run `make run-driver`
* The script will open two browser tabs - one for the logs dashboard and one for the metrics dashboard.
* When you are done, use `Ctrl-C` to stop the driver script, and close the browser tabs.

The driver script uses Infino from docker container. You can inspect the container, look at the logs etc, using the usual docker commands such 
as `docker ps`, `docker logs`, etc.

#### Run dashboards separately

To run only the logs dashboard (after starting Infino and populating data) - run `make logs-dashboard`

To run only the metrics dashboard (after starting Infino and populating data) - run `make metrics-dashboard`

### Caution
Ensure you have dependencies virtualenv, pip, pip-tools, etc. installed correctly.

## How does it look?

![Logs Dashboard](https://media.giphy.com/media/k5inTNm7tVY9MBhG2n/giphy.gif)

![Metrics Dashboard](https://media.giphy.com/media/uslUuVC1eBlG0UL6yi/giphy.gif)
