#!/bin/bash

if [[ ! -z "${RUN_DEV}" && "${RUN_DEV}" = "True" ]]; then
  echo "Starting AQI updater with small graph (dev)"
  export GRAPH_SUBSET="True"
  python aqi_updater_app.py
else
  echo "Starting AQI updater with full graph (prod)"
  python aqi_updater_app.py
fi