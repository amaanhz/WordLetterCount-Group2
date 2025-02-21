# Simple script to run both scripts in order.
#
# Note that the run-experiments script feeds in
# to plot-experiments a timestamp string. This is
# so that each script can be run independent of
# each other, but this script (run-and-plot) aligns
# usage of both.
#
# USAGE
# -----
# ./run-and-plot
#

# TODO: add downloading requirements

# run experiments script
TIMESTAMP=$(./run-experiments)

# then activate venv, download dependencies, and run plotting script
source .venv/bin/activate
pip install -r requirements.txt
python plot-experiments.py "$TIMESTAMP"
deactivate
