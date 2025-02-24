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

# TODO: maybe add a bit here that will move the jar to the kubectl pod


# then activate venv, download dependencies, and run plotting script
source .venv/bin/activate
pip install -r requirements.txt

# run experiments script
TIMESTAMP=$(python ./run-experiments.py)

# run plotting script
python plot-experiments.py "$TIMESTAMP"

# and deactivate 
deactivate


