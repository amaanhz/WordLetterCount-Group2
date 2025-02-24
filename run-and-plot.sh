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

kubectl create -f deployment.yaml
sleep 10
kubectl cp jars/WordLetterCount.jar cc-group2/group-2-ubuntu-volume:/test-data/WordLetterCount.jar
kubectl delete pod group-2-ubuntu-volume

# to make the python programs actually print
export PYTHONUNBUFFERED=1

# then activate venv, download dependencies, and run plotting script
source .venv/bin/activate
pip install -r requirements.txt

# run experiments script
TIMESTAMP=$(python ./run-experiments.py | tail -n 1)  # tail captures the last line

# run plotting script
python plot-experiments.py "$TIMESTAMP"

# and deactivate 
deactivate


