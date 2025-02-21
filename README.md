# Project 1 Cloud Computing, Part II CST

*Group 2,  consisting of Amaan Hamza, Arvind Raghu and Mathew Blowers*

## Command to run project

After fetching and unzipping the zipped folder, the only command to run is:

`cd WordLetterCount-Group2 & ./run-and-plot.sh`

This script will activate the Python virtual environment, download any module dependencies e.g. `seaborn` for plotting etc., and then run `run-experiments.sh` and `plot-experiments.py` in order, feeding the timestamp output of the former into the latter.

## NOTES:

### Scripting

The scripting was designed for `Python v3.12`, which is the version of Python present on the remote server.

### Batch vs. Sequential

Since the aim of the project is accurate timing measurements of execution, varying the number of executors and input file size, to minimize resource contention and performance degradation as a result of this (which would then affect the accuracy and validity of the experiment measurements), sequential execution is favoured.

This is executed as suggested in the deliverables: starting a new experiment every hour i.e. requiring 27 hours total to run all experiments.

### TODOs not mentioned in code:

- Remove `test-timestamp` dir and related contents from `experiments` when finished with scripting
