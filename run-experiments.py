"""
Script to run experiments and save outputs to csv file.

Should also write to terminal the timestamp.
"""


import os
import time
import subprocess
import csv
from datetime import datetime


# TODO: once jar is complete and built, run as follows sequentially (i.e. one of these every hour)
# For every data file (3 files), for every number of executors ({2, 4, 6}), run three times.
# Remember to pass in the data file as a command line argument with flag -i
# And then change the number of executors in the command line argument of number of executors.


# First get the timestamp in ISO-8601 fashion
timestamp: str = datetime.utcnow().strftime("%Y%m%dT%H%M%S")


# Then create experiments directory if not present, and then the timestamp dir, and then measurements dir
if not os.path.exists("experiments"):
	os.makedirs("experiments")
os.chdir("experiments")
os.makedirs(timestamp, exist_ok=True)
os.chdir(timestamp)
os.makedirs("measurements")
os.chdir("..")
os.chdir("..")


# TODO: get the data file names properly i.e. so that they are from the mounted volume
DATA_FILES: list[str] = ["data_100MB.txt", "data_200MB.txt", "data_500MB.txt"]
# Output for timings
OUTPUT_FILES: list[str] = [f"./experiments/{timestamp}/measurements/"+file for file in ["data_100MB.csv", "data_200MB.csv", "data_500MB.csv"]]


# Defining executors and repetitions for experiment
EXECUTORS: list[str] = [str(i) for i in [2, 4, 6]]
REPETITIONS: int = 3


# For each experiment we get the multiline string for command, and then we run it
for file_index in range(len(DATA_FILES)):

	data_file = DATA_FILES[file_index]

	# We prepare the results table
	results: list[list[str | None]] = [
		["Workers"] + [f"Execution Time {i}" for i in range(1, REPETITIONS + 1)]
	]
	for executor_count in EXECUTORS:
		row = [None for i in range(REPETITIONS + 1)]
		row[0] = executor_count
		results.append(row)

	"""
	Giving a table that looks like
	[["Workers", "Execution Count 1", "Execution Count 2", "Execution Count 3"]
	["2", None, None, None]
	["4", None, None, None]
	["6", None, None, None]],
	for each data file
	"""

	# then iterating over executor counts and repetitions
	for i, executor_count in enumerate(EXECUTORS):
		for repetition in range(1, REPETITIONS + 1):

			# TODO: Add the spark submit command here, should look similar to 
			# TODO: Change the ./bin/spark-submit to wherever the spark-submit is relative to the home dir of our cloud server (they unzip in there and cd into it)
			command: str = f"""./bin/spark-submit \
				PLEASE ADD THE RELEVANT STUFF HERE NOT SURE
				LOOK AT https://www.vle.cam.ac.uk/mod/page/view.php?id=19306719
				AND MAKE SURE THAT IT RUNS
				AND INJECT THE EXECUTOR COUNT AS --conf spark.executor.instances={executor_count}
				AND INJECT THE DATA FILE INPUT (REMEMBER JAR TAKES INPUT -i {data_file})
			"""

			# Use python's time module to get start time
			start_time: float = time.time()

			# Then we run the command in blocking fashion such that the program does not move on
			proc = subprocess.run(command, shell=True, text=True, capture_output=True)

			# And finally we get the end time, and calculate time elapsed in seconds
			end_time: float = time.time()
			elapsed_time: int = round(end_time - start_time)

			# And we store the value in data_results
			results[i+1][repetition] = str(elapsed_time)

			# We then wait 10 mins before running the next experiment to let the system recover
			time.sleep(10*60)


	# Finally we attempt to write this data_results to memory
	input_file = data_file
	output_file_path = OUTPUT_FILES[file_index]

	with open(output_file_path, mode="w", newline="") as file:
		writer = csv.writer(file, delimiter=",")
		for row in results:
			writer.writerow([value if value is not None else "" for value in row])


# And output timestamp to command line so next script can pick it up
print(timestamp)
