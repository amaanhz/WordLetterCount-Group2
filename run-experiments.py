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
print(f"Got timestamp {timestamp}.")


# Then create experiments directory if not present, and then the timestamp dir, and then measurements dir
if not os.path.exists("experiments"):
	os.makedirs("experiments")
os.chdir("experiments")
os.makedirs(timestamp, exist_ok=True)
os.chdir(timestamp)
os.makedirs("measurements")
os.chdir("..")
os.chdir("..")

print("Created directories to write to.")


# Get data from the mounted volume
DATA_FILES: list[str] = ["/test-data/data_100MB.txt", "/test-data/data_200MB.txt", "/test-data/data_500MB.txt"]
# Output for timings
OUTPUT_FILES: list[str] = [f"./experiments/{timestamp}/measurements/"+file for file in ["data_100MB.csv", "data_200MB.csv", "data_500MB.csv"]]


# Defining executors and repetitions for experiment
# EXECUTORS: list[str] = [str(i) for i in [2, 4, 6]]
# REPETITIONS: int = 3
# TODO: REMOVE
EXECUTORS: list[str] = [str(i) for i in [6]]
REPETITIONS: int = 1


# Record starting times for each experiment
starting_times_for_report = []


# For each experiment we get the multiline string for command, and then we run it
for file_index in range(len(DATA_FILES)):

	# We get a data file
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

			print(f"Attempting word and letter count for {data_file}, for {executor_count} executors, on repetition {repetition}.")

			# Spark submit command
			command: str = f"""../spark-3.5.4-bin-hadoop3/bin/spark-submit \
			--master k8s://128.232.80.18:6443 \
			--deploy-mode cluster \
			--name wordlettercount \
			--class org.wordlettercount.SimpleApp \
			--conf spark.executor.instances={executor_count} \
			--conf spark.kubernetes.namespace=cc-group2 \
			--conf spark.kubernetes.authenticate.driver.serviceAccountName=cc-group2-user \
			--conf spark.kubernetes.container.image=andylamp/spark:v3.5.4-amd64 \
			--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
			--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
			--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
			--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.path=/test-data \
			--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.mount.readOnly=false \
			--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfs-cc-group2.options.claimName=nfs-cc-group2 \
			local:///test-data/WordLetterCount.jar -i {data_file}
			"""

			# Add starting times for report
			starting_times_for_report.append(datetime.utcnow().strftime("%Y%m%dT%H%M%S"))

			# TODO: CHANGE THIS TO USE time ... AS SPECIFIED
			# Use python's time module to get start time
			start_time: float = time.time()

			# Then we run the command in blocking fashion such that the program does not move on
			proc = subprocess.run(command, shell=True, text=True, capture_output=True)

			# And finally we get the end time, and calculate time elapsed in seconds
			end_time: float = time.time()
			elapsed_time: int = round(end_time - start_time)

			# And we store the value in data_results
			results[i+1][repetition] = str(elapsed_time)

			# Then get rid of the pod
			# TODO: check this command works
			command = "kubectl get pods --no-headers=true | awk '/wordlettercount*/{print $1}' | xargs kubectl delete pod"
			_ = subprocess.run(command, shell=True, capture_output=True)

			print("Completed execution, and deleted pod. Attempting to wait before next experiment.")

			# We then wait 1 minute before running the next experiment to let the system recover
			# TODO: add back
			# time.sleep(1*60)

	print(f"Writing results for {data_file} to memory.")
	# Finally we attempt to write this data_results to memory
	input_file = data_file
	output_file_path = OUTPUT_FILES[file_index]

	with open(output_file_path, mode="w", newline="") as file:
		writer = csv.writer(file, delimiter=",")
		for row in results:
			writer.writerow([value if value is not None else "" for value in row])
	print(f"Results for {data_file} written to csv file in memory.")

with open(f"./experiments/{timestamp}/measurements/{starting_times_for_report}.txt", mode="w") as file:
	for i in starting_times_for_report:
		file.write(i+"\n")

# And output timestamp to command line so next script can pick it up
print(timestamp)
