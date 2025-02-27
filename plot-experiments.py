"""
Bash script to plot given experiment results, with minimal error handling.

For every csv file in given directory, plot and save.

Plot should be bar chart with error bars for
each number of workers for which time is measured.

INPUTS
------
- Timestamp in ISO-8601 format

EXAMPLE USAGE
-------------
./plot-experiments 20210205T052537

NOTE
----
The `gnuplot` being used is version 5.4.
"""


import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os

# Check that timestamp was passed in
if len(sys.argv) != 2:
	print("ERROR: invalid arguments passed. Must require timestamp to use.")
	sys.exit(1)

EXPERIMENTS_DIR: str = "experiments"
TIMESTAMP_DIR: str = sys.argv[1]
MEASUREMENTS_DIR: str = "measurements"
GRAPHS_DIR: str = "graphs"

# Navigate into experiments dir if present, else exit with error
if not os.path.isdir(EXPERIMENTS_DIR):
	print("ERROR: experiments directory not present.")
	sys.exit(2)
os.chdir(EXPERIMENTS_DIR)

# Navigate into timestamp dir if present, else exit with error
if not os.path.isdir(TIMESTAMP_DIR):
	print("ERROR: timestamp directory not present.")
	sys.exit(2)
os.chdir(TIMESTAMP_DIR)

# Check that measurements dir present, else exit
if not os.path.isdir(MEASUREMENTS_DIR):
	print("ERROR: measurements directory not present.")
	sys.exit(3)

# Create graphs directory if not present
if not os.path.isdir(GRAPHS_DIR):
	os.makedirs(GRAPHS_DIR)

# NOW ITERATING OVER FILES

# Iterate over .csv data files
for data_file in os.listdir(MEASUREMENTS_DIR):
	if not data_file.endswith(".csv"):
		continue

	# Read into dataframe, and melt so we can pass to seaborn
	df = pd.read_csv(os.path.join(MEASUREMENTS_DIR, data_file))
	id_var = "Workers"
	value_vars = [f"Execution Time {i}" for i in range(1,4)]
	sns_df = df.melt(
		id_vars=[id_var], 
		value_vars=value_vars,
		var_name="Execution #", 
		value_name="Time taken"
	)

	"""
	Gives an example that looks like this:

	   Workers       Execution #  Time taken
	0        2  Execution Time 1           7
	1        4  Execution Time 1           3
	2        6  Execution Time 1           1
	3        2  Execution Time 2           4
	4        4  Execution Time 2           7
	5        6  Execution Time 2           2
	6        2  Execution Time 3           6
	7        4  Execution Time 3           3
	8        6  Execution Time 3           2

	This is because `sns.barplot` is built to aggregate data.
	"""

	# Then plot graph with seaborn, with following properties
	# - each bar represents number of workers
	# - height of each bar represents average execution time
	# - error bars correspond to one standard deviation for each row
	plt.figure(figsize=(8,8))
	plot = sns.barplot(
		data=sns_df,
		x="Workers",
		y="Time taken",
		errorbar="sd",
		capsize=.05,
		alpha=0.75,
		hue="Workers",
		palette="muted",
		linewidth=1.2,
		edgecolor="black",
		legend=False
	)

	# More graph aesthetics
	plt.xlabel("Number of workers")
	plt.ylabel("Average execution time (across 3 attempts)")
	plot.set_title(f"Execution times against workers for {data_file}")

	# And finally save the graph to graphs directory
	output_file = os.path.join(
		GRAPHS_DIR,
		f"{data_file[:-4]}.pdf"
	)
	plot.get_figure().savefig(output_file)

# Navigate out of subdir
os.chdir("..")
os.chdir("..")
