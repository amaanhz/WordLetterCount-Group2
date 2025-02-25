import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

def process_file(filename, label):
    """
    Reads a CSV file and computes the mean and standard deviation
    of execution times across repetitions.
    """
    # Read CSV into DataFrame
    df = pd.read_csv(filename)
    # Compute mean and standard deviation for the three execution times
    exec_cols = ['Execution Time 1', 'Execution Time 2', 'Execution Time 3']
    df['Mean'] = df[exec_cols].mean(axis=1)
    df['Std'] = df[exec_cols].std(axis=1)
    # Add file label
    df['File'] = label
    # Keep only the necessary columns: Workers, Mean, Std, and File
    return df[['Workers', 'Mean', 'Std', 'File']]

# Process each file with an appropriate label
df_100 = process_file("experiments/20250225T103940/measurements/data_100MB.csv", "100MB")
df_200 = process_file("experiments/20250225T103940/measurements/data_200MB.csv", "200MB")
df_500 = process_file("experiments/20250225T103940/measurements/data_500MB.csv", "500MB")

# Combine the three DataFrames
df = pd.concat([df_100, df_200, df_500], ignore_index=True)

# For plotting, convert Workers to a string (categorical x-axis)
df['Workers'] = df['Workers'].astype(str)

# Set the Seaborn style and a muted color palette
sns.set(style="whitegrid", palette="muted")

# Create the bar plot. We set ci=None because we'll add our own error bars.
ax = sns.barplot(
    x="Workers",
    y="Mean",
    hue="File",
    data=df,
    ci=None
)

# Now we add custom error bars to each bar.
# To ensure error bars match the correct bars, we sort the data in the same order as seaborn's plotting order:
df_sorted = df.sort_values(by=["Workers", "File"]).reset_index(drop=True)

# Iterate over the patches (bars) in the plot.
# Seaborn creates one patch per bar in the order it processes the data.
for i, patch in enumerate(ax.patches):
    # Get the corresponding standard deviation from the sorted DataFrame.
    # Note: This assumes that the order of bars in the plot matches the sorted order of df_sorted.
    std = df_sorted.iloc[i]["Std"]
    # Calculate the center of the bar.
    x_center = patch.get_x() + patch.get_width() / 2.0
    height = patch.get_height()
    # Add an errorbar with the standard deviation as the error.
    ax.errorbar(
        x_center,
        height,
        yerr=std,
        color="black",
        capsize=5,
        fmt="none"
    )

# Set labels and title
ax.set_xlabel("Number of Executors")
ax.set_ylabel("Average Elapsed Time")
ax.set_title("Grouped Bar Chart: Execution Time vs. Executor Count")
plt.legend(title="Input File")

# Display the plot
plt.show()
