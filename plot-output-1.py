import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Assume df is your combined DataFrame with columns: Workers, Mean, Std, File
# (Workers as a string, File as category, Mean and Std computed)

# Sort the DataFrame in the same order as the expected plotting order.
df_sorted = df.sort_values(by=["Workers", "File"]).reset_index(drop=True)

# Create the bar plot with error bars disabled in Seaborn (ci=None)
ax = sns.barplot(
    x="Workers",
    y="Mean",
    hue="File",
    data=df,
    ci=None,
    palette="muted"
)

# Debug: Print the counts
num_patches = len(ax.patches)
num_rows = len(df_sorted)
print("Number of patches:", num_patches)
print("Number of rows in df_sorted:", num_rows)

# Add error bars. We'll iterate over the minimum number of patches/rows.
for i, patch in enumerate(ax.patches):
    if i >= num_rows:
        break
    # Compute center of the bar
    x_center = patch.get_x() + patch.get_width() / 2.0
    height = patch.get_height()
    # Get the corresponding standard deviation from df_sorted
    std = df_sorted.iloc[i]["Std"]
    ax.errorbar(
        x_center, 
        height, 
        yerr=std, 
        color="black", 
        capsize=5, 
        fmt="none"
    )

# Customize labels and title
ax.set_xlabel("Number of Executors")
ax.set_ylabel("Average Elapsed Time")
ax.set_title("Grouped Bar Chart: Execution Time vs. Executor Count")
plt.legend(title="Input File")
plt.show()
