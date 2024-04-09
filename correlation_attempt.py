# Databricks notebook source
import pandas as pd
df = pd.read_csv("/Workspace/Repos/23118415@studentmail.ul.ie/DataPipeline/dataConverted/2014/2014_419410.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

type(df)

# COMMAND ----------





# Compute the correlation matrix
correlation_matrix = df.corr()

# Print the correlation matrix
print("Correlation Matrix:")
print(correlation_matrix)

# Optionally, visualize the correlation matrix
import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Correlation Matrix')
plt.show()


# COMMAND ----------


