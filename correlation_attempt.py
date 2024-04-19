# Databricks notebook source
!cp -r /Workspace/Repos/23118415@studentmail.ul.ie/DataPipeline/dataConverted/2014/2014_419410.csv$RESTOFPATH /dbfs/my_new_directory_weathercsv/2014_419410.csv


# COMMAND ----------

# Read the CSV file into a DataFrame
df = spark.read.csv("dbfs:/my_new_directory_weathercsv/2014_419410.csv", header=False)

# Show the DataFrame schema
df.printSchema()

# Show the first few rows of the DataFrame
df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

# List of numeric columns
numeric_cols = [col_name for col_name, data_type in df.dtypes if data_type == 'int' or data_type == 'double']

# Compute the correlation matrix for all numeric columns
correlation_matrix = {}

for col1 in numeric_cols:
    correlation_matrix[col1] = {col2: df.stat.corr(col1, col2) for col2 in numeric_cols}

# Print the correlation matrix
print("Correlation Matrix:")
for col1 in numeric_cols:
    for col2 in numeric_cols:
        print(f"{col1}-{col2}: {correlation_matrix[col1][col2]:.2f}", end="\t")
    print()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
# Convert Spark DataFrame to pandas DataFrame
pandas_df = df.toPandas()
# Compute the correlation matrix using pandas
correlation_matrix = pandas_df.corr()
print(correlation_matrix)

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls

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


