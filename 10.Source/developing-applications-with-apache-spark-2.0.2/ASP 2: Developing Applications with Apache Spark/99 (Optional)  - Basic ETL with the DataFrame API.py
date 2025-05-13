# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 99 (Optional) - Basic ETL with the DataFrame API
# MAGIC
# MAGIC This demonstration will walk through common ETL operations using the Flights dataset. We'll cover data loading, cleaning, transformation, and analysis using the DataFrame API.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Implement common ETL operations using Spark DataFrames
# MAGIC - Handle data cleaning and type conversion
# MAGIC - Create derived features through transformations
# MAGIC
# MAGIC NOTE: This section is optional and should be taught at start to give intro to Dataframe API

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC   - In the drop-down, select **More**.
# MAGIC
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to **dbacademy** and the schema to your specific schema name shown below using the `USE` statements.
# MAGIC <br></br>
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-Common

# COMMAND ----------

# MAGIC %md
# MAGIC View your default catalog and schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Data Loading and Inspection
# MAGIC
# MAGIC First, let's load and inspect the flight data.

# COMMAND ----------

# Read the flights data
flights_df = spark.read.table("dbacademy_airline.v01.flights_small")

# COMMAND ----------

# Print the schema
flights_df.printSchema()

# COMMAND ----------

# Visually inspect a subset of the data
display(flights_df.limit(10))

# COMMAND ----------

# Let's remove columns we dont need, remember "filter early, filter often"
flights_required_cols_df = flights_df.select(
    "Year",
    "Month",
    "DayofMonth",
    "DepTime",
    "FlightNum",
    "ActualElapsedTime",
    "CRSElapsedTime",
    "ArrDelay")

# Alternatively we could have used the drop() method to remove the columns we didnt want...

# COMMAND ----------

# Get a count of the source data records
initial_count = flights_required_cols_df.count()

print(f"Source data has {initial_count} records")

# COMMAND ----------

# Let's examine the data for invalid values, these can include nulls or invalid values for string columns "ArrDelay", "ActualElapsedTime", "DepTime" which we intend on performing mathematical opeations on, we can use the Spark SQL COUNT_IF function to perform the analysis

# Register the DataFrame as a temporary SQL table with cast columns
flights_required_cols_df \
    .selectExpr(
        "Year",
        "Month",
        "DayofMonth",
        "CAST(DepTime AS INT) AS DepTime",
        "FlightNum",
        "CAST(ActualElapsedTime AS INT) AS ActualElapsedTime",
        "CRSElapsedTime",
        "CAST(ArrDelay AS INT) AS ArrDelay"
    ) \
    .createOrReplaceTempView("flights_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Data Cleaning
# MAGIC
# MAGIC The flights data contains some invalid and missing values, lets find them and clean them (in this case we will drop them)

# COMMAND ----------

# To drop rows where any specified columns are null, we can use the na.drop DataFrame method
non_null_flights_df = flights_required_cols_df.na.drop(
    how='any',
    subset=['CRSElapsedTime']
)

# COMMAND ----------

from pyspark.sql.functions import col

# Let's remove rows with invalid values for "ArrDelay", "ActualElapsedTime" and "DepTime" columns
flights_with_valid_data_df = non_null_flights_df.filter(
    col("ArrDelay").cast("integer").isNotNull() & 
    col("ActualElapsedTime").cast("integer").isNotNull() &
    col("DepTime").cast("integer").isNotNull()
)

# COMMAND ----------

# Now that we know "ArrDelay" and "ActualElapsedTime" contain integer values only, lets cast them from strings to integers (replacing the existing columns)
clean_flights_df = flights_with_valid_data_df \
    .withColumn("ArrDelay", col("ArrDelay").cast("integer")) \
    .withColumn("ActualElapsedTime", col("ActualElapsedTime").cast("integer"))

clean_flights_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Data Enrichment
# MAGIC
# MAGIC Now let's create a useful derived column to categorize delays.

# COMMAND ----------

# Let's start by deriving the "FlightDateTime" column from the "Year", "Month", "DayofMonth", "DepTime" columns, then drop the constituent columns
from pyspark.sql.functions import col, make_timestamp_ntz, lpad, substr, lit

flights_with_datetime_df = clean_flights_df.withColumn(
    "FlightDateTime",
    make_timestamp_ntz(
        col("Year"),
        col("Month"),
        col("DayofMonth"),
        substr(lpad(col("DepTime"), 4, "0"), lit(1), lit(2)).cast("integer"),
        substr(lpad(col("DepTime"), 4, "0"), lit(3), lit(2)).cast("integer"),
        lit(0)
    )
).drop("Year", "Month", "DayofMonth", "DepTime")

# Show the result
display(flights_with_datetime_df.limit(10))

# COMMAND ----------

# OK now lets derive the "ElapsedTimeDiff" column from the "ActualElapsedTime" and "CRSElapsedTime" columns

from pyspark.sql.functions import col

flights_with_elapsed_time_diff_df = flights_with_datetime_df.withColumn(
    "ElapsedTimeDiff", col("ActualElapsedTime") - col("CRSElapsedTime")
    ).drop("ActualElapsedTime", "CRSElapsedTime")

display(flights_with_elapsed_time_diff_df.limit(10))

# COMMAND ----------

# Now lets categorize the "ArrDelay" column into categories: "On Time", "Slight Delay", "Moderate Delay", "Severe Delay"

from pyspark.sql.functions import when

enriched_flights_df = flights_with_elapsed_time_diff_df \
    .withColumn("delay_category", when(col("ArrDelay") <= 0, "On Time")
        .when(col("ArrDelay") <= 15, "Slight Delay")
        .when(col("ArrDelay") <= 60, "Moderate Delay")
        .otherwise("Severe Delay")) \
       .drop("ArrDelay")

# COMMAND ----------

# Displaying the result 
display(enriched_flights_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
