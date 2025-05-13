# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 5L - Working with Complex Data Types in E-Commerce Data
# MAGIC
# MAGIC In this lab, you'll practice working with complex data types in Spark, including handling JSON strings, converting them to structured types, and manipulating nested data structures.
# MAGIC
# MAGIC ## Scenario
# MAGIC
# MAGIC You are a data engineer at an e-commerce company that collects data about customer orders, product reviews, and customer browsing behavior. The data contains nested structures that need to be properly processed for analysis.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Convert JSON string data to Spark SQL native complex types
# MAGIC - Work with arrays and structs
# MAGIC - Use functions like explode, collect_list, and pivot
# MAGIC - Extract and analyze valuable insights from nested data

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will set your default catalog to **dbacademy** and the schema to your specific schema name shown below using the `USE` statements.
# MAGIC
# MAGIC Also, It will create a temp table for you named `ecommerce_raw`
# MAGIC <br></br>
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-5L

# COMMAND ----------

# MAGIC %md
# MAGIC #### Querying the newly created table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Load and Inspect Raw Data with JSON Strings
# MAGIC
# MAGIC Load and examine the retail dataset which includes JSON strings.

# COMMAND ----------


## Read the sample dataset
events_df = spark.read.table("ecommerce_raw")

## Examine the schema and display sample data
<FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Convert JSON Strings to Structured Types
# MAGIC
# MAGIC The `tags`, `recent_orders`, and `browsing_history` columns contain JSON strings. Let's convert them to proper Spark structured types.

# COMMAND ----------

# 1. Get a sample of the JSON strings in each column
# 2. Infer schemas from the JSON samples
# 3. Convert the JSON strings to structured types using from_json and display the resulting DataFrame

# COMMAND ----------

## Get a sample of the JSON strings

# COMMAND ----------

## Infer schemas from the JSON samples

# COMMAND ----------

## Convert the JSON strings to structured types using from_json and display the resulting DataFrame
parsed_df = <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Working with Arrays
# MAGIC
# MAGIC Now that we have proper structured data, let's analyze the customer tags and browsing history.

# COMMAND ----------

# 1. Calculate the number of tags and browsing history items for each customer
# 2. Explode the tags array to see all unique customer tags
# 3. Find the most common browsing categories across all customers
# HINT: use the `array_size` function or its alias `size`

# COMMAND ----------

## Calculate the number of tags and browsing history items for each customer
array_sizes_df = <FILL-IN>

# COMMAND ----------

## Explode tags to analyze customer categorization
exploded_tags_df = <FILL-IN>

# COMMAND ----------

## Find the most common customer tags
tag_counts_df = <FILL-IN>

# COMMAND ----------

# 1. Explode the recent_orders array to analyze individual orders
# 2. Calculate total revenue per customer

# COMMAND ----------

## Explode the recent_orders array to analyze individual orders
orders_df = <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Bonus Challenge: Analyze Customer Purchasing Patterns
# MAGIC
# MAGIC Let's use the `collect_list` and `collect_set` aggregate functions to create summaries of customer purchasing patterns.

# COMMAND ----------

## First, create a flattened view of orders
order_items_df = orders_df.select(
    "customer_id",
    "name",
    "order.order_id",
    "order.date",
    explode("order.items").alias("item")
)

## Now extract the name field from each item
item_details_df = order_items_df.selectExpr(
    "customer_id",
    "name",
    "item.name as product_name"
)

# Inspect the data
display(item_details_df)

# COMMAND ----------

## Collect all products purchased by each customer, creating new columns called "all_products_purchased" and "unique_products_purchased" for each "customer_id"
customer_products_df = <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
