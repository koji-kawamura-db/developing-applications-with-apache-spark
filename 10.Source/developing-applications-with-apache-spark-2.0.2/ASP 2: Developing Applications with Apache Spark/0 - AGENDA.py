# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Developing Applications with Apache Spark
# MAGIC
# MAGIC ### Course Description
# MAGIC Master scalable data processing with Apache Spark in this hands-on course. Learn to build efficient ETL pipelines, perform advanced analytics, and optimize distributed transformations using Spark’s DataFrame API. Explore grouping, aggregation, joins, set operations, and window functions. You'll also work with complex data types like arrays, maps, and structs, applying best practices for performance tuning.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Prerequisites
# MAGIC You should meet the following prerequisites before starting this course:
# MAGIC
# MAGIC - Basic programming knowledge
# MAGIC - Familiarity with Python
# MAGIC - Understanding of basic SQL (`SELECT`, `JOIN`, `GROUP BY`)
# MAGIC - Knowledge of data processing concepts
# MAGIC - Completion of **Introduction to Apache Spark** or prior Databricks experience
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning Path** from Databricks Academy.
# MAGIC
# MAGIC | #    | Module Title                                                                 |
# MAGIC |------|------------------------------------------------------------------------------|
# MAGIC | 1    | [Grouping and Aggregating Data]($./1 - Grouping and Aggregating Data)   |
# MAGIC | 2L   | [Lab: Grouping and Aggregating E-Commerce DataData]($./2L - Grouping and Aggregating E-Commerce Data) |
# MAGIC | 3    | [DataFrame Relational Operations in Spark]($./3 - DataFrame Relational Operations in Spark) |
# MAGIC | 4    | [Working with Complex Data Types in Spark]($./4 - Working with Complex Data Types in Spark) |
# MAGIC | 5L   | [Lab: Working with Complex Data Types in E-Commerce Data]($./5L - Working with Complex Data Types in E-Commerce Data) |
# MAGIC | 99   | [99 (Optional)  - Basic ETL with the DataFrame API]($./99 Optional  - Basic ETL with the DataFrame API) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Requirements
# MAGIC Please ensure the following before starting:
# MAGIC
# MAGIC - Use Databricks Runtime version: **`15.4.x-scala2.12`** to run all demo and lab notebooks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
