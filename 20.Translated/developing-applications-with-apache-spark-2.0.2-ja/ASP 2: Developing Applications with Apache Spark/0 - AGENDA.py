# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apache Spark を使用したアプリケーション開発
# MAGIC
# MAGIC ### コースの説明
# MAGIC このハンズオンコースでは、Apache Spark を使用してスケーラブルなデータ処理をマスターします。Spark の DataFrame API を使用して効率的な ETL パイプラインを構築し、 高度な分析を実行し、分散トランスフォーメーションを最適化する方法を学びます。グループ化、集計、結合、セット演算、ウィンドウ関数を探索します。また、配列、マップ、構造体などの複雑なデータ型を扱い、パフォーマンス チューニングのベスト プラクティスを適用します。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 前提条件
# MAGIC このコースを開始する前に、次の前提条件を満たしている必要があります。
# MAGIC
# MAGIC - 基本的なプログラミングの知識
# MAGIC - Python に精通
# MAGIC - 基本的な SQL (`SELECT`、`JOIN`、`GROUP BY`) の理解
# MAGIC - データ処理の概念に関する知識
# MAGIC - **Apache Spark の紹介** の完了または Databricks の以前の経験
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### コースのアジェンダ
# MAGIC 次のモジュールは、Databricks Academy の **Data Engineer Learning Path** の一部です。
# MAGIC
# MAGIC | #    | モジュール タイトル                                                                 |
# MAGIC |------|------------------------------------------------------------------------------|
# MAGIC | 1    | [データのグループ化と集計]($./1 - データのグループ化と集計)   |
# MAGIC | 2L   | [ラボ: E コマース データのグループ化と集計]($./2L - E コマース データのグループ化と集計) |
# MAGIC | 3    | [Spark の DataFrame リレーショナル演算]($./3 - Spark の DataFrame リレーショナル演算) |
# MAGIC | 4    | [Spark の複雑なデータ型の操作]($./4 - Spark の複雑なデータ型の操作) |
# MAGIC | 5L   | [ラボ: E コマース データの複雑なデータ型の操作]($./5L - E コマース データの複雑なデータ型の操作) |
# MAGIC | 99   | [99 (オプション)  - DataFrame API を使用した基本的な ETL]($./99 オプション  - DataFrame API を使用した基本的な ETL) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 要件
# MAGIC 開始する前に、次の点を確認してください。
# MAGIC
# MAGIC - すべてのデモおよびラボ ノートブックを実行するには、Databricks Runtime バージョン **`15.4.x-scala2.12`** を使用します。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
