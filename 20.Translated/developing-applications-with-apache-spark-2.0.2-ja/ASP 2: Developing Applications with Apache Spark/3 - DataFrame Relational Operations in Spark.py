# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - SparkにおけるDataFrameのリレーショナル演算
# MAGIC
# MAGIC このデモンストレーションでは、DataFramesを使用してジョインとセット演算を効果的に行う方法を示します。パフォーマンスの最適化とベストプラクティスに焦点を当てています。
# MAGIC
# MAGIC ### 目標
# MAGIC - DataFrameのさまざまなタイプのジョインを理解する
# MAGIC - ジョインのパフォーマンス最適化を実装する
# MAGIC - 複雑なジョインシナリオを処理する
# MAGIC - セット演算を効果的に使用する
# MAGIC - データの偏りに対するベストプラクティスを適用する

# COMMAND ----------

# MAGIC %md
# MAGIC ## 必須 - クラシックコンピュートの選択
# MAGIC
# MAGIC このノートブックのセルを実行する前に、ラボでクラシックコンピュートクラスターを選択してください。 **Serverless** がデフォルトで有効になっていることに注意してください。
# MAGIC
# MAGIC クラシックコンピュートクラスターを選択するには、次の手順に従ってください。
# MAGIC
# MAGIC 1. ノートブックの右上隅に移動し、ドロップダウンメニューをクリックしてクラスターを選択します。デフォルトでは、ノートブックは **Serverless** を使用します。
# MAGIC
# MAGIC 1. クラスターが利用可能な場合は、それを選択し、次のセルに進みます。クラスターが表示されない場合は:
# MAGIC
# MAGIC   - ドロップダウンで **More** を選択します。
# MAGIC
# MAGIC   - **既存のコンピュートリソースにアタッチ** ポップアップで、最初のドロップダウンを選択します。そのドロップダウンに一意のクラスター名が表示されます。そのクラスターを選択してください。
# MAGIC
# MAGIC **注:** クラスターが終了している場合は、選択できるようにするために再起動する必要がある場合があります。そのためには:
# MAGIC
# MAGIC 1. 左側のナビゲーションパネルで **コンピュート** を右クリックし、 *新しいタブで開く* を選択します。
# MAGIC
# MAGIC 1. コンピュートクラスター名の右側にある三角形のアイコンを見つけてクリックします。
# MAGIC
# MAGIC 1. クラスターが起動するまで数分待ちます。
# MAGIC
# MAGIC 1. クラスターが実行中になったら、上記の手順を完了してクラスターを選択します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. セットアップとデータの読み込み
# MAGIC
# MAGIC まず、サンプルの小売データテーブルを読み込み、その構造を確認してみましょう。

# COMMAND ----------

from pyspark.sql.functions import *

# データを読み込む
transactions_df = spark.read.table("samples.bakehouse.sales_transactions")
customers_df = spark.read.table("samples.bakehouse.sales_customers")
franchises_df = spark.read.table("samples.bakehouse.sales_franchises")
suppliers_df = spark.read.table("samples.bakehouse.sales_suppliers")

# COMMAND ----------

# スキーマを調べる
transactions_df.printSchema()

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

franchises_df.printSchema()

# COMMAND ----------

suppliers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. 基本的な結合操作
# MAGIC
# MAGIC データを結合するための単純な結合操作から始めましょう。

# COMMAND ----------

# トランザクションをストア情報で豊富にするための内部結合の例
enriched_transactions = franchises_df.join(
    transactions_df,
    on="franchiseID",
    how="inner"
)

display(enriched_transactions)

# COMMAND ----------

# "on" 句には式を含めることができます
enriched_transactions = franchises_df.join(
    transactions_df,
    on= transactions_df.franchiseID == franchises_df.franchiseID,
    how="inner"
)

display(enriched_transactions)

# これは、結合キーが両方のエンティティで異なる名前になっている場合に特に便利です

# COMMAND ----------

# 両方のデータフレームのすべてのフィールドが結果に存在することに注意してください。より良い方法は、各エンティティから必要な列を投影することです。
# 一部の列に別名を付けて、列名の曖昧さを解消することも行います。
enriched_transactions = franchises_df \
    .select(
        "franchiseID", 
        col("name").alias("store_name"), 
        col("city").alias("store_city"), 
        col("country").alias("store_country")
        ) \
    .join(
        transactions_df,
        on="franchiseID",
        how="inner"
    )
    
display(enriched_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. 完全外部結合操作
# MAGIC
# MAGIC データフレーム間の関係を分析し、外部結合を使用して欠落データを特定してみましょう。

# COMMAND ----------

# フランチャイズとサプライヤーの関係を完全外部結合を使用して分析してみましょう
full_join = franchises_df \
    .withColumnRenamed("name", "franchise_name") \
    .join(
        suppliers_df.select("supplierID", col("name").alias("supplier_name")),
        on="supplierID",
        how="full_outer" # 外部結合を実行
    )

# 内部結合に表示されないレコードを見つける
# これらは、フランチャイズまたはサプライヤーのデータが null であるレコードです
non_matching_records = full_join.filter(
        col("franchiseID").isNull() | 
        col("supplier_name").isNull()
    ) \
    .select("franchiseID", "franchise_name", col("supplierID").alias("orphaned_supplier_id"))

display(non_matching_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQLの使用
# MAGIC
# MAGIC これをSpark SQLで実行してみましょう....

# COMMAND ----------

# 一時ビューを作成
franchises_df.createOrReplaceTempView("franchises")
suppliers_df.createOrReplaceTempView("suppliers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQLを使用して外部結合を行う
# MAGIC SELECT 
# MAGIC     f.franchiseID,
# MAGIC     f.name as franchise_name,
# MAGIC     f.supplierID as orphaned_supplier_id
# MAGIC FROM franchises f
# MAGIC FULL OUTER JOIN suppliers s
# MAGIC ON f.supplierID = s.supplierID
# MAGIC WHERE f.franchiseID IS NULL OR s.name IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. 集合演算
# MAGIC
# MAGIC 次に、DataFrame API を使用した集合演算を使用して関係を調べてみましょう。

# COMMAND ----------

# 各データフレーム内のサプライヤー ID を特定する
franchise_suppliers = franchises_df.select("supplierID").distinct()
all_suppliers = suppliers_df.select("supplierID").distinct()

# franchises_df にあるが suppliers_df にないサプライヤー ID を見つける
franchises_without_valid_suppliers = franchise_suppliers.subtract(all_suppliers)
display(franchises_without_valid_suppliers)

# COMMAND ----------

# 両方のテーブルに存在するサプライヤーの重複部分を見つける
common_suppliers = franchise_suppliers.intersect(all_suppliers)
display(common_suppliers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 主な学び
# MAGIC
# MAGIC 1. **結合戦略**
# MAGIC    - すべてのデータフレームにキーが存在する場合は、内部結合を使用します
# MAGIC    - 両方のデータフレームにキーが存在しない可能性がある場合は、外部結合を使用します
# MAGIC    - 列名の競合を処理します
# MAGIC
# MAGIC 2. **パフォーマンスの最適化**
# MAGIC    - 結合前にフィルタリングします
# MAGIC    - 必要な列のみを投影します
# MAGIC    - 歪んだキーを適切に処理します
# MAGIC    - 小さいデータフレームを先に参照します。あるいは、
# MAGIC    - 小さなテーブルにはブロードキャスト結合を使用します

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
