# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - データのグループ化と集計
# MAGIC
# MAGIC このデモンストレーションでは、NYC タクシーの乗車データを使用して、グループ化と集計操作を実行する方法を示します。基本的なグループ化、複数の集計、ウィンドウ関数について説明します。
# MAGIC
# MAGIC ### 目標
# MAGIC - Spark での基本的なグループ化操作を理解する
# MAGIC - 集計を使用した時間ベースの分析を実行する
# MAGIC - 複数のメトリックを使用した複雑な集計を実装する
# MAGIC - 高度な分析のためにウィンドウ関数を使用する
# MAGIC - 集計パフォーマンスを最適化する

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
# MAGIC ## A. データセットアップとロード
# MAGIC
# MAGIC まず、タクシートリップデータをロードし、その構造を調べてみましょう。

# COMMAND ----------

from pyspark.sql.functions import *

# タクシーデータの読み取りと表示
trips_df = spark.read.table("samples.nyctaxi.trips")

display(trips_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. 基本的なグループ化操作
# MAGIC
# MAGIC 場所ごとの旅行パターンを理解するために、シンプルなグループ化操作から始めましょう。

# COMMAND ----------

# ピックアップ場所別のトリップ数をカウントし、上位 5 つの人気ピックアップ場所を表示
location_counts = trips_df \
    .groupBy("pickup_zip") \
    .count() \
    .orderBy(desc("count"))

display(location_counts.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. 複数の集計を組み合わせる
# MAGIC
# MAGIC `agg()` メソッドを使用して、場所ごとに複数の集計を実行してみましょう。

# COMMAND ----------

# 場所ごとに複数の集計を実行し、最も人気のあるピックアップ場所で並べ替えます
location_stats = trips_df \
    .groupBy("pickup_zip") \
    .agg(
        count("*").alias("total_trips"),
        round(avg("trip_distance"), 2).alias("avg_distance"),
        round(avg("fare_amount"), 2).alias("avg_fare"),
        round(sum("fare_amount"), 2).alias("total_fare_amt")
    ) \
    .orderBy(desc("total_trips"))

display(location_stats.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. ウィンドウ関数
# MAGIC
# MAGIC ここで、より高度な分析のためにウィンドウ関数を使用してみましょう。

# COMMAND ----------

from pyspark.sql.window import Window

# さまざまなランキング方法のウィンドウスペックを作成する
window_by_trips = Window.orderBy(desc("total_trips"))
window_by_fare = Window.orderBy(desc("avg_fare"))

# 異なるタイプのランキングを追加する
ranked_locations = location_stats \
    .withColumn("trips_rank", rank().over(window_by_trips)) \
    .withColumn("fare_rank", rank().over(window_by_fare)) \
    .withColumn("fare_quintile", ntile(5).over(window_by_fare))  # 料金で5つのグループに分割する

# COMMAND ----------

# 結果の表示
display(ranked_locations.select(
    "pickup_zip", 
    "total_trips", 
    "avg_fare", 
    "avg_distance",
    "trips_rank",
    "fare_rank",
    "fare_quintile"
).limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 主な学習内容
# MAGIC
# MAGIC 1. **基本的なグループ化**
# MAGIC    - `groupBy()` に続いて集計メソッドを使用する
# MAGIC    - 複数の列でグループ化できる
# MAGIC    - 常にデータの分布を確認する
# MAGIC
# MAGIC 2. **ウィンドウ関数**
# MAGIC    - 比較分析に最適
# MAGIC    - パフォーマンスへの影響を考慮する
# MAGIC    - 適切なウィンドウフレームを使用する
# MAGIC
# MAGIC 3. **ベストプラクティス**
# MAGIC    - 集計列に常に別名を付ける
# MAGIC    - null 値を適切に処理する
# MAGIC    - グループ化キーでのデータの偏りを考慮する

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
