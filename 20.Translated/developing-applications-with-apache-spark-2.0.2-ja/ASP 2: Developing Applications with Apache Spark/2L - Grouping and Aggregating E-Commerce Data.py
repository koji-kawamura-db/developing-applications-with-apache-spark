# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2L - Eコマースデータのグループ化と集計
# MAGIC
# MAGIC このラボでは、Eコマースのトランザクションのデータセットを使用して、Sparkでのグループ化と集計の操作方法を練習します。顧客の購入行動のパターンと洞察を明らかにするために、さまざまな分析を実行します。
# MAGIC
# MAGIC ### 目標
# MAGIC - データを要約するために`groupBy`操作を使用する
# MAGIC - 複数の集計を実装する
# MAGIC - 異なる並べ替え技術を適用する
# MAGIC - （ボーナス）高度な分析のためにウィンドウ関数を使用する

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
# MAGIC ## A. 初期設定
# MAGIC
# MAGIC 小売取引データを読み込み、その構造を確認します。

# COMMAND ----------

from pyspark.sql.functions import *

## eコマースのトランザクションデータを読み込む
transactions_df = spark.read.table("samples.bakehouse.sales_transactions")

## データのサンプルを表示する
<FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. 基本的なグループ化操作
# MAGIC
# MAGIC 製品の販売パターンを理解するために、シンプルなグループ化操作から始めましょう。

# COMMAND ----------

# 1. 商品ごとにデータをグループ化し、販売数をカウントする
# 2. 結果を最も人気のある商品順に並べる

# COMMAND ----------

## 商品ごとにデータをグループ化し、販売数をカウントする
product_counts = <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. 複数の集計
# MAGIC
# MAGIC ここで、より深い洞察を得るために複数の集計を実行してみましょう。

# COMMAND ----------

# 1. 支払い方法別の売上分析
# 2. 各支払い方法の合計収益、平均取引額、および取引数を計算する
# 3. 合計収益の順に並べる（最高額から） 

# COMMAND ----------

## 支払い方法別の売上分析
payment_analysis = <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## ボーナスチャレンジ: ウィンドウ関数
# MAGIC
# MAGIC 時間があれば、ウィンドウ関数を使用して高度な分析を試してみましょう。

# COMMAND ----------

## まず、製品ごとの総収益を計算します
product_revenue_df = transactions_df \
    .groupBy("product") \
    .agg(
        round(sum(col("totalPrice")), 2).alias("total_revenue")
    )

## ウィンドウ関数を使用してランキングを追加します
## 総収益で製品をランク付けします
<FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
