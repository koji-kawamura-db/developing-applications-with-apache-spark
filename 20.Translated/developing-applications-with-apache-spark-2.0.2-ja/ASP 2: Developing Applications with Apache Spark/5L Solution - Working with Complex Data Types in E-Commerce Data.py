# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 5L - Eコマースデータにおける複雑なデータタイプの操作
# MAGIC
# MAGIC このラボでは、JSON文字列の処理、構造化された型への変換、ネストされたデータ構造の操作など、Sparkの複雑なデータタイプの操作方法を練習します。
# MAGIC
# MAGIC ## シナリオ
# MAGIC
# MAGIC あなたは、顧客の注文、製品レビュー、顧客のブラウジング動作に関するデータを収集するEコマース会社のデータエンジニアです。データには、分析のために適切に処理する必要があるネストされた構造が含まれています。
# MAGIC
# MAGIC ### 目標
# MAGIC - JSON文字列データをSpark SQLネイティブの複雑なタイプに変換する
# MAGIC - 配列と構造体を操作する
# MAGIC - explode、collect_list、pivotなどの関数を使用する
# MAGIC - ネストされたデータから貴重な洞察を抽出して分析する

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. クラスルームセットアップ
# MAGIC
# MAGIC 次のセルを実行して、このコースの作業環境を構成します。デフォルトのカタログを **dbacademy** に設定し、以下に示す `USE` ステートメントを使用して、スキーマを特定のスキーマ名に設定します。
# MAGIC
# MAGIC また、`ecommerce_raw` という名前の一時テーブルも作成します。
# MAGIC <br></br>
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **注:** `DA` オブジェクトは、Databricks Academy のコースでのみ使用され、コース外では使用できません。コースを実行するために必要な情報を動的に参照します。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-5L

# COMMAND ----------

# MAGIC %md
# MAGIC #### 新しく作成されたテーブルを照会する

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. JSON 文字列を含む生データの読み込みと検査
# MAGIC
# MAGIC JSON 文字列を含む小売データセットを読み込み、検査します。

# COMMAND ----------

## サンプルデータセットを読み込む
events_df = spark.read.table("ecommerce_raw")

## スキーマを調べてサンプルデータを表示する
events_df.printSchema()
display(events_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. JSON 文字列を構造化された型に変換する
# MAGIC
# MAGIC `tags`、`recent_orders`、および `browsing_history` 列には JSON 文字列が含まれています。これらを適切な Spark 構造化された型に変換してみましょう。

# COMMAND ----------

# 1. 各列のJSON文字列のサンプルを取得する
# 2. JSONサンプルからスキーマを推論する
# 3. from_jsonを使用してJSON文字列を構造化された型に変換し、結果のDataFrameを表示する

# COMMAND ----------

## サンプルのJSON文字列を取得する
tags_json = ecommerce_df.select("tags").limit(1).collect()[0][0]
recent_orders_json = ecommerce_df.select("recent_orders").limit(1).collect()[0][0]
browsing_history_json = ecommerce_df.select("browsing_history").limit(1).collect()[0][0]

print("タグのサンプル:", tags_json)
print("\n最近の注文のサンプル:", recent_orders_json)
print("\n閲覧履歴のサンプル:", browsing_history_json)

# COMMAND ----------

## JSON サンプルからスキーマを推論する

## スキーマを定義/推論する
tags_schema = schema_of_json(lit(tags_json))
recent_orders_schema = schema_of_json(lit(recent_orders_json))
browsing_history_schema = schema_of_json(lit(browsing_history_json))

# COMMAND ----------

parsed_df = ecommerce_df.select(
    "customer_id",
    "name",
    "email",
    "registration_date",
    from_json("tags", tags_schema).alias("tags"),
    from_json("recent_orders", recent_orders_schema).alias("recent_orders"),
    from_json("browsing_history", browsing_history_schema).alias("browsing_history")
)

## スキーマを調べてサンプルデータを表示する
parsed_df.printSchema()
display(parsed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. 配列の操作
# MAGIC
# MAGIC ここで、適切な構造化データができたので、顧客タグと閲覧履歴を分析してみましょう。

# COMMAND ----------

# 1. 各顧客のタグと閲覧履歴アイテムの数を計算する
# 2. タグ配列を展開して、すべての顧客の固有のタグを確認する
# 3. すべての顧客にわたる最も一般的な閲覧カテゴリを見つける
# ヒント: `array_size` 関数またはその別名 `size` を使用する

# COMMAND ----------

## 各顧客のタグと閲覧履歴アイテムの数を計算する
array_sizes_df = parsed_df.select(
    "customer_id",
    "name",
    size("tags").alias("num_tags"),
    size("browsing_history").alias("num_browsing_categories")
)

display(array_sizes_df)

# COMMAND ----------

## タグを展開してすべての顧客カテゴリを表示
exploded_tags_df = parsed_df.select(
    "customer_id",
    "name",
    explode("tags").alias("tag")
)

display(exploded_tags_df)

# COMMAND ----------

## 最も一般的な顧客タグを見つける
## 各タグの頻度を数える
tag_counts_df = exploded_tags_df.groupBy("tag").count().orderBy(desc("count"))
display(tag_counts_df)

# COMMAND ----------

# 1. 最近の注文配列を展開して個々の注文を分析する
# 2. 顧客ごとの総収益を計算する

# COMMAND ----------

## 最近の注文を分析するために、recent_ordersを展開する
orders_df = parsed_df.select(
    "customer_id",
    "name",
    explode("recent_orders").alias("order")
)

## 顧客ごとの総収益を計算する
customer_revenue_df = orders_df.groupBy(
    "customer_id",
    "name"
).agg(
    sum("order.total").alias("total_revenue"),
    count("order.order_id").alias("order_count")
).orderBy(desc("total_revenue"))

display(customer_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. ボーナスチャレンジ: 顧客の購入パターンを分析する
# MAGIC
# MAGIC `collect_list`および`collect_set`集計関数を使用して、顧客の購入パターンの概要を作成してみましょう。

# COMMAND ----------

## まず、注文のフラット化ビューを作成します
order_items_df = orders_df.select(
    "customer_id",
    "name",
    "order.order_id",
    "order.date",
    explode("order.items").alias("item")
)

## 次に、各アイテムから名前フィールドを抽出します
item_details_df = order_items_df.selectExpr(
    "customer_id",
    "name",
    "item.name as product_name"
)

# データを検査します
display(item_details_df)

# COMMAND ----------

## 各顧客が購入したすべての製品を収集し、各「customer_id」に対して「all_products_purchased」と「unique_products_purchased」という新しい列を作成します。
customer_products_df = item_details_df.groupBy(
    "customer_id"
).agg(
    collect_list("product_name").alias("all_products_purchased"),
    collect_set("product_name").alias("unique_products_purchased")
)

display(customer_products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
