# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 99 (オプション) - DataFrame API を使用した基本的な ETL
# MAGIC
# MAGIC このデモンストレーションでは、Flights データセットを使用して一般的な ETL 操作を実行します。DataFrame API を使用してデータの読み込み、クリーニング、変換、分析を行う方法を説明します。
# MAGIC
# MAGIC ### 目標
# MAGIC - Spark DataFrame を使用して一般的な ETL 操作を実装する
# MAGIC - データのクリーニングと型変換を処理する
# MAGIC - 変換を通じて派生フィーチャを作成する
# MAGIC
# MAGIC 注: このセクションはオプションであり、Dataframe API の紹介として最初に教える必要があります。

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
# MAGIC ## A. クラスルームセットアップ
# MAGIC
# MAGIC 次のセルを実行して、このコースの作業環境を構成します。また、デフォルトのカタログを **dbacademy** に設定し、以下に示す `USE` ステートメントを使用して、スキーマを特定のスキーマ名に設定します。
# MAGIC <br></br>
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **注:** `DA` オブジェクトは、Databricks Academy のコースでのみ使用され、コース外では使用できません。コースを実行するために必要な情報を動的に参照します。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-Common

# COMMAND ----------

# MAGIC %md
# MAGIC デフォルトのカタログとスキーマを表示します。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. データの読み込みと検査
# MAGIC
# MAGIC まず、フライトデータを読み込み、検査してみましょう。

# COMMAND ----------

# フライトデータを読み込む
flights_df = spark.read.table("dbacademy_airline.v01.flights_small")

# COMMAND ----------

# スキーマをプリント
flights_df.printSchema()

# COMMAND ----------

# データのサブセットを視覚的に検査する
display(flights_df.limit(10))

# COMMAND ----------

# 必要のない列を削除しましょう。"filter early, filter often" を覚えておいてください。
flights_required_cols_df = flights_df.select(
    "Year",
    "Month",
    "DayofMonth",
    "DepTime",
    "FlightNum",
    "ActualElapsedTime",
    "CRSElapsedTime",
    "ArrDelay")

# あるいは、不要な列を削除するために drop() メソッドを使用することもできました...

# COMMAND ----------

# ソースデータレコードの数を取得する
initial_count = flights_required_cols_df.count()

print(f"ソースデータには {initial_count} レコードがあります")

# COMMAND ----------

# 無効な値のデータを調べてみましょう。これには、文字列列 "ArrDelay"、"ActualElapsedTime"、"DepTime" の null 値や無効な値が含まれます。これらの列に対して数学的演算を実行する予定です。Spark SQL の COUNT_IF 関数を使用して分析を実行できます。

# キャスト列を持つ一時 SQL テーブルとして DataFrame を登録する
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
# MAGIC ## C. データクリーニング
# MAGIC
# MAGIC フライトデータには無効または欠損している値が含まれています。見つけてクリーニングしてみましょう（この場合、削除します）

# COMMAND ----------

# 指定された列に null 値が含まれる行を削除するには、na.drop DataFrame メソッドを使用できます。
non_null_flights_df = flights_required_cols_df.na.drop(
    how='any',
    subset=['CRSElapsedTime']
)

# COMMAND ----------

from pyspark.sql.functions import col

# "ArrDelay"、"ActualElapsedTime"、"DepTime" 列の無効な値を持つ行を削除しましょう
flights_with_valid_data_df = non_null_flights_df.filter(
    col("ArrDelay").cast("integer").isNotNull() & 
    col("ActualElapsedTime").cast("integer").isNotNull() &
    col("DepTime").cast("integer").isNotNull()
)

# COMMAND ----------

# "ArrDelay"と"ActualElapsedTime"に整数値のみが含まれていることがわかったので、それらを文字列から整数にキャストしてみましょう（既存の列を置き換えます）
clean_flights_df = flights_with_valid_data_df \
    .withColumn("ArrDelay", col("ArrDelay").cast("integer")) \
    .withColumn("ActualElapsedTime", col("ActualElapsedTime").cast("integer"))

clean_flights_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. データの強化
# MAGIC
# MAGIC 次に、遅延を分類するための有用な派生列を作成しましょう。

# COMMAND ----------

# まず、「Year」、「Month」、「DayofMonth」、「DepTime」列から「FlightDateTime」列を派生させ、それから構成列を削除します。
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

# 結果を表示
display(flights_with_datetime_df.limit(10))

# COMMAND ----------

# OK では、"ActualElapsedTime" と "CRSElapsedTime" の列から "ElapsedTimeDiff" 列を派生させましょう

from pyspark.sql.functions import col

flights_with_elapsed_time_diff_df = flights_with_datetime_df.withColumn(
    "ElapsedTimeDiff", col("ActualElapsedTime") - col("CRSElapsedTime")
    ).drop("ActualElapsedTime", "CRSElapsedTime")

display(flights_with_elapsed_time_diff_df.limit(10))

# COMMAND ----------

# 次に、「ArrDelay」列をカテゴリに分類します: 「時間通りに」、「軽微な遅延」、「中程度の遅延」、「深刻な遅延」

from pyspark.sql.functions import when

enriched_flights_df = flights_with_elapsed_time_diff_df \
    .withColumn("delay_category", when(col("ArrDelay") <= 0, "時間通りに")
        .when(col("ArrDelay") <= 15, "軽微な遅延")
        .when(col("ArrDelay") <= 60, "中程度の遅延")
        .otherwise("深刻な遅延")) \
       .drop("ArrDelay")

# COMMAND ----------

# 結果の表示
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
