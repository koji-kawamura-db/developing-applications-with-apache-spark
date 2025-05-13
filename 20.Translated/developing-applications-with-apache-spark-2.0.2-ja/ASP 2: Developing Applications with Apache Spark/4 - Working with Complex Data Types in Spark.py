# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 - Sparkにおける複雑なデータタイプの操作
# MAGIC
# MAGIC このデモでは、実際の電子商取引データの例を使用して、Sparkのネストされたデータ構造（構造体、配列、マップなど）を効果的に操作する方法を示します。
# MAGIC
# MAGIC ### 目標
# MAGIC - JSON文字列データをSpark SQLネイティブの複雑なタイプに変換する
# MAGIC - 複雑なデータタイプ（構造体、配列、マップ）を理解して操作する
# MAGIC - ネストされたJSONのようなデータ構造を処理する
# MAGIC - 必要に応じてデータセットを変形するためにピボットと展開関数を使用する

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
# MAGIC また、`raw_user_data` という名前の一時テーブルも作成します。
# MAGIC <br></br>
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **注:** `DA` オブジェクトは、Databricks Academy のコースでのみ使用され、コース外では使用できません。コースを実行するために必要な情報を動的に参照します。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# MAGIC %md
# MAGIC #### 新しく作成されたテーブルを照会する

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_user_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. JSON文字列をStructTypesに変換する
# MAGIC
# MAGIC ネストされたJSON文字列（配列および/またはオブジェクト）を含む生データが与えられた場合、DataFrame APIのネイティブの`StructTypes`にこのデータを変換します。
# MAGIC
# MAGIC #### なぜJSON文字列をStructTypesに変換するのか？
# MAGIC
# MAGIC Spark DataFramesのJSON文字列には、いくつかの非効率性があります。
# MAGIC
# MAGIC 1. **パースのオーバーヘッド**: JSON文字列に対してクエリを実行するたびに、Sparkはそれらをパースする必要があり、計算オーバーヘッドが追加されます。
# MAGIC 2. **メモリの非効率性**: JSON文字列は、フィールド名をすべての行に対して繰り返し保存するため、メモリを無駄にします。
# MAGIC 3. **型の安全性なし**: JSON文字列はデータ型を強制しないため、潜在的なエラーが発生します。
# MAGIC 4. **クエリのパフォーマンスが低い**: Sparkは、JSON文字列の内容に対するクエリを効果的に最適化できません。
# MAGIC 5. **述語のプッシュダウンが制限される**: フィルタ操作は、カラムストレージの最適化を活用できません。
# MAGIC
# MAGIC ### JSON文字列をStructTypesに変換する手順
# MAGIC
# MAGIC 1. **スキーマの推論**: JSONデータの構造を決定します（`schema_of_json`関数を使用できます）。
# MAGIC 2. **スキーマの適用**: `from_json()`を使用して文字列を構造化データに変換します。
# MAGIC 3. **検証**: すべてのデータが正しくパースされ、型が適切であることを確認します。
# MAGIC 4. **最適化**: 変換後、必要に応じてストレージ/処理を最適化します。
# MAGIC
# MAGIC ### StructTypesの利点
# MAGIC
# MAGIC 1. **カラムストレージ**: Parquet/Deltaを使用した効率的なストレージ。
# MAGIC 2. **型の安全性**: スキーマの強制によりデータエラーが防止されます。
# MAGIC 3. **クエリの最適化**: Sparkは、型付きデータを使用してクエリをより適切に最適化できます。
# MAGIC 4. **述語のプッシュダウン**: フィルタをストレージレイヤにプッシュダウンできます。
# MAGIC 5. **パフォーマンスの向上**: クエリの高速化とメモリ使用量の削減。

# COMMAND ----------

from pyspark.sql.functions import *

# 一部のデータ（JSON文字列を含む）を読み込む
raw_user_data_df = spark.read.table("raw_user_data")

# COMMAND ----------

# データを検査する
raw_user_data_df.printSchema()

# COMMAND ----------

# データフレームの表示
display(raw_user_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 上記のデータを表示し、 **interests** と **recent_purchases** の列を見つけます
# MAGIC 2. **interests** は文字列要素の配列列であり、 **recent_purchases** はオブジェクトを含む列です

# COMMAND ----------

# Interests は文字列の配列、事前定義されたスキーマを使用
interests_schema = ArrayType(StringType())

# COMMAND ----------

# "recent_purchases"のスキーマを取得し、生データセットのすべての列を確認済みの構造にキャストする

# "recent_purchases"列の1つの値のサンプルを取り、Driverに戻す
recent_purchases_json = raw_user_data_df.select("recent_purchases").limit(1).collect()[0][0]
print("生のJSON文字列:", recent_purchases_json)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### **schema_of_json** 関数を使用して、サンプルデータ行に基づいてスキーマを生成する
# MAGIC
# MAGIC 多くの場合、特に複数のネストされた複雑な構造がある場合、サンプルの JSON データに基づいてスキーマを生成するのが最も簡単です。これは、 **schema_of_json** 関数を使用して実行できます。
# MAGIC
# MAGIC 上記のデータセットから、 **interests** は文字列要素の配列列であり、 **recent_purchases** はオブジェクトを含む配列列であることがわかります。

# COMMAND ----------

# 最近の購入の JSON のスキーマを取得する
recent_purchases_schema = schema_of_json(lit(recent_purchases_json))

# COMMAND ----------

# 正しいスキーマで列を解析する
parsed_users_df = raw_user_data_df.select(
    col("user_id").cast("integer"),
    col("name"),
    col("active").cast("boolean"),
    col("signup_date").cast("date"),
    from_json(col("interests"), interests_schema).alias("interests"),
    from_json(col("recent_purchases"), recent_purchases_schema).alias("recent_purchases")
)

# スキーマを調べる
parsed_users_df.printSchema()

# COMMAND ----------

# ここで再びデータを確認してみましょう。recent_purchasesの順序が変更されていることに気づくはずです。
display(parsed_users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. 配列の操作
# MAGIC
# MAGIC 配列にアクセスして操作するさまざまな方法を探索してみましょう。

# COMMAND ----------

# array_size メソッドを使用して、データフレーム内の配列列の長さを確認する
display(
parsed_users_df.select(
    "user_id",
    array_size("interests").alias("number_of_interests"),
    array_size("recent_purchases").alias("number_of_recent_purchases")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Explode メソッド
# MAGIC
# MAGIC `explode` メソッドは、配列要素をレコードに展開するために使用されます。

# COMMAND ----------

# まず、必要な列のみを選択してデータを簡素化してみましょう
user_101s_interests_df = parsed_users_df.select("user_id", "interests").filter(parsed_users_df.user_id == 101)
display(user_101s_interests_df)

# COMMAND ----------

# "explode"のデモンストレーション、"user_id" 101 に関連付けられた 3 つの行に注目してください（各 "interests" 配列要素に対して 1 つずつ）
display(
    user_101s_interests_df.select(
        "user_id", 
        explode("interests").alias("interest")
    )    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. collect_setおよびcollect_listメソッド
# MAGIC
# MAGIC `collect_set`および`collect_list`メソッドは、集計関数（通常、グループ化されたデータで操作）であり、列値から配列を作成します。
# MAGIC
# MAGIC `collect_list`には重複値が含まれる場合がありますが、`collect_set`は重複する配列要素が存在する場合はそれらを削除します。

# COMMAND ----------

# "interests" 列を展開して新しい DataFrame を作成することから始めましょう
exploded_df = parsed_users_df.select("user_id", explode("interests").alias("interest"))
display(exploded_df)

# COMMAND ----------

# `collect_list`を使用して、各「user_id」に対してすべての「interests」値をリストに収集しましょう
user_interests_df = exploded_df.groupBy("user_id").agg(collect_list("interest").alias("interests"))
display(user_interests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. 構造体フィールドの参照
# MAGIC
# MAGIC 構造体（事前に定義されたスキーマを持つオブジェクト）内のフィールドにアクセスする方法を探ってみましょう。

# COMMAND ----------

# まず、"recent_purchases" 列を展開してみましょう
exploded_purchases_df = parsed_users_df.select("user_id", explode("recent_purchases").alias("purchase"))
display(exploded_purchases_df)

# COMMAND ----------

# ドット表記を使用して構造体のフィールドにアクセスする
recent_purchases_df = exploded_purchases_df.select(
                        "user_id", 
                        col("purchase.date").alias("purchase_date"), 
                        col("purchase.product_id").alias("product_id"), 
                        col("purchase.name").alias("product_name"), 
                        col("purchase.price").alias("purchase_price")
                    )
display(recent_purchases_df)

# COMMAND ----------

# フィールドにアクセスするために、getField() メソッドを使用して、構造体内の列を参照することもできます。以下は例です。

field_access_df = exploded_purchases_df.select(
    "user_id",
    col("purchase").getField("date").alias("purchase_date"),
    col("purchase").getField("product_id").alias("product_id"),
    col("purchase").getField("name").alias("product_name"),
    col("purchase").getField("price").alias("price")
)

display(field_access_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. ピボットメソッドの使用
# MAGIC
# MAGIC Sparkの`pivot`メソッドを使用すると、行データを列形式に変換し、クロス集計を作成できます。これは、カテゴリデータを分析する場合や、レポートや視覚化のためにデータを再形成する必要がある場合に、特徴エンジニアリングに特に役立ちます。

# COMMAND ----------

# 購入データをピボットして、各ユーザーが購入した各製品の数を表示しましょう
pivot_df = (recent_purchases_df
    .groupBy("user_id")
    .pivot("product_name")
    .agg(count("product_id").alias("quantity_purchased"))
)

# 結果を表示
display(pivot_df)

# COMMAND ----------

# 読みやすくするためにnull値をゼロに置き換えます
pivot_df_no_nulls = (recent_purchases_df
    .groupBy("user_id")
    .pivot("product_name")
    .agg(count("product_id").alias("quantity_purchased"))
    .fillna(0)
)

# 結果を表示します
display(pivot_df_no_nulls)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 主な学習内容
# MAGIC
# MAGIC 1. **Struct Type の操作**:
# MAGIC    - 簡単なアクセスにはドット表記を使用
# MAGIC    - 動的列アクセスには `getField()` を使用
# MAGIC    - スキーマの明確さを維持
# MAGIC
# MAGIC 2. **配列操作**:
# MAGIC    - 配列の操作には配列関数を使用
# MAGIC    - 詳細な分析には explode を活用
# MAGIC    - 大規模な配列のパフォーマンスを考慮
# MAGIC
# MAGIC 3. **複雑な集計関数**:
# MAGIC    - グループ化されたデータから配列を作成するには `collect_list` および `collect_set` メソッドを使用
# MAGIC    - 分析およびレポートのために行値を列に変換するには `pivot` 関数を使用

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
