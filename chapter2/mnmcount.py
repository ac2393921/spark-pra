import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()

    mnm_file = sys.argv[1]
    mnm_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file)
    )

    # 1. DataFrameからState, Color, Countのカラムを選択
    # 2. State, Colorでグループ化
    # 3. Countの合計を計算
    # 4. Countの合計で降順にソート
    count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .groupby("State", "Color")
        .sum("Count")
        .orderBy("sum(Count)", ascending=False)
    )

    count_mnm_df.show(n=60, truncate=False)
    print(f"Total Rows = {count_mnm_df.count()}")

    # 1. DataFrameからState, Color, Countのカラムを選択
    # 2. StateがCAの行をフィルタ
    # 3. State, Colorでグループ化
    # 4. Countの合計を計算
    # 5. Countの合計で降順にソート
    ca_count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy("sum(Count)", ascending=False)
    )

    ca_count_mnm_df.show(n=10, truncate=False)
    spark.stop()
