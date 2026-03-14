import sys
from pyspark.sql import SparkSession


def process_data(target_date):
    local_jars = (
        "/opt/spark/jars/delta-spark_2.12-3.1.0.jar,"
        "/opt/spark/jars/delta-storage-3.1.0.jar,"
        "/opt/spark/jars/postgresql-42.7.3.jar,"
        "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    )

    spark = (
        SparkSession.builder.appName("PostgresToMinioDelta")
        .config("spark.jars", local_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "matrix")
        .config("spark.hadoop.fs.s3a.secret.key", "matrix123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    query = f"(SELECT * FROM transactions WHERE transaction_date = '{target_date}') as tmp_table"

    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/airflow")
        .option("dbtable", query)
        .option("user", "airflow")
        .option("password", "airflow")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    minio_path = "s3a://airflowpractice/transactions_delta"

    df.write.format("delta").mode("append").partitionBy("transaction_date").save(
        minio_path
    )

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Xəta: Tarix parametri təqdim edilməyib.")
        sys.exit(1)

    execution_date = sys.argv[1]
    process_data(execution_date)
