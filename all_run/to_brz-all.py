from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os

spark = SparkSession.builder \
    .appName("a") \
    .master('local[*]') \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config('spark.dynamicAllocation.minExecutors', '1') \
    .config('spark.dynamicAllocation.maxExecutors', '2') \
    .config('spark.dynamicAllocation.enabled', 'true') \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('S3_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('S3_SECRET_KEY')) \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('S3_ENDPOINT')) \
    .config("spark.hadoop.fs.s3a.impl", 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .config("spark.hadoop.fs.s3a.path.style.access", 'true') \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0," 
            "org.apache.hadoop:hadoop-aws:3.3.4," 
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", 'io.delta.sql.DeltaSparkSessionExtension') \
    .config("spark.sql.catalog.spark_catalog", 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()
)

start_date_str = ""
end_date_str = "
start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
end_date = datetime.strptime(end_date_str, "%d-%m-%Y")

current_date = start_date
print(f"Bắt đầu xử lý dữ liệu từ {start_date_str} đến {end_date_str}...")

while current_date <= end_date:
    _date_str = current_date.strftime("%d-%m-%Y")
    _month_year_str = current_date.strftime("%m-%Y")

    file_path = f"file://{os.environ['BUS_TICKET_PATH']}/{_month_year_str}/bus_data_{_date_str}.csv"
    s3_path = f"s3a://bronze/ticket/{_month_year_str}/{_date_str}"

    try:
        raw = spark.read.csv(file_path, header=True)
        raw.write.format("delta").mode("append").save(s3_path)
        print(f"  Ghi thành công vào Delta Lake cho ngày {_date_str}.")
    except Exception as e:
        print(f"  LỖI khi xử lý ngày {_date_str}: {e}")
        print(f"  Bỏ qua ngày này và tiếp tục...")


    current_date += timedelta(days=1)

print("\nHoàn tất quá trình xử lý dữ liệu theo ngày.")

spark.stop()
