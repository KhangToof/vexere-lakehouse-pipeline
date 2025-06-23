from pyspark.sql import SparkSession
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("test") \
    .master('local[*]') \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config('spark.dynamicAllocation.minExecutors', '1') \
    .config('spark.dynamicAllocation.maxExecutors', '2') \
    .config('spark.dynamicAllocation.enabled', 'true') \
    .config("spark.hadoop.fs.s3a.access.key", 'xdDPuIep2C9PzFaPQmJ7') \
    .config("spark.hadoop.fs.s3a.secret.key", 'GMqTLyTksNX75JrYUA1g2FfpePDJbQpqLJY6b4y2') \
    .config("spark.hadoop.fs.s3a.endpoint", "http://100.69.155.39:9000") \
    .config("spark.hadoop.fs.s3a.impl", 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .config("spark.hadoop.fs.s3a.path.style.access", 'true') \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0," 
            "org.apache.hadoop:hadoop-aws:3.3.4," 
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", 'io.delta.sql.DeltaSparkSessionExtension') \
    .config("spark.sql.catalog.spark_catalog", 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

start_date_str = "05-05-2025"
end_date_str = "10-05-2025"

start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
end_date = datetime.strptime(end_date_str, "%d-%m-%Y")

# Biến để lặp qua từng ngày
current_date = start_date
print(f"Bắt đầu xử lý dữ liệu từ {start_date_str} đến {end_date_str}...")

# Vòng lặp qua từng ngày trong khoảng thời gian
while current_date <= end_date:
    # Định dạng ngày và tháng-năm cho ngày hiện tại trong vòng lặp
    _date_str = current_date.strftime("%d-%m-%Y")
    _month_year_str = current_date.strftime("%m-%Y")

    # print(f"\nĐang xử lý cho ngày: {_date_str}")

    
    # Xây dựng đường dẫn file và S3 cho ngày hiện tại
    file_path = f"file:///home/aduankan/Documents/Airflow/raw/ticket/{_month_year_str}/bus_data_{_date_str}.csv"
    s3_path = f"s3a://bronze/ticket/{_month_year_str}/{_date_str}" # Sử dụng s3_date_str

    # print(f"  Đường dẫn file CSV: {file_path}")
    # print(f"  Đường dẫn S3 Delta: {s3_path}")

    try:
        # Đọc file CSV cho ngày hiện tại
        raw = spark.read.csv(file_path, header=True)
        raw.write.format("delta").mode("append").save(s3_path)
        print(f"  Ghi thành công vào Delta Lake cho ngày {_date_str}.")

    except Exception as e:
        # Xử lý lỗi nếu file không tồn tại hoặc có vấn đề khác khi đọc/ghi
        # Phổ biến nhất là AnalysisException khi file không tìm thấy
        print(f"  LỖI khi xử lý ngày {_date_str}: {e}")
        print(f"  Bỏ qua ngày này và tiếp tục...")
        # Bạn có thể thêm logic log lỗi chi tiết hơn ở đây nếu cần

    # Chuyển sang ngày tiếp theo
    current_date += timedelta(days=1)

print("\nHoàn tất quá trình xử lý dữ liệu theo ngày.")

spark.stop()
