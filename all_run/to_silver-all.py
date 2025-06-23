from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import regexp_replace, split, col, size, array_contains, lower, lit
from pyspark.sql.functions import to_date, when, max as spark_max, round as F_round
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.functions import udf, round as F_round
from pyspark.sql.types import FloatType, IntegerType, ArrayType, StringType
from pyspark.sql.window import Window
import numpy as np
import pandas as pd
import os
import re
from typing import Optional
import traceback

def standardize_string(text: str) -> Optional[str]:
    if text is None:
        return None
    try:
        text = text.lower()
        text = re.sub(r"\bsai gon\b", "sài gòn", text)
        text = re.sub(r"\bsai gon nga tư ga\b", "sài gòn ngã 4 ga", text)
        text = re.sub(r"\bngã tư an sươngquốc lộ 1a\b", "ngã tư an sương quốc lộ 1a", text)
        text = re.sub(r"sg sài gòn", "sài gòn", text)
        text = re.sub(r"sài gòn sài gòn", "sài gòn", text)
        text = re.sub(r"\b(q([0-9]+))\b", r"quận \2", text)
        text = re.sub(r"(?<=\w)(ql\s*([0-9]+[a-z]?))\b", r" quốc lộ \2", text)
        text = re.sub(r"\b(ql\s*([0-9]+[a-z]?))\b", r"quốc lộ \2", text)
        text = re.sub(r"\btp\b", "thành phố", text)
        text = re.sub(r"\bvp\b", "văn phòng", text)
        text = re.sub(r"\bkcn\b", "khu công nghiệp", text)
        text = re.sub(r"\bhcm\b", "hồ chí minh", text)
        text = re.sub(r"\bbx\b", "bến xe", text)
        text = re.sub(r"\btx\b", "thị xã", text)
        text = re.sub(r"\bsg\b", "sài gòn", text)
        text = re.sub(r"\bubnd\b", "uỷ ban nhân dân", text)
        text = re.sub(r"\btt\b", "", text)
        text = re.sub(r"\bcd\b", "", text)
        vietnamese_chars = "áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ"
        allowed_chars_pattern = fr"[^a-z0-9\s{vietnamese_chars}]"
        text = re.sub(allowed_chars_pattern, "", text, flags=re.IGNORECASE)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    except Exception as e:
        print(f"Error standardizing text: '{text[:50]}...' - {e}")
        return None

def convert_duration(duration):
    if duration is None:
        return None
    match = re.match(r"(\d+)h?(\d*)m?", duration)
    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        return hours + minutes / 60
    return None

def remove_price_chars(price_str):
    if price_str is not None:
        price_str = re.sub(r"[^0-9]", "", price_str)
        return int(price_str) if price_str else None
    return None

def add_bus_id(bus_ticket, bus_ids_company, join_column="Bus_Name"):
    bus_ticket_with_id = bus_ticket.join(
        bus_ids_company.select(col(join_column), col("Bus_Id")),
        on=join_column,
        how="left"
    )
    
    # Sắp xếp lại thứ tự cột
    columns_order = ["Bus_Key", "Bus_Id" , "Bus_Name"] + [col for col in bus_ticket_with_id.columns if col not in ["Bus_Key", "Bus_Id", "Bus_Name"]]
    bus_ticket_with_id = bus_ticket_with_id.select(*columns_order)
    
    return bus_ticket_with_id

def add_bus_id_z(bus_facility, bus_ids_company, join_column="Bus_Name"):
    bus_facility_with_id = bus_facility.join(
        bus_ids_company.select(col(join_column), col("Bus_Id")),
        on=join_column,
        how="left"
    )
    
    # Sắp xếp lại thứ tự cột
    columns_order = ["Id", "Bus_Id" , "Bus_Name"] + [col for col in bus_facility_with_id.columns if col not in ["Id", "Bus_Id" , "Bus_Name"]]
    bus_facility_with_id = bus_facility_with_id.select(*columns_order)
    
    return bus_facility_with_id

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
end_date_str = "05-05-2025"

start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
end_date = datetime.strptime(end_date_str, "%d-%m-%Y")

# Biến để lặp qua từng ngày
current_date = start_date
print(f"Bắt đầu xử lý dữ liệu từ {start_date_str} đến {end_date_str}...")

# Vòng lặp qua từng ngày trong khoảng thời gian
while current_date <= end_date:
    _date = current_date.strftime("%d-%m-%Y")
    _month_year = current_date.strftime("%m-%Y")
    try:
        bus_ticket_new = spark.read.format("delta").load(f"s3a://bronze/ticket/{_month_year}/{_date}")
        # bus_ticket_new = spark.read.format("delta").load("s3a://bronze/ticket_all/")
        bus_ids_company = spark.read.format("delta").load("s3a://silver/bus_ids/")
        
        # --- Start: Determine Max Existing ID ---
        target_delta_path = "s3a://silver/ticket/"
        max_existing_id = 0
        print(f"Attempting to read existing tickets from: {target_delta_path}") # Thêm log
        try:
            existing_tickets = spark.read.format("delta").load(target_delta_path)
            print(f"Successfully loaded DataFrame from {target_delta_path}. Checking if empty...") # Thêm log
            # Sử dụng .rdd.isEmpty() có thể hiệu quả hơn cho một số trường hợp
            if not existing_tickets.rdd.isEmpty():
                print("DataFrame is not empty. Calculating max(Bus_Key)...") # Thêm log
                # In schema để kiểm tra cột Bus_Key
                print("Schema of existing_tickets:")
                existing_tickets.printSchema()
    
                max_id_result = existing_tickets.select(spark_max(col("Bus_Key").cast(IntegerType())).alias("max_id")).first() # Thêm alias cho rõ ràng
                print(f"Result of max calculation: {max_id_result}") # Thêm log
                if max_id_result and max_id_result["max_id"] is not None: # Kiểm tra bằng alias
                    max_existing_id = max_id_result["max_id"]
                else:
                    print("Max Bus_Key calculation returned None or Row with None.") # Thêm log
            else:
                print(f"DataFrame from {target_delta_path} is empty.") # Thêm log
    
        except Exception as e:
            print(f"CRITICAL ERROR reading or processing {target_delta_path}:") # Đánh dấu lỗi quan trọng
            print(f"Exception Type: {type(e).__name__}")
            print(f"Exception Message: {str(e)}") # Chuyển e thành string để chắc chắn in được
            print("------ Full Traceback ------")
            traceback.print_exc() # In đầy đủ stack trace để biết lỗi ở dòng nào
            print("---------------------------")
            print(f"Proceeding with max_existing_id = 1 due to error reading existing table.")
            max_existing_id = 0
    
        print(f"Max existing Bus_Key determined as: {max_existing_id}. New keys will start from {max_existing_id + 1}.")
        
        bus_ticket_new = bus_ticket_new.withColumn("Start_Date", to_date("Start_Date", "dd-MM-yyyy"))
        
        standardize_string_udf_final = udf(standardize_string, StringType())
        bus_ticket_new = bus_ticket_new.withColumn("Departure_Place", standardize_string_udf_final(bus_ticket_new["Departure_Place"]))
        bus_ticket_new = bus_ticket_new.withColumn("Arrival_Place", standardize_string_udf_final(bus_ticket_new["Arrival_Place"]))
        
        convert_duration_udf = udf(convert_duration, FloatType())
        bus_ticket_new = bus_ticket_new.withColumn("Duration", F_round(convert_duration_udf(bus_ticket_new["Duration"]), 2))
        
        remove_price_chars_udf = udf(remove_price_chars, IntegerType())
        bus_ticket_new = bus_ticket_new.withColumn("Price", remove_price_chars_udf(col("Price")))
        
        bus_ticket_new = bus_ticket_new.withColumn("Type_Bus", lower(col("Type_Bus")))
        
        bus_ticket_new = bus_ticket_new.withColumn(
            "Bus_Type_Category",
            when(col("Type_Bus").like("%giường nằm%") & ~col("Type_Bus").like("%limousine%"), "giường nằm")
            .when(col("Type_Bus").like("%huyndai solati 11 chỗ%"), "limousine ghế ngồi")
            .when(col("Type_Bus").like("%limousine%") & col("Type_Bus").like("%ghế ngồi%"), "limousine ghế ngồi")
            .when(col("Type_Bus").like("%limousine%") & col("Type_Bus").like("%giường nằm có wc%"), "limousine giường nằm có WC")
            .when(col("Type_Bus").like("%limousine%") & col("Type_Bus").like("%giường nằm%") | col("Type_Bus").like("%giường%") , "limousine giường nằm")
            .when(col("Type_Bus").rlike(r"limousine.*[0-9]+\s*chỗ"), "limousine giường nằm")
            .when(col("Type_Bus").like("%limousine%") & col("Type_Bus").like("%giường phòng có wc%"), "limousine giường phòng có WC")
            .when(col("Type_Bus").like("%limousine%") & col("Type_Bus").like("%giường phòng%") | col("Type_Bus").like("%phòng%"), "limousine giường phòng")
            .when(col("Type_Bus").like("%ghế ngồi%"), "ghế ngồi")
            .otherwise("khác")
        )
        
        bus_ticket_new = bus_ticket_new.withColumn("temp_id_for_ordering", monotonically_increasing_id())
        window_spec = Window.orderBy("temp_id_for_ordering")
        bus_ticket_new = bus_ticket_new.withColumn("Bus_Key", row_number().over(window_spec) + lit(max_existing_id))
        bus_ticket_new = bus_ticket_new.drop("temp_id_for_ordering")
        bus_ticket_new = bus_ticket_new.withColumn("Bus_Key", col("Bus_Key").cast(IntegerType()))
        bus_ticket_final = add_bus_id(bus_ticket_new, bus_ids_company)
        bus_ticket_final.write.format("delta").mode("append").save(target_delta_path)

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
