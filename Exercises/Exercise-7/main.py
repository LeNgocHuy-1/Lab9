import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def create_reports_directory():
    """Tạo thư mục reports nếu chưa có."""
    if not os.path.exists('reports'):
        os.makedirs('reports')

def read_data_from_zip(zip_file_path):
    """Đọc dữ liệu từ tệp ZIP."""
    return spark.read.csv(f"zip://{zip_file_path}/*.csv", header=True, inferSchema=True)

def add_source_file_column(df, source_file_name):
    """Thêm cột source_file chứa tên tệp."""
    return df.withColumn("source_file", F.lit(source_file_name))

def extract_date_from_filename(df):
    """Lấy ngày từ tên tệp và tạo cột file_date."""
    # Giả sử tên tệp là 'hard-drive-2022-01-01-failures.csv.zip'
    return df.withColumn("file_date", 
                         F.to_date(F.regexp_extract(F.col("source_file"), r'(\d{4}-\d{2}-\d{2})', 1), 'yyyy-MM-dd'))

def add_brand_column(df):
    """Thêm cột brand từ cột model."""
    return df.withColumn("brand", 
                         F.when(F.col("model").contains(" "), 
                                F.split(F.col("model"), " ").getItem(0)).otherwise("unknown"))

def add_storage_ranking_column(df):
    """Tạo cột storage_ranking theo capacity_bytes."""
    # Tạo bảng phụ để xếp hạng theo capacity_bytes
    ranking_df = df.select("capacity_bytes", "model").distinct().orderBy(F.col("capacity_bytes"), ascending=False)
    ranking_df = ranking_df.withColumn("storage_ranking", F.row_number().over(F.Window.orderBy(F.col("capacity_bytes").desc())))
    
    # Join lại với df chính
    return df.join(ranking_df.select("model", "storage_ranking"), on="model", how="left")

def add_primary_key_column(df):
    """Tạo cột primary_key là băm các cột quan trọng."""
    return df.withColumn("primary_key", 
                         F.sha2(F.concat_ws("-", "date", "serial_number", "model", "capacity_bytes"), 256))

def main():
    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("Exercise7").getOrCreate()

    # Đọc dữ liệu từ tệp ZIP
    zip_file_path = 'data/hard-drive-2022-01-01-failures.csv.zip'  # Đảm bảo đường dẫn tệp chính xác
    df = read_data_from_zip(zip_file_path)
    
    # Thêm cột source_file
    source_file_name = "hard-drive-2022-01-01-failures.csv.zip"  # Tên tệp
    df = add_source_file_column(df, source_file_name)
    
    # Thêm cột file_date từ source_file
    df = extract_date_from_filename(df)
    
    # Thêm cột brand từ model
    df = add_brand_column(df)
    
    # Tạo cột storage_ranking
    df = add_storage_ranking_column(df)
    
    # Tạo cột primary_key
    df = add_primary_key_column(df)
    
    # Kiểm tra kết quả
    df.show(5, truncate=False)
    
    # Lưu kết quả vào thư mục reports
    df.write.csv('reports/final_report.csv', header=True)

if __name__ == "__main__":
    main()
