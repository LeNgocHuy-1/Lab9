import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, datediff, rank
from pyspark.sql.window import Window

def create_reports_directory():
    """Tạo thư mục reports nếu nó chưa tồn tại."""
    if not os.path.exists('reports'):
        os.makedirs('reports')

def read_data_from_zip(zip_file_path):
    """Đọc dữ liệu từ tệp ZIP."""
    return spark.read.csv(f"zip://{zip_file_path}/*.csv", header=True, inferSchema=True)

def average_trip_duration_per_day(df):
    """Tính toán thời gian chuyến đi trung bình mỗi ngày."""
    df = df.withColumn("date", to_date(col("start_time")))  # Tạo cột "date" từ "start_time"
    result = df.groupBy("date").agg({"tripduration": "avg"}).withColumnRenamed("avg(tripduration)", "average_trip_duration")
    result.write.csv('reports/average_trip_duration_per_day.csv', header=True)

def trips_taken_per_day(df):
    """Đếm số chuyến đi mỗi ngày."""
    df = df.withColumn("date", to_date(col("start_time")))
    result = df.groupBy("date").count().withColumnRenamed("count", "num_trips")
    result.write.csv('reports/trips_taken_per_day.csv', header=True)

def most_popular_starting_station_per_month(df):
    """Tìm trạm bắt đầu chuyến đi phổ biến nhất cho mỗi tháng."""
    df = df.withColumn("month", month(to_date(col("start_time"))))
    result = df.groupBy("month", "from_station_name").count()
    window = Window.partitionBy("month").orderBy(col("count").desc())
    result = result.withColumn("rank", rank().over(window)).filter(col("rank") == 1).drop("rank")
    result.write.csv('reports/most_popular_starting_station_per_month.csv', header=True)

def top_3_trip_stations_last_two_weeks(df):
    """Tìm 3 trạm chuyến đi phổ biến nhất mỗi ngày trong 2 tuần gần nhất."""
    df = df.withColumn("date", to_date(col("start_time")))
    current_date = df.select("date").distinct().orderBy(col("date").desc()).limit(1).collect()[0][0]
    last_two_weeks_df = df.filter(datediff(current_date, col("date")) <= 14)
    
    result = last_two_weeks_df.groupBy("date", "from_station_name").count().orderBy("date", col("count").desc())
    result = result.groupBy("date").agg({"count": "max"}).withColumnRenamed("max(count)", "max_trips")
    result.write.csv('reports/top_3_trip_stations_last_two_weeks.csv', header=True)

def gender_trip_duration_comparison(df):
    """So sánh thời gian chuyến đi trung bình của nam và nữ."""
    result = df.groupBy("gender").agg({"tripduration": "avg"}).withColumnRenamed("avg(tripduration)", "average_trip_duration")
    result.write.csv('reports/gender_trip_duration_comparison.csv', header=True)

def top_10_ages_longest_shortest_trips(df):
    """Lấy độ tuổi của 10 người có chuyến đi dài nhất và ngắn nhất."""
    df = df.withColumn("age", 2021 - col("birthyear"))  # Giả sử năm hiện tại là 2021
    df = df.withColumn("tripduration", col("tripduration").cast("integer"))
    
    # Sắp xếp theo thời gian chuyến đi
    longest_trips = df.orderBy(col("tripduration").desc()).limit(10)
    shortest_trips = df.orderBy(col("tripduration")).limit(10)
    
    longest_trips.select("age", "tripduration").write.csv('reports/top_10_longest_trips.csv', header=True)
    shortest_trips.select("age", "tripduration").write.csv('reports/top_10_shortest_trips.csv', header=True)

def main():
    # Tạo thư mục reports nếu chưa có
    create_reports_directory()

    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("Exercise6").getOrCreate()
    
    # Đọc dữ liệu từ các tệp ZIP
    zip_file_path = 'data/your_file.zip'  # Thay thế bằng tên tệp ZIP của bạn
    df = read_data_from_zip(zip_file_path)
    
    # Giải quyết các câu hỏi
    average_trip_duration_per_day(df)
    trips_taken_per_day(df)
    most_popular_starting_station_per_month(df)
    top_3_trip_stations_last_two_weeks(df)
    gender_trip_duration_comparison(df)
    top_10_ages_longest_shortest_trips(df)

if __name__ == "__main__":
    main()
