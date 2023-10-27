import os
import zipfile
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.functions import date_format, datediff, col, rank, avg, first,desc,row_number,lit
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
 # Define the selected columns to filter by
selected_columns = ["trip_id", "start_time", "end_time", "bikeid", "tripduration", "from_station_id", "from_station_name",
                        "to_station_id", "to_station_name", "usertype", "gender", "birthyear"]


# Define the function to load data from zipped CSV files
def load_data(folder_path):
    zip_files = [os.path.join(folder_path, file_name) for file_name in os.listdir(folder_path) if file_name.endswith(".zip")]
    data_frames = []

    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.csv'):
                    with zip_ref.open(file_info) as csv_file:
                        try:
                            # Create a temporary file to store the CSV contents
                            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                                temp_file.write(csv_file.read())
                                temp_file_name = temp_file.name

                            # Read the CSV file from the temporary file and create a DataFrame
                            df = spark.read.option("header", "true").csv(temp_file_name, inferSchema=True)
                            if all(col_name in df.columns for col_name in selected_columns):
                                data_frames.append(df)
                            else:
                                print(f"Skipping file {file_info.filename} as it does not contain the selected columns.")
                        except Exception as e:
                            # Handle any errors that occur during reading
                            print(f"Skipping file {file_info.filename} due to an error: {str(e)}")

                            # Remove the temporary file in case of an error
                            os.remove(temp_file_name)
    
    return data_frames

# Define the function to combine DataFrames into one DataFrame
def combine_dataframes(data_frames):
    if data_frames:
        combined_df = data_frames[0]
        for i in range(1, len(data_frames)):
            combined_df = combined_df.union(data_frames[i])
        return combined_df
    else:
        return None

def main():

    # Define the path to the folder containing the zip files
    folder_path = "data"  # Replace with the path to your data folder

    # Load data and combine DataFrames
    data_frames = load_data(folder_path)
    combined_df = combine_dataframes(data_frames)

    # Check if combined_df is not None
    if combined_df:
        
        
        # 1. What is the average trip duration per day?
        def average_trip_duration_per_day(df: DataFrame):
            result = df.withColumn("start_date", date_format("start_time", "yyyy-MM-dd"))
            result = result.dropDuplicates()
            result = result.groupBy("start_date").agg(avg("tripduration").alias("avg_trip_duration"))
            result = result.orderBy("start_date")  # Sort by start_date in ascending order
            result.write.csv("reports/average_trip_duration_per_day.csv", header=True, mode="overwrite")
        
        # 2. How many trips were taken each day?
        def trips_per_day(df: DataFrame):
            result = df.withColumn("start_date", date_format("start_time", "yyyy-MM-dd"))
            result = result.dropDuplicates()
            result = result.groupBy("start_date").count()
            result = result.orderBy("start_date") 
            result.write.csv("reports/trips_per_day.csv", header=True, mode="overwrite")
        
        # 3. What was the most popular starting trip station for each month?
        def popular_starting_stations_by_month(df: DataFrame):
            result = df.withColumn("start_month", date_format("start_time", "yyyy-MM"))
            result = result.dropDuplicates()
            result = result.groupBy("start_month", "from_station_id", "from_station_name").count()
            result = result.orderBy("count").sort(desc("count"))
            window = Window.partitionBy("start_month").orderBy(desc("count"))
            result=result.withColumn("station_rank", row_number().over(window))
            result = result.filter(col("station_rank") == 1).drop("station_rank")
            result.write.csv("reports/popular_starting_stations_by_month.csv", header=True, mode="overwrite")

        # 4. What were the top 3 trip stations each day for the last two weeks?
        def top_trip_stations_last_two_weeks(df: DataFrame):
            result = df.withColumn("start_date", date_format("start_time", "yyyy-MM-dd"))
            result = result.dropDuplicates()
            last_date = result.agg({"start_date": "max"}).collect()[0][0]
            result = result.withColumn("days_ago", datediff(lit(last_date), "start_date"))
            result = result.filter((col("days_ago") >= 0) & (col("days_ago") <= 14))
            result = result.groupBy("start_date", "from_station_id", "from_station_name").count()
            result = result.orderBy("count").sort(desc("count"))
            window = Window.partitionBy("start_date").orderBy(desc("count"))
            result=result.withColumn("station_rank", row_number().over(window))
            result = result.filter(col("station_rank") <= 3).drop("station_rank")
            result.write.csv("reports/top_trip_stations_last_two_weeks.csv", header=True, mode="overwrite")

            
        # 5. Do Males or Females take longer trips on average?
        def average_trip_duration_by_gender(df: DataFrame):
            result = df.withColumn("tripduration", col("tripduration").cast(DoubleType()))
            result = df.groupBy("gender").agg(avg("tripduration").alias("avg_trip_duration"))
            result.write.csv("reports/average_trip_duration_by_gender.csv", header=True, mode="overwrite")

        # 6. What is the top 10 ages of those that take the longest trips, and shortest?
        def top_10_ages_longest_and_shortest_trips(df: DataFrame):
            result = df.withColumn("birthyear", col("birthyear").cast(DoubleType()))
            result = df.groupBy("birthyear").agg(avg("tripduration").alias("avg_trip_duration"))
            result_longest = result.orderBy(col("avg_trip_duration").desc()).limit(10)
            result_shortest = result.orderBy(col("avg_trip_duration")).limit(10)
            result_longest.write.csv("reports/top_10_ages_longest_trips.csv", header=True, mode="overwrite")
            result_shortest.write.csv("reports/top_10_ages_shortest_trips.csv", header=True, mode="overwrite")

        trips_per_day(combined_df)
        average_trip_duration_per_day(combined_df)
        popular_starting_stations_by_month(combined_df)
        top_trip_stations_last_two_weeks(combined_df)
        average_trip_duration_by_gender(combined_df)
        top_10_ages_longest_and_shortest_trips(combined_df)

    else:
        print("No valid data loaded.")

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()





