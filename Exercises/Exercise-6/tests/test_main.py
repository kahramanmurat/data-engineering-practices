import pytest
from main import load_data, combine_dataframes, average_trip_duration_per_day

@pytest.fixture
def sample_data(spark_session):
    # Create a Spark DataFrame with sample data
    data = [
        (1, "2023-10-01 08:00:00", "2023-10-01 08:30:00", 101, 1800, 201, "Station A", 301, "Station B", "Subscriber", "Male", 1980),
        (2, "2023-10-01 09:00:00", "2023-10-01 09:45:00", 102, 2700, 202, "Station B", 302, "Station C", "Customer", "Female", 1990),
        # Add more sample data as needed
    ]
    
    columns = ["trip_id", "start_time", "end_time", "bikeid", "tripduration", "from_station_id", "from_station_name", "to_station_id", "to_station_name", "usertype", "gender", "birthyear"]
    
    return spark_session.createDataFrame(data, columns)

def test_load_data(sample_data):
    # Test the load_data function
    # Check if it loads data correctly
    data_frames = load_data("data")
    assert len(data_frames) > 0

def test_combine_dataframes(sample_data):
    # Test the combine_dataframes function
    # Check if it combines DataFrames correctly
    combined_df = combine_dataframes([sample_data])
    assert combined_df.count() == sample_data.count()

def test_average_trip_duration_per_day(sample_data, spark_session):
    # Test the average_trip_duration_per_day function
    # Create a DataFrame with specific data for testing
    test_data = [
        (1, "2023-10-01 08:00:00", "2023-10-01 08:30:00", 101, 1800),
        (2, "2023-10-01 09:00:00", "2023-10-01 09:45:00", 102, 2700),
        (3, "2023-10-02 08:30:00", "2023-10-02 09:15:00", 103, 2700),
        # Add more test data for different dates
    ]

    columns = ["trip_id", "start_time", "end_time", "bikeid", "tripduration"]
    
    test_df = spark_session.createDataFrame(test_data, columns)
    test_result = average_trip_duration_per_day(test_df)
    # Assert that the result is as expected based on the test data
    assert test_result.collect() == [('2023-10-01', 2250.0), ('2023-10-02', 2700.0)]

# Add more test cases for other functions as needed
