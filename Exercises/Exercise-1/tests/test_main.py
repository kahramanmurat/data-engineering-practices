import os
import pytest
from main import download_and_unzip


def test_download_and_unzip():
    # Write your test cases using pytest
    # Example:
    download_and_unzip(
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip"
    )
    download_and_unzip(
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip"
    )

    download_and_unzip(
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip"
    )
    # Add assertions to check the expected behavior

    # Check if the file exists after downloading and unzipping
    assert os.path.exists("downloads/Divvy_Trips_2018_Q4.csv")
    assert os.path.exists("downloads/Divvy_Trips_2220_Q1.csv")
    assert os.path.exists("downloads/Divvy_Trips_2019_Q4.csv")
    # Add more test cases as needed


if __name__ == "__main__":
    pytest.main([__file__])
