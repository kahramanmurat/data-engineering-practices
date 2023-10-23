import os
import requests
import zipfile
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiohttp
import unittest


download_urls = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

# Create a 'downloads' directory if it doesn't exist
download_dir = "downloads"
os.makedirs(download_dir, exist_ok=True)

# Function to download and unzip a file
async def download_and_unzip(url):
    try:
        # Extract the filename from the URL
        filename = url.split("/")[-1]
        filepath = os.path.join(download_dir, filename)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    file_data = await response.read()

                    # Download the file
                    with open(filepath, "wb") as file:
                        file.write(file_data)

                    # Unzip the file and delete the zip
                    with zipfile.ZipFile(filepath, "r") as zip_ref:
                        zip_ref.extractall(download_dir)
                    os.remove(filepath)

                    print(f"Downloaded and unzipped: {filename}")
                else:
                    print(f"Failed to download: {filename}")
    except Exception as e:
        print(f"Error downloading/unzipping: {str(e)}")


async def download_files_async(urls):
    tasks = [download_and_unzip(url) for url in urls]
    await asyncio.gather(*tasks)


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_files_async(download_urls))


if __name__ == "__main__":
    main()
