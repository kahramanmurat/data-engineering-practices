import requests
import os
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
time_stamp = "2022-02-07 14:03"

# Create a directory to store downloaded files
download_directory = "downloaded_files"
os.makedirs(download_directory, exist_ok=True)


def download_files_with_timestamp(url, time_stamp):
    # Send an HTTP GET request to the URL
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table containing the file information
        table = soup.find("table")

        if table:
            # Find the headers of the table (column names)
            headers = [header.text for header in table.find_all("th")]

            # Initialize variables to store the highest temperature
            max_temperature = float("-inf")
            records_with_max_temperature = []

            # Initialize empty lists to store timestamps and corresponding file links
            timestamps = []
            file_links = []

            # Find the rows in the table
            rows = table.find_all("tr")[3:-1]  # Skip the first row (headers)

            # Loop through the rows to extract data
            for row in rows:
                columns = row.find_all("td")

                # Extract data from the columns
                column_data = [column.text.strip() for column in columns]

                if time_stamp in column_data[headers.index("Last modified")]:
                    timestamps.append(column_data[headers.index("Last modified")])

                    file_link = columns[headers.index("Name")].find("a")["href"]
                    file_links.append(file_link)

                    if file_link:
                        download_link = url + file_link

                    # Extract the filename from the link
                    filename = file_link.split("/")[-1]

                    # Download the file
                    file_response = requests.get(download_link)
                    if file_response.status_code == 200:
                        file_path = os.path.join(download_directory, filename)
                        with open(file_path, "wb") as file:
                            file.write(file_response.content)
                        print(
                            f"Downloaded: {filename} Timestamp: {column_data[headers.index('Last modified')]}"
                        )
                        # Load the CSV file using Pandas
                        df = pd.read_csv(file_path, low_memory=False)

                        # Convert the 'HourlyDryBulbTemperature' column to numeric
                        df["HourlyDryBulbTemperature"] = pd.to_numeric(
                            df["HourlyDryBulbTemperature"], errors="coerce"
                        )

                        # Find the maximum temperature
                        max_temp_in_file = df["HourlyDryBulbTemperature"].max()
                        if not pd.isna(max_temp_in_file):
                            records_with_max_temperature = df[
                                df["HourlyDryBulbTemperature"] == max_temp_in_file
                            ]
                        else:
                            records_with_max_temperature = pd.DataFrame()

                        # Print records with the highest temperature
                        print("Records with the highest HourlyDryBulbTemperature:")
                        print(records_with_max_temperature)
        else:
            print("Table not found on the webpage.")
    else:
        print("Failed to retrieve the webpage.")


# def merge_csv_files(directory):
#     # List all CSV files in the directory
#     csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]

#     if len(csv_files) == 0:
#         print("No CSV files found for merging.")
#         return

#     # Initialize an empty DataFrame to store the merged data
#     dataframes = []

#     # Loop through each CSV file and merge them
#     for csv_file in csv_files:
#         file_path = os.path.join(directory, csv_file)
#         df = pd.read_csv(file_path, low_memory=False)
#         dataframes.append(df)

#     # Concatenate all DataFrames
#     merged_data = pd.concat(dataframes, ignore_index=True)

#     # Save the merged data to a new CSV file
#     merged_csv_file = "merged_data.csv"
#     merged_data.to_csv(merged_csv_file, index=False)
#     print(f"Merged data saved to {merged_csv_file}")

#     # Optionally, you can remove the individual CSV files if you no longer need them
#     for csv_file in csv_files:
#         file_path = os.path.join(directory, csv_file)
#         os.remove(file_path)


def main():
    download_files_with_timestamp(url, time_stamp)
    # merge_csv_files(download_directory)


if __name__ == "__main__":
    main()
