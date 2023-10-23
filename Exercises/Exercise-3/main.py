import boto3
import botocore
import gzip
import io
import time
import os


def download_file_with_retry(s3, bucket_name, object_key, max_retries=5):
    delay = 1

    for retry in range(max_retries):
        try:
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            return response["Body"]
        except botocore.exceptions.ClientError as e:
            if "SlowDown" in str(e):
                print(f"Received SlowDown error, retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"Error downloading the file: {e}")
                return None

    print("Max retries reached. Download failed.")
    return None


def get_first_uri(file_stream):
    with gzip.GzipFile(fileobj=file_stream) as gz_file:
        try:
            # Read the first 100 bytes to skip metadata
            metadata = gz_file.read(100)
            gz_file = io.BytesIO(metadata + gz_file.read())
            first_line = gz_file.readline()
            uri = first_line.decode("utf-8").strip()
            print(uri)
        except OSError as e:
            print(f"Error reading Gzip file: {e}")
            return None

        return uri


def stream_file_lines(file_stream):
    with gzip.GzipFile(fileobj=file_stream) as gz_file:
        for line in gz_file:
            yield line.decode("utf-8")


def main():

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )
    bucket_name = "commoncrawl"
    object_key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    file_stream = download_file_with_retry(s3, bucket_name, object_key)
    if file_stream:
        uri = get_first_uri(file_stream)
        if uri:
            uri_file_stream = download_file_with_retry(s3, bucket_name, uri)
            if uri_file_stream:
                for line in stream_file_lines(uri_file_stream):
                    print(line, end="")


if __name__ == "__main__":
    main()
