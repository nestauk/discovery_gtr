"""Workflow to fetch a paginated bulk data resource from GtR API, and save it to S3.
This script is designed to be run as a GitHub Action. It can also be run locally.
This workflow:
- Calls the GtR API to get the total number of pages for a given endpoint.
- Calls the GtR API to get the data for each page.
- Saves the data to S3.

Usage:
    python gtr_to_s3.py
"""
import requests
import json
import logging
from io import BytesIO
import datetime
import boto3
import os
from dotenv import load_dotenv
import io


# Set up environment variables, either from .env file or GitHub secrets. This includes:
# - AWS_ACCESS_KEY: the AWS access key for S3
# - AWS_SECRET_KEY: the AWS secret key for S3
# - MY_BUCKET_NAME: the name of the S3 bucket
# - DESTINATION_S3_PATH: the path to the S3 destination folder

# Check if running in GitHub Actions
if os.getenv("CI") == "true":
    pass
else:
    # If running locally, load environment variables from .env file
    load_dotenv()

# Retrieve AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
# Retrieve S3 bucket name and destination path from environment variables
MY_BUCKET_NAME = os.getenv("MY_BUCKET_NAME")
DESTINATION_S3_PATH = os.getenv("DESTINATION_S3_PATH")


# Set up logging and set desired logging level
logging.basicConfig(level=logging.INFO)

# Define the API endpoints
ENDPOINTS = [
    "funds",
    "organisations",
    "outcomes",
    "outcomes/keyfindings",
    "outcomes/impactsummaries",
    "outcomes/publications",
    "outcomes/collaborations",
    "outcomes/intellectualproperties",
    "outcomes/policyinfluences",
    "outcomes/products",
    "outcomes/researchmaterials",
    "outcomes/spinouts",
    "outcomes/furtherfundings",
    "outcomes/disseminations",
    "persons",
    "projects",
]

# Define the API URL
BASE_URL = "https://gtr.ukri.org/gtr/api/"

# Initialize the S3 client with credentials
S3 = boto3.client(
    "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
)


def main_request(base_url: str, endpoint: str, page_parameter: str = ""):
    """Call GtR API to get a response object.
    Args:
        url (str): The base URL for the API.
        endpoint (str): The endpoint to call.
        Returns:
            A response object."""
    return requests.get(
        base_url + endpoint + "?s=100" + page_parameter,
        headers={"Accept": "application/vnd.rcuk.gtr.json-v7"},
    )


def get_total_pages(response) -> int:
    """Get the total number of pages from the response object.
    Args:
        response: The response object.
    Returns:
        The total number of pages."""
    # Parse the JSON response
    json_response = json.loads(response)

    # Extract the total number of pages
    total_pages = json_response.get("totalPages", 0)

    return int(total_pages)


def get_page_range(total_pages: int) -> range:
    """Get a range of integers from 1 to the total number of pages.
    Args:
        total_pages (int): The total number of pages.
    Returns:
        A range of integers."""
    return range(1, total_pages + 1)


def content_type(response, endpoint) -> None:
    """
    Prints the content type for each endpoint.
    Args:
        response: The response object.
    Returns:
        None
    """
    # Check the content type
    content_type = response.headers["content-type"]
    # Print
    logging.info(f"{content_type}: Content Type for {endpoint}")


def get_s3_key(name, destination_path) -> str:
    """
    Generates the S3 key for the file.
    Args:
        name: The name of the file.
        destination_path: The path to the S3 destination folder.
    Returns:
        str: The S3 key for the given file.
    """
    # Define the timestamp
    now = datetime.datetime.now()
    # Format timestamp as YYYYMMDD_HHMMSS
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    # Return the S3 key with the directory and file name
    return f"{destination_path}GtR_{timestamp}/gtr_{name}.csv"


def upload_data_to_s3(data, s3_client, bucket_name, s3_key) -> None:
    """Upload data to a JSON file stored on S3.
    Args:
        data (list): The list of data to upload.
        s3_client (str): The S3 client.
        bucket_name (str): The name of the bucket.
        s3_key (str): The S3 key.
    Returns:
        None"""
    # Serialize the data to JSON
    json_data = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    # Upload JSON content to S3 using s3.put_object()
    s3_client.put_object(Body=json_data.encode("utf-8"), Bucket=bucket_name, Key=s3_key)


def gtr_to_s3(endpoint: str) -> None:
    """Fetches a paginated bulk data resource from GtR API, and saves it to S3.
    Args:
        endpoint (str): The endpoint to call.
    Returns:
        None"""
    # Get the total number of pages
    r = main_request(BASE_URL, endpoint)
    total_pages = get_total_pages(r.content)
    logging.info(f"{total_pages}: Total number of pages for {endpoint}")
    # Get the page range
    range = get_page_range(total_pages)

    # Get S3 key
    s3_key = get_s3_key(endpoint, DESTINATION_S3_PATH)

    # Accumulate data for all pages
    all_data = []

    for page_no in range:
        # Fetch the page
        r = main_request(BASE_URL, endpoint, f"&p={page_no}")
        # Accumulate data
        all_data.extend(r.json())

        # Log the percentage of completion when it reaches a whole number
        percentage_complete = (page_no / total_pages) * 100
        if percentage_complete.is_integer():
            logging.info(f"{endpoint}: Downloaded {percentage_complete:.0f}%")

    # Upload all data to S3 using s3.put_object()
    upload_data_to_s3(all_data, S3, MY_BUCKET_NAME, s3_key)


# Run the workflow
if __name__ == "__main__":
    gtr_to_s3(ENDPOINTS[0])


# Old code


def append_to_s3(data, s3_client, bucket_name, s3_key) -> None:
    """Append data to a JSON file stored on S3.
    Args:
        data (json): The data to append.
        s3_client (str): The S3 client.
        bucket_name (str): The name of the bucket.
        s3_key (str): The S3 key.
    Returns:
        None"""
    # Create a StringIO object to store JSON content as a string
    json_content_buffer = io.StringIO()

    # Check if the file already exists on S3
    try:
        # Download existing JSON content from S3
        existing_content = (
            s3_client.get_object(Bucket=bucket_name, Key=s3_key)["Body"]
            .read()
            .decode("utf-8")
        )
        existing_data = json.loads(existing_content)
    except s3_client.exceptions.NoSuchKey:
        # If the key does not exist, create an empty list
        existing_data = []

    # Append the new data to the existing data
    existing_data.extend(data)

    # Serialize the updated data to JSON directly to the buffer
    json.dump(
        existing_data,
        json_content_buffer,
        ensure_ascii=False,
        indent=None,
        separators=(",", ":"),
    )

    # Seek to the beginning of the buffer
    json_content_buffer.seek(0)

    # Upload JSON content to S3 using s3.put_object()
    s3_client.put_object(
        Body=json_content_buffer.read().encode("utf-8"), Bucket=bucket_name, Key=s3_key
    )


def gtr_to_s3_paginatedupload(endpoint: str) -> None:
    """Fetches a paginated bulk data resource from GtR API, and saves it to S3.
    Args:
        endpoint (str): The endpoint to call.
    Returns:
        None"""
    # Get the total number of pages
    r = main_request(BASE_URL, endpoint)
    total_pages = get_total_pages(r.content)
    logging.info(f"{total_pages}: Total number of pages for {endpoint}")
    # Get the page range
    range = get_page_range(total_pages)

    # Get S3 key
    s3_key = get_s3_key(endpoint, DESTINATION_S3_PATH)

    for page_no in range:
        # Fetch the page
        r = main_request(BASE_URL, endpoint, f"&p={page_no}")
        # Upload file to S3 using s3.put_object()
        append_to_s3(r.json(), S3, MY_BUCKET_NAME, s3_key)
