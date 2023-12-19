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
import sys
import math


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
# Retrieve the endpoint from the environment variable
ENDPOINTS = os.getenv("ENDPOINTS")


# Define the API URL
BASE_URL = "https://gtr.ukri.org/gtr/api/"


# Initialize the S3 client with credentials
S3 = boto3.client(
    "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
)


# Set up logging and set desired logging level
logging.basicConfig(level=logging.INFO)


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
    logging.info("Parsing total number of pages from API response")
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
    logging.info(f"Generating page range from 1 to {total_pages}")
    return range(1, total_pages + 1)


def content_type(response, endpoint: str) -> None:  # Not used
    """
    Prints the content type for each endpoint.
    Args:
        response: The response object.
    Returns:
        None
    """
    logging.info(f"Checking content type for endpoint: {endpoint}")
    content_type = response.headers["content-type"]
    # Print
    logging.info(f"{content_type}: Content Type for {endpoint}")


def get_s3_key(name: str, destination_path: str) -> str:
    """
    Generates the S3 key for the file.
    Args:
        name: The name of the file.
        destination_path: The path to the S3 destination folder.
    Returns:
        str: The S3 key for the given file.
    """
    logging.info("Generating S3 key for the file")
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    return f"{destination_path}GtR_{timestamp}/gtr_{name}.csv"


def upload_data_to_s3(
    data: list, s3_client: str, bucket_name: str, s3_key: str
) -> None:
    """Upload data to a JSON file stored on S3.
    Args:
        data (list): The list of data to upload.
        s3_client (str): The S3 client.
        bucket_name (str): The name of the bucket.
        s3_key (str): The S3 key.
    Returns:
        None"""
    logging.info(f"Uploading data to S3: {s3_key}")
    json_data = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    s3_client.put_object(Body=json_data.encode("utf-8"), Bucket=bucket_name, Key=s3_key)


def log_percentage_complete(
    page_no: int, total_pages: int, prev_percentage: int, endpoint: str
) -> int:
    """Logs the percentage of completion.
    Args:
        page_no (int): The current page number.
        total_pages (int): The total number of pages.
        prev_percentage (int): The previously logged percentage.
    Returns:
        int: The updated previously logged percentage."""
    # Log the percentage of completion when it reaches a milestone
    percentage_complete = (page_no / total_pages) * 100
    rounded_percentage = math.floor(percentage_complete)

    # Check if the rounded percentage has changed
    if rounded_percentage != prev_percentage:
        logging.info(f"{endpoint}: Downloaded {rounded_percentage}%")

    return rounded_percentage


def gtr_to_s3(endpoint: str) -> None:
    """Fetches a paginated bulk data resource from GtR API, and saves it to S3.
    Args:
        endpoint (str): The endpoint to call.
    Returns:
        None"""
    logging.info(f"Starting workflow for endpoint: {endpoint}")
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

    # Initialize the previously logged percentage
    prev_percentage = None

    for page_no in range:
        # Fetch the page
        r = main_request(BASE_URL, endpoint, f"&p={page_no}")
        # Accumulate data
        all_data.extend(r.json())

        # Log the percentage of completion
        prev_percentage = log_percentage_complete(
            page_no, total_pages, prev_percentage, endpoint
        )

    # Upload all data to S3 using s3.put_object()
    upload_data_to_s3(all_data, S3, MY_BUCKET_NAME, s3_key)


# Run the workflow
if __name__ == "__main__":
    logging.info("Script execution started")
    if ENDPOINTS:
        gtr_to_s3(ENDPOINTS)
    else:
        logging.error(
            "Endpoint not specified. Please set the ENDPOINTS environment variable."
        )
    logging.info("Script execution completed")
