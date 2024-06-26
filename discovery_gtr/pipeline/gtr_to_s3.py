"""Workflow to fetch a paginated bulk data resource from GtR API, and save it to S3.
This script is designed to be run as a GitHub Action. It can also be run locally.
This workflow:
- Calls the GtR API to get the total number of pages for a given endpoint.
- Calls the GtR API to get the data for each page.
- Saves the data to S3.

This workflow works on GitHub Actions and locally.
On GitHub Actions:
- Each instance of the workflow fetches a different endpoint. The ENDPOINT environment
variable is set to the endpoint to fetch.
- The AWS_ACCESS_KEY, AWS_SECRET_KEY, MY_BUCKET_NAME and DESTINATION_S3_PATH environment
variables are set in GitHub Secrets.
Locally:
- The ENDPOINTS environment variable is set to a list of endpoints to fetch one after the other.
- The AWS_ACCESS_KEY, AWS_SECRET_KEY, MY_BUCKET_NAME and DESTINATION_S3_PATH environment
variables are set in a .env file.

Also, an identifier (key_to_extract) is generated to extract the relevant data from the JSON response.
For the currently used endpoints (funds, projects, organisations, persons), the identifier is created
by removing the last letter from the endpoint name. For example, the identifier for the "funds" endpoint
is "fund", and the identifier for the "projects" endpoint is "project".

Usage:
    python gtr_to_s3.py
"""
import requests
import json
import logging
import datetime
import boto3
import os
from dotenv import load_dotenv
import math
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# Set up environment variables, either from .env file or GitHub secrets. This includes:
# - AWS_ACCESS_KEY: the AWS access key for S3
# - AWS_SECRET_KEY: the AWS secret key for S3
# - MY_BUCKET_NAME: the name of the S3 bucket
# - DESTINATION_S3_PATH: the path to the S3 destination folder
# - ENDPOINT / ENDPOINTS: the endpoint(s) to call

# Check if running in GitHub Actions
if os.getenv("CI") == "true":
    # Retrieve the endpoint from the environment variable
    ENDPOINT = os.getenv("ENDPOINT")
else:
    # If running locally, load environment variables from .env file
    load_dotenv()
    # Retrieve the endpoints from the environment variable
    ENDPOINTS = json.loads(os.getenv("ENDPOINTS"))

# Retrieve AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
# Retrieve S3 bucket name and destination path from environment variables
MY_BUCKET_NAME = os.getenv("MY_BUCKET_NAME")
DESTINATION_S3_PATH = os.getenv("DESTINATION_S3_PATH")


# Define the API URL
BASE_URL = "https://gtr.ukri.org/gtr/api/"


# Initialize the S3 client with credentials
S3 = boto3.client(
    "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
)


# Generate timestamp
now = datetime.datetime.now()
TIMESTAMP = now.strftime("%Y%m%d")


# Define endpoint headers based on the structure
ENDPOINT_HEADERS = {
    "funds": [
        "end",
        "id",
        "start",
        "category",
        "rel",
        "amount",
        "currencyCode",
        "project_id",
    ],
    "persons": [
        "id",
        "firstName",
        "surname",
        "rel",
        "project_id",
        "otherNames",
    ],
    "organisations": [
        "id",
        "name",
        "addresses",
    ],
    "projects": [
        "id",
        "name",
        "addresses",
    ],
}


# Set up logging and set desired logging level
logging.basicConfig(level=logging.INFO)

# Log the timestamp for debugging purposes
logging.info(f"TIMESTAMP variable is: {TIMESTAMP}; type: {type(TIMESTAMP)}")


def main_request(
    base_url: str, endpoint: str, page_parameter: str = "", max_retries: int = 3
):
    """Call GtR API to get a response object.
    Args:
        url (str): The base URL for the API.
        endpoint (str): The endpoint to call.
        max_retries (int): Maximum number of retries in case of failure.
        Returns:
            A response object or None if unsuccessful after retries."""

    full_url = base_url + endpoint + "?s=100" + page_parameter

    # Create a session with retry configuration
    session = requests.Session()
    retries = Retry(
        total=max_retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    for attempt in range(max_retries + 1):
        try:
            response = session.get(
                full_url,
                headers={"Accept": "application/vnd.rcuk.gtr.json-v7"},
            )
            # Check if the response is successful (status code 2xx)
            if response.ok:
                return response
            else:
                response.raise_for_status()  # Raise an exception for non-successful responses
        except requests.RequestException as e:
            if attempt < max_retries:
                # Retry if it's not the last attempt
                logging.info(f"Attempt {attempt + 1} failed. Retrying...")
            else:
                logging.info(f"All attempts failed. Exception: {e}")
                return None  # All attempts failed, return None


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


def get_s3_key(name: str, destination_path: str, timestamp: str) -> str:
    """
    Generates the S3 key for the file.
    Args:
        name: The name of the file.
        destination_path: The path to the S3 destination folder.
    Returns:
        str: The S3 key for the given file.
    """
    logging.info("Generating S3 key for the file")
    return f"{destination_path}GtR_{timestamp}/gtr_{name}.json"


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
    s3_client.put_object(
        Body=json_data.encode("utf-8"),
        Bucket=bucket_name,
        Key=s3_key,
        ContentType="application/json",
    )


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


def save_data_locally(data: list, file_name: str) -> None:
    """Save data to a JSON file locally.
    Args:
        data (list): The list of data to save.
        file_name (str): The name of the JSON file.
    Returns:
        None"""
    logging.info(f"Saving data locally: {file_name}")
    with open(file_name, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, separators=(",", ":"))


def gtr_to_s3(endpoint: str) -> None:
    """Fetches data from a few pages of a GtR API endpoint and saves it locally as JSON.
    Args:
        endpoint (str): The endpoint to call.
    Returns:
        None"""
    logging.info(f"Starting workflow for endpoint: {endpoint}")
    # Get the total number of pages
    r = main_request(BASE_URL, endpoint)
    total_pages = get_total_pages(r.content)
    logging.info(f"{total_pages}: Total number of pages for {endpoint}")

    # Get S3 key
    s3_key = get_s3_key(endpoint, DESTINATION_S3_PATH, TIMESTAMP)
    logging.info(f"S3 key: {s3_key}")

    # Define the maximum number of pages to append
    # Change this to fetch more or fewer pages for testing
    max_pages_to_append = total_pages

    # Initialize the previously logged percentage
    prev_percentage = None

    # Accumulate data for the specified number of pages
    all_data = []

    # Access the corresponding headers list based on the endpoint
    if endpoint in ENDPOINT_HEADERS:
        headers = ENDPOINT_HEADERS[endpoint]
        print(f"Headers for {endpoint}: {headers}")
    else:
        print(f"No headers found for {endpoint}")

    for page_no in range(1, max_pages_to_append + 1):
        # Fetch the page
        r = main_request(BASE_URL, endpoint, f"&p={page_no}")

        # Parse the JSON response
        response_data = json.loads(r.text)

        # Initialize an empty list to store the extracted data
        extracted_data = []

        # Loop through the response_data
        for item in response_data:
            # Initialize an empty dictionary to store the extracted data for each item
            extracted_item = {}

            # Loop through the headers and extract data for each header
            for header in headers:
                extracted_item[header] = item.get(header)

            # Append the extracted item to the list
            extracted_data.append(extracted_item)

        # Extend the all_data list with the extracted data
        all_data.extend(extracted_data)

        # Log the percentage of completion
        prev_percentage = log_percentage_complete(
            page_no, total_pages, prev_percentage, endpoint
        )

    # Upload all data to S3 using s3.put_object()
    upload_data_to_s3(all_data, S3, MY_BUCKET_NAME, s3_key)

    # Save all data to a file locally as JSON
    # save_data_locally(all_data, f"{endpoint}.json")


def local_wrapper():
    """Wrapper function for local execution."""
    if ENDPOINTS:
        logging.info("local_wrapper used")
        for endpoint in ENDPOINTS:
            gtr_to_s3(endpoint)
    else:
        logging.error(
            "Endpoints not specified. Please set the ENDPOINTS environment variable."
        )


def github_wrapper():
    """Wrapper function for GitHub Actions execution."""
    if ENDPOINT:
        logging.info("github_wrapper used")
        gtr_to_s3(ENDPOINT)
    else:
        logging.error(
            "Endpoint not specified. Please set the ENDPOINT environment variable."
        )


# Run the workflow
if __name__ == "__main__":
    logging.info("Script execution started")
    # Check if running in GitHub Actions
    if os.getenv("CI") == "true":
        github_wrapper()
    else:
        local_wrapper()
    logging.info("Script execution completed")
