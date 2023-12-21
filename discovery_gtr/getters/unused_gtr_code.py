import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


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
                print(f"Attempt {attempt + 1} failed. Retrying...")
            else:
                print(f"All attempts failed. Exception: {e}")
                return None  # All attempts failed, return None


ENDPOINT = "organisations"

# Define the API URL
BASE_URL = "https://gtr.ukri.org/gtr/api/"

r = main_request(BASE_URL, ENDPOINT, "&p=1")

print(r.content)


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
