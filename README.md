# discovery_gtr: GtR to S3 Pipeline

## :wave: About

The GtR to S3 Pipeline is a workflow designed to fetch paginated bulk data resources from the GtR (Gateway to Research) API and save them to an AWS S3 bucket. This script is designed to be run as a GitHub Action, but it can also be executed locally.

This is an experimental script that should be used with caution, as it can use up Nesta's GitHub Actions Minutes.

## Workflow Overview

The pipeline performs the following steps:

1. Calls the GtR API to get the total number of pages for a specified endpoint.
2. Calls the GtR API to fetch data for each page.
3. Saves the fetched data to an AWS S3 bucket.

## Usage

### GitHub Action

When using the pipeline as a GitHub Action, each instance of the workflow fetches data from a specific endpoint. The `ENDPOINT` environment variable is set to specify the endpoint to fetch. Additionally, the following environment variables should be set as GitHub Secrets:

- `AWS_ACCESS_KEY`: AWS access key for S3.
- `AWS_SECRET_KEY`: AWS secret key for S3.
- `MY_BUCKET_NAME`: Name of the S3 bucket to store the data.
- `DESTINATION_S3_PATH`: Path to the S3 destination folder.
- `ENDPOINT`: The endpoint to fetch.

To trigger the GitHub Action, you can include it in your GitHub Actions workflow configuration file.

To activate the GitHub Action, the path of the `.github/main.yaml` file needs to be changed to `.github/workflows/main.yaml`. It is currently the former to disable it for now.

### Local Execution

To run the pipeline locally, you need to set the following environment variables in a `.env` file:

- `AWS_ACCESS_KEY`: AWS access key for S3.
- `AWS_SECRET_KEY`: AWS secret key for S3.
- `MY_BUCKET_NAME`: Name of the S3 bucket to store the data.
- `DESTINATION_S3_PATH`: Path to the S3 destination folder.
- `ENDPOINTS`: A list of endpoints to fetch data from.

You can execute the pipeline locally using the command:

`bash`
`python gtr_to_s3.py`

## Data Extraction

The pipeline extracts relevant data from the GtR API response using an identifier (`key_to_extract`). The identifier is generated based on the endpoint being fetched. Here's how the identifier is determined for commonly used endpoints:

- **Funds Endpoint:** The identifier is set to "fund."
- **Projects Endpoint:** The identifier is set to "project."
- **Organisations Endpoint:** The identifier is set to "organisation."
- **Persons Endpoint:** The identifier is set to "person."

For each endpoint, the pipeline extracts data by iterating through the API response, mapping specific headers to the extracted data, and creating a list of dictionaries, where each dictionary represents a single data entry.

The pipeline is designed to accommodate variations in data structure for different endpoints, allowing for flexible data extraction.

### Customization

If you need to customize data extraction for a specific endpoint or handle unique data structures, you can modify the `ENDPOINT_HEADERS` dictionary within the script. This dictionary maps endpoint names to lists of headers to extract. Adjusting the headers allows you to control which data fields are extracted for each endpoint.

# Example: Customized headers for a hypothetical 'custom' endpoint

ENDPOINT_HEADERS = {
"custom": [
"field1",
"field2",
"field3",

# Add additional headers as needed

],
}

## Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter/quickstart), in brief:
  - Install: `direnv` and `conda`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure `pre-commit`

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>
