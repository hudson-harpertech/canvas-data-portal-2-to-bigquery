# Data Acquisition and Processing (DAP) to BigQuery

## Description

This repository contains a Python script (`main.py`) for interfacing with the DAP (Data Acquisition and Processing) system, downloading data, and loading it into Google BigQuery. It's designed for working with Canvas LMS data. The script handles tasks such as running DAP commands, downloading table data and schema, loading data into BigQuery, and updating table schemas based on JSON definitions.

## Requirements

-   Python 3.8+
-   Google Cloud Platform account with BigQuery access
-   Access to Canvas LMS Data (for DAP commands)

## Setup

1. **Clone the Repository:**

```sh
git clone [repository-url]
cd [repository-name]
```

2. **Configure Environment Variables:**
   Set the following environment variables:

-   `API_KEY`: Client ID for the DAP API.
-   `API_SECRET`: Client Secret for the DAP API.
-   `PROJECT`: Your GCP project ID.
-   `DATASET`: The BigQuery dataset to use.

3. **Install Dependencies:**

```sh
pip install -r requirements.txt
```

## Docker Usage

This project includes a `Dockerfile` for containerization.

1. **Build the Docker Image:**

```sh
docker build -t dap-bigquery .
```

2. **Run the Docker Container:**

```sh
docker run -e API_KEY=[your-api-key] -e API_SECRET=[your-api-secret] -e PROJECT=[gcp-project] -e DATASET=[bigquery-dataset] dap-bigquery
```

## Usage

To run the script manually without Docker:

```sh
python3 main.py
```

## Features

-   **List Tables:** Lists all tables available in CanvasData.
-   **Download Table Data:** Downloads data for specified tables in Parquet format.
-   **Load Data to BigQuery:** Automatically loads downloaded data to a specified BigQuery table.
-   **Schema Management:** Downloads and updates BigQuery table schemas based on JSON definitions.

## Contributing

Contributions to this project are welcome. Please follow the standard fork and pull request workflow.

# Deploying to Google Cloud Run

This guide explains how to deploy the `canvas-data-portal-2-sync` application to Google Cloud Run. This setup assumes you have a Google Cloud project with BigQuery set up and a dataset for storing data from Canvas Data Portal 2.

## Prerequisites

-   A Google Cloud account with an active project.
-   BigQuery setup within your Google Cloud project.
-   Google Cloud Artifact Registry and Secret Manager enabled.
-   `gcloud` CLI and Docker installed on your machine.

## Steps

### 1. Setup Google Cloud Environment

-   **Google Cloud Project:** Ensure you have a Google Cloud project created with BigQuery setup.
-   **Canvas Data Portal 2 Documentation:** Refer to [Canvas Data Portal 2 documentation](https://community.canvaslms.com/t5/Admin-Guide/What-is-Canvas-Data-2/ta-p/560956) for general information and setup.

### 2. Create Repository in Artifact Registry

-   Navigate to Google Cloud Console.
-   Go to Artifact Registry and create a new repository for storing Docker images.

### 3. Configure Secret Manager

-   Generate API key and secret in Canvas Data Portal 2.
-   Create secrets in Google Cloud Secret Manager for the API key and secret.

### 4. Authentication

Authenticate your Google Cloud account and configure Docker to push images to Google Cloud Artifact Registry:

```sh
gcloud auth login
gcloud auth print-access-token | sudo docker login -u oauth2accesstoken --password-stdin https://[REGION]-docker.pkg.dev
```

Replace `[REGION]` with your Google Cloud region.

### 5. Build and Push Docker Image

-   **Build Docker Image:**

```sh
sudo docker build -t [image-name] .
```

Replace `[image-name]` with your chosen image name.

-   **Tag the Docker Image:**

```sh
sudo docker tag [image-name] [REGION]-docker.pkg.dev/[project-id]/[repository-name]/[image-name]:latest
```

Replace placeholders with your region, project ID, repository name, and image name.

-   **Push to Google Cloud Artifact Registry:**

```sh
sudo docker push [REGION]-docker.pkg.dev/[project-id]/[repository-name]/[image-name]:latest
```

### 6. Deploy to Google Cloud Run

-   Go to the Google Cloud Run console.
-   Create a new service.
-   Select the pushed Docker image.
-   Configure the service with the necessary environment variables.
-   Set the memory allocation to 8 GB, timeout to 1000 minutes,
