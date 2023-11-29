import subprocess
import re
import os
import logging
import glob
import json
import shutil
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

logging.info("Starting DAP to BigQuery")

def run_dap_command(command, namespace="canvas"):
    """
    Runs a DAP command using the specified command, base URL, client ID, client secret, and namespace.

    Args:
        command (str): The DAP command to run.
        namespace (str, optional): The namespace to use for the command. Defaults to "canvas".

    Returns:
        str: The output of the DAP command.

    Raises:
        subprocess.CalledProcessError: If the DAP command fails to run.
    """
    try:
        base_url = "https://api-gateway.instructure.com"
        client_id = os.getenv("API_KEY")
        client_secret = os.getenv("API_SECRET")

        result = subprocess.run(f"dap --base-url {base_url} --client-id {client_id} --client-secret {client_secret} {command} --namespace {namespace}", 
                                check=True, shell=True, text=True, capture_output=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running command '{command}': {e.output}")
        return None

def list_tables():
    """
    Returns a list of tables by running the 'list' command.
    """
    return run_dap_command("list").split()

def download_table_data(table_name):
    """
    Downloads the data for a given table name in the CanvasData system.

    Args:
        table_name (str): The name of the table to download data from.

    Returns:
        str: The command to run for downloading the table data in parquet format.
    """
    return run_dap_command(f"snapshot --table {table_name} --format parquet")

def download_incremental_table_data(table_name, since_datetime):
    """
    Downloads incremental table data in parquet format since the specified datetime.

    Args:
        table_name (str): The name of the table to download data from.
        since_datetime (str): The datetime from which to start downloading data.

    Returns:
        str: The result of the DAP command for downloading the data.
    """
    return run_dap_command(f"incremental --table {table_name} --format parquet --since {since_datetime}")

def download_table_schema(table_name):
    """
    Downloads the schema for a given table.

    Args:
        table_name (str): The name of the table.

    Returns:
        str: The schema of the table.
    """
    return run_dap_command(f"schema --table {table_name}")

def get_job_id():
    """
    Get the job ID of the latest directory in the 'downloads' folder.

    Returns:
        str or None: The job ID of the latest directory, or None if no directories exist.
    """
    dirs = next(os.walk('downloads'))[1]
    if dirs:
        return dirs[0]  # Assuming the latest directory is the relevant one
    return None

def load_json_schema(file_path):
    """
    Load a JSON schema from the given file path.

    Args:
        file_path (str): The path to the JSON schema file.

    Returns:
        dict: The loaded JSON schema.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
    """
    with open(file_path, 'r') as file:
        return json.load(file)

def schema_field_to_dict(field):
    """
    Convert a SchemaField to a dictionary.

    Args:
        field (SchemaField): The SchemaField object to convert.

    Returns:
        dict: A dictionary representation of the SchemaField.
    """
    result = {
        "name": field.name,
        "type": field.field_type,
        "mode": field.mode,
        "description": field.description,
    }
    if field.field_type == 'RECORD':
        result["fields"] = [schema_field_to_dict(f) for f in field.fields]
    return result

def update_schema_description(bq_schema_fields, json_schema):
    """
    Update the description of BigQuery schema fields based on a JSON schema.

    Args:
        bq_schema_fields (List[bigquery.SchemaField]): List of BigQuery schema fields.
        json_schema (dict): JSON schema containing field descriptions.

    Returns:
        List[bigquery.SchemaField]: Updated list of BigQuery schema fields with updated descriptions.
    """
    updated_fields = []
    max_description_length = 1024

    for field in bq_schema_fields:
        json_field = json_schema.get('properties', {}).get(field.name)
        description = field.description

        if json_field:
            if 'description' in json_field:
                # Truncate the description if it's too long
                description = json_field['description'][:max_description_length]

            if field.field_type == 'RECORD':
                nested_fields = update_schema_description(field.fields, json_field) if field.fields else []
                new_field = bigquery.SchemaField(
                    name=field.name,
                    field_type=field.field_type,
                    mode=field.mode,
                    description=description,
                    fields=nested_fields
                )
            else:
                new_field = bigquery.SchemaField(
                    name=field.name,
                    field_type=field.field_type,
                    mode=field.mode,
                    description=description
                )
        else:
            # If json_field is None, use the original field
            new_field = field

        updated_fields.append(new_field)

    return updated_fields

def update_bigquery_schema_from_json(client, table_id, json_schema_file):
    """
    Updates the schema of a BigQuery table based on a JSON schema file.

    Args:
        client (BigQueryClient): The BigQuery client object.
        table_id (str): The ID of the table to update.
        json_schema_file (str): The path to the JSON schema file.

    Returns:
        None
    """
    # Load JSON schema
    json_schema = load_json_schema(json_schema_file)

    # Retrieve the current schema from BigQuery
    table = client.get_table(table_id)
    bq_schema = table.schema

    # Update the schema descriptions
    updated_schema = update_schema_description(bq_schema, json_schema['schema'])

    # Update the table with the new schema
    table.schema = updated_schema
    client.update_table(table, ['schema'])

def get_latest_schema_file(table_name, directory="downloads"):
    """
    Get the latest schema file for a given table name.

    Args:
        table_name (str): The name of the table.
        directory (str, optional): The directory where the schema files are located. Defaults to "downloads".

    Returns:
        str: The path to the latest schema file, or None if no matching file is found.
    """
    schema_file_pattern = re.compile(rf"{table_name}_schema_version_(\d+)\.json")
    highest_version = 0
    latest_schema_file = None

    for file in os.listdir(directory):
        match = schema_file_pattern.match(file)
        if match:
            version = int(match.group(1))
            if version > highest_version:
                highest_version = version
                latest_schema_file = file

    return os.path.join(directory, latest_schema_file) if latest_schema_file else None

# Initialize a BigQuery client
client = bigquery.Client()

# List tables
tables = list_tables()
if not tables:
    print("No tables found.")

for table in tables:
    try:
        # Download table data
        download_table_data(table)

        job_id = get_job_id()

        # Find the downloaded parquet file
        parquet_files = glob.glob(f'downloads/{job_id}/*.parquet')
        if not parquet_files:
            print(f"No parquet files found for job {job_id}.")
            continue
        parquet_file = parquet_files[0]

        table_ref = client.dataset(os.getenv("DATASET")).table(table)

        # Load parquet file into BigQuery
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        with open(parquet_file, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        # Wait for the load job to complete
        job.result()

        # Download table schema
        download_table_schema(table)

        update_bigquery_schema_from_json(client, f"{os.getenv('PROJECT')}.{os.getenv('DATASET')}.{table}", get_latest_schema_file(table))

        logging.info(f"Table {table} loaded to BigQuery.")

        # Clean up downloaded files
        os.remove(parquet_file)
        shutil.rmtree(os.path.dirname(parquet_file))
    except Exception as e:
        logging.error(f"Error loading table {table} to BigQuery: {e}")