import os
import json
import boto3


import logging

def load_json_from_s3(prefix, file_key, logger=None):
    """
    Load a JSON file from an S3 bucket and return its contents as a Python dictionary.

    Args:
        bucket_name (str): The name of the S3 bucket where the file is stored.
        prefix (str): The prefix of the files within the S3 bucket.
        file_key (str): The key (path) of the file within the S3 bucket.
        logger (logging.Logger, optional): A logger instance to use for logging. If not provided, a default logger will be used.

    Returns:
        dict: The contents of the JSON file as a Python dictionary.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
    
    # Initialize a dictionary to store AWS credentials and bucket information
    source_bucket = {}

    # Attempt to load AWS credentials from a local configuration file
    try:
        with open('.secrets/tests_config.json') as f:
            config = json.load(f)
            logger.info("Loaded AWS credentials from local configuration file.")
    except FileNotFoundError:
        # If the configuration file is not found, attempt to load credentials from environment variables
        config = {
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        }
        print(config)
        logger.warning("Local configuration file not found. Loaded AWS credentials from environment variables.")

    # Populate the source_bucket dictionary with AWS credentials and bucket name
    source_bucket['aws_access_key_id'] = config.get("aws_access_key_id")
    source_bucket['aws_secret_access_key'] = config.get("aws_secret_access_key")
    source_bucket['bucket_name'] = 'tests'
    source_bucket['prefix'] = prefix

    # Create a new session with the AWS credentials
    iam_session = boto3.Session(
        aws_access_key_id=source_bucket['aws_access_key_id'],
        aws_secret_access_key=source_bucket['aws_secret_access_key'],
    )
    logger.info("AWS session created successfully.")

    json_content = None

    # Use the session to create an S3 client
    iam_s3_client = iam_session.client("s3")
    logger.info("S3 client initialized.")

    # List all objects under the specified prefix
    response = iam_s3_client.list_objects_v2(Bucket=source_bucket['bucket_name'], Prefix=source_bucket['prefix'])
    logger.info(f"Listed objects under the specified path: {source_bucket['bucket_name']}/{source_bucket['prefix']}")

    # Check if there are any contents
    if 'Contents' in response:
        # Iterate over each object in the response
        for obj in response['Contents']:
            # Extract the object key (full path)
            object_key = obj['Key']
            if object_key.endswith(file_key):
                # Retrieve the specified object from the S3 bucket
                response = iam_s3_client.get_object(Bucket=source_bucket['bucket_name'], Key=object_key)
                logger.info(f"Retrieved object from S3 bucket: {source_bucket['bucket_name']}/{object_key}")

                # Read the object's content, decode it from bytes to a string, and parse it as JSON
                json_content = json.loads(response['Body'].read().decode('utf-8'))
                logger.info("JSON content loaded and parsed successfully.")
                break
    else:
        logger.error("No contents found under the specified path: %s/%s", source_bucket['bucket_name'], source_bucket['prefix'])

    # Return the parsed JSON content
    return json_content


def compare_dicts(dict1, dict2):
    # Check if both are dictionaries
    if isinstance(dict1, dict) and isinstance(dict2, dict):
        # Check if they have the same keys
        if dict1.keys() != dict2.keys():
            return False

        # Recursively compare each key-value pair
        for key in dict1:
            if not compare_dicts(dict1[key], dict2[key]):
                return False
        return True
    # If they are lists, compare each item
    elif isinstance(dict1, list) and isinstance(dict2, list):
        if len(dict1) != len(dict2):
            return False
        for item1, item2 in zip(dict1, dict2):
            if not compare_dicts(item1, item2):
                return False
        return True
    # For simple types (int, str, etc.), directly compare
    else:
        return dict1 == dict2
