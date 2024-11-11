import json
import boto3

# Initialize the S3 client
source_bucket = {}
import os

try:
    with open('.secrets/tests_config.json') as f:
        config = json.load(f)
except FileNotFoundError:
    config = {
        'aws_access_key_id': os.getenv('CONNECTOR_TESTS_ACCESS_KEY'),
        'aws_secret_access_key': os.getenv('CONNECTOR_TESTS_SECRET_ACCESS_KEY'),
    }

source_bucket['aws_access_key_id'] = config.get("aws_access_key_id")
source_bucket['aws_secret_access_key'] = config.get("aws_secret_access_key")
source_bucket['bucket_name'] = 'tests/tap-woocommerce/default'

def load_json_from_s3(bucket_name, file_key):
    iam_session = boto3.Session(
                    aws_access_key_id=source_bucket['aws_access_key_id'],
                    aws_secret_access_key=source_bucket['aws_secret_access_key'],
                )
    iam_s3_client = iam_session.client("s3")
    response = iam_s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return json.loads(response['Body'].read().decode('utf-8'))


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
