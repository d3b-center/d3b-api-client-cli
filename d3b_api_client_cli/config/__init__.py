"""
All configuration values for the CLI
"""

import os

from dotenv import find_dotenv, load_dotenv

# File paths and directories
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname((__file__))))
ROOT_DATA_DIR = os.path.join(ROOT_DIR, "data")
LOG_DIR = os.path.join(ROOT_DATA_DIR, "logs")

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)

# Dewrangle
DEWRANGLE_DEV_PAT = os.environ.get("DEWRANGLE_DEV_PAT")
DEWRANGLE_BASE_URL = os.environ.get("DEWRANGLE_BASE_URL")

# DB
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_USER_PW = os.environ.get("DB_USER_PW")


class SECRETS:
    """
    Used in logger initialization to obfuscate sensitive env variables
    """

    DEWRANGLE_DEV_PAT = "DEWRANGLE_DEV_PAT"
    DB_USER_PW = "DB_USER_PW"


def check_dewrangle_http_config():
    """
    Check if env vars are set
    """
    if not (DEWRANGLE_DEV_PAT and DEWRANGLE_BASE_URL):
        raise ValueError(
            "❌ Missing required configuration! Please set the environment"
            " variables: DEWRANGLE_BASE_URL with the url to Dewrangle, and"
            " DEWRANGLE_DEV_PAT with your personal access token on Dewrangle."
        )


config = {
    "logging": {
        "default_log_filename": "d3b_api_client_cli",
        "default_log_level": "info",
        "default_log_dir": LOG_DIR,
    },
    "dewrangle": {
        "base_url": DEWRANGLE_BASE_URL,
        "pagination": {"max_page_size": 10},
        "client": {"execution_timeout": 30},  # seconds
        "endpoints": {
            "graphql": "/api/graphql",
            "rest": {
                "hash_report": "/api/rest/jobs/{job_id}/report/volume-hash",
                "job_errors": "/api/rest/jobs/{job_id}/errors",
            },
        },
        "output_dir": os.path.join(ROOT_DATA_DIR, "dewrangle"),
        "credential_type": "AWS",
        "billing_group_id": os.environ.get("CAVATICA_BILLING_GROUP_ID"),
    },
    "aws": {
        "region": os.environ.get("AWS_DEFAULT_REGION") or "us-east-1",
        "s3": {
            "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "test_bucket_name": os.environ.get("AWS_BUCKET_DATA_TRANSFER_TEST"),
        },
    },
}
