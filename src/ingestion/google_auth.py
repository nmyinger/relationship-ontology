"""
src/ingestion/google_auth.py — Shared Google OAuth2 credential loading.

Both the Gmail and Calendar connectors use this module to load and refresh
OAuth2 credentials from a single token file.
"""

import os

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

load_dotenv()

# Default env var name for the shared credentials path.
_ENV_VAR = "GOOGLE_CREDENTIALS_PATH"
_DEFAULT_PATH = "credentials/google_oauth.json"


def load_credentials(credentials_path: str | None = None) -> Credentials:
    """
    Load OAuth2 credentials from a token JSON file and refresh if expired.

    Parameters
    ----------
    credentials_path:
        Path to the OAuth2 token JSON file. Falls back to
        GOOGLE_CREDENTIALS_PATH env var.

    Raises
    ------
    EnvironmentError
        If no path is provided and the env var is unset.
    FileNotFoundError
        If the file does not exist at the resolved path.
    """
    path = credentials_path or os.environ.get(_ENV_VAR) or _DEFAULT_PATH

    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Google credentials file not found: {path}. "
            "See credentials/google_oauth_template.jsonc for the required format."
        )

    creds = Credentials.from_authorized_user_file(path)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(creds.to_json())
    return creds


def build_google_service(api: str, version: str, credentials_path: str | None = None):
    """
    Build and return an authorised Google API service object.

    Parameters
    ----------
    api:
        The API name (e.g. "gmail", "calendar").
    version:
        The API version (e.g. "v1", "v3").
    credentials_path:
        Path to the OAuth2 token JSON. Falls back to GOOGLE_CREDENTIALS_PATH.
    """
    from googleapiclient.discovery import build

    creds = load_credentials(credentials_path)
    return build(api, version, credentials=creds)
