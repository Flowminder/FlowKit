import os
from flask_appbuilder.security.manager import AUTH_OAUTH


use_google_auth = os.getenv("AIRFLOW__WEBSERVER__AUTH_TYPE", "airflow") == "google"
ENABLE_PROXY_FIX = os.getenv(
    "AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX", "False"
).lower() in ("1", "true", "yes")

if use_google_auth:
    AUTH_TYPE = AUTH_OAUTH
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = os.getenv(
        "AIRFLOW__WEBSERVER__AUTH_USER_REGISTRATION_ROLE", "Viewer"
    )
    AUTH_ROLES_SYNC_AT_LOGIN = False

    OAUTH_PROVIDERS = [
        {
            "name": "google",
            "icon": "fa-google",
            "token_key": "access_token",
            "remote_app": {
                "server_metadata_url": "https://accounts.google.com/.well-known/openid-configuration",
                "client_id": os.environ["AIRFLOW__WEBSERVER__OAUTH_GOOGLE_CLIENT_ID"],
                "client_secret": os.environ[
                    "AIRFLOW__WEBSERVER__OAUTH_GOOGLE_CLIENT_SECRET"
                ],
                "api_base_url": "https://www.googleapis.com/oauth2/v2/",
                "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
                "access_token_url": "https://oauth2.googleapis.com/token",
                "client_kwargs": {
                    "scope": "openid email profile",
                    "prompt": "consent",
                    "access_type": "offline",
                },
            },
        },
    ]
