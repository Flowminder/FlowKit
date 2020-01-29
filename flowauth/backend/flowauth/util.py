from cryptography.fernet import Fernet
from flask import current_app


def get_fernet() -> Fernet:
    """
    Get the app's Fernet object to encrypt & decrypt things.
    Returns
    -------
    crypography.fernet.Fernet
    """
    return Fernet(current_app.config["FLOWAUTH_FERNET_KEY"])
