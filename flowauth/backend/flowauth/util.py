from cryptography.fernet import Fernet
from flask import current_app, g, has_request_context, request, session


def request_context_processor(logger, method_name, event_dict) -> dict:
    """
    Pre-processor for structlog which injects details about the current request and user,
    if any.

    """
    if has_request_context():
        try:
            user = dict(
                username=g.user.username,
                id=g.user.id,
                is_admin=g.user.is_admin,
            )
        except AttributeError as exc:
            user = None
        event_dict = dict(
            **event_dict,
            user=user,
            session=session,
            session_id=session.get("_id", None),
            request_id=request.id if hasattr(request, "id") else None
        )
    return event_dict


def get_fernet() -> Fernet:
    """
    Get the app's Fernet object to encrypt & decrypt things.
    Returns
    -------
    crypography.fernet.Fernet
    """
    return Fernet(current_app.config["FLOWAUTH_FERNET_KEY"])
