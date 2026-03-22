"""Secret dependency for API authentication."""
import secrets
from typing import Optional
from fastapi import Security
from fastapi.security import APIKeyHeader
from starlette.exceptions import HTTPException
from starlette.status import HTTP_403_FORBIDDEN
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings

class SecretManager:
    """Manages the secret key for API authentication."""

    def __init__(self) -> None:
        self._secret = None

    @property
    def value(self):
        if self._secret is None:
            self._secret = self._get_secret_value()
        return self._secret

    def _get_secret_value(self):
        """Get secret value from environment variable or generate a new one."""
        if settings.secret_api_key:
            try:
                return settings.secret_api_key
            except KeyError:
                secret_value = secrets.token_urlsafe(32)
                logger.warning(
                    "Environment variable 'SECRET_API_KEY' is in incorrect format. "
                    "Generated a single-use secret key for this session."
                )
        else:
            secret_value = secrets.token_urlsafe(32)
            logger.warning(
                "Environment variable 'SECRET_API_KEY' not found. "
                "Generated a single-use secret key for this session."
            )
        return secret_value

secret_manager = SecretManager()

SECRET_KEY_NAME = "secret-key"  # noqa: S105
secret_header = APIKeyHeader(
    name=SECRET_KEY_NAME,
    scheme_name="Secret header",
    auto_error=False,
)

async def secret_based_security(header_param: Optional[str] = Security(secret_header)):
    """
    Validate the secret key provided in the header.

    Args:
        header_param: The secret key parsed from the header field.

    Returns:
        True if the authentication was successful.

    Raises:
        HTTPException: If the authentication failed.
    """
    if not header_param:
        logger.warning("Secret key not provided in the header.")
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Secret key must be provided in the header.",
        )

    if not secrets.compare_digest(header_param, secret_manager.value):
        logger.warning("Invalid secret key provided.")
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail=(
                "Invalid secret key. If not set through the 'secret_api_key' environment variable, "
                "it was automatically generated at startup and can be found in the server logs."
            ),
        )
    
    return True