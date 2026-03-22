from uuid import UUID

from fastapi import HTTPException

from stream_fusion.logging_config import logger


def ensure_uuid(api_key: str) -> UUID:
    if isinstance(api_key, UUID):
        return api_key
    try:
        return UUID(api_key)
    except ValueError:
        logger.error(f"Invalid API key format: {api_key}")
        raise HTTPException(status_code=400, detail="Invalid API key format")
