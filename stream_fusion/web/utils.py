from fastapi import Request

from stream_fusion.logging_config import logger


def get_client_ip(request: Request) -> str:
    """Extract real client IP from headers (X-Forwarded-For) or fallback to direct connection IP."""
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
        logger.debug(f"Client IP extracted from X-Forwarded-For: {client_ip}")
        return client_ip

    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        logger.debug(f"Client IP extracted from X-Real-IP: {real_ip}")
        return real_ip

    direct_ip = request.client.host
    logger.debug(f"Client IP extracted from direct connection: {direct_ip}")
    return direct_ip
