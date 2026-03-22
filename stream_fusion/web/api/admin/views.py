from fastapi import APIRouter, Request, Depends, HTTPException, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import APIKeyHeader
from fastapi_simple_rate_limiter import RateLimiter
from starlette.status import HTTP_303_SEE_OTHER
import secrets
import uuid
from datetime import timedelta

from stream_fusion.services.postgresql.schemas.apikey_schemas import APIKeyCreate, APIKeyUpdate
from stream_fusion.utils.security.security_secret import SecretManager
from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.web.api.auth.schemas import UsageLogs, UsageLog
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.services.redis.redis_config import get_redis_dependency
from stream_fusion.web.api.utils import ensure_uuid

router = APIRouter()

templates = Jinja2Templates(directory=settings.admin_template_dir)

login_rate_limiter = RateLimiter(
    limit=5,
    seconds=60,
    exception_message="Too many login attempts. Please try again in 60 seconds.",
)

SECRET_KEY_NAME = "secret-key"
secret_header = APIKeyHeader(name=SECRET_KEY_NAME, auto_error=False)

secret = SecretManager()


def custom_url_for(name: str, **path_params: any) -> str:
    def wrapper(request: Request):
        url = request.url_for(name, **path_params)
        if settings.use_https:
            return str(url.replace(scheme="https"))
        return str(url)

    return wrapper


templates.env.globals["url_for"] = custom_url_for


def redirect_to_login(request: Request):
    return RedirectResponse(
        url=custom_url_for("login_page")(request), status_code=HTTP_303_SEE_OTHER
    )


async def get_session_id_from_request(request: Request):
    session_id = request.session.get("session_id")
    if not session_id:
        logger.warning("Attempt to access protected route without session ID")
        return None
    return session_id


async def session_based_security(
    request: Request,
    session_id: str = Depends(get_session_id_from_request),
    redis_client=get_redis_dependency(),
):
    if not session_id:
        logger.warning("No session ID found, redirecting to login")
        return redirect_to_login(request)

    secret_key = redis_client.get(session_id)
    if not secret_key:
        logger.warning("Session expired or invalid, redirecting to login")
        request.session.clear()
        return redirect_to_login(request)

    if not secrets.compare_digest(secret_key.decode(), secret.value):
        logger.warning("Invalid secret key in session, redirecting to login")
        redis_client.delete(session_id)
        request.session.clear()
        return redirect_to_login(request)

    # Refresh the session TTL
    redis_client.expire(session_id, timedelta(hours=2))
    return True


@router.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    logger.info("Rendering login page")
    return templates.TemplateResponse("login.html", {"request": request})


@router.post("/login")
@login_rate_limiter
async def login(
    request: Request, secret_key: str = Form(...), redis_client=get_redis_dependency()
):
    logger.info("Processing login attempt")
    if secrets.compare_digest(secret_key, secret.value):
        session_id = str(uuid.uuid4())
        redis_client.setex(session_id, timedelta(hours=2), secret_key)
        request.session["session_id"] = session_id
        logger.info("Login successful")
        return RedirectResponse(
            url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
        )
    else:
        logger.warning("Login attempt with invalid secret key")
        return templates.TemplateResponse(
            "login.html", {"request": request, "error": "Invalid secret key"}
        )


@router.get("/api-keys", response_class=HTMLResponse)
async def list_api_keys(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    logger.info("Retrieving API key usage stats")
    usage_stats = await apikey_dao.get_usage_stats()
    usage_logs = UsageLogs(
        logs=[
            UsageLog(
                api_key=key.api_key,
                is_active=key.is_active,
                never_expire=key.never_expire,
                expiration_date=(
                    key.expiration_date if key.expiration_date else "Unlimited"
                ),
                latest_query_date=(
                    key.latest_query_date if key.latest_query_date else "None"
                ),
                total_queries=key.total_queries,
                name=key.name if key.name else "JohnDoe",
                proxied_links=key.proxied_links,
            )
            for key in usage_stats
        ]
    )
    logger.info(f"Retrieved {len(usage_logs.logs)} API key usage logs")
    return templates.TemplateResponse(
        "api_keys.html", {"request": request, "logs": usage_logs.logs}
    )


@router.get("/create-api-key", response_class=HTMLResponse)
async def create_api_key_page(
    request: Request, authenticated: bool = Depends(session_based_security)
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    logger.info("Rendering create API key page")
    return templates.TemplateResponse("create_api_key.html", {"request": request})


@router.post("/create-api-key")
async def create_api_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    name: str = Form(None),
    never_expires: bool = Form(False),
    proxied_links: bool = Form(False),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    logger.info(f"Creating new API key. Name: {name}, Never expires: {never_expires}, Proxied links: {proxied_links}")
    key = APIKeyCreate(name=name, never_expire=never_expires, proxied_links=proxied_links)
    new_key = await apikey_dao.create_key(key)
    logger.info("New API key created successfully.")
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )



@router.post("/revoke-api-key")
async def revoke_api_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    api_key: str = Form(...),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Revoking API key: {api_key_uuid}")
    result = await apikey_dao.revoke_key(api_key_uuid)
    if result:
        logger.info(f"API key revoked successfully: {api_key_uuid}")
    else:
        logger.warning(f"Failed to revoke API key: {api_key_uuid}")
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )

@router.post("/renew-api-key")
async def renew_api_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    api_key: str = Form(...),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Renewing API key: {api_key_uuid}")
    try:
        renewed_key = await apikey_dao.renew_key(api_key_uuid)
        logger.info(f"API key renewed successfully: {api_key_uuid}")
    except Exception as e:
        logger.error(f"Failed to renew API key {api_key_uuid}: {str(e)}")
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )

@router.post("/delete-api-key")
async def delete_api_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    api_key: str = Form(...),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Deleting API key: {api_key_uuid}")
    try:
        result = await apikey_dao.delete_key(api_key_uuid)
        if result:
            logger.info(f"API key deleted successfully: {api_key_uuid}")
        else:
            logger.warning(f"API key not found for deletion: {api_key_uuid}")
    except Exception as e:
        logger.error(f"Failed to delete API key {api_key_uuid}: {str(e)}")
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


@router.post("/toggle-proxied-links")
async def toggle_proxied_links(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    api_key: str = Form(...),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Toggling proxied links for API key: {api_key_uuid}")
    
    try:
        # Récupérer la clé API
        key_info = await apikey_dao.get_key_by_uuid(api_key_uuid)
        if not key_info:
            logger.warning(f"API key not found for toggling proxied links: {api_key_uuid}")
            return RedirectResponse(
                url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
            )
        
        # Mettre à jour avec la valeur inverse
        current_value = getattr(key_info, 'proxied_links', False)
        update_data = APIKeyUpdate(proxied_links=not current_value)
        
        await apikey_dao.update_key(api_key_uuid, update_data)
        logger.info(f"Proxied links toggled to {not current_value} for API key: {api_key_uuid}")
    except Exception as e:
        logger.error(f"Error toggling proxied links for API key {api_key_uuid}: {str(e)}")
    
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )

@router.get("/logout")
async def logout(request: Request, redis_client=get_redis_dependency()):
    logger.info("User logging out")
    session_id = request.session.get("session_id")
    if session_id:
        redis_client.delete(session_id)
    request.session.clear()
    return RedirectResponse(
        url=custom_url_for("login_page")(request), status_code=HTTP_303_SEE_OTHER
    )
