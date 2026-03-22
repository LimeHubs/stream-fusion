from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID
import aiohttp
from fastapi import APIRouter, Depends, HTTPException, Query

from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.schemas.apikey_schemas import APIKeyCreate, APIKeyUpdate
from stream_fusion.services.rd_conn.token_manager import RealDebridService
from stream_fusion.settings import settings
from stream_fusion.utils.security import secret_based_security
from stream_fusion.web.api.auth.schemas import UsageLog, UsageLogs
from stream_fusion.logging_config import logger

router = APIRouter()
rd_service = RealDebridService()

def ensure_uuid(api_key: str) -> UUID:
    if isinstance(api_key, UUID):
        return api_key
    try:
        return UUID(api_key)
    except ValueError:
        logger.error(f"Invalid API key format: {api_key}")
        raise HTTPException(status_code=400, detail="Invalid API key format")

@router.post(
    "/new",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def create_new_api_key(
    name: str = Query(None, description="Set API key name"),
    never_expires: bool = Query(False, description="If set, the created API key will never be considered expired"),
    apikey_dao: APIKeyDAO = Depends()
) -> str:
    logger.info(f"Creating new API key. Name: {name}, Never expires: {never_expires}")
    api_key = await apikey_dao.create_key(APIKeyCreate(
        name=name,
        never_expire=never_expires
    ))
    logger.info("New API key created successfully.")
    return api_key.api_key

@router.get(
    "/get/{api_key}",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def get_api_key(
    api_key: str,
    apikey_dao: APIKeyDAO = Depends()
):
    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Retrieving API key: {api_key_uuid}")
    key = await apikey_dao.get_key_by_uuid(api_key_uuid)
    if not key:
        logger.warning(f"API key not found: {api_key_uuid}")
        raise HTTPException(status_code=404, detail="API key not found")
    return key.dict()

@router.get(
    "/get_by_name/{name}",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def get_api_keys_by_name(
    name: str,
    apikey_dao: APIKeyDAO = Depends()
):
    logger.info(f"Retrieving API keys with name: {name}")
    keys = await apikey_dao.get_keys_by_name(name)
    return [key.dict() for key in keys]

@router.put(
    "/revoke/{api_key}",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def revoke_api_key(
    api_key: str,
    apikey_dao: APIKeyDAO = Depends()
):
    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Attempting to revoke API key: {api_key_uuid}")
    result = await apikey_dao.revoke_key(api_key_uuid)
    if not result:
        logger.warning(f"API key not found for revocation: {api_key_uuid}")
        raise HTTPException(status_code=404, detail="API key not found")
    logger.info(f"API key successfully revoked: {api_key_uuid}")
    return {"message": "API key successfully revoked"}

@router.put(
    "/renew/{api_key}",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def renew_api_key(
    api_key: str,
    apikey_dao: APIKeyDAO = Depends()
):
    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Attempting to renew API key: {api_key_uuid}")
    try:
        updated_key = await apikey_dao.renew_key(api_key_uuid)
        logger.info(f"API key renewed successfully: {api_key_uuid}")
        return updated_key.dict()
    except HTTPException as e:
        logger.error(f"HTTP exception occurred while renewing API key {api_key_uuid}: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error occurred while renewing API key {api_key_uuid}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get(
    "/logs",
    dependencies=[Depends(secret_based_security)],
    response_model=UsageLogs,
    include_in_schema=settings.security_hide_docs,
)
async def get_api_key_usage_logs(
    apikey_dao: APIKeyDAO = Depends()
):
    logger.info("Retrieving usage logs for all API keys")
    usage_stats = await apikey_dao.get_usage_stats()
    logger.info(f"Retrieved usage stats for {len(usage_stats)} API keys")
    
    current_time = datetime.now(timezone.utc)
    
    return UsageLogs(
        logs=[
            UsageLog(
                api_key=key.api_key,
                # Update is_active based on expiration status
                is_active=key.is_active and (key.never_expire or (key.expiration_date and key.expiration_date > current_time)),
                never_expire=key.never_expire,
                expiration_date=key.expiration_date,
                latest_query_date=key.latest_query_date,
                total_queries=key.total_queries,
                name=key.name,
            )
            for key in usage_stats
        ],
    )

@router.get(
    "/list",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def list_active_api_keys(
    apikey_dao: APIKeyDAO = Depends()
) -> List[dict]:
    logger.info("Retrieving list of active API keys")
    active_keys = await apikey_dao.list_active_keys()
    logger.info(f"Retrieved {len(active_keys)} active API keys")
    return [key.dict() for key in active_keys]

@router.put(
    "/update/{api_key}",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def update_api_key(
    api_key: str,
    name: Optional[str] = Query(None, description="New name for the API key"),
    is_active: Optional[bool] = Query(None, description="Set the active status of the API key"),
    expiration_date: Optional[datetime] = Query(None, description="New expiration date for the API key"),
    apikey_dao: APIKeyDAO = Depends()
):
    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Attempting to update API key: {api_key_uuid}")
    update_data = APIKeyUpdate(
        name=name,
        is_active=is_active,
        expiration_date=expiration_date
    )
    try:
        updated_key = await apikey_dao.update_key(api_key_uuid, update_data)
        logger.info(f"API key updated successfully: {api_key_uuid}")
        return updated_key.dict()
    except HTTPException as e:
        logger.error(f"HTTP exception occurred while updating API key {api_key_uuid}: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error occurred while updating API key {api_key_uuid}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete(
    "/delete/{api_key}",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def delete_api_key(
    api_key: str,
    apikey_dao: APIKeyDAO = Depends()
):
    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Attempting to delete API key: {api_key_uuid}")
    try:
        result = await apikey_dao.delete_key(api_key_uuid)
        if result:
            logger.info(f"API key deleted successfully: {api_key_uuid}")
            return {"message": "API key successfully deleted"}
        else:
            logger.warning(f"API key not found for deletion: {api_key_uuid}")
            raise HTTPException(status_code=404, detail="API key not found")
    except Exception as e:
        logger.error(f"An error occurred while deleting API key {api_key_uuid}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while deleting the API key: {str(e)}")

@router.get(
    "/check/{api_key}",
    dependencies=[Depends(secret_based_security)],
    include_in_schema=settings.security_hide_docs,
)
async def check_api_key(
    api_key: str,
    apikey_dao: APIKeyDAO = Depends()
):
    api_key_uuid = ensure_uuid(api_key)
    logger.info(f"Checking API key: {api_key_uuid}")
    is_valid = await apikey_dao.check_key(api_key_uuid)
    return {"is_valid": is_valid}


@router.post(
    "/realdebrid/device_code",
    include_in_schema=settings.security_hide_docs,
)
async def get_device_code():
    logger.info("Requesting device code from RealDebrid")
    results = await rd_service.get_device_code()
    logger.info("Device code received successfully")
    return results

@router.post("/realdebrid/credentials",
    include_in_schema=settings.security_hide_docs
)
async def get_credentials(device_code: str):
    logger.info(f"Requesting credentials for device code: {device_code}")
    results = await rd_service.get_credentials(device_code)
    logger.info("Credentials received successfully")
    return results

@router.post("/realdebrid/token",
    include_in_schema=settings.security_hide_docs
)
async def get_token(client_id: str, client_secret: str, device_code: str):
    logger.info(f"Requesting token for device code: {device_code}")
    results = await rd_service.get_token(client_id, client_secret, device_code)
    logger.info("Token received successfully")
    return results


@router.get("/alldebrid/pin/get")
async def get_alldebrid_pin():
    logger.info("Requesting PIN code for AllDebrid")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.alldebrid.com/v4/pin/get?agent={settings.ad_user_app}") as response:
            if response.status != 200:
                logger.error(f"Error retrieving AllDebrid PIN: {response.status}")
                raise HTTPException(status_code=response.status, detail="Error retrieving AllDebrid PIN")
            data = await response.json()
            logger.info("AllDebrid PIN code received successfully")
            return data

@router.get("/alldebrid/pin/check")
async def check_alldebrid_pin(check: str, pin: str):
    logger.info(f"Verifying AllDebrid PIN code: {pin}")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.alldebrid.com/v4/pin/check?check={check}&pin={pin}&agent={settings.ad_user_app}") as response:
            if response.status != 200:
                logger.error(f"Error verifying AllDebrid PIN: {response.status}")
                raise HTTPException(status_code=response.status, detail="Error verifying AllDebrid PIN")
            data = await response.json()
            logger.info("AllDebrid PIN code verification successful")
            return data