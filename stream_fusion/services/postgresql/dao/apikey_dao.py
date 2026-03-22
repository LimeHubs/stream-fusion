import uuid

from typing import List, Optional
from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, timezone

from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.services.postgresql.models.apikey_model import APIKeyModel
from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.schemas import (
    APIKeyCreate,
    APIKeyUpdate,
    APIKeyInDB,
)
from stream_fusion.utils.general import datetime_to_timestamp, timestamp_to_datetime

class APIKeyDAO:
    """Class for accessing API key table."""

    def __init__(self, session: AsyncSession = Depends(get_db_session)) -> None:
        self.session = session
        self.expiration_limit = 15  # Default expiration limit in days

    @staticmethod
    def _model_to_schema(key: APIKeyModel) -> APIKeyInDB:
        return APIKeyInDB(
            id=key.id,
            api_key=key.api_key,
            is_active=key.is_active,
            never_expire=key.never_expire,
            expiration_date=timestamp_to_datetime(key.expiration_date),
            latest_query_date=timestamp_to_datetime(key.latest_query_date),
            total_queries=key.total_queries,
            name=key.name,
            proxied_links=key.proxied_links,
        )

    async def create_key(self, api_key_create: APIKeyCreate) -> APIKeyInDB:
        async with self.session.begin():
            try:
                api_key = str(uuid.uuid4())
                expiration_timestamp = (
                    None
                    if api_key_create.never_expire
                    else datetime_to_timestamp(datetime.now(timezone.utc) + timedelta(days=self.expiration_limit))
                )

                new_key = APIKeyModel(
                    api_key=api_key,
                    is_active=True,
                    never_expire=api_key_create.never_expire,
                    expiration_date=expiration_timestamp,
                    name=api_key_create.name,
                    proxied_links=api_key_create.proxied_links,
                )

                self.session.add(new_key)
                await self.session.flush()

                logger.success(f"Created new API key: {api_key}")
                return self._model_to_schema(new_key)
            except Exception as e:
                logger.error(f"Error creating API key: {str(e)}")
                raise HTTPException(status_code=500, detail="Internal server error")

    async def get_all_keys(self, limit: int, offset: int) -> List[APIKeyInDB]:
        async with self.session.begin():
            try:
                query = select(APIKeyModel).limit(limit).offset(offset)
                result = await self.session.execute(query)
                keys = [self._model_to_schema(key) for key in result.scalars().all()]
                logger.info(f"Retrieved {len(keys)} API keys")
                return keys
            except Exception as e:
                logger.error(f"Error retrieving API keys: {str(e)}")
                raise HTTPException(status_code=500, detail="Internal server error")
            
    async def get_key_by_uuid(self, api_key: uuid.UUID) -> Optional[APIKeyInDB]:
        try:
            query = select(APIKeyModel).where(APIKeyModel.api_key == str(api_key))
            result = await self.session.execute(query)
            db_key = result.scalar_one_or_none()
            if db_key:
                logger.info(f"Retrieved API key: {api_key}")
                return self._model_to_schema(db_key)
            else:
                logger.warning(f"API key not found: {api_key}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving API key {api_key}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def get_keys_by_name(self, name: str) -> List[APIKeyInDB]:
        try:
            query = select(APIKeyModel).where(APIKeyModel.name == name)
            result = await self.session.execute(query)
            keys = [self._model_to_schema(key) for key in result.scalars().all()]
            logger.info(f"Retrieved {len(keys)} API keys with name: {name}")
            return keys
        except Exception as e:
            logger.error(f"Error retrieving API keys by name {name}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def update_key(
        self, api_key: uuid.UUID, update_data: APIKeyUpdate
    ) -> APIKeyInDB:
        try:
            query = select(APIKeyModel).where(APIKeyModel.api_key == str(api_key))
            result = await self.session.execute(query)
            db_key = result.scalar_one_or_none()

            if not db_key:
                logger.warning(f"API key not found for update: {api_key}")
                raise HTTPException(status_code=404, detail="API key not found")

            if update_data.is_active is not None:
                db_key.is_active = update_data.is_active

            if not db_key.never_expire and update_data.expiration_date:
                db_key.expiration_date = datetime_to_timestamp(update_data.expiration_date)
                
            if update_data.proxied_links is not None:
                db_key.proxied_links = update_data.proxied_links
                logger.info(f"Updated proxied_links to {update_data.proxied_links} for API key: {api_key}")

            await self.session.commit()
            await self.session.refresh(db_key)

            logger.info(f"Updated API key: {api_key}")
            return self._model_to_schema(db_key)
        except HTTPException:
            raise
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error updating API key {api_key}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
        
    async def delete_key(self, api_key: uuid.UUID) -> bool:
        try:
            query = select(APIKeyModel).where(APIKeyModel.api_key == str(api_key))
            result = await self.session.execute(query)
            db_key = result.scalar_one_or_none()

            if db_key:
                await self.session.delete(db_key)
                await self.session.commit()
                logger.info(f"Deleted API key: {api_key}")
                return True
            else:
                logger.warning(f"API key not found for deletion: {api_key}")
                return False
        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error deleting API key {api_key}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def check_key(self, api_key: uuid.UUID) -> bool:
        async with self.session.begin():
            try:
                query = select(APIKeyModel).where(
                    APIKeyModel.api_key == str(api_key),
                    APIKeyModel.is_active == True,
                    (APIKeyModel.never_expire == True) | 
                    (APIKeyModel.expiration_date > datetime_to_timestamp(datetime.now(timezone.utc)))
                )
                result = await self.session.execute(query)
                db_key = result.scalar_one_or_none()
                
                if db_key:
                    # Enregistrer automatiquement l'utilisation (comme dans l'ancienne version)
                    now = datetime.now(timezone.utc)
                    db_key.latest_query_date = datetime_to_timestamp(now)
                    db_key.total_queries += 1
                    await self.session.commit()
                    logger.debug(f"Checked and recorded usage for API key {api_key}")
                    return True
                else:
                    logger.debug(f"Checked API key {api_key}: invalid")
                    return False
                    
            except Exception as e:
                await self.session.rollback()
                logger.error(f"Error checking API key {api_key}: {str(e)}")
                return False

    async def record_query(self, api_key: uuid.UUID) -> None:
        async with self.session.begin():
            try:
                query = select(APIKeyModel).where(APIKeyModel.api_key == str(api_key))
                result = await self.session.execute(query)
                db_key = result.scalar_one_or_none()

                if db_key:
                    now = datetime.now(timezone.utc)
                    db_key.latest_query_date = datetime_to_timestamp(now)
                    db_key.total_queries += 1
                    await self.session.commit()
                    logger.debug(f"Recorded query for API key: {api_key}")
            except Exception as e:
                await self.session.rollback()
                logger.error(f"Error recording query for API key {api_key}: {str(e)}")

    async def get_usage_stats(self) -> List[APIKeyInDB]:
        try:
            query = select(APIKeyModel).order_by(APIKeyModel.latest_query_date.desc())
            result = await self.session.execute(query)
            keys = [self._model_to_schema(key) for key in result.scalars().all()]
            logger.info(f"Retrieved usage stats for {len(keys)} API keys")
            return keys
        except Exception as e:
            logger.error(f"Error retrieving usage stats: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def revoke_key(self, api_key: uuid.UUID) -> bool:
        """Revoke (deactivate) an API key"""
        try:
            update_data = APIKeyUpdate(is_active=False)
            await self.update_key(api_key, update_data)
            logger.info(f"API key revoked (deactivated): {api_key}")
            return True
        except HTTPException as e:
            if e.status_code == 404:
                logger.warning(f"API key not found for revocation: {api_key}")
                return False
            raise
        except Exception as e:
            logger.error(f"Error revoking API key {api_key}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def renew_key(self, api_key: uuid.UUID) -> APIKeyInDB:
        """Renew (reactivate) an API key"""
        try:
            update_data = APIKeyUpdate(is_active=True)
            updated_key = await self.update_key(api_key, update_data)
            logger.info(f"API key renewed (reactivated): {api_key}")
            return updated_key
        except Exception as e:
            logger.error(f"Error renewing API key {api_key}: {str(e)}")
            raise

    async def list_active_keys(self) -> List[APIKeyInDB]:
        """List all active API keys"""
        try:
            current_time = datetime.now(timezone.utc)
            query = select(APIKeyModel).where(
                APIKeyModel.is_active == True,
                (APIKeyModel.never_expire == True) | 
                (APIKeyModel.expiration_date > datetime_to_timestamp(current_time))
            )
            result = await self.session.execute(query)
            keys = [self._model_to_schema(key) for key in result.scalars().all()]
            logger.info(f"Retrieved {len(keys)} active API keys")
            return keys
        except Exception as e:
            logger.error(f"Error retrieving active keys: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
