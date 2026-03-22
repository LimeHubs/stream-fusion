import asyncio
import inspect
import json
import time
from typing import Any, List
import hashlib
from redis.asyncio import Redis
from stream_fusion.settings import settings
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series
from stream_fusion.utils.cache.cache_base import CacheBase

class RedisCache(CacheBase):
    def __init__(self, config):
        super().__init__(config)
        self.redis_host = settings.redis_host
        self.redis_port = settings.redis_port
        self.redis_db = settings.redis_db
        self.redis_url = f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
        self._redis_client = None
        self.media_expiration = settings.redis_expiration

    async def get_redis_client(self):
        if not self._redis_client:
            try:
                self._redis_client = Redis.from_url(
                    self.redis_url,
                    max_connections=100,
                    socket_connect_timeout=5,
                    socket_timeout=10
                )
            except Exception as e:
                self.logger.error(f"RedisCache: Failed to create Redis client: {e}")
                self._redis_client = None
        return self._redis_client
    
    async def reconnect(self):
        self._redis_client = None
        return await self.get_redis_client()

    async def execute_with_retry(self, operation, *args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return await operation(*args, **kwargs)
            except ConnectionError:
                if attempt < max_retries - 1:
                    self.logger.warning(f"RedisCache: Connection lost. Attempting reconnection (attempt {attempt + 1}/{max_retries})")
                    await self.reconnect()
                else:
                    self.logger.error("RedisCache: Max retries reached. Unable to reconnect to Redis.")
                    raise

    async def get_list(self, key: str) -> List[Any]:
        result = await self.get(key)
        if isinstance(result, list):
            return result
        elif result is not None:
            return [result]
        return None

    def generate_key(self, func_name: str, *args, **kwargs) -> str:
        media = kwargs.get('media')
        if media is None and args:
            media = next((arg for arg in args if isinstance(arg, (Movie, Series))), None)
        
        if media:
            if isinstance(media, Movie):
                key_string = f"movie:{media.titles[0]}:{media.languages[0]}"
            elif isinstance(media, Series):
                key_string = f"series:{media.titles[0]}:{media.languages[0]}:{media.season}"
            else:
                raise TypeError("Only Movie and Series are allowed as media!")
        else:
            key_string = f"{func_name}:{str(args)}:{str(kwargs)}"
        
        hashed_key = hashlib.sha256(key_string.encode("utf-8")).hexdigest()
        return hashed_key[:16]

    async def clear(self) -> None:
        try:
            client = await self.get_redis_client()
            await client.flushdb()
        except Exception as e:
            self.logger.error(f"RedisCache: Error clearing cache: {e}")

    async def get_or_set(self, func: callable, *args, **kwargs) -> Any:
        self.logger.debug(f"RedisCache: Entering get_or_set for function: {func.__name__}")
        start_time = time.time()

        if not await self.can_cache():
            self.logger.debug("RedisCache: Cache is not available, executing function directly")
            return await self._execute_func(func, *args, **kwargs)

        key = self.generate_key(func.__name__, *args, **kwargs)
        self.logger.debug(f"RedisCache: Generated cache key: {key}")

        cached_result = await self.get(key)
        self.logger.debug(f"RedisCache: Attempted to get result from cache. Found: {cached_result is not None}")

        if cached_result is not None:
            self.logger.debug(f"RedisCache: Returning cached result for key: {key}")
            return cached_result

        self.logger.debug(f"RedisCache: Cache miss for key: {key}. Executing function.")
        result = await self._execute_func(func, *args, **kwargs)
        self.logger.debug(f"RedisCache: Function execution completed. Setting result in cache.")
        await self.set(key, result)

        end_time = time.time()
        self.logger.debug(f"RedisCache: get_or_set completed in {end_time - start_time:.2f} seconds")
        return result

    async def _execute_func(self, func, *args, **kwargs):
        self.logger.debug(f"RedisCache: Executing function: {func.__name__}")
        start_time = time.time()

        sig = inspect.signature(func)
        params = sig.parameters

        self.logger.debug(f"RedisCache: Function {func.__name__} expects {len(params)} arguments")
        self.logger.trace(f"RedisCache: Received args: {args}, kwargs: {kwargs}")

        call_args = {}
        for i, (param_name, param) in enumerate(params.items()):
            if i < len(args):
                call_args[param_name] = args[i]
            elif param_name in kwargs:
                call_args[param_name] = kwargs[param_name]
            elif param.default is not param.empty:
                call_args[param_name] = param.default
            else:
                self.logger.error(f"RedisCache: Missing required argument: {param_name}")
                raise TypeError(f"Missing required argument: {param_name}")

        self.logger.trace(f"RedisCache: Prepared arguments for {func.__name__}: {call_args}")

        if asyncio.iscoroutinefunction(func):
            self.logger.debug(f"RedisCache: Function {func.__name__} is asynchronous")
            try:
                result = await func(**call_args)
                self.logger.debug(f"RedisCache: Asynchronous function {func.__name__} completed successfully")
            except Exception as e:
                self.logger.error(f"RedisCache: Error executing asynchronous function {func.__name__}: {str(e)}")
                raise
        else:
            self.logger.debug(f"RedisCache: Function {func.__name__} is synchronous")
            try:
                result = func(**call_args)
                self.logger.debug(f"RedisCache: Synchronous function {func.__name__} completed successfully")
            except Exception as e:
                self.logger.error(f"RedisCache: Error executing synchronous function {func.__name__}: {str(e)}")
                raise

        end_time = time.time()
        self.logger.debug(f"RedisCache: Function {func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result

    async def can_cache(self) -> bool:
        self.logger.debug("RedisCache: Checking if caching is possible")
        try:
            client = await self.get_redis_client()
            result = await client.ping()
            self.logger.debug(f"RedisCache: Redis ping result: {result}")
            return result
        except Exception as e:
            self.logger.error(f"RedisCache: Unable to connect to Redis: {e}")
            return False

    @staticmethod
    def _serialize(value: Any) -> str:
        if isinstance(value, (Movie, Series)):
            return json.dumps(value.to_dict())
        return json.dumps(value)

    @staticmethod
    def _deserialize(data: str) -> Any:
        obj = json.loads(data)
        if isinstance(obj, dict):
            media_type = obj.get("__type__")
            if media_type == "movie":
                return Movie.from_dict(obj)
            if media_type == "series":
                return Series.from_dict(obj)
        return obj

    async def get(self, key: str) -> Any:
        async def get_operation():
            client = await self.get_redis_client()
            cached_result = await client.get(key)
            if cached_result:
                return self._deserialize(cached_result)
            return None

        return await self.execute_with_retry(get_operation)

    async def set(self, key: str, value: Any, expiration: int = None) -> None:
        if expiration is None:
            expiration = self.media_expiration

        async def set_operation():
            client = await self.get_redis_client()
            cached_data = self._serialize(value)
            return await client.set(key, cached_data, ex=expiration)

        await self.execute_with_retry(set_operation)

    async def delete(self, key: str) -> bool:
        async def delete_operation():
            client = await self.get_redis_client()
            return bool(await client.delete(key))

        return await self.execute_with_retry(delete_operation)

    async def exists(self, key: str) -> bool:
        async def exists_operation():
            client = await self.get_redis_client()
            return bool(await client.exists(key))

        return await self.execute_with_retry(exists_operation)

    async def get_ttl(self, key: str) -> int:
        async def get_ttl_operation():
            client = await self.get_redis_client()
            return await client.ttl(key)

        return await self.execute_with_retry(get_ttl_operation)
    
    async def update_expiration(self, key: str, expiration: int) -> bool:
        async def update_expiration_operation():
            client = await self.get_redis_client()
            return bool(await client.expire(key, expiration))

        return await self.execute_with_retry(update_expiration_operation)

    async def close(self):
        if self._redis_client:
            try:
                # Set a short timeout for closing to avoid blocking
                await asyncio.wait_for(
                    self._redis_client.close(),
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                self.logger.warning("RedisCache: Timeout while closing Redis connection, ignoring")
            except Exception as e:
                self.logger.error(f"RedisCache: Error while closing Redis connection: {e}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
