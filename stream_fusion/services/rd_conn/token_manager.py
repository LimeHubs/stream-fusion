import hashlib
import aiohttp
import redis
import requests

from stream_fusion.settings import settings
from stream_fusion.logging_config import logger


class RealDebridService:
    BASE_URL = "https://api.real-debrid.com/oauth/v2"
    CLIENT_ID = "X245A4XAIBGVM"  # Client ID for open source apps

    async def get_device_code(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.BASE_URL}/device/code",
                params={"client_id": self.CLIENT_ID, "new_credentials": "yes"},
            ) as response:
                return await response.json()

    async def get_credentials(self, device_code):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.BASE_URL}/device/credentials",
                params={"client_id": self.CLIENT_ID, "code": device_code},
            ) as response:
                if response.status == 200:
                    return await response.json()
                return {"error": "Authorization pending"}

    async def get_token(self, client_id, client_secret, device_code):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.BASE_URL}/token",
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "code": device_code,
                    "grant_type": "http://oauth.net/grant_type/device/1.0",
                },
            ) as response:
                return await response.json()


class RDTokenManager:
    BASE_URL = "https://api.real-debrid.com/oauth/v2"

    def __init__(self, config):
        self.redis = redis.Redis(host=settings.redis_host, port=settings.redis_port, db=settings.redis_db)
        self.config = config
        self.rd_config = self.config.get("RDToken", {})
        if not self.rd_config:
            raise Exception("Real Debrid configuration not found")

        self.client_id = self.rd_config.get("client_id")
        self.client_secret = self.rd_config.get("client_secret")
        self.refresh_token = self.rd_config.get("refresh_token")
        self.apikey = self.config.get("apiKey")
        self.logger = logger

        if not all(
            [self.client_id, self.client_secret, self.refresh_token, self.apikey]
        ):
            raise Exception("Missing required Real Debrid configuration")

        self.token_key = self.generate_token_key()
        self.logger.info("RDTokenManager initialized successfully")

    def generate_token_key(self):
        unique_string = f"{self.client_id}:{self.client_secret}:{self.refresh_token}"
        return f"rd_access_token:{hashlib.sha256(unique_string.encode()).hexdigest()}"

    async def get_access_token(self):
        token = self.redis.get(self.token_key)
        if token:
            self.logger.debug("Access token found in Redis")
            return token.decode("utf-8")
        self.logger.info("Access token not found in Redis, generating new token")
        return await self.new_access_token()

    async def new_access_token(self):
        self.logger.info("Requesting new access token from Real Debrid")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.BASE_URL}/token",
                    data={
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                        "code": self.refresh_token,
                        "grant_type": "http://oauth.net/grant_type/device/1.0",
                    },
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    self.logger.info("New access token received successfully")
                    self.redis.setex(self.token_key, 43200, data["access_token"])
                    self.logger.debug(
                        f"Access token stored in Redis with expiry: {data['expires_in']} seconds"
                    )
                    return data["access_token"]
        except aiohttp.ClientError as e:
            self.logger.error(f"Error requesting new access token: {str(e)}")
            raise
