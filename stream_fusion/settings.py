import enum
import multiprocessing
import os
import sys
from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from yarl import URL


class LogLevel(str, enum.Enum):
    """Possible log levels."""

    NOTSET = "NOTSET"
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    FATAL = "FATAL"


class DebridService(str, enum.Enum):
    """Possible debrid services."""

    RD = "Real-Debrid"
    AD = "AllDebrid"
    TB = "TorBox"
    PM = "Premiumize"
    DL = "Debrid-Link"
    ED = "EasyDebrid"
    OC = "Offcloud"
    PP = "PikPak"


class NoCacheVideoLanguages(str, enum.Enum):
    """Possible languages for which to not cache video results."""

    FR = "https://github.com/LimeDrive/stream-fusion/raw/refs/heads/develop/stream_fusion/static/videos/fr_download_video.mp4"
    EN = "https://github.com/LimeDrive/stream-fusion/raw/refs/heads/develop/stream_fusion/static/videos/en_download_video.mp4"

    @classmethod
    def get_url(cls, language):
        """Get the video URL for a given language."""
        return cls[language.upper()].value


def get_default_worker_count():
    """
    Calculate the default number of workers based on CPU cores.
    Returns the number of CPU cores multiplied by 2, with a minimum of 2 and a maximum of 10.
    """
    return min(max(multiprocessing.cpu_count() * 2, 2), 8)


def check_env_variable(var_name):
    value = os.getenv(var_name.upper())

    if value and isinstance(value, str) and len(value.strip()) >= 10:
        return True
    return False


class Settings(BaseSettings):
    """Settings for the application"""

    # STREAM-FUSION
    version_path: str = "/app/pyproject.toml"
    workers_count: int = get_default_worker_count()
    port: int = 8080
    host: str = "0.0.0.0"
    gunicorn_timeout: int = 180
    aiohttp_timeout: int = 7200
    session_key: str = Field(
        default_factory=lambda: os.getenv(
            "SESSION_KEY",
            "331cbfe48117fcba53d09572b10d2fc293d86131dc51be46d8aa9843c2e9f48d",
        )
    )
    use_https: bool = False
    download_service: DebridService | None = None
    # When False, download (non-cached) streams are hidden from Stremio results.
    # Set ALLOW_DEBRID_DOWNLOAD=false on public instances to prevent users from
    # triggering debrid downloads directly from Stremio.
    allow_debrid_download: bool = True
    # When True, a public self-registration page is available at /register for users to
    # generate their own API keys. Set ALLOW_PUBLIC_KEY_REGISTRATION=true on open/public
    # instances. Defaults to False (admin-managed keys only).
    allow_public_key_registration: bool = False
    no_cache_video_language: NoCacheVideoLanguages = NoCacheVideoLanguages.FR

    # PROXY
    proxied_link: bool = check_env_variable("RD_TOKEN") or check_env_variable(
        "AD_TOKEN"
    )
    proxy_url: str | URL | None = None
    playback_proxy: bool | None = (
        None  # If set, the link will be proxied through the given proxy.
    )
    proxy_buffer_size: int = 1024 * 1024
    playback_limit_requests: int = 60   # max requests per user per window
    playback_limit_seconds: int = 60    # window size in seconds
    # PEER CACHE (inter-instance sharing)
    peer_streamfusion_url: str = ""        # URL of the peer stream-fusion instance
    peer_streamfusion_key_id: str = ""     # key_id issued by the peer instance
    peer_streamfusion_secret: str = ""     # secret issued by the peer instance (256-bit hex)

    # REALDEBRID
    rd_token: str | None = None
    rd_unique_account: bool = check_env_variable("RD_TOKEN")
    rd_base_url: str = "https://api.real-debrid.com/rest"
    rd_api_version: str = "1.0"

    # ALLDEBRID
    ad_token: str | None = None
    ad_unique_account: bool = check_env_variable("AD_TOKEN")
    ad_user_app: str = "streamfusion"
    ad_user_ip: str | None = None
    ad_use_proxy: bool = check_env_variable("PROXY_URL")
    ad_base_url: str = "https://api.alldebrid.com"
    ad_api_version: str = "v4"

    # TORBOX
    tb_token: str | None = None
    tb_unique_account: bool = check_env_variable("TB_TOKEN")
    tb_base_url: str = "https://api.torbox.app"
    tb_api_version: str = "v1"

    # PREMIUMIZE
    pm_token: str | None = None
    pm_unique_account: bool = check_env_variable("PM_TOKEN")
    pm_base_url: str = "https://www.premiumize.me/api"
    
    # STREMTHRU
    stremthru_url: str = "https://stremthru.13377001.xyz"

    # LOGGING
    log_level: LogLevel = LogLevel.INFO
    log_path: str = "/app/config/logs/stream-fusion.log"
    log_redacted: bool = True

    # SECURITY
    secret_api_key: str | None = None
    security_hide_docs: bool = True
    # Fernet encryption key for config tokens embedded in addon URLs.
    # Must persist across restarts — changing this key invalidates all existing config URLs.
    # If not set, falls back to plain Base64 encoding (unencrypted).
    config_secret_key: str | None = None

    # POSTGRESQL_DB
    # TODO: Change the values, but break dev environment
    pg_host: str = "stremio-postgres"
    pg_port: int = 5432
    pg_user: str = "streamfusion"  # "stremio"
    pg_pass: str = "streamfusion"  # "stremio"
    pg_base: str = "streamfusion"
    pg_echo: bool = False
    pg_pool_size: int = 5
    pg_max_overflow: int = 5

    # REDIS
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 5
    redis_expiration: int = 604800  # 7 jours
    redis_password: str | None = None
    bg_refresh_indexer_ttl: int = 21600  # 6 heures — TTL du verrou Redis par indexeur pour le background refresh

    # SCHEDULER — nettoyage automatique de la base de données
    scheduler_enabled: bool = True
    scheduler_torrent_orphan_max_age_days: int = 7    # supprime torrent_items sans tmdb_id depuis N jours
    scheduler_debrid_cleanup_interval_hours: int = 6  # fréquence purge debrid_cache expiré
    scheduler_torrent_cleanup_interval_hours: int = 24 # fréquence purge + dédup torrent_items
    scheduler_keys_cleanup_interval_hours: int = 6    # fréquence désactivation api_keys/peer_keys expirées
    scheduler_tmdb_match_interval_hours: int = 6      # fréquence résolution orphelins TMDB
    scheduler_tmdb_match_batch_size: int = 500        # nb d'items traités par exécution

    # TMDB
    tmdb_api_key: str | None = None
    tmdb_language: str = "fr-FR"

    # JACKETT
    jackett_host: str = "jackett"
    jackett_schema: str = "http"
    jackett_port: int = 9117
    jackett_api_key: str | None = None
    jackett_enable: bool = check_env_variable("JACKETT_API_KEY")

    # SERVER-SIDE INDEXER ENABLE FLAGS
    # Controls whether an indexer is available on this server instance.
    # When False, the indexer is hidden from the config page and disabled for ALL users,
    # regardless of their personal config (even if they have their own credentials).
    # When True (default), users can enable the indexer and optionally provide their own
    # credentials — unless a server-side unique account is configured, in which case
    # the server credentials are used for everyone.
    c411_enable: bool = True
    torr9_enable: bool = True
    lacale_enable: bool = True
    generationfree_enable: bool = True
    abn_enable: bool = True
    g3mini_enable: bool = True
    theoldschool_enable: bool = True

    # ZILEAN DMM API
    zilean_schema: str = "https"
    zilean_host: str = "zileanfortheweebs.midnightignite.me" # use direct docker container name for internal communication, but can be overridden by environment variable ZILEAN_HOST
    zilean_port: int | None = None # default port is 8181, but can be overridden by environment variable ZILEAN_PORT
    
    # DEBRIDLINK
    dl_token: str | None = None
    dl_unique_account: bool = check_env_variable("DL_TOKEN")
    
    # EASYDEBRID
    ed_token: str | None = None
    ed_unique_account: bool = check_env_variable("ED_TOKEN")
    
    # OFFCLOUD
    oc_credentials: str | None = None
    oc_unique_account: bool = check_env_variable("OC_CREDENTIALS")
    
    # PIKPAK
    pp_credentials: str | None = None
    pp_unique_account: bool = check_env_variable("PP_CREDENTIALS")
    zilean_max_workers: int = 4
    zilean_pool_connections: int = 10
    zilean_api_pool_maxsize: int = 10
    zilean_max_retry: int = 3

    # U2P / UTOPEER — Nostr WebSocket relays
    utopeer_max_workers: int = 4
    utopeer_passkey: str | None = None
    utopeer_unique_account: bool = False
    # Relay list stored as newline-separated URLs (editable in admin)
    utopeer_relay_urls_raw: str = (
        "wss://u2p.anhkagi.net\n"
        "wss://u2prelais.eliottb.dev\n"
        "wss://relay.ygg.gratis\n"
        "wss://u2p.notrusted.me"
    )
    utopeer_relay_timeout: int = 8      # seconds per relay query
    utopeer_ping_timeout: int = 2       # seconds for health-check ping
    utopeer_relay_cache_ttl: int = 300  # seconds to cache healthy relay list

    @property
    def utopeer_relay_urls(self) -> list[str]:
        """Parse utopeer_relay_urls_raw into a list of WSS URLs."""
        return [u.strip() for u in self.utopeer_relay_urls_raw.splitlines() if u.strip()]

    # C411 TORZNAB
    c411_url: str = "https://c411.org"
    c411_api_key: str | None = None  # Env: C411_API_KEY — Torznab access key
    c411_passkey: str | None = None  # Env: C411_PASSKEY — passkey for announce URL
    c411_unique_account: bool = check_env_variable("C411_API_KEY")

    # TORR9 TORZNAB
    torr9_url: str = "https://api.torr9.net"
    torr9_api_key: str | None = None  # Env: TORR9_API_KEY
    torr9_unique_account: bool = check_env_variable("TORR9_API_KEY")

    # LACALE TORZNAB
    lacale_url: str = "https://la-cale.space/api/external/torznab"
    lacale_api_key: str | None = None  # Env: LACALE_API_KEY
    lacale_unique_account: bool = check_env_variable("LACALE_API_KEY")

    # GENERATIONFREE
    generationfree_url: str = "https://generation-free.org"
    generationfree_api_key: str | None = None
    generationfree_passkey: str | None = None          # Env: GENERATIONFREE_PASSKEY
    generationfree_unique_account: bool = check_env_variable("GENERATIONFREE_API_KEY")

    # ABN (Abnormal)
    abn_url: str = "https://abn.lol"
    abn_api_url: str = "https://api.abn.lol"
    abn_api_key: str | None = None                     # Env: ABN_API_KEY
    abn_passkey: str | None = None                     # Env: ABN_PASSKEY (announce URL)
    abn_unique_account: bool = check_env_variable("ABN_API_KEY")

    # G3MINI (Gemini Tracker)
    g3mini_url: str = "https://gemini-tracker.org"
    g3mini_api_key: str | None = None                  # Env: G3MINI_API_KEY
    g3mini_passkey: str | None = None                  # Env: G3MINI_PASSKEY
    g3mini_unique_account: bool = check_env_variable("G3MINI_API_KEY")

    # THEOLDSCHOOL
    theoldschool_url: str = "https://theoldschool.cc"
    theoldschool_api_key: str | None = None            # Env: THEOLDSCHOOL_API_KEY
    theoldschool_passkey: str | None = None            # Env: THEOLDSCHOOL_PASSKEY
    theoldschool_unique_account: bool = check_env_variable("THEOLDSCHOOL_API_KEY")

    # ADMIN
    admin_template_dir: str = "/app/stream_fusion/static/admin"

    # DEVELOPMENT
    debug: bool = False
    dev_host: str = "0.0.0.0"
    dev_port: int = 8080
    develop: bool = False
    reload: bool = False

    @field_validator("proxy_url")
    @classmethod
    def validate_and_create_proxy_url(cls, v: str | None) -> URL | None:
        if v is None:
            return None

        v = v.strip("\"'")
        if not v.startswith(("http://", "https://")):
            v = "http://" + v
        try:
            return URL(v)
        except ValueError as e:
            raise ValueError(f"Invalid proxy URL: {e}")

    @property
    def pg_url(self) -> URL:
        """
        Assemble database URL from settings.

        :return: database URL.
        """
        return URL.build(
            scheme="postgresql+asyncpg",
            host=self.pg_host,
            port=self.pg_port,
            user=self.pg_user,
            password=self.pg_pass,
            path=f"/{self.pg_base}",
        )

    @property
    def jackett_url(self) -> URL:
        """
        Assemble Jackett URL from settings.
        :return: Jackett URL.
        """
        url = URL.build(
            scheme=self.jackett_schema,
            host=self.jackett_host,
            port=self.jackett_port,
        )
        if self.jackett_api_key:
            url = url.with_query({"apikey": self.jackett_api_key})
        return url

    @property
    def zilean_url(self) -> URL:
        """
        Assemble Zilean URL from settings.
        :return: Zilean URL.
        """
        return URL.build(
            scheme=self.zilean_schema,
            host=self.zilean_host,
            port=self.zilean_port,
        )

    @property
    def redis_url(self) -> URL:
        """
        Assemble Redis URL from settings.
        :return: Redis URL.
        """
        url = URL.build(
            scheme="redis",
            host=self.redis_host,
            port=self.redis_port,
        )
        if self.redis_password:
            url = url.with_password(self.redis_password)
        return url

    model_config = SettingsConfigDict(
        env_file=".env",
        secrets_dir="/run/secrets" if os.path.isdir("/run/secrets") else None,
        env_file_encoding="utf-8",
    )

    @property
    def no_cache_video_url(self) -> str:
        """
        Get the URL for the no-cache video based on the selected language.
        """
        return self.no_cache_video_language.value

    @property
    def banned_video_url(self) -> str:
        """
        Get the URL for the banned video when torrent is unavailable for legal reasons.
        """
        return "https://raw.githubusercontent.com/Telkaoss/stream-fusion/refs/heads/master/stream_fusion/static/videos/banned_error.mp4"

    @property
    def slots_full_video_url(self) -> str:
        """
        Get the URL for the slots full video when TorBox slots are full.
        """
        return "https://raw.githubusercontent.com/Telkaoss/stream-fusion/refs/heads/master/stream_fusion/static/videos/slots_full.mp4"


_DEFAULT_SESSION_KEY = "331cbfe48117fcba53d09572b10d2fc293d86131dc51be46d8aa9843c2e9f48d"

try:
    settings = Settings()
except ValidationError as e:
    raise RuntimeError(f"Configuration validation error: {e}")

if settings.session_key == _DEFAULT_SESSION_KEY:
    print(
        "WARNING: SESSION_KEY is using the insecure default value. "
        "Set the SESSION_KEY environment variable to a unique secret.",
        file=sys.stderr,
    )
