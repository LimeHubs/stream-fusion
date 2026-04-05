import time
from fastapi import APIRouter, Request, Depends, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.security import APIKeyHeader
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import HTTP_303_SEE_OTHER
import secrets
import uuid
from datetime import timedelta

from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.services.postgresql.schemas.apikey_schemas import APIKeyCreate, APIKeyUpdate
from stream_fusion.utils.security.security_secret import SecretManager
from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.dao.peerkey_dao import PeerKeyDAO
from stream_fusion.services.postgresql.dao.debridcache_dao import DebridCacheDAO
from stream_fusion.services.postgresql.dao.torrentgroup_dao import TorrentGroupDAO
from stream_fusion.web.api.auth.schemas import UsageLogs, UsageLog
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.services.redis.redis_config import get_redis_dependency
from stream_fusion.utils.string_encoding import generate_csrf_token, verify_csrf_token
from stream_fusion.web.api.utils import ensure_uuid

router = APIRouter()

templates = Jinja2Templates(directory=settings.admin_template_dir)
# Expose the CSRF token generator as a Jinja2 global —
# base.html calls {% set csrf_token = csrf_token_gen() %} once per render.
templates.env.globals["csrf_token_gen"] = generate_csrf_token

from stream_fusion.version import get_version
try:
    templates.env.globals["app_version"] = get_version()
except Exception:
    templates.env.globals["app_version"] = "?"

_DEFAULT_SESSION_KEY = "331cbfe48117fcba53d09572b10d2fc293d86131dc51be46d8aa9843c2e9f48d"


def _masked(value) -> bool:
    return bool(value and str(value).strip())


def _build_config_view(s) -> dict:
    def proxy_host_only(url) -> str | None:
        if not url:
            return None
        try:
            from yarl import URL as YURL
            u = YURL(str(url))
            return f"{u.scheme}://{u.host}:{u.port}"
        except Exception:
            return str(url)[:80]

    return {
        "session_key_is_default": s.session_key == _DEFAULT_SESSION_KEY,
        "general": {
            "host": s.host,
            "port": s.port,
            "workers_count": s.workers_count,
            "use_https": s.use_https,
            "log_level": s.log_level.value,
            "debug": s.debug,
            "allow_debrid_download": s.allow_debrid_download,
            "download_service": s.download_service.value if s.download_service else None,
        },
        "security": {
            "session_key_is_default": s.session_key == _DEFAULT_SESSION_KEY,
            "security_hide_docs": s.security_hide_docs,
            "secret_api_key_set": _masked(s.secret_api_key),
            "config_secret_key_set": _masked(s.config_secret_key),
        },
        "debrid": [
            {"name": "Real-Debrid",    "token_set": _masked(s.rd_token),         "unique": s.rd_unique_account},
            {"name": "AllDebrid",      "token_set": _masked(s.ad_token),         "unique": s.ad_unique_account},
            {"name": "TorBox",         "token_set": _masked(s.tb_token),         "unique": s.tb_unique_account},
            {"name": "Premiumize",     "token_set": _masked(s.pm_token),         "unique": s.pm_unique_account},
            {"name": "Debrid-Link",    "token_set": _masked(s.dl_token),         "unique": s.dl_unique_account},
            {"name": "EasyDebrid",     "token_set": _masked(s.ed_token),         "unique": s.ed_unique_account},
            {"name": "Offcloud",       "token_set": _masked(s.oc_credentials),   "unique": s.oc_unique_account},
            {"name": "PikPak",         "token_set": _masked(s.pp_credentials),   "unique": s.pp_unique_account},
        ],
        "proxy": {
            "proxy_url": proxy_host_only(s.proxy_url),
            "playback_proxy": s.playback_proxy,
            "proxy_buffer_size_kb": s.proxy_buffer_size // 1024,
            "playback_limit_requests": s.playback_limit_requests,
            "playback_limit_seconds": s.playback_limit_seconds,
        },
        "peer": {
            "url": s.peer_streamfusion_url or None,
            "key_id_set": _masked(s.peer_streamfusion_key_id),
            "secret_set": _masked(s.peer_streamfusion_secret),
        },
        "indexers": [
            {"name": "Jackett",        "enabled": s.jackett_enable,        "url": f"{s.jackett_schema}://{s.jackett_host}:{s.jackett_port}", "key_set": _masked(s.jackett_api_key)},
            {"name": "u2p / Utopeer",  "enabled": _masked(s.utopeer_passkey),  "url": "wss://u2p.anhkagi.net",  "key_set": _masked(s.utopeer_passkey)},
            {"name": "C411",           "enabled": s.c411_enable,           "url": s.c411_url,      "key_set": _masked(s.c411_api_key) or _masked(s.c411_passkey)},
            {"name": "Torr9",          "enabled": s.torr9_enable,          "url": s.torr9_url,     "key_set": _masked(s.torr9_api_key)},
            {"name": "LaCale",         "enabled": s.lacale_enable,         "url": s.lacale_url,    "key_set": _masked(s.lacale_api_key)},
            {"name": "GénérationFree", "enabled": s.generationfree_enable, "url": s.generationfree_url, "key_set": _masked(s.generationfree_api_key)},
            {"name": "ABN",            "enabled": s.abn_enable,            "url": s.abn_url,        "key_set": _masked(s.abn_api_key)},
            {"name": "G3MINI",         "enabled": s.g3mini_enable,         "url": s.g3mini_url,     "key_set": _masked(s.g3mini_api_key)},
            {"name": "TheOldSchool",   "enabled": s.theoldschool_enable,   "url": s.theoldschool_url, "key_set": _masked(s.theoldschool_api_key)},
            {"name": "Zilean",         "enabled": True,                    "url": f"{s.zilean_schema}://{s.zilean_host}", "key_set": None},
        ],
        "tmdb": {
            "language": s.tmdb_language,
            "api_key_set": _masked(s.tmdb_api_key),
        },
        "redis": {
            "host": s.redis_host,
            "port": s.redis_port,
            "db": s.redis_db,
            "expiration_days": round(s.redis_expiration / 86400, 1),
            "password_set": _masked(s.redis_password),
        },
        "postgresql": {
            "host": s.pg_host,
            "port": s.pg_port,
            "database": s.pg_base,
            "user": s.pg_user,
            "pool_size": s.pg_pool_size,
            "max_overflow": s.pg_max_overflow,
        },
    }

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


def _fmt_date(value) -> str:
    """Format a Unix timestamp or datetime to 'DD MMM. YYYY HH:MM'."""
    from datetime import datetime as _dt
    if value is None or value in ('', 'None', '—'):
        return '—'
    if value == 'Unlimited':
        return 'Unlimited'
    if isinstance(value, (int, float)) and value > 0:
        return _dt.fromtimestamp(int(value)).strftime('%d %b. %Y %H:%M')
    if hasattr(value, 'strftime'):
        return value.strftime('%d %b. %Y %H:%M')
    return str(value)


def _fmt_relative(value) -> str:
    """Show relative time (il y a Xj) for recent dates, absolute for older ones."""
    import time as _t
    from datetime import datetime as _dt
    if value is None or value in ('', 'None', '—'):
        return '—'
    ts = None
    if isinstance(value, (int, float)) and value > 0:
        ts = int(value)
    elif hasattr(value, 'timestamp'):
        ts = int(value.timestamp())
    if ts is None:
        return '—'
    diff = int(_t.time()) - ts
    if diff < 60:
        return "À l'instant"
    if diff < 3600:
        return f"il y a {diff // 60} min"
    if diff < 86400:
        return f"il y a {diff // 3600}h"
    if diff < 7 * 86400:
        return f"il y a {diff // 86400}j"
    return _dt.fromtimestamp(ts).strftime('%d %b. %Y')


def _fmt_size(value) -> str:
    try:
        b = int(value)
    except (TypeError, ValueError):
        return '—'
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.2f} Go"
    if b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.1f} Mo"
    if b >= 1024:
        return f"{b / 1024:.0f} Ko"
    return f"{b} o"


templates.env.filters['fmt_date'] = _fmt_date
templates.env.filters['fmt_relative'] = _fmt_relative
templates.env.filters['fmt_size'] = _fmt_size


def redirect_to_login(request: Request):
    return RedirectResponse(
        url=custom_url_for("login_page")(request), status_code=HTTP_303_SEE_OTHER
    )


async def get_session_id_from_request(request: Request):
    session_id = request.session.get("session_id")
    if not session_id:
        return None
    return session_id


async def session_based_security(
    request: Request,
    session_id: str = Depends(get_session_id_from_request),
    redis_client=get_redis_dependency(),
):
    if not session_id:
        return redirect_to_login(request)

    secret_key = redis_client.get(session_id)
    if not secret_key:
        request.session.clear()
        return redirect_to_login(request)

    if not secrets.compare_digest(secret_key.decode(), secret.value):
        redis_client.delete(session_id)
        request.session.clear()
        return redirect_to_login(request)

    redis_client.expire(session_id, timedelta(hours=2))
    return True


def admin_context(request: Request, **kwargs) -> dict:
    """Base context for all admin templates — injects the CSRF token."""
    return {"request": request, "csrf_token": generate_csrf_token(), **kwargs}


async def require_csrf(request: Request):
    """
    Validates the CSRF token on admin POST routes.
    Reads the token from the form field 'csrf_token' or the 'X-CSRF-Token' header.
    """
    token = ""
    try:
        form = await request.form()
        token = form.get("csrf_token", "")
    except Exception:
        pass
    if not token:
        token = request.headers.get("X-CSRF-Token", "")
    if not verify_csrf_token(token):
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Invalid or expired CSRF token.")


# ── Auth ───────────────────────────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@router.post("/login")
async def login(
    request: Request,
    secret_key: str = Form(...),
    redis_client=get_redis_dependency(),
):
    if secrets.compare_digest(secret_key, secret.value):
        session_id = str(uuid.uuid4())
        redis_client.setex(session_id, timedelta(hours=2), secret_key)
        request.session["session_id"] = session_id
        logger.info("Admin login successful")
        return RedirectResponse(
            url=custom_url_for("dashboard")(request), status_code=HTTP_303_SEE_OTHER
        )
    logger.warning("Admin login attempt with invalid secret key")
    return templates.TemplateResponse(
        "login.html", {"request": request, "error": "Clé secrète invalide"}
    )


@router.get("/logout")
async def logout(request: Request, redis_client=get_redis_dependency()):
    session_id = request.session.get("session_id")
    if session_id:
        redis_client.delete(session_id)
    request.session.clear()
    return RedirectResponse(
        url=custom_url_for("login_page")(request), status_code=HTTP_303_SEE_OTHER
    )


# ── Dashboard ─────────────────────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    apikey_dao: APIKeyDAO = Depends(),
    db: AsyncSession = Depends(get_db_session),
    redis_client=get_redis_dependency(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    # API keys stats
    usage_stats = await apikey_dao.get_usage_stats()
    total_keys = len(usage_stats)
    active_keys = sum(1 for k in usage_stats if k.is_active)

    # Peer keys count — direct SQL to avoid session.begin() conflict
    try:
        r = await db.execute(text("SELECT COUNT(*) FROM peer_keys WHERE is_active = true"))
        active_peers = r.scalar() or 0
    except Exception:
        active_peers = 0

    # Debrid cache count
    try:
        result = await db.execute(text("SELECT COUNT(*) FROM debrid_cache"))
        debrid_cache_count = result.scalar() or 0
    except Exception:
        debrid_cache_count = 0

    # Torrent items count
    try:
        result = await db.execute(text("SELECT COUNT(*) FROM torrent_items"))
        torrent_count = result.scalar() or 0
    except Exception:
        torrent_count = 0

    # Matched/orphan torrents
    try:
        result = await db.execute(text("SELECT COUNT(*) FROM torrent_items WHERE tmdb_id IS NOT NULL"))
        torrent_matched_count = result.scalar() or 0
    except Exception:
        torrent_matched_count = 0
    torrent_orphan_count = torrent_count - torrent_matched_count

    # Redis status
    try:
        redis_ok = bool(redis_client.ping())
    except Exception:
        redis_ok = False

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "total_keys": total_keys,
        "active_keys": active_keys,
        "active_peers": active_peers,
        "debrid_cache_count": debrid_cache_count,
        "torrent_count": torrent_count,
        "torrent_matched_count": torrent_matched_count,
        "torrent_orphan_count": torrent_orphan_count,
        "redis_ok": redis_ok,
    })


# ── API Keys ──────────────────────────────────────────────────────────────────

@router.get("/api-keys", response_class=HTMLResponse)
async def list_api_keys(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    usage_stats = await apikey_dao.get_usage_stats()
    usage_logs = UsageLogs(
        logs=[
            UsageLog(
                api_key=key.api_key,
                is_active=key.is_active,
                never_expire=key.never_expire,
                expiration_date=(key.expiration_date if key.expiration_date else "Unlimited"),
                latest_query_date=(key.latest_query_date if key.latest_query_date else "None"),
                total_queries=key.total_queries,
                name=key.name if key.name else "JohnDoe",
                proxied_links=key.proxied_links,
            )
            for key in usage_stats
        ]
    )
    return templates.TemplateResponse(
        "api_keys.html", {"request": request, "logs": usage_logs.logs}
    )


@router.get("/create-api-key", response_class=HTMLResponse)
async def create_api_key_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    return templates.TemplateResponse("create_api_key.html", {"request": request})


@router.post("/create-api-key")
async def create_api_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    name: str = Form(None),
    never_expires: bool = Form(False),
    proxied_links: bool = Form(False),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    key = APIKeyCreate(name=name, never_expire=never_expires, proxied_links=proxied_links)
    await apikey_dao.create_key(key)
    logger.info(f"Admin: API key created (name={name})")
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


@router.post("/revoke-api-key")
async def revoke_api_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    api_key: str = Form(...),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    api_key_uuid = ensure_uuid(api_key)
    await apikey_dao.revoke_key(api_key_uuid)
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


@router.post("/renew-api-key")
async def renew_api_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    api_key: str = Form(...),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    api_key_uuid = ensure_uuid(api_key)
    try:
        await apikey_dao.renew_key(api_key_uuid)
    except Exception as e:
        logger.error(f"Admin: failed to renew API key: {e}")
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


@router.post("/delete-api-key")
async def delete_api_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    api_key: str = Form(...),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    api_key_uuid = ensure_uuid(api_key)
    try:
        await apikey_dao.delete_key(api_key_uuid)
    except Exception as e:
        logger.error(f"Admin: failed to delete API key: {e}")
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


@router.post("/toggle-proxied-links")
async def toggle_proxied_links(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    api_key: str = Form(...),
    apikey_dao: APIKeyDAO = Depends(),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    api_key_uuid = ensure_uuid(api_key)
    try:
        key_info = await apikey_dao.get_key_by_uuid(api_key_uuid)
        if key_info:
            current_value = getattr(key_info, "proxied_links", False)
            await apikey_dao.update_key(api_key_uuid, APIKeyUpdate(proxied_links=not current_value))
    except Exception as e:
        logger.error(f"Admin: failed to toggle proxied links: {e}")
    return RedirectResponse(
        url=custom_url_for("list_api_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


# ── Peer Keys ──────────────────────────────────────────────────────────────────

@router.get("/peer-keys", response_class=HTMLResponse)
async def list_peer_keys(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    dao = PeerKeyDAO(db)
    keys = await dao.list_keys()
    new_key = request.session.pop("new_peer_key", None)
    return templates.TemplateResponse(
        "peer_keys.html", {"request": request, "keys": keys, "new_key": new_key}
    )


@router.get("/create-peer-key", response_class=HTMLResponse)
async def create_peer_key_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    return templates.TemplateResponse("create_peer_key.html", {"request": request})


@router.post("/create-peer-key")
async def create_peer_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    name: str = Form(...),
    rate_limit: int = Form(60),
    rate_window: int = Form(60),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    dao = PeerKeyDAO(db)
    result = await dao.create_key(name=name, rate_limit=rate_limit, rate_window=rate_window)
    logger.success(f"Admin: peer key created for '{name}' ({result['key_id'][:8]}…)")
    request.session["new_peer_key"] = result
    return RedirectResponse(
        url=custom_url_for("list_peer_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


@router.post("/revoke-peer-key")
async def revoke_peer_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    key_id: str = Form(...),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    dao = PeerKeyDAO(db)
    await dao.revoke_key(key_id)
    return RedirectResponse(
        url=custom_url_for("list_peer_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


@router.post("/delete-peer-key")
async def delete_peer_key(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    key_id: str = Form(...),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    dao = PeerKeyDAO(db)
    await dao.delete_key(key_id)
    return RedirectResponse(
        url=custom_url_for("list_peer_keys")(request), status_code=HTTP_303_SEE_OTHER
    )


# ── Maintenance ────────────────────────────────────────────────────────────────

@router.get("/maintenance", response_class=HTMLResponse)
async def maintenance_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    return templates.TemplateResponse("maintenance.html", {"request": request})


@router.post("/maintenance/clean-expired-api-keys")
async def clean_expired_api_keys(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    days: int = Form(30),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    try:
        result = await db.execute(
            text("""
                DELETE FROM api_keys
                WHERE NOT never_expire
                  AND (latest_query_date IS NULL OR latest_query_date < EXTRACT(EPOCH FROM NOW())::BIGINT - :seconds)
                  AND expiration_date < EXTRACT(EPOCH FROM NOW())::BIGINT
            """),
            {"seconds": days * 86400},
        )
        await db.commit()
        count = result.rowcount
        logger.warning(f"Admin maintenance: deleted {count} expired API keys (inactive > {days} days)")
        return JSONResponse({"success": True, "deleted": count, "message": f"{count} clé(s) API supprimée(s)"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: clean-expired-api-keys failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/clean-expired-debrid-cache")
async def clean_expired_debrid_cache(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    try:
        dao = DebridCacheDAO(db)
        count = await dao.delete_expired()
        logger.warning(f"Admin maintenance: deleted {count} expired debrid cache entries")
        return JSONResponse({"success": True, "deleted": count, "message": f"{count} entrée(s) expirée(s) supprimée(s)"})
    except Exception as e:
        logger.error(f"Admin maintenance: clean-expired-debrid-cache failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/purge-debrid-cache")
async def purge_debrid_cache(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    try:
        result = await db.execute(text("DELETE FROM debrid_cache"))
        await db.commit()
        count = result.rowcount
        logger.warning(f"Admin maintenance: purged entire debrid cache ({count} rows)")
        return JSONResponse({"success": True, "deleted": count, "message": f"Cache debrid vidé ({count} entrées)"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: purge-debrid-cache failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/clean-old-torrents")
async def clean_old_torrents(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    days: int = Form(90),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    try:
        cutoff = int(time.time()) - (days * 86400)
        result = await db.execute(
            text("DELETE FROM torrent_items WHERE updated_at < :cutoff").bindparams(cutoff=cutoff)
        )
        await db.commit()
        count = result.rowcount
        logger.warning(f"Admin maintenance: deleted {count} torrent items not updated in {days} days")
        return JSONResponse({"success": True, "deleted": count, "message": f"{count} torrent(s) non mis à jour depuis {days} jours supprimé(s)"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: clean-old-torrents failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/flush-redis-all")
async def flush_redis_all(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    redis_client=get_redis_dependency(),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    try:
        redis_client.flushdb()
        logger.warning("Admin maintenance: Redis FLUSHDB executed")
        return JSONResponse({"success": True, "message": "Cache Redis entièrement vidé"})
    except Exception as e:
        logger.error(f"Admin maintenance: flush-redis-all failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/flush-redis-pattern")
async def flush_redis_pattern(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    pattern: str = Form(...),
    redis_client=get_redis_dependency(),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    # Whitelist des patterns autorisés pour éviter toute suppression accidentelle de session
    allowed_patterns = {
        "searchcache":      ["search:*"],
        "title_matching":   ["title_matching:*"],
        "rd_tokens":        ["rd_access_token:*"],
        "debrid_avail":     ["debrid_avail:*"],
        "debrid_avail_rd":  ["debrid_avail:realdebrid:*"],
        "debrid_avail_ad":  ["debrid_avail:alldebrid:*"],
        "debrid_avail_tb":  ["debrid_avail:torbox:*"],
        "debrid_avail_pm":  ["debrid_avail:premiumize:*"],
        "ratelimit_peer":   ["ratelimit:peer:*"],
        "bg_refresh":       ["bg_refresh:*"],
    }

    if pattern not in allowed_patterns:
        return JSONResponse({"success": False, "message": "Pattern non autorisé"}, status_code=400)

    try:
        total_deleted = 0
        for p in allowed_patterns[pattern]:
            keys = redis_client.keys(p)
            if keys:
                total_deleted += redis_client.delete(*keys)
        logger.warning(f"Admin maintenance: flushed Redis pattern '{pattern}' ({total_deleted} keys deleted)")
        return JSONResponse({"success": True, "deleted": total_deleted, "message": f"{total_deleted} clé(s) Redis supprimée(s) (pattern: {pattern})"})
    except Exception as e:
        logger.error(f"Admin maintenance: flush-redis-pattern failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/reset-api-key-counters")
async def reset_api_key_counters(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        result = await db.execute(text("UPDATE api_keys SET total_queries = 0, latest_query_date = NULL"))
        await db.commit()
        count = result.rowcount
        logger.warning(f"Admin maintenance: reset query counters for {count} API keys")
        return JSONResponse({"success": True, "message": f"Compteurs réinitialisés pour {count} clé(s) API"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: reset-api-key-counters failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/reset-peer-key-counters")
async def reset_peer_key_counters(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        result = await db.execute(text("UPDATE peer_keys SET total_queries = 0, last_used_at = NULL"))
        await db.commit()
        count = result.rowcount
        logger.warning(f"Admin maintenance: reset query counters for {count} peer keys")
        return JSONResponse({"success": True, "message": f"Compteurs réinitialisés pour {count} peer key(s)"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: reset-peer-key-counters failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/disable-expired-api-keys")
async def disable_expired_api_keys(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        result = await db.execute(
            text("""
                UPDATE api_keys SET is_active = false
                WHERE never_expire = false
                  AND expiration_date < EXTRACT(EPOCH FROM NOW())::BIGINT
                  AND is_active = true
            """)
        )
        await db.commit()
        count = result.rowcount
        logger.warning(f"Admin maintenance: disabled {count} expired API keys")
        return JSONResponse({"success": True, "updated": count, "message": f"{count} clé(s) API expirée(s) désactivée(s)"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: disable-expired-api-keys failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/disable-expired-peer-keys")
async def disable_expired_peer_keys(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        result = await db.execute(
            text("""
                UPDATE peer_keys SET is_active = false
                WHERE expires_at IS NOT NULL
                  AND expires_at < EXTRACT(EPOCH FROM NOW())::BIGINT
                  AND is_active = true
            """)
        )
        await db.commit()
        count = result.rowcount
        logger.warning(f"Admin maintenance: disabled {count} expired peer keys")
        return JSONResponse({"success": True, "updated": count, "message": f"{count} peer key(s) expirée(s) désactivée(s)"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: disable-expired-peer-keys failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/clean-orphan-torrents")
async def clean_orphan_torrents(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    days: int = Form(30),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        cutoff = int(time.time()) - (days * 86400)
        result = await db.execute(
            text("DELETE FROM torrent_items WHERE tmdb_id IS NULL AND created_at < :cutoff").bindparams(cutoff=cutoff)
        )
        await db.commit()
        count = result.rowcount
        logger.warning(f"Admin maintenance: deleted {count} orphan torrent items (tmdb_id IS NULL, older than {days} days)")
        return JSONResponse({"success": True, "deleted": count, "message": f"{count} torrent(s) orphelin(s) supprimé(s)"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: clean-orphan-torrents failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/migrate-u2p-indexer")
async def migrate_u2p_indexer(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    """Rename legacy u2p/Nostr indexer names to the canonical 'u2p - Cache'."""
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        legacy_names = ("Yggtorrent - API", "Nostr - Cache", "Ygg - Nostr", "Yggtorrent - Nostr")
        placeholders = ", ".join(f":n{i}" for i in range(len(legacy_names)))
        params = {f"n{i}": v for i, v in enumerate(legacy_names)}
        result = await db.execute(
            text(
                f"UPDATE torrent_items SET indexer = 'u2p - Cache'"
                f" WHERE indexer IN ({placeholders})"
            ).bindparams(**params)
        )
        await db.commit()
        count = result.rowcount
        logger.info(f"Admin maintenance: migrated {count} torrent_items to indexer='u2p - Cache' (from: {legacy_names})")
        return JSONResponse({"success": True, "updated": count, "message": f"{count} entrée(s) migrée(s) vers « u2p - Cache »"})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: migrate-u2p-indexer failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/group-by-infohash")
async def group_torrents_by_infohash(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    """Create/extend groups for all torrent_items that share the same info_hash."""
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        group_dao = TorrentGroupDAO(db)
        result = await group_dao.batch_group_by_info_hash()
        await db.commit()
        msg = (
            f"{result['groups_created']} groupe(s) créé(s), "
            f"{result['items_grouped']} torrent(s) groupé(s) par info_hash"
        )
        logger.info(f"Admin maintenance: group-by-infohash — {msg}")
        return JSONResponse({"success": True, **result, "message": msg})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: group-by-infohash failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/group-by-title-size")
async def group_torrents_by_title_size(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    """Create/extend groups for torrents with matching normalized title and similar size."""
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        group_dao = TorrentGroupDAO(db)
        result = await group_dao.batch_group_by_title_size()
        await db.commit()
        msg = (
            f"{result['groups_created']} groupe(s) créé(s), "
            f"{result['items_grouped']} torrent(s) groupé(s) par titre+taille"
        )
        logger.info(f"Admin maintenance: group-by-title-size — {msg}")
        return JSONResponse({"success": True, **result, "message": msg})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: group-by-title-size failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/propagate-tmdb-groups")
async def propagate_tmdb_to_groups(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    """Propagate TMDB IDs to all members of every existing group."""
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        group_dao = TorrentGroupDAO(db)
        result = await group_dao.propagate_tmdb_all_groups()
        await db.commit()
        msg = (
            f"{result['groups_updated']} groupe(s) mis à jour, "
            f"{result['items_updated']} torrent(s) avec TMDB ID propagé"
        )
        logger.info(f"Admin maintenance: propagate-tmdb-groups — {msg}")
        return JSONResponse({"success": True, **result, "message": msg})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: propagate-tmdb-groups failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/maintenance/cleanup-empty-groups")
async def cleanup_empty_groups(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    """Delete torrent groups that have no members."""
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        group_dao = TorrentGroupDAO(db)
        count = await group_dao.delete_empty_groups()
        await db.commit()
        msg = f"{count} groupe(s) vide(s) supprimé(s)"
        logger.info(f"Admin maintenance: cleanup-empty-groups — {msg}")
        return JSONResponse({"success": True, "deleted": count, "message": msg})
    except Exception as e:
        await db.rollback()
        logger.error(f"Admin maintenance: cleanup-empty-groups failed: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.get("/maintenance/stats")
async def maintenance_stats(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    db: AsyncSession = Depends(get_db_session),
    redis_client=get_redis_dependency(),
):
    """Return table row counts and Redis key counts as JSON for the maintenance stats panel."""
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    pg_stats = {}

    try:
        r = await db.execute(text("""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE is_active),
                   COUNT(*) FILTER (WHERE NOT never_expire AND expiration_date < EXTRACT(EPOCH FROM NOW())::BIGINT AND is_active)
            FROM api_keys
        """))
        row = r.fetchone()
        pg_stats["api_keys"] = {"total": row[0] or 0, "active": row[1] or 0, "expired_active": row[2] or 0}
    except Exception:
        pg_stats["api_keys"] = {"total": "?", "active": "?", "expired_active": "?"}

    try:
        r = await db.execute(text("""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE is_active),
                   COUNT(*) FILTER (WHERE expires_at IS NOT NULL AND expires_at < EXTRACT(EPOCH FROM NOW())::BIGINT AND is_active)
            FROM peer_keys
        """))
        row = r.fetchone()
        pg_stats["peer_keys"] = {"total": row[0] or 0, "active": row[1] or 0, "expired_active": row[2] or 0}
    except Exception:
        pg_stats["peer_keys"] = {"total": "?", "active": "?", "expired_active": "?"}

    try:
        r = await db.execute(text("""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE expires_at < EXTRACT(EPOCH FROM NOW())::BIGINT)
            FROM debrid_cache
        """))
        row = r.fetchone()
        pg_stats["debrid_cache"] = {"total": row[0] or 0, "expired": row[1] or 0}
    except Exception:
        pg_stats["debrid_cache"] = {"total": "?", "expired": "?"}

    try:
        cutoff_90d = int(time.time()) - 7776000
        r = await db.execute(
            text("""
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE tmdb_id IS NULL),
                       COUNT(*) FILTER (WHERE updated_at < :cutoff)
                FROM torrent_items
            """),
            {"cutoff": cutoff_90d},
        )
        row = r.fetchone()
        pg_stats["torrent_items"] = {"total": row[0] or 0, "orphans": row[1] or 0, "old_90d": row[2] or 0}
    except Exception:
        pg_stats["torrent_items"] = {"total": "?", "orphans": "?", "old_90d": "?"}

    try:
        r = await db.execute(text(
            "SELECT indexer, COUNT(*) AS cnt FROM torrent_items GROUP BY indexer ORDER BY cnt DESC LIMIT 5"
        ))
        pg_stats["top_indexers"] = [{"indexer": row[0], "count": row[1]} for row in r.fetchall()]
    except Exception:
        pg_stats["top_indexers"] = []

    try:
        sizes = {}
        for table in ["torrent_items", "debrid_cache"]:
            r = await db.execute(text(f"SELECT pg_size_pretty(pg_total_relation_size('{table}'))"))
            sizes[table] = r.scalar() or "?"
        pg_stats["table_sizes"] = sizes
    except Exception:
        pg_stats["table_sizes"] = {}

    try:
        r = await db.execute(text("SELECT COUNT(*) FROM metadata_mappings"))
        pg_stats["metadata_mappings"] = r.scalar() or 0
    except Exception:
        pg_stats["metadata_mappings"] = "?"

    try:
        r = await db.execute(text("""
            SELECT
                (SELECT COUNT(*) FROM torrent_groups) AS total_groups,
                (SELECT COUNT(*) FROM torrent_items WHERE group_id IS NOT NULL) AS grouped_items,
                (SELECT COUNT(*) FROM torrent_items WHERE group_id IS NULL) AS ungrouped_items,
                (SELECT COUNT(*) FROM torrent_groups WHERE tmdb_id IS NOT NULL) AS groups_with_tmdb
        """))
        row = r.fetchone()
        pg_stats["torrent_groups"] = {
            "total_groups": row[0] or 0,
            "grouped_items": row[1] or 0,
            "ungrouped_items": row[2] or 0,
            "groups_with_tmdb": row[3] or 0,
        }
    except Exception:
        pg_stats["torrent_groups"] = {
            "total_groups": "?",
            "grouped_items": "?",
            "ungrouped_items": "?",
            "groups_with_tmdb": "?",
        }

    redis_stats = {}
    try:
        info = redis_client.info("memory")
        redis_stats["used_memory_human"] = info.get("used_memory_human", "?")
        redis_stats["total_keys"] = redis_client.dbsize()
    except Exception:
        redis_stats = {"used_memory_human": "?", "total_keys": "?"}

    redis_patterns = {
        "searchcache":      ["search:*"],
        "title_matching":   ["title_matching:*"],
        "rd_tokens":        ["rd_access_token:*"],
        "debrid_avail":     ["debrid_avail:*"],
        "debrid_avail_rd":  ["debrid_avail:realdebrid:*"],
        "debrid_avail_ad":  ["debrid_avail:alldebrid:*"],
        "debrid_avail_tb":  ["debrid_avail:torbox:*"],
        "debrid_avail_pm":  ["debrid_avail:premiumize:*"],
        "ratelimit_peer":   ["ratelimit:peer:*"],
        "bg_refresh":       ["bg_refresh:*"],
    }
    key_counts = {}
    try:
        for name, patterns in redis_patterns.items():
            count = 0
            for pat in patterns:
                count += sum(1 for _ in redis_client.scan_iter(pat, count=500))
            key_counts[name] = count
        redis_stats["key_counts"] = key_counts
    except Exception:
        redis_stats["key_counts"] = {}

    return JSONResponse({"pg": pg_stats, "redis": redis_stats})


# ── Scheduler ─────────────────────────────────────────────────────────────────

@router.get("/scheduler", response_class=HTMLResponse)
async def scheduler_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    return templates.TemplateResponse("scheduled_tasks.html", {"request": request})


@router.get("/scheduler/status")
async def scheduler_status(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler is None:
        return JSONResponse({"error": "Scheduler non initialisé"}, status_code=503)
    return JSONResponse(scheduler.get_status())


@router.post("/scheduler/trigger/{job_id}")
async def scheduler_trigger(
    request: Request,
    job_id: str,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler is None:
        return JSONResponse({"success": False, "message": "Scheduler non initialisé"}, status_code=503)
    triggered = await scheduler.trigger_job(job_id)
    if triggered:
        logger.warning(f"Admin: manual trigger of scheduler job '{job_id}'")
        return JSONResponse({"success": True, "message": f"Job '{job_id}' déclenché manuellement"})
    return JSONResponse({"success": False, "message": f"Job '{job_id}' introuvable"}, status_code=404)


# ── Torrent search ────────────────────────────────────────────────────────────

@router.get("/search/hash", response_class=HTMLResponse)
async def search_hash_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    q: str = None,
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    results = []
    error = None
    if q:
        q = q.strip().lower()
        try:
            rows = await db.execute(
                text("SELECT * FROM torrent_items WHERE info_hash = :h ORDER BY created_at DESC").bindparams(h=q)
            )
            results = [dict(r) for r in rows.mappings().all()]
        except Exception as e:
            error = str(e)

    return templates.TemplateResponse("search_hash.html", {
        "request": request, "results": results, "query": q or "", "error": error,
    })


@router.get("/search/tmdb", response_class=HTMLResponse)
async def search_tmdb_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    q: str = None,
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    results = []
    error = None
    if q:
        try:
            tmdb_id = int(q.strip())
            rows = await db.execute(
                text("""
                    SELECT raw_title, size, info_hash, trackers, indexer,
                           seeders, languages, type, tmdb_id, created_at, updated_at
                    FROM torrent_items
                    WHERE tmdb_id = :tid
                    ORDER BY seeders DESC
                    LIMIT 500
                """).bindparams(tid=tmdb_id)
            )
            results = [dict(r) for r in rows.mappings().all()]
        except ValueError:
            error = "Le TMDB ID doit être un entier."
        except Exception as e:
            error = str(e)

    return templates.TemplateResponse("search_tmdb.html", {
        "request": request, "results": results, "query": q or "", "error": error,
    })


# ── Metadata Mappings ─────────────────────────────────────────────────────────

@router.get("/mappings", response_class=HTMLResponse)
async def list_mappings(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    from stream_fusion.services.postgresql.dao.metadatamapping_dao import MetadataMappingDAO
    dao = MetadataMappingDAO(db)
    mappings = await dao.list_all()
    return templates.TemplateResponse("mappings.html", {"request": request, "mappings": mappings})


@router.post("/mappings/create")
async def create_mapping(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    imdb_id: str = Form(...),
    tmdb_id: str = Form(None),
    title_override: str = Form(None),
    media_type: str = Form("series"),
    notes: str = Form(None),
    search_titles: str = Form(None),
    year_override: int = Form(None),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    from stream_fusion.services.postgresql.dao.metadatamapping_dao import MetadataMappingDAO
    dao = MetadataMappingDAO(db)
    parsed_search_titles = [t.strip() for t in search_titles.split(",") if t.strip()] if search_titles else None
    try:
        mapping = await dao.create(
            imdb_id=imdb_id.strip(),
            tmdb_id=tmdb_id.strip() if tmdb_id else None,
            title_override=title_override.strip() if title_override else None,
            media_type=media_type,
            notes=notes.strip() if notes else None,
            search_titles=parsed_search_titles,
            year_override=year_override,
        )
        logger.info(f"Admin: metadata mapping created for {imdb_id}")
        return JSONResponse({"success": True, "id": mapping.id})
    except Exception as e:
        logger.error(f"Admin: failed to create mapping: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/mappings/edit")
async def edit_mapping(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    mapping_id: int = Form(...),
    imdb_id: str = Form(...),
    tmdb_id: str = Form(None),
    title_override: str = Form(None),
    media_type: str = Form("series"),
    notes: str = Form(None),
    search_titles: str = Form(None),
    year_override: int = Form(None),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    from stream_fusion.services.postgresql.dao.metadatamapping_dao import MetadataMappingDAO
    dao = MetadataMappingDAO(db)
    parsed_search_titles = [t.strip() for t in search_titles.split(",") if t.strip()] if search_titles else None
    try:
        await dao.update(
            mapping_id=mapping_id,
            imdb_id=imdb_id.strip(),
            tmdb_id=tmdb_id.strip() if tmdb_id else None,
            title_override=title_override.strip() if title_override else None,
            media_type=media_type,
            notes=notes.strip() if notes else None,
            search_titles=parsed_search_titles,
            year_override=year_override,
        )
        logger.info(f"Admin: metadata mapping {mapping_id} updated")
        return JSONResponse({"success": True})
    except Exception as e:
        logger.error(f"Admin: failed to update mapping {mapping_id}: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.get("/config", response_class=HTMLResponse)
async def config_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    from stream_fusion.services.settings.settings_registry import REGISTRY_BY_CATEGORY, PAGE_CATEGORIES
    # Build summary: count of settings per sub-page
    page_counts = {
        page: sum(len(REGISTRY_BY_CATEGORY.get(cat, [])) for cat in cats)
        for page, cats in PAGE_CATEGORIES.items()
    }
    return templates.TemplateResponse(
        "config_page.html",
        {"request": request, "page_counts": page_counts},
    )


async def _get_settings_service(request: Request):
    from stream_fusion.services.settings.settings_service import SettingsService
    return request.app.state.settings_service


async def _config_subpage(
    request: Request,
    page_name: str,
    template_name: str,
    authenticated,
    extra_ctx: dict = None,
):
    """Shared logic for all config sub-pages (GET)."""
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    from stream_fusion.services.settings.settings_registry import (
        REGISTRY_BY_CATEGORY, PAGE_CATEGORIES, SETTINGS_REGISTRY,
    )
    svc = await _get_settings_service(request)
    effective = await svc.get_all_effective()
    cats = PAGE_CATEGORIES.get(page_name, [])
    registry_sections = {cat: REGISTRY_BY_CATEGORY.get(cat, []) for cat in cats}
    ctx = {
        "request": request,
        "effective": effective,
        "registry_sections": registry_sections,
        "page_name": page_name,
    }
    if extra_ctx:
        ctx.update(extra_ctx)
    return templates.TemplateResponse(template_name, ctx)


async def _config_subpage_post(request: Request, page_name: str, authenticated):
    """Shared logic for all config sub-pages (POST)."""
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    from stream_fusion.services.settings.settings_registry import (
        REGISTRY_BY_KEY, PAGE_CATEGORIES, REGISTRY_BY_CATEGORY,
    )
    svc = await _get_settings_service(request)
    form = await request.form()

    # Collect all keys that belong to this page's categories
    cats = PAGE_CATEGORIES.get(page_name, [])
    page_keys = {
        defn.key
        for cat in cats
        for defn in REGISTRY_BY_CATEGORY.get(cat, [])
    }

    updates: dict[str, str] = {}
    validation_errors: list[str] = []

    for key in page_keys:
        defn = REGISTRY_BY_KEY.get(key)
        if defn is None:
            continue
        raw = form.get(key)
        if raw is None:
            # Unchecked checkboxes don't appear in form data — treat as False
            if defn.type == "bool":
                updates[key] = "false"
            continue
        try:
            svc._coerce(key, str(raw))
            updates[key] = str(raw)
        except (ValueError, TypeError) as exc:
            validation_errors.append(f"{defn.label}: {exc}")

    if validation_errors:
        return JSONResponse({"success": False, "errors": validation_errors}, status_code=422)

    requires_restart = await svc.set_many(updates)
    logger.info(
        f"Admin: config/{page_name} — saved {len(updates)} setting(s), "
        f"{len(requires_restart)} require restart"
    )
    return JSONResponse({
        "success": True,
        "saved": len(updates),
        "requires_restart": requires_restart,
    })


# ── Config sub-pages ──────────────────────────────────────────────────────────

@router.get("/config/general", response_class=HTMLResponse)
async def config_general_get(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    return await _config_subpage(request, "general", "config_general.html", authenticated)


@router.post("/config/general")
async def config_general_post(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_post(request, "general", authenticated)


@router.get("/config/proxy", response_class=HTMLResponse)
async def config_proxy_get(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    return await _config_subpage(request, "proxy", "config_proxy.html", authenticated)


@router.post("/config/proxy")
async def config_proxy_post(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_post(request, "proxy", authenticated)


@router.get("/config/cache", response_class=HTMLResponse)
async def config_cache_get(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    return await _config_subpage(request, "cache", "config_cache.html", authenticated)


@router.post("/config/cache")
async def config_cache_post(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_post(request, "cache", authenticated)


@router.get("/config/indexers", response_class=HTMLResponse)
async def config_indexers_get(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    cfg = _build_config_view(settings)
    return await _config_subpage(request, "indexers", "config_indexers.html", authenticated,
                                  extra_ctx={"cfg_readonly": cfg})


@router.post("/config/indexers")
async def config_indexers_post(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_post(request, "indexers", authenticated)


@router.get("/config/tmdb", response_class=HTMLResponse)
async def config_tmdb_get(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    # Page TMDB fusionnée dans Indexeurs & TMDB
    return RedirectResponse(url=str(request.url_for("config_indexers_get")), status_code=301)


@router.post("/config/tmdb")
async def config_tmdb_post(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_post(request, "tmdb", authenticated)


@router.get("/config/system", response_class=HTMLResponse)
async def config_system_get(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    cfg = _build_config_view(settings)
    return await _config_subpage(
        request, "system", "config_system.html", authenticated,
        extra_ctx={"cfg_readonly": cfg},
    )


@router.post("/config/system")
async def config_system_post(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_post(request, "system", authenticated)


# ── Config reset routes ───────────────────────────────────────────────────────

async def _config_subpage_reset(request: Request, page_name: str, authenticated):
    """Restore all settings of a page to their original env-var defaults."""
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    from stream_fusion.services.settings.settings_registry import PAGE_CATEGORIES, REGISTRY_BY_CATEGORY
    svc = await _get_settings_service(request)
    cats = PAGE_CATEGORIES.get(page_name, [])
    keys = [
        defn.key
        for cat in cats
        for defn in REGISTRY_BY_CATEGORY.get(cat, [])
    ]
    requires_restart = await svc.reset_many(keys)
    logger.info(f"Admin: config/{page_name} — reset {len(keys)} setting(s) to env defaults")
    return JSONResponse({"success": True, "reset": len(keys), "requires_restart": requires_restart})


@router.post("/config/general/reset")
async def config_general_reset(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_reset(request, "general", authenticated)


@router.post("/config/proxy/reset")
async def config_proxy_reset(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_reset(request, "proxy", authenticated)


@router.post("/config/cache/reset")
async def config_cache_reset(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_reset(request, "cache", authenticated)


@router.post("/config/indexers/reset")
async def config_indexers_reset(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_reset(request, "indexers", authenticated)


@router.post("/config/tmdb/reset")
async def config_tmdb_reset(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_reset(request, "tmdb", authenticated)


@router.post("/config/system/reset")
async def config_system_reset(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    return await _config_subpage_reset(request, "system", authenticated)


@router.post("/mappings/delete")
async def delete_mapping(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    mapping_id: int = Form(...),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    from stream_fusion.services.postgresql.dao.metadatamapping_dao import MetadataMappingDAO
    dao = MetadataMappingDAO(db)
    try:
        await dao.delete(mapping_id)
        logger.info(f"Admin: metadata mapping {mapping_id} deleted")
        return JSONResponse({"success": True})
    except Exception as e:
        logger.error(f"Admin: failed to delete mapping {mapping_id}: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


# ── Matching & Language rules ──────────────────────────────────────────────────

@router.get("/matching/title-rules", response_class=HTMLResponse)
async def matching_title_rules(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    from stream_fusion.services.postgresql.dao.title_normalization_rule_dao import TitleNormalizationRuleDAO
    dao = TitleNormalizationRuleDAO(db)
    rules = await dao.get_all()
    grouped: dict[str, list] = {}
    for r in rules:
        grouped.setdefault(r.rule_type, []).append(r)
    return templates.TemplateResponse(
        "matching_title_rules.html",
        admin_context(request, grouped=grouped, rule_types=["substitution", "release_tag", "article", "ligature"]),
    )


@router.post("/matching/title-rules/create")
async def matching_title_rules_create(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
    rule_type: str = Form(...),
    pattern: str = Form(...),
    replacement: str = Form(""),
    description: str = Form(""),
    is_active: bool = Form(True),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    from stream_fusion.services.postgresql.dao.title_normalization_rule_dao import TitleNormalizationRuleDAO
    dao = TitleNormalizationRuleDAO(db)
    try:
        rule = await dao.create(
            rule_type=rule_type.strip(),
            pattern=pattern.strip(),
            replacement=replacement.strip(),
            description=description.strip() or None,
            is_active=is_active,
        )
        return JSONResponse({"success": True, "id": rule.id})
    except Exception as e:
        logger.error(f"Admin: failed to create title rule: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/matching/title-rules/update")
async def matching_title_rules_update(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
    rule_id: int = Form(...),
    rule_type: str = Form(...),
    pattern: str = Form(...),
    replacement: str = Form(""),
    description: str = Form(""),
    is_active: bool = Form(True),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    from stream_fusion.services.postgresql.dao.title_normalization_rule_dao import TitleNormalizationRuleDAO
    dao = TitleNormalizationRuleDAO(db)
    try:
        rule = await dao.update(
            rule_id=rule_id,
            rule_type=rule_type.strip(),
            pattern=pattern.strip(),
            replacement=replacement.strip(),
            description=description.strip() or None,
            is_active=is_active,
        )
        if not rule:
            return JSONResponse({"success": False, "message": "Règle introuvable"}, status_code=404)
        return JSONResponse({"success": True})
    except Exception as e:
        logger.error(f"Admin: failed to update title rule {rule_id}: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/matching/title-rules/delete")
async def matching_title_rules_delete(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
    rule_id: int = Form(...),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    from stream_fusion.services.postgresql.dao.title_normalization_rule_dao import TitleNormalizationRuleDAO
    dao = TitleNormalizationRuleDAO(db)
    try:
        await dao.delete(rule_id)
        return JSONResponse({"success": True})
    except Exception as e:
        logger.error(f"Admin: failed to delete title rule {rule_id}: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/matching/title-rules/reload")
async def matching_title_rules_reload(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        from stream_fusion.utils.filter.title_matching import get_normalizer
        await get_normalizer().reload()
        return JSONResponse({"success": True, "message": "Règles de normalisation rechargées"})
    except Exception as e:
        logger.error(f"Admin: failed to reload title rules: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


# ── Language rules ─────────────────────────────────────────────────────────────

@router.get("/matching/language-rules", response_class=HTMLResponse)
async def matching_language_rules(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    from stream_fusion.services.postgresql.dao.language_rule_dao import LanguageRuleDAO
    dao = LanguageRuleDAO(db)
    rules = await dao.get_all()
    grouped: dict[str, list] = {}
    for r in rules:
        grouped.setdefault(r.rule_type, []).append(r)
    return templates.TemplateResponse(
        "matching_language_rules.html",
        admin_context(
            request,
            grouped=grouped,
            rule_types=["french_pattern", "release_group", "code_mapping", "priority_group"],
        ),
    )


@router.post("/matching/language-rules/create")
async def matching_language_rules_create(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
    rule_type: str = Form(...),
    key: str = Form(...),
    value: str = Form(...),
    description: str = Form(""),
    is_active: bool = Form(True),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    from stream_fusion.services.postgresql.dao.language_rule_dao import LanguageRuleDAO
    dao = LanguageRuleDAO(db)
    try:
        rule = await dao.create(
            rule_type=rule_type.strip(),
            key=key.strip(),
            value=value.strip(),
            description=description.strip() or None,
            is_active=is_active,
        )
        return JSONResponse({"success": True, "id": rule.id})
    except Exception as e:
        logger.error(f"Admin: failed to create language rule: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/matching/language-rules/update")
async def matching_language_rules_update(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
    rule_id: int = Form(...),
    rule_type: str = Form(...),
    key: str = Form(...),
    value: str = Form(...),
    description: str = Form(""),
    is_active: bool = Form(True),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    from stream_fusion.services.postgresql.dao.language_rule_dao import LanguageRuleDAO
    dao = LanguageRuleDAO(db)
    try:
        rule = await dao.update(
            rule_id=rule_id,
            rule_type=rule_type.strip(),
            key=key.strip(),
            value=value.strip(),
            description=description.strip() or None,
            is_active=is_active,
        )
        if not rule:
            return JSONResponse({"success": False, "message": "Règle introuvable"}, status_code=404)
        return JSONResponse({"success": True})
    except Exception as e:
        logger.error(f"Admin: failed to update language rule {rule_id}: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/matching/language-rules/delete")
async def matching_language_rules_delete(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
    rule_id: int = Form(...),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    from stream_fusion.services.postgresql.dao.language_rule_dao import LanguageRuleDAO
    dao = LanguageRuleDAO(db)
    try:
        await dao.delete(rule_id)
        return JSONResponse({"success": True})
    except Exception as e:
        logger.error(f"Admin: failed to delete language rule {rule_id}: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.post("/matching/language-rules/reload")
async def matching_language_rules_reload(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        from stream_fusion.utils.filter.title_matching import get_lang_manager
        await get_lang_manager().reload()
        return JSONResponse({"success": True, "message": "Règles de langue rechargées"})
    except Exception as e:
        logger.error(f"Admin: failed to reload language rules: {e}")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


# ── Matching test page ─────────────────────────────────────────────────────────

@router.get("/matching/test", response_class=HTMLResponse)
async def matching_test_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    return templates.TemplateResponse("matching_test.html", admin_context(request))


@router.post("/matching/test")
async def matching_test_run(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    raw_title: str = Form(...),
    tmdb_titles: str = Form(...),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    try:
        from stream_fusion.utils.filter.title_matching import get_matcher
        matcher = get_matcher()
        titles = [t.strip() for t in tmdb_titles.splitlines() if t.strip()]
        result = matcher.analyze(raw_title.strip(), titles)
        return JSONResponse(result)
    except Exception as e:
        logger.error(f"Admin: matching test failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


# ── TMDB Mismatches ───────────────────────────────────────────────────────────

@router.get("/mismatches", response_class=HTMLResponse)
async def list_mismatches(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    from stream_fusion.services.postgresql.dao.mismatch_dao import TmdbMismatchDAO
    dao = TmdbMismatchDAO(db)
    mismatches = await dao.list_all()
    return templates.TemplateResponse(
        "mismatches.html", {"request": request, "mismatches": mismatches}
    )


@router.post("/mismatches/create")
async def create_mismatch(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    info_hash: str = Form(...),
    tmdb_id: int = Form(...),
    raw_title: str = Form(...),
    indexer: str = Form(...),
    notes: str = Form(None),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    from stream_fusion.services.postgresql.dao.mismatch_dao import TmdbMismatchDAO
    dao = TmdbMismatchDAO(db)
    result = await dao.create(
        info_hash=info_hash.strip().lower(),
        tmdb_id=tmdb_id,
        raw_title=raw_title.strip(),
        indexer=indexer.strip(),
        notes=notes.strip() if notes else None,
    )
    if result:
        logger.info(f"Admin: mismatch created for ({info_hash[:8]}…, tmdb_id={tmdb_id})")
        return JSONResponse({"success": True, "id": result.id})
    return JSONResponse({"error": "Déjà signalé ou erreur"}, status_code=409)


@router.post("/mismatches/delete")
async def delete_mismatch(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    mismatch_id: int = Form(...),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    from stream_fusion.services.postgresql.dao.mismatch_dao import TmdbMismatchDAO
    dao = TmdbMismatchDAO(db)
    deleted = await dao.delete(mismatch_id)
    if deleted:
        return JSONResponse({"success": True})
    return JSONResponse({"error": "Non trouvé"}, status_code=404)


# ── Unmatched Torrents ────────────────────────────────────────────────────────

@router.get("/search/unmatched", response_class=HTMLResponse)
async def search_unmatched_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    q: str = None,
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    from stream_fusion.services.postgresql.dao.torrentitem_dao import TorrentItemDAO
    dao = TorrentItemDAO(db)
    items = await dao.search_unmatched(query=q) if q else []
    return templates.TemplateResponse(
        "search_unmatched.html",
        {"request": request, "items": items, "query": q or ""},
    )


@router.post("/search/assign-tmdb")
async def assign_tmdb_to_selection(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    form = await request.form()
    tmdb_id_raw = form.get("tmdb_id", "").strip()
    info_hashes = [v.strip().lower() for v in form.getlist("info_hashes") if v.strip()]

    if not tmdb_id_raw or not tmdb_id_raw.isdigit():
        return JSONResponse({"error": "TMDB ID invalide"}, status_code=400)
    if not info_hashes:
        return JSONResponse({"error": "Aucun torrent sélectionné"}, status_code=400)

    from stream_fusion.services.postgresql.dao.torrentitem_dao import TorrentItemDAO
    dao = TorrentItemDAO(db)
    updated = await dao.assign_tmdb_by_info_hashes(info_hashes, int(tmdb_id_raw))
    logger.info(f"Admin: assigned tmdb_id={tmdb_id_raw} to {updated} torrents")
    return JSONResponse({"success": True, "updated": updated})


# ── Torrent Groups Browser ────────────────────────────────────────────────────

@router.get("/search/groups", response_class=HTMLResponse)
async def search_groups_page(
    request: Request,
    q: str = "",
    authenticated: bool = Depends(session_based_security),
    db: AsyncSession = Depends(get_db_session),
):
    """Admin page to browse torrent groups — search by TMDB ID, info_hash, or title."""
    if isinstance(authenticated, RedirectResponse):
        return authenticated

    results = []
    error = None
    query = q.strip()
    search_type = None

    if query:
        try:
            from stream_fusion.services.postgresql.dao.torrentgroup_dao import TorrentGroupDAO
            group_dao = TorrentGroupDAO(db)

            if query.isdigit():
                # TMDB ID
                search_type = "tmdb"
                results = await group_dao.search_groups_by_tmdb_id(int(query))
            elif len(query) == 40 and all(c in "0123456789abcdefABCDEF" for c in query):
                # info_hash
                search_type = "hash"
                results = await group_dao.search_groups_by_info_hash(query)
            else:
                # Titre libre
                search_type = "title"
                results = await group_dao.search_groups_by_title(query)
        except Exception as e:
            logger.error(f"Admin search_groups_page: {e}")
            error = str(e)

    return templates.TemplateResponse("search_groups.html", {
        "request": request,
        "query": query,
        "search_type": search_type,
        "results": results,
        "error": error,
    })


# ── TMDB Orphan Matcher ───────────────────────────────────────────────────────

@router.get("/tmdb-matcher", response_class=HTMLResponse)
async def tmdb_matcher_page(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return authenticated
    return templates.TemplateResponse("tmdb_matcher.html", {"request": request})


@router.get("/tmdb-matcher/history")
async def tmdb_matcher_history(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    run_id: str = None,
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    scheduler = getattr(request.app.state, "scheduler", None)
    tracker = getattr(scheduler, "_history_tracker", None) if scheduler else None

    if tracker is None:
        # Tracker not yet initialised (no run has occurred); build one on the fly
        from stream_fusion.services.tmdb_matcher.history import MatchHistoryTracker
        redis_pool = getattr(request.app.state, "redis_pool", None)
        if redis_pool is None:
            return JSONResponse({"error": "Redis non disponible"}, status_code=503)
        tracker = MatchHistoryTracker(redis_pool)

    if run_id:
        detail = tracker.get_run(run_id)
        if detail is None:
            return JSONResponse({"error": "Run introuvable"}, status_code=404)
        from dataclasses import asdict
        return JSONResponse(asdict(detail))

    from dataclasses import asdict
    summaries = tracker.list_runs()
    return JSONResponse([asdict(s) for s in summaries])


@router.post("/tmdb-matcher/revert")
async def tmdb_matcher_revert(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
    db: AsyncSession = Depends(get_db_session),
    info_hash: str = Form(...),
    tmdb_id: int = Form(...),
    raw_title: str = Form(""),
    indexer: str = Form(""),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)

    from stream_fusion.services.postgresql.dao.mismatch_dao import TmdbMismatchDAO
    dao = TmdbMismatchDAO(db)
    mismatch = await dao.create(
        info_hash=info_hash,
        tmdb_id=tmdb_id,
        raw_title=raw_title or info_hash,
        indexer=indexer or "auto_match",
        notes="Revert depuis l'historique TMDB matcher",
    )
    if mismatch:
        logger.warning(
            f"Admin: reverted TMDB match ({info_hash[:8]}…, tmdb_id={tmdb_id}) "
            f"via tmdb_matcher history"
        )
        return JSONResponse({"success": True, "message": "Match revert avec succès"})
    return JSONResponse(
        {"success": False, "message": "Revert échoué (déjà revert ou erreur DB)"},
        status_code=400,
    )


# ── u2p / Utopeer relay status ────────────────────────────────────────────────

# In-memory relay status cache (latency + online flag per URL, timestamp)
_relay_status_cache: dict | None = None
_relay_status_ts: float = 0.0
_RELAY_STATUS_TTL = 60.0   # seconds


async def _measure_relay(url: str, timeout: int = 3) -> dict:
    """Ping a relay WebSocket and return {url, online, latency_ms}."""
    import asyncio, time as _time
    try:
        import websockets
        t0 = _time.monotonic()
        async with websockets.connect(url, open_timeout=timeout, close_timeout=1):
            latency_ms = round((_time.monotonic() - t0) * 1000)
        return {"url": url, "online": True, "latency_ms": latency_ms}
    except Exception:
        return {"url": url, "online": False, "latency_ms": None}


async def _get_relay_status(force: bool = False) -> dict:
    """Return relay status dict, using in-memory cache unless force=True."""
    import asyncio, time as _time

    global _relay_status_cache, _relay_status_ts

    age = _time.monotonic() - _relay_status_ts
    if not force and _relay_status_cache is not None and age < _RELAY_STATUS_TTL:
        return {**_relay_status_cache, "cached": True}

    relay_urls = settings.utopeer_relay_urls
    results = await asyncio.gather(*[_measure_relay(u) for u in relay_urls])
    payload = {
        "relays": list(results),
        "checked_at": int(_time.time()),
        "cached": False,
    }
    _relay_status_cache = payload
    _relay_status_ts = _time.monotonic()
    return payload


@router.get("/utopeer/relay-status")
async def utopeer_relay_status(
    request: Request,
    authenticated: bool = Depends(session_based_security),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    payload = await _get_relay_status(force=False)
    return JSONResponse(payload)


@router.post("/utopeer/relay-refresh")
async def utopeer_relay_refresh(
    request: Request,
    authenticated: bool = Depends(session_based_security),
    _csrf: None = Depends(require_csrf),
):
    if isinstance(authenticated, RedirectResponse):
        return JSONResponse({"error": "Non authentifié"}, status_code=401)
    # Also invalidate the UtopeerService in-memory health cache
    try:
        import stream_fusion.utils.utopeer.utopeer_service as _svc
        _svc._relay_healthy = None
        _svc._relay_checked_at = 0.0
    except Exception:
        pass
    payload = await _get_relay_status(force=True)
    return JSONResponse(payload)
