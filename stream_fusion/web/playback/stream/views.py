import hashlib
import json
from urllib.parse import unquote
import redis.asyncio as redis
import asyncio
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from redis.exceptions import LockError
from fastapi.responses import RedirectResponse, StreamingResponse
from starlette.background import BackgroundTask

from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.redis.redis_config import get_redis_cache_dependency
from stream_fusion.utils.cache.local_redis import RedisCache
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.debrid.get_debrid_service import (
    get_debrid_service,
    get_download_service,
)
from stream_fusion.utils.debrid.realdebrid import RealDebrid
from stream_fusion.utils.debrid.torbox import Torbox
from stream_fusion.utils.debrid.debrid_exceptions import DebridError
from stream_fusion.utils.debrid.status_video import build_status_video_response, get_status_video_url
from stream_fusion.utils.parse_config import parse_config
from stream_fusion.utils.string_encoding import decodeb64
from stream_fusion.utils.security import check_api_key
from stream_fusion.web.utils import get_client_ip
from stream_fusion.web.playback.stream.schemas import (
    ErrorResponse,
    HeadResponse,
)

router = APIRouter()

redis_client = redis.Redis(
    host=settings.redis_host, port=settings.redis_port, db=settings.redis_db
)

DOWNLOAD_IN_PROGRESS_FLAG = "DOWNLOAD_IN_PROGRESS"



class ProxyStreamer:
    def __init__(self, request: Request, url: str, headers: dict):
        self.request = request
        self.url = url
        self.headers = headers
        self.response = None

    async def stream_content(self):
        async with self.request.app.state.http_session.get(
            self.url, headers=self.headers
        ) as self.response:
            async for chunk in self.response.content.iter_any():
                yield chunk

    async def close(self):
        if self.response:
            await self.response.release()
        logger.debug("Playback: Streaming connection closed")


async def handle_download(
    query: dict, config: dict, ip: str, redis_cache: RedisCache, debrid_session=None
) -> str:
    api_key = config.get("apiKey")
    cache_key = f"download:{api_key}:{json.dumps(query)}_{ip}"
    ready_cache_key = f"ready:{api_key}:{json.dumps(query)}_{ip}"
    if await redis_cache.get(ready_cache_key) == "READY":
        logger.info("Playback: File already marked as ready, checking for cached direct link")
        
        direct_link_cache_key = f"direct_link:{api_key}:{json.dumps(query)}_{ip}"
        cached_direct_link = await redis_cache.get(direct_link_cache_key)
        
        if cached_direct_link:
            logger.info("Playback: Direct link found in cache, returning immediately")
            return cached_direct_link
        
        debrid_service = get_download_service(config, debrid_session)
        if debrid_service:
            try:
                direct_link = await debrid_service.get_stream_link(query, config, ip)
                if direct_link and direct_link != settings.no_cache_video_url:
                    await redis_cache.set(direct_link_cache_key, direct_link, expiration=600)
                    logger.info("Playback: Direct link generated and cached")
                    return direct_link
            except Exception:
                pass

    download_flag = await redis_cache.get(cache_key)
    if download_flag == DOWNLOAD_IN_PROGRESS_FLAG:
        logger.info("Playback: Download in progress, checking if file is now ready")

        try:
            debrid_service = get_download_service(config, debrid_session)
            if debrid_service:
                try:
                    direct_link = await debrid_service.get_stream_link(query, config, ip)
                    if direct_link and direct_link != settings.no_cache_video_url:
                        logger.success("Playback: File is now ready! Clearing download flag and returning direct link")
                        await redis_cache.delete(cache_key)
                        ready_cache_key = f"ready:{api_key}:{json.dumps(query)}_{ip}"
                        await redis_cache.set(ready_cache_key, "READY", expiration=300)
                        direct_link_cache_key = f"direct_link:{api_key}:{json.dumps(query)}_{ip}"
                        await redis_cache.set(direct_link_cache_key, direct_link, expiration=600)
                        return direct_link
                except DebridError as debrid_err:
                    logger.warning(f"Playback: Debrid error in progress check {debrid_err.status_keys}, returning status video")
                    await redis_cache.delete(cache_key)
                    error_url = get_status_video_url(debrid_err.status_keys, default_key="UNKNOWN")
                    return RedirectResponse(url=error_url, status_code=302)
                except Exception as link_error:
                    logger.debug(f"Playback: File not ready yet: {str(link_error)}")
        except Exception as e:
            logger.warning(f"Playback: Error checking download status: {str(e)}")

        return settings.no_cache_video_url

    # Mark the start of the download
    await redis_cache.set(
        cache_key, DOWNLOAD_IN_PROGRESS_FLAG, expiration=600  
    )

    try:
        debrid_service = get_download_service(config, debrid_session)
        if not debrid_service:
            raise HTTPException(
                status_code=500, detail="Download service not available"
            )

        if isinstance(debrid_service, RealDebrid):
            torrent_id = await debrid_service.add_magnet_or_torrent_and_select(query, ip)
            logger.success(
                f"Playback: Added magnet or torrent to Real-Debrid: {torrent_id}"
            )
            if not torrent_id:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to add magnet or torrent to Real-Debrid",
                )
        elif isinstance(debrid_service, Torbox):
            magnet = query["magnet"]
            torrent_download = (
                unquote(query["torrent_download"])
                if query["torrent_download"] is not None
                else None
            )
            privacy = query.get("privacy", "private")
            torrent_info = await debrid_service.add_magnet_or_torrent(
                magnet, torrent_download, ip, privacy
            )
            logger.success(
                f"Playback: Added magnet or torrent to TorBox: {magnet[:50]}"
            )
            if not torrent_info:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to add magnet or torrent to TorBox",
                )
        else:
            magnet = query["magnet"]
            torrent_download = (
                unquote(query["torrent_download"])
                if query["torrent_download"] is not None
                else None
            )
            try:
                if await debrid_service.start_background_caching(magnet, query):
                    logger.success(
                        f"Playback: Started background caching for magnet: {magnet[:50]}"
                    )
                else:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to start background caching"
                    )
            except DebridError:
                raise
            except Exception as e:
                logger.error(f"Error starting background caching: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to start background caching: {str(e)}"
                )
        return settings.no_cache_video_url
    except Exception as e:
        await redis_cache.delete(cache_key)
        logger.error(f"Playback: Error handling download: {str(e)}", exc_info=True)
        # Check if torrent is banned (451 - Unavailable For Legal Reasons)
        if "451" in str(e):
            logger.warning(f"Playback: Torrent banned (451), returning banned video")
            return settings.banned_video_url
        if isinstance(e, DebridError):
            logger.warning(f"Playback: Debrid error {e.status_keys}, returning status video")
            error_url = get_status_video_url(e.status_keys, default_key="UNKNOWN")
            return RedirectResponse(url=error_url, status_code=302)
        raise e


async def get_stream_link(
    decoded_query: str, config: dict, ip: str, redis_cache: RedisCache, cache_user_identifier: str, stream_id: str = None, debrid_session=None
) -> str:
    logger.debug(f"Playback: Getting stream link for query: {decoded_query}, IP: {ip}")
    
    query = json.loads(decoded_query)
    if stream_id and query.get("type") == "series":
        cache_key = f"stream_link:{cache_user_identifier}:{stream_id}:{query.get('service', '')}"
        current_source_key = f"current_source:{cache_user_identifier}:{stream_id}:{query.get('service', '')}"
        logger.info(f"Playback: get_stream_link using series cache key: {cache_key}")
        logger.info(f"Playback: get_stream_link Stream ID: {stream_id}, Service: {query.get('service', '')}")
    else:
        cache_key = f"stream_link:{cache_user_identifier}:{decoded_query}"
        current_source_key = f"current_source:{cache_user_identifier}:{decoded_query}"
        logger.info(f"Playback: get_stream_link using fallback cache key: {cache_key}")

    magnet = query.get("magnet")
    info_hash = query.get("info_hash")
    service = query.get("service", False)
    
    if magnet or info_hash:
        source_info = {
            "magnet": magnet,
            "info_hash": info_hash,
            "raw_title": query.get("title", ""),
            "service": service,
            "indexer": query.get("indexer", "")
        }
        await redis_cache.set(current_source_key, source_info, expiration=1200)
        logger.debug(f"Playback: Stored current source for binge group: {magnet[:50] if magnet else info_hash}")

    cached_link = await redis_cache.get(cache_key)
    if cached_link:
        logger.info(f"Playback: Stream link found in cache: {cached_link}")
        return cached_link

    debrid_service = get_download_service(config, debrid_session)

    if not debrid_service:
        logger.error("Playback: No debrid service available")
        raise HTTPException(status_code=500, detail="No debrid service available")

    if service:
        logger.debug(f"Playback: Getting stream link from {service}")
        link = await debrid_service.get_stream_link(query, config, ip)
        
        if link is None:
            logger.warning("Playback: Debrid service returned None instead of a valid link")
            logger.warning(f"Playback: Query: {decoded_query}")
            logger.warning(f"Playback: Service: {service}")
            raise DebridError("Debrid service failed to provide a valid stream link", error_code="UNKNOWN")
    else:
        logger.error("Playback: Service not found in query")
        raise HTTPException(status_code=500, detail="Service not found in query")

    if link != settings.no_cache_video_url:
        logger.debug(f"Playback: Caching new stream link: {link}")
        await redis_cache.set(cache_key, link, expiration=1200)  # Cache for 20 minutes
        logger.info(f"Playback: New stream link generated and cached: {link}")
    else:
        logger.debug("Playback: Stream link not cached (NO_CACHE_VIDEO_URL)")
    return link


@router.api_route("/{config}/{query:path}", methods=["GET", "HEAD"], responses={500: {"model": ErrorResponse}})
async def get_playback(
    config: str,
    query: str,
    request: Request,
    redis_cache: RedisCache = Depends(get_redis_cache_dependency),
    apikey_dao: APIKeyDAO = Depends(),
):
    if request.method == "HEAD":
        return Response(status_code=200)
    try:
        config = parse_config(config)
        api_key = config.get("apiKey")
        ip = get_client_ip(request)
        cache_user_identifier = api_key if api_key else ip

        # Only validate the API key if it exists
        if api_key:
            try:
                await check_api_key(api_key, apikey_dao)
                logger.info(f"Playback: Valid API key provided by {ip}")
            except HTTPException as e:
                logger.warning(f"Playback: Invalid API key provided by {ip}. Error: {e.detail}")
                raise e # Re-raise if validation fails for a provided key
        else:
            logger.info(f"Playback: No API key provided by {ip}. Proceeding without API key validation.")

        if not query:
            logger.warning("Playback: Query is empty")
            raise HTTPException(status_code=400, detail="Query required.")

        decoded_query = decodeb64(query)
        logger.debug(f"Playback: Decoded query: {decoded_query}, Client IP: {ip}")

        query_dict = json.loads(decoded_query)
        logger.debug(f"Playback: Received playback request for query: {decoded_query}")
        service = query_dict.get("service", False)

        debrid_session = getattr(request.app.state, 'debrid_session', None)

        if service == "DL":
            result = await handle_download(query_dict, config, ip, redis_cache, debrid_session)
            if isinstance(result, Response):
                return result
            return RedirectResponse(url=result, status_code=status.HTTP_302_FOUND)

        # Extract stream_id from referer
        stream_id = None
        referer = request.headers.get("referer", "")
        if "/stream/" in referer:
            try:
                stream_id = referer.split("/stream/")[1].split("/")[-1].replace(".json", "")
            except:
                pass

        # Fast path: check cache before acquiring lock
        if stream_id and query_dict.get("type") == "series":
            fast_cache_key = f"stream_link:{cache_user_identifier}:{stream_id}:{query_dict.get('service', '')}"
        else:
            fast_cache_key = f"stream_link:{cache_user_identifier}:{decoded_query}"
        fast_cached = await redis_cache.get(fast_cache_key)
        if fast_cached:
            logger.info("Playback: Fast path cache hit, skipping lock")
            link = fast_cached
        else:
            # Use cache_user_identifier for lock key
            lock_key = f"lock:stream:{cache_user_identifier}:{decoded_query}"
            lock = redis_client.lock(lock_key, timeout=60)

            try:
                if await lock.acquire(blocking=False):
                    logger.debug("Playback: Lock acquired, getting stream link")
                    link = await get_stream_link(decoded_query, config, ip, redis_cache, cache_user_identifier, stream_id, debrid_session)
                else:
                    logger.debug("Playback: Lock not acquired, waiting for cached link")
                    for _ in range(60):
                        cached_link = await redis_cache.get(fast_cache_key)
                        if cached_link:
                            logger.debug("Playback: Cached link found while waiting")
                            link = cached_link
                            break
                        await asyncio.sleep(0.5)
                    else:
                        logger.warning("Playback: Timed out waiting for cached link")
                        raise HTTPException(
                            status_code=503,
                            detail="Service temporarily unavailable. Please try again.",
                        )
            finally:
                try:
                    await lock.release()
                    logger.debug("Playback: Lock released")
                except LockError:
                    logger.warning("Playback: Failed to release lock (already released)")

        # Vérifier si la proxification est activée pour cette clé API
        use_proxy = settings.proxied_link  # Valeur par défaut
        
        if api_key:
            try:
                # Récupérer les informations de la clé API
                api_key_info = await apikey_dao.get_key_by_uuid(api_key)
                if api_key_info and hasattr(api_key_info, 'proxied_links'):
                    use_proxy = api_key_info.proxied_links
                    logger.info(f"Playback: API key {api_key[:4] + '***' if api_key else None} has proxied_links={use_proxy}")
            except Exception as e:
                logger.error(f"Playback: Error checking API key proxification status: {e}")
        
        if not use_proxy:
            logger.debug(f"Playback: Redirecting to non-proxied link: {link}")
            return RedirectResponse(
                url=link, status_code=status.HTTP_302_FOUND
            )

        if service == "TB":  # TODO: Check this for torbox
            logger.debug("Playback: Bypass proxied link for TorBox")
            return RedirectResponse(url=link, status_code=status.HTTP_302_FOUND)

        logger.debug("Playback: Preparing to proxy stream")
        headers = {}
        range_header = request.headers.get("range")
        if range_header and "=" in range_header:
            logger.debug(f"Playback: Range header found: {range_header}")
            range_value = range_header.strip().split("=")[1]
            range_parts = range_value.split("-")
            start = int(range_parts[0]) if range_parts[0] else 0
            end = int(range_parts[1]) if len(range_parts) > 1 and range_parts[1] else ""
            headers["Range"] = f"bytes={start}-{end}"
            logger.debug(f"Playback: Range header set: {headers['Range']}")

        streamer = ProxyStreamer(request, link, headers)

        logger.debug(f"Playback: Initiating request to: {link}")
        async with request.app.state.http_session.head(
            link, headers=headers
        ) as response:
            logger.debug(f"Playback: Response status: {response.status}")
            stream_headers = {
                "Content-Type": "video/mp4",
                "Accept-Ranges": "bytes",
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Connection": "keep-alive",
                "Content-Disposition": "inline",
                "Access-Control-Allow-Origin": "*",
            }

            if response.status == 206:
                logger.debug("Playback: Partial content response")
                stream_headers["Content-Range"] = response.headers["Content-Range"]

            for header in ["Content-Length", "ETag", "Last-Modified"]:
                if header in response.headers:
                    stream_headers[header] = response.headers[header]
                    logger.debug(
                        f"Playback: Header set: {header}: {stream_headers[header]}"
                    )

            logger.debug("Playback: Preparing streaming response")
            return StreamingResponse(
                streamer.stream_content(),
                status_code=206 if "Range" in headers else 200,
                headers=stream_headers,
                background=BackgroundTask(streamer.close),
            )

    except DebridError as e:
        logger.warning(f"Playback: Debrid error {e.status_keys}, returning status video")
        error_url = get_status_video_url(e.status_keys, default_key="UNKNOWN")
        return RedirectResponse(url=error_url, status_code=302)
    except Exception as e:
        logger.error(f"Playback: Playback error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                detail="An error occurred while processing the request."
            ).model_dump(),
        )


# HEAD handler removed - FastAPI automatically handles HEAD requests
# by routing them to the GET handler (same behavior as Comet)
