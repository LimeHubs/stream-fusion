import hashlib
import time
from fastapi import APIRouter, Depends, HTTPException, Request
from uuid import UUID
import asyncio


from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.dao.torrentitem_dao import TorrentItemDAO
from stream_fusion.services.redis.redis_config import get_redis_cache_dependency
from stream_fusion.utils.cache.cache import search_public
from stream_fusion.utils.cache.local_redis import RedisCache
from stream_fusion.utils.debrid.get_debrid_service import get_all_debrid_services
from stream_fusion.utils.filter.results_per_quality_filter import (
    ResultsPerQualityFilter,
)
from stream_fusion.utils.filter_results import (
    filter_items,
    merge_items,
    sort_items,
)
from stream_fusion.logging_config import logger
from stream_fusion.utils.jackett.jackett_result import JackettResult
from stream_fusion.utils.jackett.jackett_service import JackettService
from stream_fusion.utils.parser.parser_service import StreamParser
from stream_fusion.utils.sharewood.sharewood_service import SharewoodService
from stream_fusion.utils.yggfilx.yggflix_service import YggflixService
from stream_fusion.utils.yggfilx.yggflix_result import YggflixResult
from stream_fusion.utils.metdata.cinemeta import Cinemeta
from stream_fusion.utils.metdata.tmdb import TMDB
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series
from stream_fusion.utils.parse_config import parse_config
from stream_fusion.utils.security.security_api_key import check_api_key
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.web.root.search.schemas import SearchResponse, Stream
from stream_fusion.utils.torrent.torrent_service import TorrentService
from stream_fusion.utils.torrent.torrent_smart_container import TorrentSmartContainer
from stream_fusion.utils.zilean.zilean_result import ZileanResult
from stream_fusion.utils.zilean.zilean_service import ZileanService
from stream_fusion.utils.c411.c411_service import C411Service
from stream_fusion.utils.c411.c411_result import C411Result as C411SearchResult
from stream_fusion.utils.torr9.torr9_service import Torr9Service
from stream_fusion.utils.torr9.torr9_result import Torr9Result as Torr9SearchResult
from stream_fusion.utils.lacale.lacale_service import LaCaleService
from stream_fusion.utils.lacale.lacale_result import LaCaleResult as LaCaleSearchResult
from stream_fusion.utils.generationfree.generationfree_service import GenerationFreeService
from stream_fusion.utils.generationfree.generationfree_result import GenerationFreeResult as GenerationFreeSearchResult
from stream_fusion.settings import settings
from stream_fusion.web.utils import get_client_ip


router = APIRouter()


async def full_prefetch_from_cache(media, config, redis_cache, stream_cache_key, get_metadata, stream_type, debrid_services, torrent_dao, request):
    try:
        await asyncio.sleep(1.0)
        http_session = getattr(request.app.state, 'http_session', None)

        current_season_num = int(media.season.replace("S", ""))
        current_episode_num = int(media.episode.replace("E", ""))
        next_episode_num = current_episode_num + 1

        next_episode_id = f"{media.id.split(':')[0]}:{current_season_num}:{next_episode_num}"

        next_media_mock = type(media)(
            id=next_episode_id,
            tmdb_id=media.tmdb_id,
            titles=media.titles,
            season=f"S{current_season_num:02d}",
            episode=f"E{next_episode_num:02d}",
            languages=media.languages
        )

        next_stream_key = stream_cache_key(next_media_mock)
        cached_next = await redis_cache.get(next_stream_key)

        if cached_next is None:
            logger.debug(f"Pre-fetch: Starting full background search for next episode {next_episode_id}")

            async def fetch_next_metadata():
                return await get_metadata(next_episode_id, stream_type)

            next_media = await asyncio.wait_for(
                redis_cache.get_or_set(fetch_next_metadata, next_episode_id, stream_type, config["metadataProvider"]),
                timeout=5.0
            )

            search_results = []
            postgres_results = []

            background_session = request.app.state.db_session_factory()
            try:
                background_torrent_dao = TorrentItemDAO(background_session)
                torrent_service = TorrentService(config, background_torrent_dao)

                if hasattr(next_media, 'tmdb_id') and next_media.tmdb_id:
                    try:
                        postgres_items = await background_torrent_dao.search_by_tmdb_id(int(next_media.tmdb_id))
                        if postgres_items:
                            logger.debug(f"Pre-fetch: Found {len(postgres_items)} results from Postgres for TMDB ID {next_media.tmdb_id}")
                            for db_item in postgres_items:
                                if db_item.indexer in ['Yggtorrent - API', 'C411 - API', 'Torr9 - API']:
                                    postgres_results.append(db_item.to_torrent_item())
                            postgres_results = filter_items(postgres_results, next_media, config=config)
                            logger.debug(f"Pre-fetch: After filtering: {len(postgres_results)} Postgres results for {next_media.season}{next_media.episode}")
                    except Exception as pg_error:
                        logger.debug(f"Pre-fetch: Postgres search failed: {str(pg_error)}")

                if config["zilean"]:
                    zilean_service = ZileanService(config, session=http_session)
                    zilean_search_results = await zilean_service.search(next_media)
                    if zilean_search_results:
                        zilean_search_results = [
                            ZileanResult().from_api_cached_item(torrent, next_media)
                            for torrent in zilean_search_results
                            if len(getattr(torrent, "info_hash", "")) == 40
                        ]
                        zilean_search_results = filter_items(zilean_search_results, next_media, config=config)
                        zilean_search_results = await torrent_service.convert_and_process(zilean_search_results)
                        search_results = merge_items(search_results, zilean_search_results)

                if config.get("c411") and settings.c411_enable:
                    try:
                        c411_service = C411Service(config, session=http_session)
                        c411_results = await c411_service.search(next_media)
                        if c411_results:
                            c411_results = [
                                C411SearchResult().from_api_item(item, next_media)
                                for item in c411_results
                                if getattr(item, "info_hash", None) and len(item.info_hash) == 40
                            ]
                            c411_results = await torrent_service.convert_and_process(c411_results)
                            search_results = merge_items(search_results, c411_results)
                            logger.debug(f"Pre-fetch: C411 API: {len(c411_results)} results")
                    except Exception as e:
                        logger.debug(f"Pre-fetch: C411 search failed: {e}")

                if config.get("torr9") and settings.torr9_enable:
                    try:
                        torr9_service = Torr9Service(config, session=http_session)
                        torr9_results = await torr9_service.search(next_media)
                        if torr9_results:
                            torr9_results = [
                                Torr9SearchResult().from_api_item(item, next_media)
                                for item in torr9_results
                                if getattr(item, "info_hash", None) and len(item.info_hash) == 40
                            ]
                            torr9_results = await torrent_service.convert_and_process(torr9_results)
                            search_results = merge_items(search_results, torr9_results)
                            logger.debug(f"Pre-fetch: Torr9 API: {len(torr9_results)} results")
                    except Exception as e:
                        logger.debug(f"Pre-fetch: Torr9 search failed: {e}")

                if config.get("lacale") and settings.lacale_enable:
                    try:
                        lacale_service = LaCaleService(config, session=http_session)
                        lacale_results = await lacale_service.search(next_media)
                        if lacale_results:
                            lacale_results = await torrent_service.convert_and_process(lacale_results)
                            search_results = merge_items(search_results, lacale_results)
                            logger.debug(f"Pre-fetch: LaCale API: {len(lacale_results)} results")
                    except Exception as e:
                        logger.debug(f"Pre-fetch: LaCale search failed: {e}")

                if config.get("generationfree"):
                    try:
                        generationfree_service = GenerationFreeService(config, session=http_session)
                        generationfree_results = await generationfree_service.search(next_media)
                        if generationfree_results:
                            generationfree_results = await torrent_service.convert_and_process(generationfree_results)
                            search_results = merge_items(search_results, generationfree_results)
                            logger.debug(f"Pre-fetch: GenerationFree API: {len(generationfree_results)} results")
                    except Exception as e:
                        logger.debug(f"Pre-fetch: GenerationFree search failed: {e}")

                if postgres_results:
                    search_results = merge_items(postgres_results, search_results)
                    logger.debug(f"Pre-fetch: Merged {len(postgres_results)} Postgres + {len(search_results)} external = {len(search_results)} total")

                if search_results:
                    # Sort by indexer priority (C411/Torr9=1 before Yggtorrent=2)
                    # so ResultsPerQualityFilter keeps complete packs first
                    search_results = filter_items(search_results, next_media, config=config)
                    filtered_results = ResultsPerQualityFilter(config).filter(search_results)
                    torrent_smart_container = TorrentSmartContainer(filtered_results, next_media)

                    for debrid in debrid_services:
                        hashes = torrent_smart_container.get_unaviable_hashes()
                        ip = get_client_ip(request)
                        result = await debrid.get_availability_bulk(hashes, ip)
                        if result:
                            torrent_smart_container.update_availability(result, type(debrid), next_media)

                    if config["cache"]:
                        asyncio.create_task(asyncio.to_thread(torrent_smart_container.cache_container_items))

                    best_matching_results = torrent_smart_container.get_best_matching()
                    best_matching_results = sort_items(best_matching_results, config)

                    parser = StreamParser(config)
                    stream_list = await parser.parse_to_stremio_streams(best_matching_results, next_media)
                    next_stream_objects = [Stream(**stream) for stream in stream_list]

                    await redis_cache.set(stream_cache_key(next_media), next_stream_objects, expiration=1200)
                    logger.success(f"Pre-fetch: Successfully background pre-cached {len(next_stream_objects)} streams for episode {next_episode_id}")
                else:
                    logger.debug(f"Pre-fetch: No results found for episode {next_episode_id}")
                    
            finally:
                await background_session.commit()
                await background_session.close()
                
        else:
            logger.debug(f"Pre-fetch: Next episode {next_episode_id} already cached")
            
    except Exception as e:
        logger.debug(f"Pre-fetch: Error during full background pre-fetch: {str(e)}")


async def simple_prefetch_next_episode(media, config, redis_cache, stream_cache_key, get_metadata, stream_type):
    try:
        await asyncio.sleep(0.5)
        
        current_season_num = int(media.season.replace("S", ""))
        current_episode_num = int(media.episode.replace("E", ""))
        next_episode_num = current_episode_num + 1
        
        next_episode_id = f"{media.id.split(':')[0]}:{current_season_num}:{next_episode_num}"
        
        next_media_mock = type(media)(
            id=next_episode_id,
            tmdb_id=media.tmdb_id,
            titles=media.titles,
            season=f"S{current_season_num:02d}",
            episode=f"E{next_episode_num:02d}",
            languages=media.languages
        )
        
        next_stream_key = stream_cache_key(next_media_mock)
        cached_next = await redis_cache.get(next_stream_key)
        
        if cached_next is None:
            logger.debug(f"Pre-fetch: Starting simple background search for next episode {next_episode_id}")

            async def fetch_next_metadata():
                return await get_metadata(next_episode_id, stream_type)

            await asyncio.wait_for(
                redis_cache.get_or_set(fetch_next_metadata, next_episode_id, stream_type, config["metadataProvider"]),
                timeout=3.0
            )
            logger.debug(f"Pre-fetch: Metadata cached for episode {next_episode_id}")
            
        else:
            logger.debug(f"Pre-fetch: Next episode {next_episode_id} already cached")
            
    except asyncio.TimeoutError:
        logger.debug(f"Pre-fetch: Timeout during simple background pre-fetch")
    except Exception as e:
        logger.debug(f"Pre-fetch: Error during simple background pre-fetch: {str(e)}")


async def prefetch_next_episode(media, config, redis_cache, stream_cache_key, get_metadata, get_and_filter_results, stream_processing, ResultsPerQualityFilter, Stream, stream_type):
    try:
        current_season_num = int(media.season.replace("S", ""))
        current_episode_num = int(media.episode.replace("E", ""))
        next_episode_num = current_episode_num + 1
        
        next_episode_id = f"{media.id.split(':')[0]}:{current_season_num}:{next_episode_num}"
        
        next_media_mock = type(media)(
            id=next_episode_id,
            tmdb_id=media.tmdb_id,
            titles=media.titles,
            season=f"S{current_season_num:02d}",
            episode=f"E{next_episode_num:02d}",
            languages=media.languages
        )
        
        next_stream_key = stream_cache_key(next_media_mock)
        cached_next = await redis_cache.get(next_stream_key)
        
        if cached_next is None:
            logger.info(f"Pre-fetch: Starting background search for next episode {next_episode_id}")

            expiration_time = 1200

            async def fetch_next_metadata():
                return await get_metadata(next_episode_id, stream_type)

            next_media = await asyncio.wait_for(
                redis_cache.get_or_set(fetch_next_metadata, next_episode_id, stream_type, config["metadataProvider"]),
                timeout=8.0
            )
            
            raw_results = await asyncio.wait_for(
                get_and_filter_results(next_media, config),
                timeout=12.0
            )
            filtered_results = ResultsPerQualityFilter(config).filter(raw_results)
            next_streams = stream_processing(filtered_results, next_media, config)
            next_stream_objects = [Stream(**stream) for stream in next_streams]
            
            await redis_cache.set(stream_cache_key(next_media), next_stream_objects, expiration=expiration_time)
            logger.success(f"Pre-fetch: Successfully background pre-cached {len(next_stream_objects)} streams for episode {next_episode_id}")
            
        else:
            logger.debug(f"Pre-fetch: Next episode {next_episode_id} already cached")
            
    except asyncio.TimeoutError:
        logger.debug(f"Pre-fetch: Timeout during background pre-fetch")
    except Exception as e:
        error_msg = str(e).lower()
        if any(keyword in error_msg for keyword in ["connection", "timeout", "reset", "closed"]):
            logger.debug(f"Pre-fetch: Network issue during background pre-fetch: {str(e)}")
        else:
            logger.warning(f"Pre-fetch: Error during background pre-fetch: {str(e)}")


@router.get("/{config}/stream/{stream_type}/{stream_id}", response_model=SearchResponse)
async def get_results(
    request: Request,
    config: str,
    stream_type: str,
    stream_id: str,
    redis_cache: RedisCache = Depends(get_redis_cache_dependency),
    apikey_dao: APIKeyDAO = Depends(),
    torrent_dao: TorrentItemDAO = Depends(),
) -> SearchResponse:
    start = time.time()
    logger.info(f"Search: Stream request initiated for {stream_type} - {stream_id}")

    stream_id = stream_id.replace(".json", "")
    config = parse_config(config)
    api_key = config.get("apiKey")
    ip_address = get_client_ip(request)
    # Only validate the API key if it exists
    if api_key:
        await check_api_key(api_key, apikey_dao)
    else:
        logger.warning("Search: API key not found in config.")
        raise HTTPException(status_code=401, detail="API key not found in config.")

    debrid_session = getattr(request.app.state, 'debrid_session', None)
    debrid_services = get_all_debrid_services(config, debrid_session)
    logger.debug(f"Search: Found {len(debrid_services)} debrid services")
    logger.info(
        f"Search: Debrid services: {[debrid.__class__.__name__ for debrid in debrid_services]}"
    )

    http_session = getattr(request.app.state, 'http_session', None)

    async def get_metadata(episode_id=None, media_type=None):
        logger.info(f"Search: Fetching metadata from {config['metadataProvider']}")
        actual_id = episode_id if episode_id is not None else stream_id
        actual_type = media_type if media_type is not None else stream_type

        if config["metadataProvider"] == "tmdb" and settings.tmdb_api_key:
            try:
                metadata_provider = TMDB(config, session=http_session)
                return await metadata_provider.get_metadata(actual_id, actual_type)
            except (ValueError, IndexError, KeyError) as e:
                logger.warning(f"Search: TMDB metadata fetch failed ({str(e)}), falling back to Cinemeta")

        metadata_provider = Cinemeta(config, session=http_session)
        return await metadata_provider.get_metadata(actual_id, actual_type)

    media = await redis_cache.get_or_set(
        get_metadata, stream_id, stream_type, config["metadataProvider"]
    )
    logger.debug(f"Search: Retrieved media metadata for {str(media.titles)}")

    def stream_cache_key(media):
        cache_user_identifier = api_key if api_key else ip_address
        if isinstance(media, Movie):
            key_string = f"stream:{cache_user_identifier}:{media.titles[0]}:{media.year}:{media.languages[0]}"
        elif isinstance(media, Series):
            key_string = f"stream:{cache_user_identifier}:{media.titles[0]}:{media.languages[0]}:{media.season}{media.episode}"
        else:
            logger.error("Search: Only Movie and Series are allowed as media!")
            raise HTTPException(
                status_code=500, detail="Only Movie and Series are allowed as media!"
            )
        hashed_key = hashlib.sha256(key_string.encode("utf-8")).hexdigest()
        return hashed_key[:16]

    cached_result = await redis_cache.get(stream_cache_key(media))
    if cached_result is not None:
        logger.info("Search: Returning cached processed results")

        if isinstance(media, Series):
            asyncio.create_task(full_prefetch_from_cache(media, config, redis_cache, stream_cache_key, get_metadata, stream_type, debrid_services, torrent_dao, request))
            await asyncio.sleep(0.5)  # 500ms de délai pour les séries

        total_time = time.time() - start
        logger.success(f"Search: Request completed in {total_time:.2f} seconds")
        return SearchResponse(streams=cached_result)

    def media_cache_key(media):
        if isinstance(media, Movie):
            key_string = f"media:{media.titles[0]}:{media.year}:{media.languages[0]}"
        elif isinstance(media, Series):
            key_string = f"media:{media.titles[0]}:{media.languages[0]}:{media.season}{media.episode}"
        else:
            raise TypeError("Only Movie and Series are allowed as media!")
        hashed_key = hashlib.sha256(key_string.encode("utf-8")).hexdigest()
        return hashed_key[:16]

    async def get_search_results(media, config):
        search_results = []
        torrent_service = TorrentService(config, torrent_dao)

        async def perform_search(update_cache=False):
            nonlocal search_results
            search_results = []

            async def _fetch_c411_raw():
                if not config.get("c411") or not settings.c411_enable:
                    return []
                try:
                    c411_service = C411Service(config, session=http_session)
                    raw = await c411_service.search(media)
                    return [
                        C411SearchResult().from_api_item(item, media)
                        for item in raw
                        if getattr(item, "info_hash", None) and len(item.info_hash) == 40
                    ] if raw else []
                except Exception as e:
                    logger.warning(f"Search: C411 search failed, skipping: {str(e)}")
                return []

            async def _fetch_torr9_raw():
                if not config.get("torr9") or not settings.torr9_enable:
                    return []
                try:
                    torr9_service = Torr9Service(config, session=http_session)
                    raw = await torr9_service.search(media)
                    return [
                        Torr9SearchResult().from_api_item(item, media)
                        for item in raw
                        if getattr(item, "info_hash", None) and len(item.info_hash) == 40
                    ] if raw else []
                except Exception as e:
                    logger.warning(f"Search: Torr9 search failed, skipping: {str(e)}")
                return []

            async def _fetch_lacale_raw():
                if not config.get("lacale") or not settings.lacale_enable:
                    return []
                try:
                    lacale_service = LaCaleService(config, session=http_session)
                    raw = await lacale_service.search(media)
                    return raw if raw else []
                except Exception as e:
                    logger.warning(f"Search: LaCale search failed, skipping: {str(e)}")
                return []

            async def _fetch_generationfree_raw():
                if not config.get("generationfree"):
                    return []
                try:
                    generationfree_service = GenerationFreeService(config, session=http_session)
                    raw = await generationfree_service.search(media)
                    return raw if raw else []
                except Exception as e:
                    logger.warning(f"Search: GenerationFree search failed, skipping: {str(e)}")
                return []

            async def _fetch_yggflix_raw():
                if not config.get("yggflix"):
                    return []
                try:
                    yggflix_service = YggflixService(config)
                    raw = await asyncio.to_thread(yggflix_service.search, media)
                    return raw if raw else []
                except Exception as e:
                    logger.warning(f"Search: Yggflix search failed, skipping: {str(e)}")
                return []

            c411_raw, torr9_raw, lacale_raw, yggflix_raw, generationfree_raw = await asyncio.gather(
                _fetch_c411_raw(),
                _fetch_torr9_raw(),
                _fetch_lacale_raw(),
                _fetch_yggflix_raw(),
                _fetch_generationfree_raw(),
            )

            if c411_raw:
                c411_search_results = await torrent_service.convert_and_process(c411_raw)
                logger.success(f"Search: Found {len(c411_search_results)} results from C411")
                search_results = merge_items(search_results, c411_search_results)
            if torr9_raw:
                torr9_search_results = await torrent_service.convert_and_process(torr9_raw)
                logger.success(f"Search: Found {len(torr9_search_results)} results from Torr9")
                search_results = merge_items(search_results, torr9_search_results)
            if lacale_raw:
                lacale_search_results = await torrent_service.convert_and_process(lacale_raw)
                logger.success(f"Search: Found {len(lacale_search_results)} results from LaCale")
                search_results = merge_items(search_results, lacale_search_results)
            if generationfree_raw:
                generationfree_search_results = await torrent_service.convert_and_process(generationfree_raw)
                logger.success(f"Search: Found {len(generationfree_search_results)} results from GenerationFree")
                search_results = merge_items(search_results, generationfree_search_results)
            if yggflix_raw:
                yggflix_search_results = await torrent_service.convert_and_process(yggflix_raw)
                logger.success(f"Search: Found {len(yggflix_search_results)} results from Yggflix")
                search_results = merge_items(search_results, yggflix_search_results)

            # 2. Public cache (DMM etc.)
            if config["cache"] and not update_cache and len(search_results) < int(
                config["minCachedResults"]
            ):
                public_cached_results = await asyncio.to_thread(search_public, media)
                if public_cached_results:
                    logger.success(
                        f"Search: Found {len(public_cached_results)} public cached results"
                    )
                    public_cached_results = [
                        JackettResult().from_cached_item(torrent, media)
                        for torrent in public_cached_results
                        if isinstance(torrent, dict) and len(torrent.get("hash", "")) == 40
                    ]
                    public_cached_results = await torrent_service.convert_and_process(
                        public_cached_results
                    )
                    search_results = merge_items(search_results, public_cached_results)

            # 3. Zilean si pas assez de résultats
            if config["zilean"] and len(search_results) < int(
                config["minCachedResults"]
            ):
                zilean_service = ZileanService(config, session=http_session)
                zilean_search_results = await zilean_service.search(media)
                if zilean_search_results:
                    logger.success(
                        f"Search: Found {len(zilean_search_results)} results from Zilean"
                    )
                    zilean_search_results = [
                        ZileanResult().from_api_cached_item(torrent, media)
                        for torrent in zilean_search_results
                        if len(getattr(torrent, "info_hash", "")) == 40
                    ]
                    zilean_search_results = await torrent_service.convert_and_process(
                        zilean_search_results
                    )
                    logger.info(
                        f"Search: Zilean final search results: {len(zilean_search_results)}"
                    )
                    search_results = merge_items(search_results, zilean_search_results)

            if config["sharewood"] and settings.sharewood_enable and len(search_results) < int(
                config["minCachedResults"]
            ):
                try:
                    sharewood_service = SharewoodService(config, session=http_session)
                    sharewood_search_results = await sharewood_service.search(media)
                    if sharewood_search_results:
                        logger.success(
                            f"Search: Found {len(sharewood_search_results)} results from Sharewood"
                        )
                        sharewood_search_results = (
                            await torrent_service.convert_and_process(
                                sharewood_search_results
                            )
                        )
                        search_results = merge_items(search_results, sharewood_search_results)
                except Exception as e:
                    logger.warning(f"Search: Sharewood search failed, skipping: {str(e)}")

            if config["jackett"] and len(search_results) < int(
                config["minCachedResults"]
            ):
                jackett_service = JackettService(config, session=http_session)
                jackett_search_results = await jackett_service.search(media)
                logger.success(
                    f"Search: Found {len(jackett_search_results)} results from Jackett"
                )
                if jackett_search_results:
                    torrent_results = await torrent_service.convert_and_process(
                        jackett_search_results
                    )
                    search_results = merge_items(search_results, torrent_results)

            if update_cache and search_results:
                logger.info(
                    f"Search: Updating cache with {len(search_results)} results"
                )
                try:
                    cache_key = media_cache_key(media)
                    search_results_dict = [item.to_dict() for item in search_results]
                    await redis_cache.set(cache_key, search_results_dict, expiration=settings.redis_expiration)
                    logger.success("Search: Cache update successful")
                except Exception as e:
                    logger.error(f"Search: Error updating cache: {e}")

        await perform_search()
        return search_results

    async def get_and_filter_results(media, config):
        # Postgres acts as a local cache for private indexers (Yggtorrent, C411, Torr9)
        # and is always queried directly, bypassing Redis
        postgres_results = []
        if hasattr(media, 'tmdb_id') and media.tmdb_id:
            try:
                postgres_items = await torrent_dao.search_by_tmdb_id(int(media.tmdb_id))
                if postgres_items:
                    logger.success(
                        f"Search: Found {len(postgres_items)} results from Postgres (local cache) for TMDB ID {media.tmdb_id}"
                    )
                    torrent_service = TorrentService(config, torrent_dao)
                    for db_item in postgres_items:
                        if db_item.indexer in ['Yggtorrent - API', 'C411 - API', 'Torr9 - API', 'LaCale - API', 'GenerationFree - API']:
                            torrent_item = db_item.to_torrent_item()
                            postgres_results.append(torrent_item)
            except Exception as pg_error:
                logger.error(f"Search: Postgres search failed: {str(pg_error)}")

        cache_key = media_cache_key(media)
        external_results = await redis_cache.get(cache_key)

        if external_results is None:
            logger.info("Search: No external sources in Redis cache. Performing new search.")
            external_results = await get_search_results(media, config)
            external_results_dict = [item.to_dict() for item in external_results]
            await redis_cache.set(cache_key, external_results_dict, expiration=settings.redis_expiration)
            logger.success(
                f"Search: Cached {len(external_results)} external results in Redis (Sharewood/Zilean/Jackett)"
            )
        else:
            logger.success(
                f"Search: Retrieved {len(external_results)} external results from Redis cache"
            )
            external_results = [
                TorrentItem.from_dict(item) for item in external_results
            ]

        all_results = merge_items(postgres_results, external_results)
        logger.info(f"Search: Merged Postgres ({len(postgres_results)}) + External ({len(external_results)}) = {len(all_results)} total results")

        filtered_results = filter_items(all_results, media, config=config)

        min_results = int(config.get("minCachedResults", 8))
        external_filtered = filter_items(external_results, media, config=config)
        if len(external_filtered) < min_results:
            logger.warning(
                f"Search: Insufficient external results ({len(external_filtered)} < {min_results}). Recreating external cache."
            )
            await redis_cache.delete(cache_key)
            external_results = await get_search_results(media, config)
            external_results_dict = [item.to_dict() for item in external_results]
            await redis_cache.set(cache_key, external_results_dict, expiration=settings.redis_expiration)
            logger.success(
                f"Search: Recreated external cache with {len(external_results)} results"
            )
            all_results = merge_items(postgres_results, external_results)
            filtered_results = filter_items(all_results, media, config=config)

        logger.success(
            f"Search: Final number of filtered results: {len(filtered_results)}"
        )
        return filtered_results

    raw_search_results = await get_and_filter_results(media, config)
    logger.debug(f"Search: Filtered search results: {len(raw_search_results)}")
    search_results = ResultsPerQualityFilter(config).filter(raw_search_results)
    logger.info(f"Search: Filtered search results per quality: {len(search_results)}")

    async def stream_processing(search_results, media, config):
        torrent_smart_container = TorrentSmartContainer(search_results, media)

        if config["debrid"]:
            for debrid in debrid_services:
                hashes = torrent_smart_container.get_unaviable_hashes()
                ip = get_client_ip(request)
                result = await debrid.get_availability_bulk(hashes, ip)
                if result:
                    torrent_smart_container.update_availability(
                        result, type(debrid), media
                    )
                    if isinstance(result, dict):
                        count = len(result.items())
                    else:
                        count = len(result)

                    is_stremthru = (type(debrid).__name__ == "StremThru" or
                                   hasattr(debrid, 'store_name') and getattr(debrid, 'store_name', None) is not None)
                    
                    logger.info(
                        f"Search: Checked availability for {count} items with {type(debrid).__name__}"
                    )
                else:
                    logger.warning(
                        "Search: No availability results found in debrid service"
                    )

        if config["cache"]:
            asyncio.create_task(asyncio.to_thread(torrent_smart_container.cache_container_items))

        best_matching_results = torrent_smart_container.get_best_matching()
        best_matching_results = sort_items(best_matching_results, config)
        logger.info(f"Search: Found {len(best_matching_results)} best matching results")

        parser = StreamParser(config)
        stream_list = await parser.parse_to_stremio_streams(best_matching_results, media)
        logger.success(f"Search: Processed {len(stream_list)} streams for Stremio")

        return stream_list

    stream_list = await stream_processing(search_results, media, config)
    streams = [Stream(**stream) for stream in stream_list]

    expiration_time = 1200
    has_stremthru = any(
        type(debrid).__name__ == "StremThru" or hasattr(debrid, 'store_name')
        for debrid in debrid_services
    )
    if has_stremthru:
        expiration_time = 600
        logger.info(f"Search: Using reduced cache expiration ({expiration_time}s) for StremThru")

    await redis_cache.set(stream_cache_key(media), streams, expiration=expiration_time)

    if isinstance(media, Series):
        asyncio.create_task(full_prefetch_from_cache(media, config, redis_cache, stream_cache_key, get_metadata, stream_type, debrid_services, torrent_dao, request))

    total_time = time.time() - start
    logger.info(f"Search: Request completed in {total_time:.2f} seconds")
    return SearchResponse(streams=streams)
