import hashlib
import time
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request
import asyncio

from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.dao.torrentitem_dao import TorrentItemDAO
from stream_fusion.services.postgresql.dao.torrentgroup_dao import TorrentGroupDAO
from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.services.redis.redis_config import get_redis_cache_dependency
from stream_fusion.utils.cache.local_redis import RedisCache
from stream_fusion.utils.debrid.get_debrid_service import get_all_debrid_services
from stream_fusion.utils.filter.results_per_quality_filter import (
    ResultsPerQualityFilter,
)
from stream_fusion.utils.filter_results import (
    apply_correctness_filters,
    apply_preference_filters,
    filter_items,
    merge_items,
    sort_items,
)
from stream_fusion.logging_config import logger
from stream_fusion.utils.jackett.jackett_result import JackettResult
from stream_fusion.utils.jackett.jackett_service import JackettService
from stream_fusion.utils.parser.parser_service import StreamParser
from stream_fusion.utils.utopeer.utopeer_service import UtopeerService
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
from stream_fusion.utils.generationfree.generationfree_service import GenerationFreeService
from stream_fusion.utils.abn.abn_service import AbnService
from stream_fusion.utils.g3mini.g3mini_service import G3MiniService
from stream_fusion.utils.theoldschool.theoldschool_service import TheOldSchoolService
from stream_fusion.settings import settings
from stream_fusion.web.utils import get_client_ip
from sqlalchemy.ext.asyncio import AsyncSession


router = APIRouter()


def serialize_streams_for_cache(streams):
    return [
        stream.model_dump() if hasattr(stream, "model_dump")
        else stream.dict() if hasattr(stream, "dict")
        else stream.__dict__
        for stream in streams
    ]


def deserialize_streams_from_cache(cached_streams):
    if not isinstance(cached_streams, list):
        return []
    return [
        Stream(**item) if not isinstance(item, Stream) else item
        for item in cached_streams
    ]


def has_stremthru_enabled(debrid_services):
    return any(type(debrid).__name__ == "StremThru" for debrid in debrid_services)


# Indexers stored in Postgres (sourced from direct APIs, not Jackett/Zilean)
_POSTGRES_INDEXERS = frozenset({
    "Yggtorrent - API",
    "C411 - API",
    "Torr9 - API",
    "LaCale - API",
    "GenerationFree - API",
    "ABN - API",
    "G3MINI - API",
    "TheOldSchool - API",
})

# Maps Postgres indexer display name → user config key
# Used to check whether a user has an indexer configured when serving non-instant results from cache.
_INDEXER_CONFIG_KEY = {
    "C411 - API":           "c411",
    "Torr9 - API":          "torr9",
    "LaCale - API":         "lacale",
    "GenerationFree - API": "generationfree",
    "ABN - API":            "abn",
    "G3MINI - API":         "g3mini",
    "TheOldSchool - API":   "theoldschool",
    "Yggtorrent - API":     "utopeer",
}

# Maps ANY indexer display name → its user config/category key.
# Used for the public-indexer download-exclusion filter.
# Covers cases where split()[0].lower() doesn't match the config key
# (e.g. "u2p - Cache" → "u2p" ≠ "utopeer").
_INDEXER_TO_CONFIG_KEY: dict[str, str] = {
    **_INDEXER_CONFIG_KEY,
    "u2p - Cache": "utopeer",
}


def _indexer_config_key(indexer: str) -> str:
    """Return the config/category key for an indexer display name."""
    return _INDEXER_TO_CONFIG_KEY.get(indexer) or (indexer.split()[0].lower() if indexer else "")


def _build_next_episode_media(media):
    """Return (next_episode_id, next_media_mock) for the episode following the given one."""
    current_season_num = int(media.season.replace("S", ""))
    current_episode_num = int(media.episode.replace("E", ""))
    next_episode_num = current_episode_num + 1
    next_episode_id = f"{media.id.split(':')[0]}:{current_season_num}:{next_episode_num}"
    next_media = type(media)(
        id=next_episode_id,
        tmdb_id=media.tmdb_id,
        titles=media.titles,
        season=f"S{current_season_num:02d}",
        episode=f"E{next_episode_num:02d}",
        languages=media.languages,
    )
    return next_episode_id, next_media


async def _bg_assign_tmdb_ids(session_factory, tmdb_data: list):
    """Background task: bulk TMDB ID assignment with retry on deadlock.

    Groups entries by tmdb_id and fires ONE bulk UPDATE per group (single SQL
    statement, atomic row-lock acquisition).

    A short initial delay lets the inserting request session commit first —
    this avoids the ShareLock-on-transaction deadlock that occurs when the
    UPDATE tries to see rows inserted by a still-open concurrent transaction.
    On deadlock the session is fully discarded and a fresh one is created.
    """
    from collections import defaultdict

    # Separate hashes (bulk path) from title-only entries (rare fallback)
    hashes_by_tmdb: dict[int, list[str]] = defaultdict(list)
    no_hash_entries: list[dict] = []
    for entry in tmdb_data:
        if entry.get("info_hash"):
            hashes_by_tmdb[entry["tmdb_id"]].append(entry["info_hash"])
        else:
            no_hash_entries.append(entry)

    _MAX_RETRIES = 3
    _RETRY_DELAYS = [0.15, 0.35, 0.70]   # seconds — progressive back-off

    for attempt in range(_MAX_RETRIES):
        # Brief delay on first attempt so the inserting transaction can commit.
        # Longer delays on retries to let any concurrent UPDATE finish.
        await asyncio.sleep(_RETRY_DELAYS[attempt])

        session = session_factory()
        try:
            dao = TorrentItemDAO(session)

            for tmdb_id, hashes in hashes_by_tmdb.items():
                await dao.bulk_update_tmdb_id_by_info_hashes(hashes, tmdb_id)

            for entry in no_hash_entries:
                await dao.update_tmdb_id_by_raw_title(
                    entry["raw_title"], entry["tmdb_id"], indexer=entry.get("indexer")
                )

            await session.commit()
            return   # success — exit retry loop

        except Exception as e:
            try:
                await session.rollback()
            except Exception:
                pass
            if "deadlock" in str(e).lower() and attempt < _MAX_RETRIES - 1:
                logger.debug(f"BG TMDB assign: deadlock on attempt {attempt + 1}/{_MAX_RETRIES}, retrying…")
            else:
                logger.debug(f"BG TMDB assign error (attempt {attempt + 1}): {e}")
                return
        finally:
            await session.close()


async def _bg_touch_items(session_factory, info_hashes: list):
    """Background task: refresh updated_at (TTL) with its own DB session."""
    session = session_factory()
    try:
        dao = TorrentItemDAO(session)
        await dao.touch_items_by_info_hash(info_hashes)
        await session.commit()
    except Exception as e:
        logger.debug(f"BG touch error: {e}")
    finally:
        await session.close()


async def full_prefetch_from_cache(
    media,
    config,
    redis_cache,
    stream_cache_key,
    get_metadata,
    stream_type,
    debrid_services,
    torrent_dao,
    request,
):
    try:
        await asyncio.sleep(1.0)
        http_session = getattr(request.app.state, "http_session", None)

        next_episode_id, next_media_mock = _build_next_episode_media(media)

        next_stream_key = stream_cache_key(next_media_mock)
        cached_next = await redis_cache.get(next_stream_key)

        if cached_next is None:
            logger.debug(
                f"Pre-fetch: Starting full background search for next episode {next_episode_id}"
            )

            async def fetch_next_metadata():
                return await get_metadata(next_episode_id, stream_type)

            next_media = await asyncio.wait_for(
                redis_cache.get_or_set(
                    fetch_next_metadata,
                    next_episode_id,
                    stream_type,
                    config["metadataProvider"],
                ),
                timeout=5.0,
            )

            search_results = []

            background_session = request.app.state.db_session_factory()
            try:
                background_torrent_dao = TorrentItemDAO(background_session)
                torrent_service = TorrentService(config, background_torrent_dao, TorrentGroupDAO(background_session))

                # Pre-fetch goes directly to live indexers — no Postgres cache check.
                # The user is not impacted by latency here, and a fresh live search
                # ensures new torrents are discovered and saved to Postgres.

                # Phase-based indexer search (mirrors get_search_results logic)
                categories = config.get("indexerCategories", {})

                def _get_cat(key):
                    return categories.get(key, "fallback_private")

                def _count_private(results):
                    return sum(
                        1 for r in results
                        if _get_cat((r.indexer.split()[0] if r.indexer else "").lower()) != "public"
                    )

                async def _fetch_c411():
                    if not config.get("c411") or not settings.c411_enable:
                        return []
                    try:
                        c411_service = C411Service(config, session=http_session)
                        raw = await c411_service.search(next_media)
                        return [
                            C411SearchResult().from_api_item(item, next_media)
                            for item in raw
                            if getattr(item, "info_hash", None) and len(item.info_hash) == 40
                        ] if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: C411 search failed: {e}")
                    return []

                async def _fetch_torr9():
                    if not config.get("torr9") or not settings.torr9_enable:
                        return []
                    try:
                        torr9_service = Torr9Service(config, session=http_session)
                        raw = await torr9_service.search(next_media)
                        return [
                            Torr9SearchResult().from_api_item(item, next_media)
                            for item in raw
                            if getattr(item, "info_hash", None) and len(item.info_hash) == 40
                        ] if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: Torr9 search failed: {e}")
                    return []

                async def _fetch_lacale():
                    if not config.get("lacale") or not settings.lacale_enable:
                        return []
                    try:
                        lacale_service = LaCaleService(config, session=http_session)
                        raw = await lacale_service.search(next_media)
                        return raw if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: LaCale search failed: {e}")
                    return []

                async def _fetch_generationfree():
                    if not config.get("generationfree"):
                        return []
                    try:
                        generationfree_service = GenerationFreeService(config, session=http_session)
                        raw = await generationfree_service.search(next_media)
                        return raw if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: GenerationFree search failed: {e}")
                    return []

                async def _fetch_abn():
                    if not config.get("abn") or not settings.abn_enable:
                        return []
                    try:
                        abn_service = AbnService(config, session=http_session)
                        raw = await abn_service.search(next_media)
                        return raw if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: ABN search failed: {e}")
                    return []

                async def _fetch_g3mini():
                    if not config.get("g3mini") or not settings.g3mini_enable:
                        return []
                    try:
                        g3mini_service = G3MiniService(config, session=http_session)
                        raw = await g3mini_service.search(next_media)
                        return raw if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: G3MINI search failed: {e}")
                    return []

                async def _fetch_theoldschool():
                    if not config.get("theoldschool") or not settings.theoldschool_enable:
                        return []
                    try:
                        theoldschool_service = TheOldSchoolService(config, session=http_session)
                        raw = await theoldschool_service.search(next_media)
                        return raw if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: TheOldSchool search failed: {e}")
                    return []

                async def _fetch_zilean():
                    if not config.get("zilean"):
                        return []
                    try:
                        zilean_service = ZileanService(config, session=http_session)
                        raw = await zilean_service.search(next_media)
                        if not raw:
                            return []
                        return [
                            ZileanResult().from_api_cached_item(torrent, next_media)
                            for torrent in raw
                            if len(getattr(torrent, "info_hash", "")) == 40
                        ]
                    except Exception as e:
                        logger.debug(f"Pre-fetch: Zilean search failed: {e}")
                    return []

                async def _fetch_jackett():
                    if not config.get("jackett"):
                        return []
                    try:
                        jackett_service = JackettService(config, session=http_session)
                        raw = await jackett_service.search(next_media)
                        return raw if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: Jackett search failed: {e}")
                    return []

                async def _fetch_utopeer():
                    if not config.get("utopeer"):
                        return []
                    try:
                        utopeer_service = UtopeerService(config)
                        raw = await utopeer_service.search(next_media)
                        return raw if raw else []
                    except Exception as e:
                        logger.debug(f"Pre-fetch: Utopeer search failed: {e}")
                    return []

                ALL_FETCHERS = [
                    ("c411",           _fetch_c411,           "C411"),
                    ("torr9",          _fetch_torr9,          "Torr9"),
                    ("lacale",         _fetch_lacale,         "LaCale"),
                    ("generationfree", _fetch_generationfree, "GenerationFree"),
                    ("abn",            _fetch_abn,            "ABN"),
                    ("g3mini",         _fetch_g3mini,         "G3MINI"),
                    ("theoldschool",   _fetch_theoldschool,   "TheOldSchool"),
                    ("zilean",         _fetch_zilean,         "Zilean"),
                    ("jackett",        _fetch_jackett,        "Jackett"),
                    ("utopeer",        _fetch_utopeer,        "Utopeer"),
                ]

                async def _run_phase(target_categories):
                    fetchers = [(k, fn, name) for k, fn, name in ALL_FETCHERS if _get_cat(k) in target_categories]
                    if not fetchers:
                        return []
                    raw_list = await asyncio.gather(*[fn() for _, fn, _ in fetchers])
                    phase_results = []
                    for (_, _, name), raw in zip(fetchers, raw_list):
                        if not raw:
                            continue
                        processed = await torrent_service.convert_and_process(raw)
                        if processed:
                            logger.debug(f"Pre-fetch: {name}: {len(processed)} results")
                            phase_results = merge_items(phase_results, processed)
                    return phase_results

                min_cached = int(config.get("minCachedResults", 8))
                utopeer_priority = config.get("utopeerPriority", True)

                phase1_cats = {"priority_private", "public"} if utopeer_priority else {"priority_private"}
                phase1 = await _run_phase(phase1_cats)
                search_results = merge_items(search_results, phase1)

                if _count_private(search_results) < min_cached:
                    phase2 = await _run_phase({"intermediary_private"})
                    search_results = merge_items(search_results, phase2)

                if _count_private(search_results) < min_cached:
                    phase3 = await _run_phase({"fallback_private"})
                    search_results = merge_items(search_results, phase3)

                if not utopeer_priority:
                    phase4 = await _run_phase({"public"})
                    search_results = merge_items(search_results, phase4)

                if search_results:
                    # Step 1: correctness filters (season/episode + title matching)
                    confirmed_results = await asyncio.to_thread(
                        apply_correctness_filters, search_results, next_media
                    )
                    logger.debug(
                        f"Pre-fetch: {len(confirmed_results)} results after correctness filters "
                        f"for {next_media.season}{next_media.episode}"
                    )

                    # Step 2: retroactive TMDB ID assignment for confirmed items
                    if hasattr(next_media, "tmdb_id") and next_media.tmdb_id:
                        tmdb_id_int = int(next_media.tmdb_id)
                        bulk_hashes: list[str] = []
                        title_updates = []
                        for item in confirmed_results:
                            if item.tmdb_id is None and item.indexer in _POSTGRES_INDEXERS:
                                if item.info_hash:
                                    bulk_hashes.append(item.info_hash)
                                else:
                                    title_updates.append(background_torrent_dao.update_tmdb_id_by_raw_title(item.raw_title, tmdb_id_int, indexer=item.indexer))
                                item.tmdb_id = tmdb_id_int
                        # One bulk UPDATE for all hashes — avoids per-row deadlocks
                        if bulk_hashes:
                            await background_torrent_dao.bulk_update_tmdb_id_by_info_hashes(bulk_hashes, tmdb_id_int)
                        for coro in title_updates:
                            await coro

                    # Step 3: refresh TTL for Postgres items so cache-first stays valid
                    postgres_hashes = [
                        item.info_hash for item in confirmed_results
                        if item.info_hash and item.indexer in _POSTGRES_INDEXERS
                    ]
                    if postgres_hashes:
                        await background_torrent_dao.touch_items_by_info_hash(postgres_hashes)

                    # Step 4: user-preference filters + per-quality cap
                    filtered_results = ResultsPerQualityFilter(config).filter(
                        await asyncio.to_thread(apply_preference_filters, confirmed_results, next_media, config)
                    )
                    torrent_smart_container = TorrentSmartContainer(
                        filtered_results, next_media
                    )

                    _avail_redis = await RedisCache(config).get_redis_client()
                    ip = get_client_ip(request)

                    pf_fast = [d for d in debrid_services if type(d).__name__ != "RealDebrid"]
                    pf_rd   = [d for d in debrid_services if type(d).__name__ == "RealDebrid"]

                    # --- Parallel: all non-RD debrids simultaneously ---
                    pf_hashes = torrent_smart_container.get_unaviable_hashes()
                    if pf_fast and pf_hashes:
                        async def _check_pf(debrid):
                            try:
                                result = await debrid.get_availability_bulk_cached(pf_hashes, ip, _avail_redis)
                                if not result:
                                    result = await debrid.get_availability_bulk(pf_hashes, ip)
                                return debrid, result
                            except Exception as e:
                                logger.debug(f"Pre-fetch: {type(debrid).__name__} availability check failed: {e}")
                                return debrid, None

                        pf_results = await asyncio.gather(*[_check_pf(d) for d in pf_fast])
                        for debrid, result in pf_results:
                            if result:
                                torrent_smart_container.update_availability(result, type(debrid), next_media)

                    # --- RealDebrid: only if below threshold, on remaining hashes ---
                    if pf_rd:
                        cached_count = torrent_smart_container.get_cached_count()
                        rd_min = int(config.get("rdMinCachedBeforeCheck", 3))
                        if rd_min > 0 and cached_count >= rd_min:
                            logger.debug(f"Pre-fetch: {cached_count} cached ≥ rdMinCachedBeforeCheck ({rd_min}), skipping RealDebrid")
                        else:
                            for rd in pf_rd:
                                rd_hashes = torrent_smart_container.get_unaviable_hashes()
                                if not rd_hashes:
                                    break
                                result = await rd.get_availability_bulk_cached(rd_hashes, ip, _avail_redis)
                                if not result:
                                    result = await rd.get_availability_bulk(rd_hashes, ip)
                                if result:
                                    torrent_smart_container.update_availability(result, type(rd), next_media)

                    best_matching_results = torrent_smart_container.get_best_matching()
                    best_matching_results = sort_items(best_matching_results, config)

                    parser = StreamParser(config)
                    stream_list = await parser.parse_to_stremio_streams(
                        best_matching_results, next_media
                    )
                    next_stream_objects = [Stream(**stream) for stream in stream_list]

                    if next_stream_objects:
                        next_stream_cache = serialize_streams_for_cache(next_stream_objects)
                        expiration_time = 600 if has_stremthru_enabled(debrid_services) else 1200
                        await redis_cache.set(
                            stream_cache_key(next_media),
                            next_stream_cache,
                            expiration=expiration_time,
                        )
                        logger.success(
                            f"Pre-fetch: Successfully background pre-cached {len(next_stream_objects)} streams for episode {next_episode_id}"
                        )
                    else:
                        logger.debug(
                            f"Pre-fetch: No available streams found for episode {next_episode_id}, skipping cache write"
                        )
                else:
                    logger.debug(
                        f"Pre-fetch: No results found for episode {next_episode_id}"
                    )

            finally:
                await background_session.commit()
                await background_session.close()

        else:
            logger.debug(f"Pre-fetch: Next episode {next_episode_id} already cached")

    except Exception as e:
        logger.debug(f"Pre-fetch: Error during full background pre-fetch: {str(e)}")


async def simple_prefetch_next_episode(
    media, config, redis_cache, stream_cache_key, get_metadata, stream_type
):
    try:
        await asyncio.sleep(0.5)

        next_episode_id, next_media_mock = _build_next_episode_media(media)

        next_stream_key = stream_cache_key(next_media_mock)
        cached_next = await redis_cache.get(next_stream_key)

        if cached_next is None:
            logger.debug(
                f"Pre-fetch: Starting simple background search for next episode {next_episode_id}"
            )

            async def fetch_next_metadata():
                return await get_metadata(next_episode_id, stream_type)

            await asyncio.wait_for(
                redis_cache.get_or_set(
                    fetch_next_metadata,
                    next_episode_id,
                    stream_type,
                    config["metadataProvider"],
                ),
                timeout=3.0,
            )
            logger.debug(f"Pre-fetch: Metadata cached for episode {next_episode_id}")

        else:
            logger.debug(f"Pre-fetch: Next episode {next_episode_id} already cached")

    except asyncio.TimeoutError:
        logger.debug("Pre-fetch: Timeout during simple background pre-fetch")
    except Exception as e:
        logger.debug(f"Pre-fetch: Error during simple background pre-fetch: {str(e)}")


@router.get("/{config}/stream/{stream_type}/{stream_id}", response_model=SearchResponse)
async def get_results(
    request: Request,
    config: str,
    stream_type: str,
    stream_id: str,
    redis_cache: RedisCache = Depends(get_redis_cache_dependency),
    apikey_dao: APIKeyDAO = Depends(),
    torrent_dao: TorrentItemDAO = Depends(),
    db: AsyncSession = Depends(get_db_session),
) -> SearchResponse:
    start = time.time()
    logger.info(f"Search: Stream request initiated for {stream_type} - {stream_id}")

    # --- Auth & config ---
    stream_id = stream_id.replace(".json", "")
    config = parse_config(config)
    api_key = config.get("apiKey")
    ip_address = get_client_ip(request)

    if api_key:
        await check_api_key(api_key, apikey_dao)
    else:
        logger.warning("Search: API key not found in config.")
        raise HTTPException(status_code=401, detail="API key not found in config.")

    debrid_session = getattr(request.app.state, "debrid_session", None)
    debrid_services = get_all_debrid_services(config, debrid_session)
    logger.trace(f"Search: Debrid services: {[debrid.__class__.__name__ for debrid in debrid_services]}")

    http_session = getattr(request.app.state, "http_session", None)

    # --- Metadata: TMDB preferred, Cinemeta as fallback; result cached in Redis ---
    async def get_metadata(episode_id=None, media_type=None):
        logger.trace(f"Search: Fetching metadata from {config['metadataProvider']}")
        actual_id = episode_id if episode_id is not None else stream_id
        actual_type = media_type if media_type is not None else stream_type

        if config["metadataProvider"] == "tmdb" and settings.tmdb_api_key:
            try:
                metadata_provider = TMDB(config, session=http_session)
                return await metadata_provider.get_metadata(actual_id, actual_type)
            except (ValueError, IndexError, KeyError) as e:
                logger.warning(
                    f"Search: TMDB metadata fetch failed ({str(e)}), falling back to Cinemeta"
                )

        metadata_provider = Cinemeta(config, session=http_session)
        return await metadata_provider.get_metadata(actual_id, actual_type)

    media = await redis_cache.get_or_set(
        get_metadata, stream_id, stream_type, config["metadataProvider"]
    )
    logger.trace(f"Search: Retrieved media metadata for {str(media.titles)}")
    logger.info(f"Search: [{media.titles[0]}] {stream_type} {stream_id}")

    # stream_cache_key: user-specific (api_key or IP) + media identifier → 16-char SHA256 prefix
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

    # --- Cache hit: return stored streams immediately, pre-fetch next episode in background ---
    cached_result = await redis_cache.get(stream_cache_key(media))
    if cached_result is not None:
        logger.info("Search: Returning cached processed results")
        cached_streams = deserialize_streams_from_cache(cached_result)

        if isinstance(media, Series):
            # Fire-and-forget: pre-fetch next episode streams in background while returning the response
            asyncio.create_task(
                full_prefetch_from_cache(
                    media,
                    config,
                    redis_cache,
                    stream_cache_key,
                    get_metadata,
                    stream_type,
                    debrid_services,
                    torrent_dao,
                    request,
                )
            )

        total_time = time.time() - start
        logger.success(f"Search: Request completed in {total_time:.2f} seconds")
        return SearchResponse(streams=cached_streams)

    # media_cache_key: shared across users (no user identifier) — caches external search results
    def media_cache_key(media):
        if isinstance(media, Movie):
            key_string = f"media:{media.titles[0]}:{media.year}:{media.languages[0]}"
        elif isinstance(media, Series):
            key_string = (
                f"media:{media.titles[0]}:{media.languages[0]}:{media.season}{media.episode}"
            )
        else:
            raise TypeError("Only Movie and Series are allowed as media!")
        hashed_key = hashlib.sha256(key_string.encode("utf-8")).hexdigest()
        return hashed_key[:16]

    async def get_search_results(media, config):
        search_results = []
        torrent_service = TorrentService(config, torrent_dao, TorrentGroupDAO(torrent_dao.session))
        categories = config.get("indexerCategories", {})

        def _get_cat(key):
            return categories.get(key, "fallback_private")

        def _count_private(results):
            """Count results from non-public indexers (used to trigger next search phases)."""
            return sum(
                1 for r in results
                if _get_cat((r.indexer.split()[0] if r.indexer else "").lower()) != "public"
            )

        # --- Fetch helpers (each checks its own enable flags) ---

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

        async def _fetch_utopeer_raw():
            if not config.get("utopeer"):
                return []
            try:
                utopeer_service = UtopeerService(config)
                raw = await utopeer_service.search(media)
                return raw if raw else []
            except Exception as e:
                logger.warning(f"Search: Utopeer search failed, skipping: {str(e)}")
            return []

        async def _fetch_abn_raw():
            if not config.get("abn") or not settings.abn_enable:
                return []
            try:
                abn_service = AbnService(config, session=http_session)
                raw = await abn_service.search(media)
                return raw if raw else []
            except Exception as e:
                logger.warning(f"Search: ABN search failed, skipping: {str(e)}")
            return []

        async def _fetch_g3mini_raw():
            if not config.get("g3mini") or not settings.g3mini_enable:
                return []
            try:
                g3mini_service = G3MiniService(config, session=http_session)
                raw = await g3mini_service.search(media)
                return raw if raw else []
            except Exception as e:
                logger.warning(f"Search: G3MINI search failed, skipping: {str(e)}")
            return []

        async def _fetch_theoldschool_raw():
            if not config.get("theoldschool") or not settings.theoldschool_enable:
                return []
            try:
                theoldschool_service = TheOldSchoolService(config, session=http_session)
                raw = await theoldschool_service.search(media)
                return raw if raw else []
            except Exception as e:
                logger.warning(f"Search: TheOldSchool search failed, skipping: {str(e)}")
            return []

        async def _fetch_zilean_raw():
            if not config.get("zilean"):
                return []
            try:
                zilean_service = ZileanService(config, session=http_session)
                raw = await zilean_service.search(media)
                if not raw:
                    return []
                logger.success(f"Search: Found {len(raw)} results from Zilean")
                return [
                    ZileanResult().from_api_cached_item(torrent, media)
                    for torrent in raw
                    if len(getattr(torrent, "info_hash", "")) == 40
                ]
            except Exception as e:
                logger.warning(f"Search: Zilean search failed, skipping: {str(e)}")
            return []

        async def _fetch_jackett_raw():
            if not config.get("jackett"):
                return []
            try:
                jackett_service = JackettService(config, session=http_session)
                raw = await jackett_service.search(media)
                logger.success(f"Search: Found {len(raw)} results from Jackett")
                return raw if raw else []
            except Exception as e:
                logger.warning(f"Search: Jackett search failed, skipping: {str(e)}")
            return []

        # Mapping: config key -> (fetch coroutine, display name)
        ALL_FETCHERS = [
            ("c411",          _fetch_c411_raw,         "C411"),
            ("torr9",         _fetch_torr9_raw,        "Torr9"),
            ("lacale",        _fetch_lacale_raw,       "LaCale"),
            ("generationfree", _fetch_generationfree_raw, "GenerationFree"),
            ("abn",           _fetch_abn_raw,          "ABN"),
            ("g3mini",        _fetch_g3mini_raw,       "G3MINI"),
            ("theoldschool",  _fetch_theoldschool_raw, "TheOldSchool"),
            ("zilean",        _fetch_zilean_raw,       "Zilean"),
            ("jackett",       _fetch_jackett_raw,      "Jackett"),
            ("utopeer",       _fetch_utopeer_raw,      "Utopeer"),
        ]

        async def _run_phase(target_categories):
            """Fetch all indexers in target_categories in parallel, process sequentially, return merged results."""
            fetchers = [(k, fn, name) for k, fn, name in ALL_FETCHERS if _get_cat(k) in target_categories]
            if not fetchers:
                return []
            raw_list = await asyncio.gather(*[fn() for _, fn, _ in fetchers])
            phase_results = []
            for (key, _, name), raw in zip(fetchers, raw_list):
                if not raw:
                    continue
                processed = await torrent_service.convert_and_process(raw)
                if processed:
                    logger.success(f"Search: Found {len(processed)} results from {name}")
                    phase_results = merge_items(phase_results, processed)
            return phase_results

        min_cached = int(config["minCachedResults"])
        utopeer_priority = config.get("utopeerPriority", True)

        # --- Phase 1: priority_private (always) + public if utopeerPriority=True ---
        phase1_cats = {"priority_private", "public"} if utopeer_priority else {"priority_private"}
        phase1 = await _run_phase(phase1_cats)
        search_results = merge_items(search_results, phase1)
        logger.info(f"Search: Phase 1 complete — {_count_private(search_results)} private results")

        # --- Phase 2: intermediary_private (if private results insufficient) ---
        if _count_private(search_results) < min_cached:
            phase2 = await _run_phase({"intermediary_private"})
            search_results = merge_items(search_results, phase2)
            logger.info(f"Search: Phase 2 complete — {_count_private(search_results)} private results")

        # --- Phase 3: fallback_private (if still insufficient) ---
        if _count_private(search_results) < min_cached:
            phase3 = await _run_phase({"fallback_private"})
            search_results = merge_items(search_results, phase3)
            logger.info(f"Search: Phase 3 complete — {_count_private(search_results)} private results")

        # --- Phase 4: public after private phases (only if utopeerPriority=False) ---
        if not utopeer_priority:
            phase4 = await _run_phase({"public"})
            search_results = merge_items(search_results, phase4)
            logger.info(f"Search: Phase 4 (public) complete — {len(search_results)} total results")

        return search_results

    async def get_and_filter_results(media, config):
        cache_key = media_cache_key(media)
        min_postgres = int(config.get("minPostgresResults", 5))
        postgres_max_age_days = int(config.get("postgresMaxAgeDays", 7))

        # --- Helper: build TMDB update data and launch background task ---
        def _assign_tmdb_ids_background(items):
            if not (hasattr(media, "tmdb_id") and media.tmdb_id):
                return
            tmdb_id_int = int(media.tmdb_id)
            tmdb_data = []
            for item in items:
                if item.tmdb_id is None and item.indexer in _POSTGRES_INDEXERS:
                    tmdb_data.append({
                        "info_hash": item.info_hash,
                        "raw_title": item.raw_title,
                        "tmdb_id": tmdb_id_int,
                        "indexer": item.indexer,
                    })
                    item.tmdb_id = tmdb_id_int
            if tmdb_data:
                session_factory = request.app.state.db_session_factory
                asyncio.ensure_future(_bg_assign_tmdb_ids(session_factory, tmdb_data))

        # --- Helper: check Postgres cache validity (sufficient + fresh) ---
        # max_ts comes from the DB model timestamps (not TorrentItem DTO which has no timestamp fields)
        def _is_postgres_cache_valid(confirmed_items, max_ts: int):
            if min_postgres <= 0 or len(confirmed_items) < min_postgres:
                return False
            if postgres_max_age_days <= 0:
                return True
            now_ts = int(datetime.now(timezone.utc).timestamp())
            max_age_ts = postgres_max_age_days * 86400
            return (now_ts - max_ts) < max_age_ts

        # --- Helper: Postgres fetch — returns (TorrentItem list, max updated_at timestamp) ---
        async def _fetch_postgres():
            if not (hasattr(media, "tmdb_id") and media.tmdb_id):
                return [], 0
            try:
                items = await torrent_dao.search_by_tmdb_id(int(media.tmdb_id))
                results = []
                max_ts = 0
                for db_item in (items or []):
                    if db_item.indexer in _POSTGRES_INDEXERS:
                        results.append(db_item.to_torrent_item())
                        # Read timestamps from DB model (TorrentItem DTO has no timestamp fields)
                        item_ts = db_item.updated_at or db_item.created_at or 0
                        if item_ts > max_ts:
                            max_ts = item_ts
                if results:
                    logger.success(
                        f"Search: Found {len(results)} results from Postgres (local cache) for TMDB ID {media.tmdb_id}"
                    )
                return results, max_ts
            except Exception as pg_error:
                logger.error(f"Search: Postgres search failed: {str(pg_error)}")
                return [], 0

        # --- Helper: Redis key for per-indexer background refresh lock (Feature C) ---
        def _bg_refresh_redis_key(indexer_key):
            if isinstance(media, Series):
                return f"bg_refresh:{indexer_key}:{media.tmdb_id}:{media.season}{media.episode}"
            return f"bg_refresh:{indexer_key}:{media.tmdb_id}"

        # --- Background DB refresh: queries all enabled indexers in parallel (Features B + C) ---
        # Fired from the fast path only. Uses an independent DB session.
        # Per-indexer Redis lock prevents redundant queries across users (TTL = bg_refresh_indexer_ttl).
        async def _background_db_refresh():
            await asyncio.sleep(getattr(settings, "bg_refresh_delay_seconds", 2.0))
            bg_session = request.app.state.db_session_factory()
            try:
                bg_torrent_dao = TorrentItemDAO(bg_session)
                bg_torrent_service = TorrentService(config, bg_torrent_dao, TorrentGroupDAO(bg_session))
                _avail_redis = await RedisCache(config).get_redis_client()
                http_session_bg = getattr(request.app.state, "http_session", None)

                async def _bg_fetch_c411():
                    if not config.get("c411") or not settings.c411_enable:
                        return []
                    c411_service = C411Service(config, session=http_session_bg)
                    raw = await c411_service.search(media)
                    return [
                        C411SearchResult().from_api_item(item, media)
                        for item in (raw or [])
                        if getattr(item, "info_hash", None) and len(item.info_hash) == 40
                    ]

                async def _bg_fetch_torr9():
                    if not config.get("torr9") or not settings.torr9_enable:
                        return []
                    torr9_service = Torr9Service(config, session=http_session_bg)
                    raw = await torr9_service.search(media)
                    return [
                        Torr9SearchResult().from_api_item(item, media)
                        for item in (raw or [])
                        if getattr(item, "info_hash", None) and len(item.info_hash) == 40
                    ]

                async def _bg_fetch_lacale():
                    if not config.get("lacale") or not settings.lacale_enable:
                        return []
                    raw = await LaCaleService(config, session=http_session_bg).search(media)
                    return raw or []

                async def _bg_fetch_generationfree():
                    if not config.get("generationfree"):
                        return []
                    raw = await GenerationFreeService(config, session=http_session_bg).search(media)
                    return raw or []

                async def _bg_fetch_abn():
                    if not config.get("abn") or not settings.abn_enable:
                        return []
                    raw = await AbnService(config, session=http_session_bg).search(media)
                    return raw or []

                async def _bg_fetch_g3mini():
                    if not config.get("g3mini") or not settings.g3mini_enable:
                        return []
                    raw = await G3MiniService(config, session=http_session_bg).search(media)
                    return raw or []

                async def _bg_fetch_theoldschool():
                    if not config.get("theoldschool") or not settings.theoldschool_enable:
                        return []
                    raw = await TheOldSchoolService(config, session=http_session_bg).search(media)
                    return raw or []

                async def _bg_fetch_zilean():
                    if not config.get("zilean"):
                        return []
                    raw = await ZileanService(config, session=http_session_bg).search(media)
                    return [
                        ZileanResult().from_api_cached_item(t, media)
                        for t in (raw or [])
                        if len(getattr(t, "info_hash", "")) == 40
                    ]

                async def _bg_fetch_jackett():
                    if not config.get("jackett"):
                        return []
                    raw = await JackettService(config, session=http_session_bg).search(media)
                    return raw or []

                async def _bg_fetch_utopeer():
                    if not config.get("utopeer"):
                        return []
                    raw = await UtopeerService(config).search(media)
                    return raw or []

                BG_FETCHERS = [
                    ("c411",           _bg_fetch_c411),
                    ("torr9",          _bg_fetch_torr9),
                    ("lacale",         _bg_fetch_lacale),
                    ("generationfree", _bg_fetch_generationfree),
                    ("abn",            _bg_fetch_abn),
                    ("g3mini",         _bg_fetch_g3mini),
                    ("theoldschool",   _bg_fetch_theoldschool),
                    ("zilean",         _bg_fetch_zilean),
                    ("jackett",        _bg_fetch_jackett),
                    ("utopeer",        _bg_fetch_utopeer),
                ]

                # Feature C: wrap each fetcher with a per-indexer Redis lock
                async def _fetch_with_guard(indexer_key, fetch_fn):
                    if settings.bg_refresh_indexer_ttl > 0:
                        redis_key = _bg_refresh_redis_key(indexer_key)
                        if await _avail_redis.exists(redis_key):
                            logger.debug(f"Search BG: {indexer_key} recently refreshed, skipping")
                            return []
                        await _avail_redis.set(redis_key, "1", ex=settings.bg_refresh_indexer_ttl)
                    try:
                        return await fetch_fn()
                    except Exception as e:
                        if settings.bg_refresh_indexer_ttl > 0:
                            await _avail_redis.delete(_bg_refresh_redis_key(indexer_key))
                        logger.debug(f"Search BG: {indexer_key} failed: {e}")
                        return []

                # All indexers in parallel — no phases, maximize coverage
                raw_list = await asyncio.gather(*[
                    _fetch_with_guard(key, fn) for key, fn in BG_FETCHERS
                ])

                bg_results = []
                for (key, _), raw in zip(BG_FETCHERS, raw_list):
                    if raw:
                        processed = await bg_torrent_service.convert_and_process(raw)
                        if processed:
                            bg_results = merge_items(bg_results, processed)

                if bg_results:
                    bg_confirmed = await asyncio.to_thread(apply_correctness_filters, bg_results, media)
                    if hasattr(media, "tmdb_id") and media.tmdb_id:
                        tmdb_id_int = int(media.tmdb_id)
                        bg_bulk_hashes: list[str] = []
                        bg_title_updates = []
                        for item in bg_confirmed:
                            if item.tmdb_id is None and item.indexer in _POSTGRES_INDEXERS:
                                if item.info_hash:
                                    bg_bulk_hashes.append(item.info_hash)
                                else:
                                    bg_title_updates.append(bg_torrent_dao.update_tmdb_id_by_raw_title(item.raw_title, tmdb_id_int, indexer=item.indexer))
                                item.tmdb_id = tmdb_id_int
                        # One bulk UPDATE for all hashes — avoids per-row deadlocks
                        if bg_bulk_hashes:
                            await bg_torrent_dao.bulk_update_tmdb_id_by_info_hashes(bg_bulk_hashes, tmdb_id_int)
                        for coro in bg_title_updates:
                            await coro
                    pg_hashes = [
                        item.info_hash for item in bg_confirmed
                        if item.info_hash and item.indexer in _POSTGRES_INDEXERS
                    ]
                    if pg_hashes:
                        await bg_torrent_dao.touch_items_by_info_hash(pg_hashes)
                    logger.debug(f"Search BG: refresh complete — {len(bg_confirmed)} confirmed items processed")
                else:
                    logger.debug("Search BG: refresh found no results")
            except Exception as e:
                logger.debug(f"Search BG: refresh failed: {e}")
            finally:
                await bg_session.commit()
                await bg_session.close()

        # --- Cache-first: check Postgres before hitting live indexers ---
        postgres_results, postgres_max_ts = await _fetch_postgres()
        confirmed_postgres = await asyncio.to_thread(apply_correctness_filters, postgres_results, media)

        if _is_postgres_cache_valid(confirmed_postgres, postgres_max_ts):
            _assign_tmdb_ids_background(confirmed_postgres)
            filtered_results = await asyncio.to_thread(apply_preference_filters, confirmed_postgres, media, config)
            min_cached = int(config.get("minCachedResults", 8))
            if len(filtered_results) >= min_cached:
                logger.info(
                    f"Search: Postgres cache fast path — {len(filtered_results)} >= minCachedResults={min_cached} results, skipping live indexers"
                )
                asyncio.ensure_future(_background_db_refresh())
                return filtered_results, True
            logger.warning(
                f"Search: Postgres cache had only {len(filtered_results)} results after preference filters "
                f"(< minCachedResults={min_cached}), falling back to live search"
            )

        logger.debug(
            f"Search: Postgres cache insufficient or stale "
            f"({len(confirmed_postgres)} confirmed, max_ts={postgres_max_ts}), proceeding with live search"
        )

        # --- Full path: Redis cache + live indexer search ---
        cached_external = await redis_cache.get(cache_key)

        if cached_external is None:
            logger.debug("Search: No external sources in Redis cache. Performing new search.")
            external_results = await get_search_results(media, config)
            just_fetched = True  # avoid double-fetch if results are insufficient
            await redis_cache.set(
                cache_key,
                [item.to_dict() for item in external_results],
                expiration=settings.redis_expiration,
            )
            logger.success(
                f"Search: Cached {len(external_results)} external results in Redis (Zilean/Jackett)"
            )
        else:
            logger.success(
                f"Search: Retrieved {len(cached_external)} external results from Redis cache"
            )
            external_results = [TorrentItem.from_dict(item) for item in cached_external]
            just_fetched = False

        all_results = merge_items(postgres_results, external_results)
        logger.debug(
            f"Search: Merged Postgres ({len(postgres_results)}) + External ({len(external_results)}) = {len(all_results)} total results"
        )

        # --- Step 1: correctness filters (year/season + title matching) ---
        confirmed_results = await asyncio.to_thread(
            apply_correctness_filters, all_results, media
        )

        # --- Re-fetch if stale cache has insufficient results (skip if just freshly fetched) ---
        min_results = int(config.get("minCachedResults", 8))
        if not just_fetched:
            external_confirmed = await asyncio.to_thread(
                apply_correctness_filters, external_results, media
            )
            if len(external_confirmed) < min_results:
                logger.warning(
                    f"Search: Insufficient external results ({len(external_confirmed)} < {min_results}). Recreating external cache."
                )
                await redis_cache.delete(cache_key)
                external_results = await get_search_results(media, config)
                await redis_cache.set(
                    cache_key,
                    [item.to_dict() for item in external_results],
                    expiration=settings.redis_expiration,
                )
                logger.success(
                    f"Search: Recreated external cache with {len(external_results)} results"
                )
                all_results = merge_items(postgres_results, external_results)
                confirmed_results = await asyncio.to_thread(
                    apply_correctness_filters, all_results, media
                )

        logger.success(f"Search: Confirmed results after correctness filters: {len(confirmed_results)}")

        # --- Step 2: retroactive TMDB ID assignment (background, non-blocking) ---
        # Runs on ALL confirmed items (including those excluded by user preferences)
        # so the DB stays up to date regardless of user config.
        _assign_tmdb_ids_background(confirmed_results)

        # --- Refresh Postgres TTL so the next request uses the cache ---
        # Touch updated_at for all confirmed Postgres items so the cache-first TTL
        # resets from now — preventing a live search loop after TTL expiry.
        postgres_info_hashes = [
            item.info_hash for item in confirmed_results
            if item.info_hash and item.indexer in _POSTGRES_INDEXERS
        ]
        if postgres_info_hashes:
            asyncio.ensure_future(_bg_touch_items(request.app.state.db_session_factory, postgres_info_hashes))

        # --- Step 3: user-preference filters (language, quality, exclusions, sorting) ---
        filtered_results = await asyncio.to_thread(
            apply_preference_filters, confirmed_results, media, config
        )
        logger.success(f"Search: Final number of filtered results: {len(filtered_results)}")

        return filtered_results, False

    # --- Search: aggregate results from Postgres + external sources, apply quality filter ---
    raw_search_results, from_cache = await get_and_filter_results(media, config)
    search_results = ResultsPerQualityFilter(config).filter(raw_search_results)
    logger.info(f"Search: Filtered search results per quality: {len(search_results)}")

    # --- Debrid availability check + stream parsing ---
    async def stream_processing(search_results, media, config, from_cache: bool = False):
        torrent_smart_container = TorrentSmartContainer(search_results, media)

        if config["debrid"]:
            _avail_redis = await RedisCache(config).get_redis_client()
            max_results = int(config.get("maxResults", 5))
            ip = get_client_ip(request)

            fast_debrids = [d for d in debrid_services if type(d).__name__ != "RealDebrid"]
            rd_debrids   = [d for d in debrid_services if type(d).__name__ == "RealDebrid"]

            # --- Phase 1: all non-RD debrids in parallel (Torbox, AllDebrid, Premiumize, StremThru…) ---
            hashes = torrent_smart_container.get_unaviable_hashes()
            if fast_debrids and hashes:
                async def _check_debrid(debrid):
                    debrid_session = request.app.state.db_session_factory()
                    try:
                        result = await debrid.get_availability_bulk_cached(hashes, ip, _avail_redis, db_session=debrid_session)
                        await debrid_session.commit()
                        return debrid, result
                    except Exception as e:
                        logger.warning(f"Search: {type(debrid).__name__} availability check failed: {e}")
                        return debrid, None
                    finally:
                        await debrid_session.close()

                parallel_results = await asyncio.gather(*[_check_debrid(d) for d in fast_debrids])

                for debrid, result in parallel_results:
                    debrid_name = type(debrid).__name__
                    if result is None:
                        logger.warning(f"Search: {debrid_name} returned None for {len(hashes)} hashes (API failure?)")
                    elif result:
                        torrent_smart_container.update_availability(result, type(debrid), media)
                        if isinstance(result, dict):
                            data = result.get("data", [])
                            returned_count = len(data) if isinstance(data, list) else len(data.get("magnets", []))
                        else:
                            returned_count = len(result)
                        logger.info(f"Search: Checked availability for {len(hashes)} items with {debrid_name} (returned {returned_count})")
                    else:
                        logger.debug(f"Search: {debrid_name} found 0 cached hashes out of {len(hashes)}")

            # --- Phase 2: RealDebrid — conditional, only on remaining unavailable hashes ---
            # RD is slow (~3s) and rate-limited — only call if fast debrids didn't find enough.
            if rd_debrids:
                cached_count = torrent_smart_container.get_cached_count()
                if cached_count >= max_results:
                    logger.info(
                        f"Search: {cached_count} cached results ≥ maxResults ({max_results}), skipping RealDebrid"
                    )
                else:
                    rd_min = int(config.get("rdMinCachedBeforeCheck", 3))
                    if rd_min > 0 and cached_count >= rd_min:
                        logger.info(
                            f"Search: {cached_count} cached results ≥ rdMinCachedBeforeCheck ({rd_min}), skipping RealDebrid"
                        )
                    else:
                        for rd in rd_debrids:
                            rd_hashes = torrent_smart_container.get_unaviable_hashes()
                            if not rd_hashes:
                                logger.debug("Search: All items already marked available, skipping RealDebrid")
                                break
                            result = await rd.get_availability_bulk_cached(rd_hashes, ip, _avail_redis, db_session=db)
                            if result is None:
                                logger.warning(f"Search: {type(rd).__name__} returned None for {len(rd_hashes)} hashes (API failure?)")
                            elif result:
                                torrent_smart_container.update_availability(result, type(rd), media)
                                if isinstance(result, dict):
                                    data = result.get("data", [])
                                    returned_count = len(data) if isinstance(data, list) else len(data.get("magnets", []))
                                else:
                                    returned_count = len(result)
                                logger.info(f"Search: Checked availability for {len(rd_hashes)} items with {type(rd).__name__} (returned {returned_count})")
                            else:
                                logger.debug(f"Search: {type(rd).__name__} found 0 cached hashes out of {len(rd_hashes)}")

        best_matching_results = torrent_smart_container.get_best_matching()

        # Exclude results from public indexers (e.g. u2p / Utopeer) that are not instantly cached
        # at the debrid service — non-cached public torrents require a download, not a stream.
        _public_indexer_keys = {
            k for k, v in config.get("indexerCategories", {}).items() if v == "public"
        }
        if _public_indexer_keys:
            before = len(best_matching_results)
            best_matching_results = [
                item for item in best_matching_results
                if _indexer_config_key(item.indexer) not in _public_indexer_keys
                or item.availability
            ]
            excluded = before - len(best_matching_results)
            if excluded:
                logger.info(f"Search: Excluded {excluded} non-cached public indexer result(s)")

        # --- Cache fast path: filter non-instant results from unconfigured indexers + rename for display ---
        # Instant results (availability=True) are always shown — debrid already has the file.
        # Non-instant results from cache require the user to have the indexer configured, otherwise
        # the announce URL won't work (private tracker credentials belong to another user).
        # Public indexers (e.g. u2p - Cache) are never shown non-instant — no credentials needed
        # but they can't be streamed without debrid caching.
        if from_cache:
            before = len(best_matching_results)
            best_matching_results = [
                item for item in best_matching_results
                if item.availability
                or (
                    _indexer_config_key(item.indexer) not in _public_indexer_keys
                    and config.get(_indexer_config_key(item.indexer), False)
                )
            ]
            excluded = before - len(best_matching_results)
            if excluded:
                logger.debug(f"Search: Excluded {excluded} non-instant cache result(s) from unconfigured indexers")
            for item in best_matching_results:
                if item.availability:
                    item.indexer = "SF - Cache"

        best_matching_results = sort_items(best_matching_results, config)
        logger.info(f"Search: Found {len(best_matching_results)} best matching results")

        parser = StreamParser(config)
        stream_list = await parser.parse_to_stremio_streams(
            best_matching_results, media
        )
        logger.success(f"Search: Processed {len(stream_list)} streams for Stremio")

        return stream_list

    stream_list = await stream_processing(search_results, media, config, from_cache=from_cache)
    streams = [Stream(**stream) for stream in stream_list]

    # --- Cache final stream list (user-specific, shorter TTL for StremThru) ---
    stream_cache_value = serialize_streams_for_cache(streams)
    expiration_time = 600 if has_stremthru_enabled(debrid_services) else 1200
    if expiration_time == 600:
        logger.info(
            f"Search: Using reduced cache expiration ({expiration_time}s) for StremThru"
        )
    await redis_cache.set(
        stream_cache_key(media),
        stream_cache_value,
        expiration=expiration_time,
    )

    # --- Background pre-fetch: start loading next episode streams while user watches ---
    if isinstance(media, Series):
        asyncio.create_task(
            full_prefetch_from_cache(
                media,
                config,
                redis_cache,
                stream_cache_key,
                get_metadata,
                stream_type,
                debrid_services,
                torrent_dao,
                request,
            )
        )

    total_time = time.time() - start
    logger.info(f"Search: Request completed in {total_time:.2f} seconds")
    return SearchResponse(streams=streams)