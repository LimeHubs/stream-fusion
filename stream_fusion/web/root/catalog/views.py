import asyncio
import json
from datetime import datetime, timedelta

from redis import Redis
from tmdbv3api import TMDb, Movie, TV, Season, Discover, Find
from fastapi import APIRouter, Depends, HTTPException, Request

from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.dao.torrentitem_dao import TorrentItemDAO
from stream_fusion.settings import settings
from stream_fusion.utils.parse_config import parse_config
from stream_fusion.utils.security.security_api_key import check_api_key
from stream_fusion.web.root.catalog.schemas import (
    ErrorResponse,
    MetaItem,
    Metas,
    Meta,
    Video,
)
from stream_fusion.services.redis.redis_config import get_redis
from stream_fusion.logging_config import logger

router = APIRouter()

tmdb = TMDb()
tmdb.api_key = settings.tmdb_api_key
tmdb.language = settings.tmdb_language
movie = Movie()
tv = TV()
season = Season()
discover = Discover()
find = Find()


async def get_movie_details(tmdb_id):
    return await asyncio.to_thread(movie.details, tmdb_id)


async def get_tv_details(tmdb_id):
    return await asyncio.to_thread(tv.details, tmdb_id)


async def get_tv_season_details(tmdb_id, season_number):
    return await asyncio.to_thread(season.details, tmdb_id, season_number)


async def validate_config_and_api_key(config: str, apikey_dao: APIKeyDAO):
    config_data = parse_config(config)
    api_key = config_data.get("apiKey")
    if api_key:  
        await check_api_key(api_key, apikey_dao)
    return api_key


async def get_cached_item(redis_client: Redis, cache_key: str):
    cached_item = await asyncio.to_thread(redis_client.get, cache_key)
    if cached_item:
        return json.loads(cached_item)
    return None


async def cache_item(
    redis_client: Redis, cache_key: str, item, duration: int = 7 * 24 * 60 * 60
):
    await asyncio.to_thread(
        redis_client.set, cache_key, item.model_dump_json(), ex=duration
    )


def extract_year(date_string):
    if date_string and len(date_string) >= 4:
        return date_string[:4]
    return None


async def create_meta_object(details, item_type: str, imdb_id: str, include_episodes: bool = True):
    """
    Crée un objet Meta à partir des détails TMDB.

    Args:
        include_episodes: Si False, ne charge pas les épisodes (pour le catalogue).
                         Si True, charge tous les épisodes (pour le endpoint /meta).
    """
    meta = Meta(
        id=imdb_id,
        name=getattr(details, "title", None) or getattr(details, "name", None),
        type=item_type,
        poster=(
            f"https://image.tmdb.org/t/p/w500{details.poster_path}"
            if details.poster_path
            else None
        ),
        background=(
            f"https://image.tmdb.org/t/p/original{details.backdrop_path}"
            if details.backdrop_path
            else None
        ),
        country=(
            details.production_countries[0].name
            if details.production_countries
            else None
        ),
        tv_language=details.original_language,
        logo=None,
        genres=[genre.name for genre in details.genres] if details.genres else None,
        description=details.overview,
        runtime=(
            f"{str(details.runtime)} minutes" if item_type == "movie" and details.runtime else None
        ),
        website=details.homepage,
        imdb_rating=str(details.vote_average) if details.vote_average else None,
        year=extract_year(
            details.release_date if item_type == "movie" else details.first_air_date
        ),
    )

    if item_type == "movie":
        meta.stream = {
            "id": imdb_id
        }
    elif item_type == "series" and include_episodes and hasattr(details, "seasons"):
        # Charger les épisodes seulement si demandé (pour /meta, pas pour /catalog)
        meta.videos = []
        for season in details.seasons:
            season_details = await get_tv_season_details(
                details.id, season.season_number
            )
            for episode in season_details.episodes:
                meta.videos.append(
                    Video(
                        id=f"{imdb_id}:{season.season_number}:{episode.episode_number}",
                        title=episode.name,
                        released=str(episode.air_date),
                        season=season.season_number,
                        episode=episode.episode_number,
                    )
                )

    return meta


async def get_tmdb_id_from_imdb(imdb_id: str) -> str:
    results = await asyncio.to_thread(find.find_by_imdb_id, imdb_id)
    if results.movie_results:
        return results.movie_results[0]["id"]
    elif results.tv_results:
        return results.tv_results[0]["id"]
    return None


@router.get(
    "/{config}/catalog/{type}/{id}.json", responses={500: {"model": ErrorResponse}}
)
@router.get(
    "/{config}/catalog/{type}/{id}/skip={skip}.json", responses={500: {"model": ErrorResponse}}
)
async def get_catalog(
    config: str,
    type: str,
    id: str,
    request: Request,
    skip: int = 0,
    redis_client: Redis = Depends(get_redis),
    apikey_dao: APIKeyDAO = Depends(),
    torrentitem_dao: TorrentItemDAO = Depends()
):
    try:
        config_data = parse_config(config)
        api_key = config_data.get("apiKey")
        if api_key:  
            await check_api_key(api_key, apikey_dao)

        logger.debug(
            f"Received catalog request from api_key: {api_key}, type: {type}, id: {id}, config: {config_data}"
        )

        if type not in {"movie", "series"} or id not in {
            "latest_movies",
            "recently_added_movies",
            "latest_tv_shows",
            "recently_added_tv_shows",
        }:
            raise HTTPException(status_code=400, detail="Invalid type or catalog id")

        cache_key = f"catalog:{type}:{id}"
        cached_catalog = await get_cached_item(redis_client, cache_key)
        if cached_catalog:
            logger.info(f"Catalog found in cache for key: {cache_key}")
            full_catalog = Metas.model_validate(cached_catalog)
            return Metas(metas=full_catalog.metas[skip:])

        logger.info(f"Catalog not found in cache for key: {cache_key}. Generating...")

        item_ids = []
        episode_info_map = {}  # Mapping tmdb_id -> episode info pour les séries

        # Map catalog IDs to item types for PostgreSQL queries
        catalog_type_map = {
            "latest_movies": "movie",
            "recently_added_movies": "movie",
            "latest_tv_shows": "series",
            "recently_added_tv_shows": "series",
        }

        item_type = catalog_type_map.get(id)

        # For "latest_*": Different logic for movies vs series
        if id.startswith("latest_"):
            try:
                if item_type == "movie":
                    # Films: TMDB discover (films récents des 6 derniers mois) + filtre FR disponible en PostgreSQL
                    logger.info(f"Fetching latest movies from TMDB discover (last 6 months) and filtering by PostgreSQL availability")
                    today = datetime.now().strftime('%Y-%m-%d')
                    six_months_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
                    tmdb_results = await asyncio.gather(
                        *[asyncio.to_thread(discover.discover_movies, {
                            'sort_by': 'popularity.desc',
                            'primary_release_date.gte': six_months_ago,
                            'primary_release_date.lte': today,
                            'page': page,
                        }) for page in range(1, 11)]
                    )
                    all_tmdb_ids = []
                    for results in tmdb_results:
                        for item in results:
                            if hasattr(item, 'id'):
                                all_tmdb_ids.append(item.id)
                    logger.info(f"Fetched {len(all_tmdb_ids)} TMDB IDs from TMDB discover for {id}")

                    # Filtre par disponibilité FR/MULTI, trié par date d'ajout en base (plus récent en premier)
                    item_ids = await torrentitem_dao.filter_existing_tmdb_ids(all_tmdb_ids, item_type, sort_by_added=True)
                    logger.info(f"Filtered to {len(item_ids)} available TMDB IDs (FR/MULTI, sorted by added date) for {id}")
                else:
                    # Séries: TMDB discover avec air_date récent (7 derniers jours) + filtre FR en PostgreSQL
                    # Utilise discover au lieu de on_the_air car on_the_air vire trop vite les séries binge-release
                    logger.info(f"Fetching latest series from TMDB discover (air_date last 7 days) and filtering by PostgreSQL availability")
                    today = datetime.now().strftime('%Y-%m-%d')
                    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                    tmdb_results = await asyncio.gather(
                        *[asyncio.to_thread(discover.discover_tv_shows, {
                            'air_date.gte': week_ago,
                            'without_genres': '10763,10764,10766,10767',
                            'air_date.lte': today,
                            'sort_by': 'popularity.desc',
                            'page': page
                        }) for page in range(1, 11)]
                    )
                    all_tmdb_ids = []
                    for results in tmdb_results:
                        for item in results:
                            if hasattr(item, 'id'):
                                all_tmdb_ids.append(item.id)
                    logger.info(f"Fetched {len(all_tmdb_ids)} series TMDB IDs from TMDB discover (air_date) for {id}")

                    # Filtre par disponibilité FR/MULTI, trié par dernier nouvel épisode en base
                    # Récupère aussi les infos d'épisode pour l'afficher dans le titre
                    episode_data = await torrentitem_dao.filter_existing_tmdb_ids(all_tmdb_ids, item_type, sort_by_added=True, return_episode_info=True)
                    item_ids = [ep['tmdb_id'] for ep in episode_data]
                    # Créer un mapping tmdb_id -> episode info
                    episode_info_map = {ep['tmdb_id']: ep for ep in episode_data}
                    logger.info(f"Filtered to {len(item_ids)} available series TMDB IDs (FR/MULTI, sorted by new episode date) for {id}")

            except Exception as e:
                logger.warning(f"Failed to fetch latest catalog for {id}: {e}. Falling back to PostgreSQL only.")
                item_ids = await torrentitem_dao.get_latest_tmdb_ids(item_type, limit=50)

        # For "recently_added_*": Use PostgreSQL directly (recent uploads)
        elif id.startswith("recently_added_"):
            try:
                logger.info(f"Fetching recently added TMDB IDs from PostgreSQL for {item_type}")
                item_ids = await torrentitem_dao.get_recently_added_tmdb_ids(item_type, limit=50)
                logger.info(f"Fetched {len(item_ids)} TMDB IDs from PostgreSQL for {id}")
            except Exception as pg_error:
                logger.warning(f"Failed to fetch catalog from PostgreSQL for {id}: {pg_error}")
                item_ids = []

        # Fallback to TMDb discover if no results
        if not item_ids:
            logger.info(f"Fallback: Fetching catalog from TMDb discover for type: {type}, id: {id}")
            try:
                tmdb_params = {"page": 1}
                if type == "movie":
                    discover_func = discover.discover_movies
                else:
                    discover_func = discover.discover_tv_shows

                results = await asyncio.to_thread(discover_func, tmdb_params)
                item_ids = [item.id for item in results if hasattr(item, 'id')]
                logger.info(f"Fetched {len(item_ids)} IDs from TMDb discover for {type}/{id}")

            except Exception as tmdb_error:
                logger.error(f"Failed to fetch catalog from TMDb for {type}/{id}: {tmdb_error}", exc_info=True)
                item_ids = []

        pipeline = redis_client.pipeline()
        process_limit = 50

        def _apply_episode_prefix(meta: Meta, tmdb_id: int) -> None:
            if tmdb_id not in episode_info_map:
                return
            ep_info = episode_info_map[tmdb_id]
            ep_season = ep_info.get('season', '').strip('[]') or ''
            ep_episode = ep_info.get('episode', '').strip('[]') or ''
            if ep_season:
                ep_prefix = f"S{ep_season.zfill(2)}E{ep_episode.zfill(2)}" if ep_episode else f"S{ep_season.zfill(2)}"
                meta.name = f"{ep_prefix} - {meta.name}"

        async def process_one(tmdb_id: int):
            item_cache_key_tmdb = f"tmdbid_item:{tmdb_id}"
            cached_item = await get_cached_item(redis_client, item_cache_key_tmdb)
            if cached_item:
                try:
                    meta = Meta.model_validate(cached_item)
                    _apply_episode_prefix(meta, tmdb_id)
                    return meta
                except Exception as validation_error:
                    logger.warning(f"Failed to validate cached meta for TMDB ID {tmdb_id}: {validation_error}")

            try:
                if type == "movie":
                    details = await get_movie_details(tmdb_id)
                    item_type = "movie"
                    imdb_id = getattr(details, 'imdb_id', None)
                else:
                    details = await get_tv_details(tmdb_id)
                    item_type = "series"
                    cached_imdb_id = await asyncio.to_thread(redis_client.get, f"tmdbid_to_imdbid:{tmdb_id}")
                    if cached_imdb_id:
                        imdb_id = cached_imdb_id.decode('utf-8') if isinstance(cached_imdb_id, bytes) else cached_imdb_id
                        logger.debug(f"IMDb ID found in cache for TMDB ID {tmdb_id}: {imdb_id}")
                    else:
                        external_ids = await asyncio.to_thread(tv.external_ids, tmdb_id)
                        imdb_id = external_ids.get("imdb_id")

                if not imdb_id:
                    logger.warning(f"No IMDb ID found for TMDB ID: {tmdb_id}")
                    return None

                meta = await create_meta_object(details, item_type, imdb_id, include_episodes=False)

                try:
                    pipeline.set(item_cache_key_tmdb, meta.model_dump_json(), ex=7 * 24 * 60 * 60)
                    pipeline.set(f"imdbid_item:{imdb_id}", meta.model_dump_json(), ex=7 * 24 * 60 * 60)
                    pipeline.set(f"tmdbid_to_imdbid:{tmdb_id}", imdb_id, ex=7 * 24 * 60 * 60)
                except Exception as cache_err:
                    logger.error(f"Error adding item TMDB:{tmdb_id}/IMDB:{imdb_id} to cache pipeline: {cache_err}")

                _apply_episode_prefix(meta, tmdb_id)
                return meta

            except Exception as e:
                logger.error(f"Error processing item with TMDB ID {tmdb_id}: {str(e)}")
                return None

        results = await asyncio.gather(*[process_one(tmdb_id) for tmdb_id in item_ids[:process_limit]])
        metas = [m for m in results if m is not None]

        try:
            await asyncio.to_thread(pipeline.execute)
        except Exception as pipe_exec_err:
            logger.error(f"Error executing cache pipeline: {pipe_exec_err}")

        catalog = Metas(metas=metas)
        try:
            await cache_item(redis_client, cache_key, catalog, 1800)
        except Exception as cat_cache_err:
             logger.error(f"Error caching final catalog {cache_key}: {cat_cache_err}")

        logger.info(f"Catalog generated and cached for key: {cache_key} with {len(metas)} items.")
        return Metas(metas=catalog.metas[skip:])

    except Exception as e:
        logger.error(f"Catalog error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )


@router.get(
    "/{config}/meta/{type}/{id}.json", responses={500: {"model": ErrorResponse}}
)
async def get_meta(
    config: str,
    type: str,
    id: str,
    request: Request,
    redis_client: Redis = Depends(get_redis),
    apikey_dao: APIKeyDAO = Depends()
):
    try:
        config_data = parse_config(config)
        api_key = config_data.get("apiKey")
        if api_key:  
            await check_api_key(api_key, apikey_dao)

        logger.debug(
            f"Received meta request from api_key: {api_key}, type: {type}, id: {id}"
        )

        if type not in {"movie", "series"}:
            raise HTTPException(status_code=400, detail="Invalid type")

        cache_key = f"imdbid_item:{id}"
        cached_meta = await get_cached_item(redis_client, cache_key)
        if cached_meta:
            logger.info(f"Meta found in cache for IMDB ID: {id}")
            meta = Meta.model_validate(cached_meta)
            return MetaItem(meta=meta)

        logger.info(f"Meta not found in cache for IMDB ID: {id}, fetching from TMDB")

        tmdb_id = await get_tmdb_id_from_imdb(id)
        if not tmdb_id:
            logger.warning(f"No TMDB ID found for IMDB ID: {id}")
            raise HTTPException(status_code=404, detail="Item not found")

        if type == "movie":
            details = await get_movie_details(tmdb_id)
            item_type = "movie"
        else:
            details = await get_tv_details(tmdb_id)
            item_type = "series"

        logger.debug(f"Creating Meta object for {item_type} with IMDB ID: {id}")
        meta = await create_meta_object(details, item_type, id)

        await cache_item(redis_client, cache_key, meta)
        await cache_item(redis_client, f"tmdbid_item:{tmdb_id}", meta)
        await cache_item(redis_client, f"tmdbid_to_imdbid:{tmdb_id}", id)

        logger.info(f"Meta generated and cached for IMDB ID: {id}, TMDB ID: {tmdb_id}")
        return MetaItem(meta=meta)

    except Exception as e:
        logger.error(f"Meta error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request."
        )
