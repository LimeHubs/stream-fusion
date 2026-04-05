from cachetools import TTLCache
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.settings import settings
from stream_fusion.utils.parse_config import parse_config
from stream_fusion.utils.security.security_api_key import check_api_key
from stream_fusion.utils.string_encoding import generate_csrf_token
from stream_fusion.version import get_version
from stream_fusion.web.root.config.schemas import ManifestResponse

router = APIRouter()

templates = Jinja2Templates(directory="/app/stream_fusion/static")
stream_cache = TTLCache(maxsize=1000, ttl=3600)


@router.get("/")
async def root():
    logger.info("Redirecting to /configure")
    return RedirectResponse(url="/configure")


@router.get("/configure")
@router.get("/{config}/configure")
async def configure(request: Request):
    logger.info("Serving configuration page")
    return templates.TemplateResponse("index.html", {
        "request": request,
        "rd_unique_account": settings.rd_unique_account,
        "ad_unique_account": settings.ad_unique_account,
        "utopeer_unique_account": settings.utopeer_unique_account,
        "jackett_enable": settings.jackett_enable,
        "tb_unique_account": settings.tb_unique_account,
        "c411_enable": settings.c411_enable,
        "torr9_enable": settings.torr9_enable,
        "lacale_enable": settings.lacale_enable,
        "c411_unique_account": settings.c411_unique_account,
        "torr9_unique_account": settings.torr9_unique_account,
        "lacale_unique_account": settings.lacale_unique_account,
        "generationfree_enable": settings.generationfree_enable,
        "generationfree_unique_account": settings.generationfree_unique_account,
        "abn_enable": settings.abn_enable,
        "abn_unique_account": settings.abn_unique_account,
        "g3mini_enable": settings.g3mini_enable,
        "g3mini_unique_account": settings.g3mini_unique_account,
        "theoldschool_enable": settings.theoldschool_enable,
        "theoldschool_unique_account": settings.theoldschool_unique_account,
        "pm_unique_account": settings.pm_unique_account,
        "allow_public_key_registration": settings.allow_public_key_registration,
        "csrf_token": generate_csrf_token(),
    })


# @router.get("/static/{file_path:path}", response_model=StaticFileResponse)
# async def serve_static(file_path: str):
#     logger.debug(f"Serving static file: {file_path}")
#     return FileResponse(f"/app/stream_fusion/static/{file_path}")


@router.get("/manifest.json")
async def get_manifest():
    logger.info("Serving manifest.json")
    return ManifestResponse(
        id="community.limedrive.streamfusion",
        logo="https://i.ibb.co/ZRZcVBLd/SF-modern.png",
        version=str(get_version()),
        resources=[
            'catalog',
            {
                'name': 'stream', 
                'types': ['movie', 'series'], 
                'idPrefixes': ['tt']
            }
        ],
        types=["movie", "series"],
        name="StreamFusion" + (" (dev)" if settings.develop else ""),
        description="StreamFusion enhances Stremio by integrating torrent indexers and debrid services, "
                    "providing access to a vast array of cached torrent sources. This plugin seamlessly bridges "
                    "Stremio with popular indexers and debrid platforms, offering users an expanded content "
                    "library and a smooth streaming experience.",
        catalogs=[
            {
                "type": "movie",
                "id": "latest_movies",
                "name": "Yggflix - Films Récents"
            },
            {
                "type": "movie",
                "id": "recently_added_movies",
                "name": "YGGtorrent - Films Récemment Ajoutés"
            },
            {
                "type": "series",
                "id": "latest_tv_shows",
                "name": "Yggflix - Séries Récentes"
            },
            {
                "type": "series",
                "id": "recently_added_tv_shows",
                "name": "YGGtorrent - Séries Récemment Ajoutées"
            }
        ],
        behaviorHints={
            "configurable": True,
            "configurationRequired": True
        },
        config=[
            {
                "key": "api_key",
                "title": "API Key",
                "type": "text",
                "required": True
            }
        ]
    )

@router.get("/{config}/manifest.json")
async def get_manifest(config: str, apikey_dao: APIKeyDAO = Depends()):
    config = parse_config(config)
    api_key = config.get("apiKey")
    if api_key:
        await check_api_key(api_key, apikey_dao)
    else:
        logger.warning("Manifest: API key not found in config.")
        raise HTTPException(status_code=401, detail="API key not found in config.")

    utopeer_ctg = config.get("utopeerCtg", True)
    yggtorrent_ctg = config.get("yggtorrentCtg", True)

    catalogs = []

    if utopeer_ctg:
        catalogs.extend([
            {
                "type": "movie",
                "id": "latest_movies",
                "name": "u2p - Utopeer"
            },
            {
                "type": "series",
                "id": "latest_tv_shows",
                "name": "u2p - Utopeer"
            }
        ])

    if yggtorrent_ctg:
        catalogs.extend([
            {
                "type": "movie",
                "id": "recently_added_movies",
                "name": "YGGtorrent - Récemment Ajoutés"
            },
            {
                "type": "series",
                "id": "recently_added_tv_shows",
                "name": "YGGtorrent - Récemment Ajoutées"
            }
        ])

    logger.info("Serving manifest.json")
    return ManifestResponse(
        id="community.limedrive.streamfusion",
        logo="https://i.ibb.co/ZRZcVBLd/SF-modern.png",
        version=str(get_version()),
        resources=[
            'catalog',
            {
                'name': 'stream', 
                'types': ['movie', 'series'], 
                'idPrefixes': ['tt']
            }
        ],
        types=["movie", "series"],
        name="StreamFusion" + (" (dev)" if settings.develop else ""),
        description="StreamFusion enhances Stremio by integrating torrent indexers and debrid services,"
         " providing access to a vast array of cached torrent sources. This plugin seamlessly bridges"
         " Stremio with popular indexers and debrid platforms, offering users an expanded content"
         " library and a smooth streaming experience.",
        catalogs=catalogs,
    )
