import aiohttp
from fastapi.exceptions import HTTPException

from stream_fusion.utils.debrid.alldebrid import AllDebrid
from stream_fusion.utils.debrid.realdebrid import RealDebrid
from stream_fusion.utils.debrid.torbox import Torbox
from stream_fusion.utils.debrid.premiumize import Premiumize
from stream_fusion.utils.debrid.debridlink import DebridLink
from stream_fusion.utils.debrid.easydebrid import EasyDebrid
from stream_fusion.utils.debrid.offcloud import Offcloud
from stream_fusion.utils.debrid.pikpak import PikPak
from stream_fusion.utils.debrid.stremthru import StremThru
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings

# (stremthru_store, config_token_key, class, st_extension)
_SERVICE_MAP = {
    "Real-Debrid": ("realdebrid",  "RDToken",        RealDebrid,  "ST:RD"),
    "AllDebrid":   ("alldebrid",   "ADToken",        AllDebrid,   "ST:AD"),
    "TorBox":      ("torbox",      "TBToken",        Torbox,      "ST:TB"),
    "Premiumize":  ("premiumize",  "PMToken",        Premiumize,  "ST:PM"),
    "Debrid-Link": ("debridlink",  "DLToken",        DebridLink,  "ST:DL"),
    "EasyDebrid":  ("easydebrid",  "EDToken",        EasyDebrid,  "ST:ED"),
    "Offcloud":    ("offcloud",    "OCCredentials",  Offcloud,    "ST:OC"),
    "PikPak":      ("pikpak",      "PPCredentials",  PikPak,      "ST:PP"),
}

_SHORT_TO_FULL = {
    "RD": "Real-Debrid",
    "AD": "AllDebrid",
    "TB": "TorBox",
    "PM": "Premiumize",
    "DL": "Debrid-Link",
    "ED": "EasyDebrid",
    "OC": "Offcloud",
    "PP": "PikPak",
}


def _build_service(full_name: str, config: dict, session: aiohttp.ClientSession):
    store_name, token_key, cls, st_extension = _SERVICE_MAP[full_name]
    use_stremthru = config.get("stremthru", False)
    if use_stremthru:
        st = StremThru(config, session)
        st.set_store_credentials(store_name, config.get(token_key, ""))
        st.extension = st_extension
        logger.debug(f"{full_name} (via StremThru): service added to be use")
        return st
    logger.debug(f"{full_name}: service added to be use")
    return cls(config, session)


def get_all_debrid_services(config, session: aiohttp.ClientSession = None):
    services = config["service"]
    if not services:
        logger.error("No service configuration found in the config file.")
        return []

    debrid_services = []
    for service in services:
        if service not in _SERVICE_MAP:
            logger.warning(f"Unknown service: {service}, skipping.")
            continue
        debrid_services.append(_build_service(service, config, session))

    if not debrid_services:
        raise HTTPException(status_code=500, detail="Invalid service configuration.")

    return debrid_services


def get_download_service(config, session: aiohttp.ClientSession = None):
    if not settings.download_service:
        service = config.get("debridDownloader")
        if not service:
            services = config.get("service", [])
            if len(services) == 1:
                service = services[0]
                logger.info(f"Using active service as download service: {service}")
            else:
                logger.error("Multiple services enabled. Please select a download service in the web interface.")
                raise HTTPException(
                    status_code=500,
                    detail="Multiple services enabled. Please select a download service in the web interface.",
                )
    else:
        service = settings.download_service

    if service not in _SERVICE_MAP:
        logger.error(f"Invalid download service: {service}")
        raise HTTPException(
            status_code=500,
            detail=f"Invalid download service: {service}. Please select a valid download service in the web interface.",
        )

    return _build_service(service, config, session)


def get_debrid_service(config, service, session: aiohttp.ClientSession = None):
    if not service:
        service = settings.download_service

    if service == "ST":
        return get_download_service(config, session)

    full_name = _SHORT_TO_FULL.get(service)
    if not full_name:
        logger.error("Invalid service configuration return by stremio in the query.")
        raise HTTPException(status_code=500, detail="Invalid service configuration return by stremio.")

    return _build_service(full_name, config, session)
