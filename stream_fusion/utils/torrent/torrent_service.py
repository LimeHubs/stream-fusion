import asyncio
import hashlib
import os
import time
import urllib.parse
from typing import List
import pathlib

import bencodepy
import requests
from RTN import parse
from RTN.models import ParsedData

from stream_fusion.services.postgresql.dao.torrentitem_dao import TorrentItemDAO
from stream_fusion.services.postgresql.dao.torrentgroup_dao import TorrentGroupDAO
from stream_fusion.utils.jackett.jackett_result import JackettResult
from stream_fusion.utils.zilean.zilean_result import ZileanResult
from stream_fusion.utils.generationfree.generationfree_result import GenerationFreeResult
from stream_fusion.utils.c411.c411_result import C411Result
from stream_fusion.utils.torr9.torr9_result import Torr9Result
from stream_fusion.utils.lacale.lacale_result import LaCaleResult
from stream_fusion.utils.abn.abn_result import AbnResult
from stream_fusion.utils.g3mini.g3mini_result import G3MiniResult
from stream_fusion.utils.theoldschool.theoldschool_result import TheOldSchoolResult
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.utils.general import get_info_hash_from_magnet
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings

# UNIT3D-based indexers whose download_link may contain an api_token to strip before caching
_UNIT3D_CREDENTIAL_INDEXERS = frozenset({
    "GenerationFree - API",
    "G3MINI - API",
    "TheOldSchool - API",
    "ABN - API",
})


class TorrentService:
    TORRENT_CACHE_DIR = pathlib.Path("/var/cache/torrents")

    def __init__(self, config, torrent_dao: TorrentItemDAO, group_dao: TorrentGroupDAO = None):
        self.config = config
        self.torrent_dao = torrent_dao
        self.group_dao = group_dao
        self.logger = logger
        self.__session = requests.Session()
        self.TORRENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def __generate_unique_id(raw_title: str, indexer: str = "cached") -> str:
        unique_string = f"{raw_title}_{indexer}"
        full_hash = hashlib.sha256(unique_string.encode()).hexdigest()
        return full_hash[:16]

    async def get_cached_torrent(self, raw_title: str, indexer: str) -> TorrentItem | None:
        unique_id = self.__generate_unique_id(raw_title, indexer)
        try:
            cached_item = await self.torrent_dao.get_torrent_item_by_id(unique_id)
            if cached_item:
                return cached_item.to_torrent_item()
            return None
        except Exception as e:
            self.logger.error(f"Error getting cached torrent: {e}")
            return None

    @staticmethod
    def _strip_private_credentials(item: TorrentItem) -> TorrentItem:
        """Remove credential-bearing URLs from private-indexer items before caching.

        After stripping, credentials are reconstructed at serve time inside
        TorrentItem.to_debrid_stream_query() using the current user/server config.
        This prevents passkeys and API keys from leaking into shared Redis/PostgreSQL.
        """
        if item.indexer in _UNIT3D_CREDENTIAL_INDEXERS and item.torrent_download:
            # Strip api_token / apikey query params from the download URL
            try:
                parsed = urllib.parse.urlparse(item.torrent_download)
                params = dict(urllib.parse.parse_qsl(parsed.query))
                params.pop("api_token", None)
                params.pop("apikey", None)
                params.pop("api_key", None)
                clean_query = urllib.parse.urlencode(params)
                item.torrent_download = parsed._replace(query=clean_query).geturl()
            except Exception as e:
                logger.warning(f"TorrentService: Failed to strip credentials from download URL: {e}")
        return item

    async def cache_torrent(self, torrent_item: TorrentItem, id: str = None):
        unique_id = self.__generate_unique_id(torrent_item.raw_title, torrent_item.indexer)

        # G3Mini, GenerationFree, TheOldSchool recherchent uniquement par TMDB/IMDB ID
        # (pas de fallback texte). Leurs résultats doivent toujours avoir un tmdb_id.
        # Si tmdb_id est None pour ces indexeurs, c'est une anomalie — on ne cache pas.
        # C411, Torr9, LaCale peuvent avoir tmdb_id=None (fallback texte ou merge) :
        # leur tmdb_id sera assigné rétroactivement après filtrage par titre.
        if torrent_item.indexer in [
            "GenerationFree - API",
            "G3MINI - API",
            "TheOldSchool - API",
        ] and not torrent_item.tmdb_id:
            self.logger.trace(
                f"TorrentService: Skipping {torrent_item.indexer} torrent without tmdb_id: {torrent_item.raw_title}"
            )
            return

        newly_created = False
        try:
            existing = await self.torrent_dao.get_torrent_item_by_id(unique_id)
            if existing:
                self.logger.trace(f"TorrentService: Torrent already cached, skipping: {unique_id}")
            else:
                await self.torrent_dao.create_torrent_item(torrent_item, unique_id)
                self.logger.trace(f"TorrentService: Created new cached torrent: {unique_id}")
                newly_created = True
        except Exception as e:
            if "duplicate key value violates unique constraint" in str(e):
                self.logger.trace(f"TorrentService: Race condition, torrent already exists: {unique_id}")
            else:
                self.logger.error(f"TorrentService: Error caching torrent {unique_id}: {str(e)}")

        # Auto-group newly inserted items so duplicates are linked incrementally
        if newly_created and self.group_dao is not None:
            await self._auto_group_item(torrent_item, unique_id)

    async def _auto_group_item(self, item: TorrentItem, item_id: str) -> None:
        """Try to assign a newly inserted item to an existing group or create one.

        Two-pass strategy:
        1. Exact info_hash match → guaranteed duplicate, group immediately.
        2. Size ±0.5% + normalized title → high-confidence soft duplicate.

        Any failure is silently swallowed — grouping must never block caching.
        """
        try:
            from stream_fusion.utils.filter.title_matching import get_normalizer

            # ── pass 1: info_hash siblings ─────────────────────────────────────
            if item.info_hash:
                siblings = await self.torrent_dao.get_torrent_items_by_info_hash(item.info_hash)
                siblings = [s for s in (siblings or []) if s.id != item_id]

                if siblings:
                    # Find if any sibling already has a group
                    grouped = next((s for s in siblings if s.group_id is not None), None)
                    if grouped:
                        group_id = grouped.group_id
                        await self.group_dao.assign_item_to_group(group_id, item_id)
                    else:
                        # Create a new group for all of them including the new item
                        all_ids = [item_id] + [s.id for s in siblings]
                        canonical_title = item.raw_title
                        tmdb_id = item.tmdb_id or next((s.tmdb_id for s in siblings if s.tmdb_id), None)
                        group = await self.group_dao.create_group(
                            canonical_info_hash=item.info_hash.lower(),
                            canonical_title=canonical_title,
                            tmdb_id=tmdb_id,
                        )
                        group_id = group.id
                        await self.group_dao.assign_items_to_group(group_id, all_ids)

                    await self.group_dao.propagate_tmdb_within_group(group_id)
                    await self.group_dao.session.commit()
                    item.group_id = group_id  # propagate to in-memory object (fixes merge_items + Redis serialisation)
                    self.logger.trace(
                        f"TorrentService: Auto-grouped {item_id} by info_hash → group {group_id}"
                    )
                    return  # Done — no need for title+size pass

            # ── pass 2: size ±0.5% + normalized title ─────────────────────────
            normalizer = get_normalizer()

            # Defensively cast size to int — some indexers return it as a string
            try:
                safe_size = int(item.size) if item.size else 0
            except (ValueError, TypeError):
                safe_size = 0

            if normalizer is None or not safe_size:
                return

            try:
                clean = normalizer.extract_clean_title(item.raw_title)
                norm = normalizer.normalize(clean)
            except Exception:
                norm = item.raw_title.lower()

            candidates = await self.torrent_dao.find_similar_by_size(
                size=safe_size, max_diff=524288, exclude_id=item_id, limit=50
            )

            for candidate in candidates:
                try:
                    cand_clean = normalizer.extract_clean_title(candidate.raw_title)
                    cand_norm = normalizer.normalize(cand_clean)
                except Exception:
                    cand_norm = candidate.raw_title.lower()

                if cand_norm != norm:
                    continue

                # Match found — join or create group
                if candidate.group_id is not None:
                    group_id = candidate.group_id
                    await self.group_dao.assign_item_to_group(group_id, item_id)
                else:
                    canonical_hash = item.info_hash or candidate.info_hash
                    tmdb_id = item.tmdb_id or candidate.tmdb_id
                    group = await self.group_dao.create_group(
                        canonical_info_hash=canonical_hash.lower() if canonical_hash else None,
                        canonical_title=norm,
                        tmdb_id=tmdb_id,
                    )
                    group_id = group.id
                    await self.group_dao.assign_items_to_group(group_id, [item_id, candidate.id])

                await self.group_dao.propagate_tmdb_within_group(group_id)
                await self.group_dao.session.commit()
                item.group_id = group_id  # propagate to in-memory object (fixes merge_items + Redis serialisation)
                self.logger.trace(
                    f"TorrentService: Auto-grouped {item_id} by title+size → group {group_id}"
                )
                return  # Grouped with the first matching candidate

        except Exception as e:
            self.logger.warning(f"TorrentService: _auto_group_item({item_id}) failed: {e}")

    async def _update_cached_item(self, cached_item: TorrentItem, new_item: TorrentItem):
        try:
            unique_id = self.__generate_unique_id(cached_item.raw_title, cached_item.indexer)
            needs_update = False

            if new_item.tmdb_id and not cached_item.tmdb_id:
                cached_item.tmdb_id = new_item.tmdb_id
                needs_update = True
                self.logger.debug(f"Updated tmdb_id for {unique_id}: {new_item.tmdb_id}")

            if new_item.torrent_file_path and not cached_item.torrent_file_path:
                cached_item.torrent_file_path = new_item.torrent_file_path
                needs_update = True
                self.logger.debug(f"Updated torrent_file_path for {unique_id}: {new_item.torrent_file_path}")

            if needs_update:
                await self.torrent_dao.update_torrent_item(unique_id, cached_item)
                self.logger.info(f"Cached torrent updated: {unique_id}")
        except Exception as e:
            self.logger.error(f"Error updating cached item: {e}")

    async def convert_and_process(
        self,
        results: List[
            JackettResult
            | ZileanResult
            | TorrentItem
            | GenerationFreeResult
            | C411Result
            | Torr9Result
            | LaCaleResult
            | AbnResult
            | G3MiniResult
            | TheOldSchoolResult
        ],
        skip_utopeer_download: bool = False,
    ):
        torrent_items_result = []

        for result in results:
            # TorrentItem objects (e.g. from UtopeerService) are already converted
            if isinstance(result, TorrentItem):
                torrent_item = result
            else:
                torrent_item = result.convert_to_torrent_item()

            cached_item = await self.get_cached_torrent(torrent_item.raw_title, torrent_item.indexer)
            if cached_item:
                if torrent_item.seeders is not None and torrent_item.seeders >= 0:
                    cached_item.seeders = torrent_item.seeders
                torrent_items_result.append(cached_item)
                continue

            # u2p / Utopeer results carry a pre-built magnet URI — there is no
            # torrent file to download; cache directly and skip all HTTP processing.
            if torrent_item.indexer == "u2p - Cache" or skip_utopeer_download:
                await self.cache_torrent(torrent_item)
                torrent_items_result.append(torrent_item)
                continue

            if torrent_item.link and torrent_item.link.startswith("magnet:"):
                processed_torrent_item = self.__process_magnet(torrent_item)
            else:
                processed_torrent_item = await asyncio.to_thread(self.__process_web_url, torrent_item)

            self._strip_private_credentials(processed_torrent_item)
            await self.cache_torrent(processed_torrent_item)
            torrent_items_result.append(processed_torrent_item)

        # Refresh group_id for all items in one batch query so that the very
        # first item in a newly-created group (which had group_id=None when it
        # was processed) gets its group_id before merge_items runs.
        if self.group_dao is not None and torrent_items_result:
            try:
                id_map = {
                    self.__generate_unique_id(item.raw_title, item.indexer): item
                    for item in torrent_items_result
                }
                group_id_map = await self.torrent_dao.get_group_ids_by_ids(list(id_map.keys()))
                for uid, item in id_map.items():
                    db_group_id = group_id_map.get(uid)
                    if db_group_id is not None:
                        item.group_id = db_group_id
            except Exception as e:
                self.logger.warning(f"TorrentService: group_id refresh failed: {e}")

        return torrent_items_result

    def __process_web_url(self, result: TorrentItem):
        if not result.link:
            self.logger.error(f"Missing link for torrent item: {result.raw_title}")
            return result

        try:
            time.sleep(0.2)
            response = self.__session.get(
                result.link,
                allow_redirects=False,
                timeout=40,
            )
        except requests.exceptions.ReadTimeout:
            self.logger.error(f"Timeout while processing URL: {result.link}")
            return result
        except requests.exceptions.RequestException:
            self.logger.error(f"Error while processing URL: {result.link}")
            return result

        if response.status_code == 200:
            return self.__process_torrent(result, response.content)
        elif response.status_code == 302:
            result.magnet = response.headers.get("Location")
            return self.__process_magnet(result)
        else:
            self.logger.error(f"Error code {response.status_code} while processing URL: {result.link}")

        return result

    def __process_torrent(self, result: TorrentItem, torrent_file):
        try:
            metadata = bencodepy.decode(torrent_file)
        except Exception as e:
            try:
                from bencodepy import Decoder
                decoder = Decoder(encoding="latin-1")
                metadata = decoder.decode(torrent_file)
            except Exception as inner_e:
                logger.error(f"Failed to decode torrent file: {str(e)} then {str(inner_e)}")
                result.torrent_download = result.link
                result.trackers = []
                result.info_hash = ""
                result.magnet = ""
                return result

        result.torrent_download = result.link

        try:
            filename = f"{result.info_hash if result.info_hash else hashlib.sha256(torrent_file).hexdigest()}.torrent"
            filepath = self.TORRENT_CACHE_DIR / filename
            with open(filepath, "wb") as f:
                f.write(torrent_file)
            result.torrent_file_path = str(filepath)
            self.logger.debug(f"Saved .torrent to disk: {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving .torrent to disk: {e}")
            result.torrent_file_path = None

        try:
            result.trackers = self.__get_trackers_from_torrent(metadata)
            result.info_hash = self.__convert_torrent_to_hash(metadata["info"])
            result.magnet = self.__build_magnet(
                result.info_hash,
                metadata["info"]["name"],
                result.trackers,
            )
        except Exception as e:
            logger.error(f"Error processing torrent metadata: {str(e)}")
            result.trackers = []
            result.info_hash = ""
            result.magnet = ""

        if "files" not in metadata["info"]:
            result.file_index = 1
            return result

        result.files = metadata["info"]["files"]

        if result.type == "series":
            if not result.parsed_data:
                result.parsed_data = parse(result.raw_title)

            if result.parsed_data and isinstance(result.parsed_data, ParsedData):
                file_details = self.__find_single_episode_file(
                    result.files,
                    result.parsed_data.seasons,
                    result.parsed_data.episodes,
                )
            else:
                file_details = None
                self.logger.warning(f"No valid parsed_data for series torrent: {result.raw_title}")

            if file_details is not None:
                self.logger.debug("File details")
                self.logger.debug(file_details)
                result.file_index = file_details["file_index"]
                result.file_name = file_details["title"]
                result.size = file_details["size"]

            result.full_index = self.__find_full_index(result.files)

        if result.type == "movie":
            result.file_index = self.__find_movie_file(result.files)

        return result

    def __process_magnet(self, result: TorrentItem):
        if result.magnet is None:
            result.magnet = result.link

        if result.info_hash is None and result.magnet:
            result.info_hash = get_info_hash_from_magnet(result.magnet)

        result.trackers = self.__get_trackers_from_magnet(result.magnet) if result.magnet else []

        return result

    def __convert_torrent_to_hash(self, torrent_contents):
        hashcontents = bencodepy.encode(torrent_contents)
        hex_hash = hashlib.sha1(hashcontents).hexdigest()
        return hex_hash.lower()

    def __build_magnet(self, hash_value, display_name, trackers):
        magnet_base = "magnet:?xt=urn:btih:"
        magnet = f"{magnet_base}{hash_value}&dn={display_name}"

        if trackers:
            magnet = f"{magnet}&tr={'&tr='.join(trackers)}"

        return magnet

    def __get_trackers_from_torrent(self, torrent_metadata):
        announce = torrent_metadata["announce"] if "announce" in torrent_metadata else []
        announce_list = torrent_metadata["announce-list"] if "announce-list" in torrent_metadata else []

        trackers = set()

        if isinstance(announce, str):
            trackers.add(announce)
        elif isinstance(announce, list):
            for tracker in announce:
                trackers.add(tracker)

        for announce_list_item in announce_list:
            if isinstance(announce_list_item, list):
                for tracker in announce_list_item:
                    trackers.add(tracker)
            elif isinstance(announce_list_item, str):
                trackers.add(announce_list_item)

        return list(trackers)

    def __get_trackers_from_magnet(self, magnet: str):
        url_parts = urllib.parse.urlparse(magnet)
        query_parts = urllib.parse.parse_qs(url_parts.query)

        trackers = []
        if "tr" in query_parts:
            trackers = query_parts["tr"]

        return trackers

    def __find_single_episode_file(self, file_structure, season, episode):
        if len(season) == 0 or len(episode) == 0:
            return None

        episode_files = []

        for files in file_structure:
            for file in files["path"]:
                parsed_file = parse(file)

                if season[0] in parsed_file.seasons and episode[0] in parsed_file.episodes:
                    episode_files.append({
                        "file_index": None,
                        "title": file,
                        "size": files["length"],
                    })

        if episode_files:
            return max(episode_files, key=lambda file: file["size"])

        return None

    def __find_full_index(self, file_structure):
        self.logger.debug("Starting to build full index of video files")

        video_formats = {
            ".mkv", ".mp4", ".avi", ".mov", ".flv", ".wmv", ".webm", ".mpg", ".mpeg",
            ".m4v", ".3gp", ".3g2", ".ogv", ".ogg", ".drc", ".gif", ".gifv", ".mng",
            ".qt", ".yuv", ".rm", ".rmvb", ".asf", ".amv", ".m4p", ".m2v", ".svi",
            ".mxf", ".roq", ".nsv", ".f4v", ".f4p", ".f4a", ".f4b",
        }

        full_index = []
        file_index = 1

        for file_entry in file_structure:
            file_path = file_entry.get("path", [])
            if isinstance(file_path, list):
                file_name = file_path[-1] if file_path else ""
            else:
                file_name = file_path

            _, file_extension = os.path.splitext(file_name.lower())

            if file_extension in video_formats:
                parsed_file = parse(file_name)
                if len(parsed_file.seasons) == 0 or len(parsed_file.episodes) == 0:
                    self.logger.debug(f"Skipping file without season or episode parsed: {file_name}")
                    file_index += 1
                    continue

                full_index.append({
                    "file_index": file_index,
                    "file_name": file_name,
                    "full_path": os.path.join(*file_path) if isinstance(file_path, list) else file_path,
                    "size": file_entry.get("length", 0),
                    "seasons": parsed_file.seasons,
                    "episodes": parsed_file.episodes,
                })
                self.logger.trace(f"Added file to index: {file_name}")

            file_index += 1

        self.logger.debug(f"Full index built with {len(full_index)} video files")
        return full_index

    def __find_movie_file(self, file_structure):
        max_size = 0
        max_file_index = 1
        current_file_index = 1

        for files in file_structure:
            if files["length"] > max_size:
                max_file_index = current_file_index
                max_size = files["length"]
            current_file_index += 1

        return max_file_index