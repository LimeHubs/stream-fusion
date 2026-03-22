import asyncio
from typing import List, Optional, Union
import re

import aiohttp
import requests

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.torr9.torr9_api import Torr9API
from stream_fusion.utils.torr9.torr9_result import Torr9Result
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


class Torr9Service:
    def __init__(self, config: dict, session: Optional[aiohttp.ClientSession] = None):
        self.config = config

        if settings.torr9_unique_account and settings.torr9_api_key:
            api_key = settings.torr9_api_key
        else:
            api_key = config.get("torr9ApiKey") or settings.torr9_api_key

        self.api = Torr9API(session=session, api_key=api_key)

    async def search(self, media: Union[Movie, Series]) -> List[Torr9Result]:
        try:
            if isinstance(media, Movie):
                return await self._search_movie(media)
            elif isinstance(media, Series):
                return await self._search_series(media)
            else:
                raise TypeError("Only Movie and Series are supported")
        except Exception as e:
            logger.error(f"Torr9: Search error: {e}")
            return []

    async def _search_movie(self, media: Movie) -> List[Torr9Result]:
        logger.info(f"Torr9: Searching movie: {media.titles[0]}")
        imdb_id = media.id if media.id and media.id.startswith("tt") else None

        if not imdb_id:
            logger.debug(f"Torr9: No IMDB ID available, skipping search for '{media.titles[0]}'")
            return []

        raw = await self.api.search_movie(imdb_id=imdb_id)
        logger.info(f"Torr9: {len(raw)} raw results for movie '{media.titles[0]}'")
        return self._build_results(raw, media)

    async def _search_series(self, media: Series) -> List[Torr9Result]:
        logger.info(f"Torr9: Searching series: {media.titles[0]}")
        raw_id = media.id.split(":")[0] if media.id else None
        imdb_id = raw_id if raw_id and raw_id.startswith("tt") else None

        if not imdb_id:
            logger.debug(f"Torr9: No IMDB ID available, skipping search for '{media.titles[0]}'")
            return []

        season_num = media.get_season_number()
        episode_num = media.get_episode_number()

        raw = await self.api.search_series(
            imdb_id=imdb_id,
            season=season_num,
            episode=episode_num,
        )

        logger.info(
            f"Torr9: {len(raw)} raw results for '{media.titles[0]}' "
            f"(S{season_num:02d}E{episode_num:02d})"
        )

        raw = await asyncio.to_thread(self._filter_series_results_for_torr9_only, raw, media)

        logger.info(
            f"Torr9: {len(raw)} raw results after Torr9 local filtering for '{media.titles[0]}'"
        )

        return self._build_results(raw, media)

    def _build_results(self, raw_results, media) -> List[Torr9Result]:
        results = []
        for item in raw_results:
            try:
                result = Torr9Result().from_api_item(item, media)
                results.append(result)
            except ValueError as e:
                logger.debug(f"Torr9: Skipping item - {e}")
            except Exception as e:
                logger.error(f"Torr9: Unexpected error while building item: {e}")

        logger.info(f"Torr9: Built {len(results)} final Torr9Result objects")
        return results

    def _filter_series_results_for_torr9_only(self, raw_results, media: Series):
        expected_year = self._get_series_first_air_year(media.tmdb_id)
        season_num = media.get_season_number()
        episode_num = media.get_episode_number()

        allowed_years = None
        if expected_year:
            allowed_years = {expected_year - 1, expected_year, expected_year + 1}
        else:
            logger.warning("Torr9: No TMDB year found, year filter disabled")

        exact_episode_items = []
        complete_pack_items = []

        for item in raw_results:
            title = self._extract_title(item)
            if not title:
                exact_episode_items.append(item)
                continue

            found_year = self._extract_year(title)
            exact_match = self._matches_exact_episode(title, season_num, episode_num)
            has_episode_marker = self._contains_any_episode_marker(title)
            is_complete_pack = self._is_complete_pack(title)

            if allowed_years is not None and found_year is not None and found_year not in allowed_years:
                logger.debug(f"Torr9: Reject wrong year: {title}")
                continue

            if is_complete_pack:
                complete_pack_items.append(item)
                continue

            if has_episode_marker and not exact_match:
                logger.debug(
                    f"Torr9: Reject wrong episode for S{season_num:02d}E{episode_num:02d}: {title}"
                )
                continue

            exact_episode_items.append(item)

        combined = exact_episode_items + complete_pack_items

        logger.info(
            f"Torr9: Filtered results for '{media.titles[0]}' "
            f"(exact={len(exact_episode_items)}, packs={len(complete_pack_items)}, total={len(combined)})"
        )

        return combined

    def _extract_title(self, item) -> str:
        if item is None:
            return ""

        if isinstance(item, dict):
            return (
                item.get("raw_title")
                or item.get("title")
                or item.get("name")
                or item.get("torrent_name")
                or item.get("filename")
                or ""
            )

        return (
            getattr(item, "raw_title", None)
            or getattr(item, "title", None)
            or getattr(item, "name", None)
            or getattr(item, "torrent_name", None)
            or getattr(item, "filename", None)
            or ""
        )

    def _extract_year(self, title: str):
        m = re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b", title)
        return int(m.group(1)) if m else None

    def _matches_exact_episode(self, title: str, season: int, episode: int) -> bool:
        patterns = [
            rf"\bS{season:02d}E{episode:02d}\b",
            rf"\bS{season}E{episode}\b",
            rf"\b{season:02d}x{episode:02d}\b",
            rf"\b{season}x{episode}\b",
        ]
        return any(re.search(pattern, title, re.IGNORECASE) for pattern in patterns)

    def _contains_any_episode_marker(self, title: str) -> bool:
        patterns = [
            r"\bS\d{1,2}E\d{1,2}\b",
            r"\b\d{1,2}x\d{1,2}\b",
        ]
        return any(re.search(pattern, title, re.IGNORECASE) for pattern in patterns)

    def _is_complete_pack(self, title: str) -> bool:
        markers = [
            "COMPLETE",
            "INTEGRALE",
            "INTÉGRALE",
            "SEASON",
            "SAISON COMPLETE",
            "SAISON COMPLÈTE",
            "PACK",
        ]
        title_upper = title.upper()
        return any(marker in title_upper for marker in markers)

    def _get_series_first_air_year(self, tmdb_id):
        if not tmdb_id:
            return None

        try:
            url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
            response = requests.get(
                url,
                params={"api_key": settings.tmdb_api_key, "language": "fr-FR"},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            first_air_date = data.get("first_air_date")
            if not first_air_date or len(first_air_date) < 4:
                return None

            return int(first_air_date[:4])

        except Exception as e:
            logger.warning(f"Torr9: Could not fetch TMDB year for series {tmdb_id}: {e}")
            return None