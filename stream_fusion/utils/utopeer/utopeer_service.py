"""
u2p / Utopeer search service — WebSocket / Nostr NIP-50 multi-relay implementation.

Replaces the old Torznab HTTP client with direct WebSocket queries to Nostr relays
that expose YGG torrent metadata via NIP-35 (kind 2003).

Search strategy:
  ┌─ MOVIES ──────────────────────────────────────────────────────────────────┐
  │ Phase 1 — strict category (#l = film cats),  ≤6 title variants, limit 100 │
  │ Phase 2 — broad category  (#l = pcat:2145),  same variants,     limit 100 │
  │ Phase 3 — no category,  most distinctive single variant,        limit 100 │
  └───────────────────────────────────────────────────────────────────────────┘

  ┌─ SERIES ──────────────────────────────────────────────────────────────────┐
  │ Queries use TITLE ONLY (no SxxExx appended) with limit=200.               │
  │ Results are filtered locally by _filter_series_by_episode().              │
  │                                                                           │
  │ Phase 1 — strict category,  ≤6 title variants, limit 200                 │
  │ Phase 2 — broad category,   same variants,      limit 200                 │
  │ Phase 3 — no category,  most distinctive variant, limit 200               │
  │                                                                           │
  │ _filter_series_by_episode() priority buckets (from best to fallback):     │
  │   1. Exact episode match  (S{sn}E{en})                                    │
  │   2. Season pack          (S{sn}, no episode code)                        │
  │   3. Complete/intégrale   (INTEGRALE, COMPLET, COMPLETE…)                 │
  │   4. Unclassified         (RTN couldn't parse + no regex markers)         │
  └───────────────────────────────────────────────────────────────────────────┘

Title variant generation:
  For each TMDB title (all language alternates):
    1. Full cleaned  ("Star Wars Andor")
    2. Subtitle only ("Andor")           — extracted after ':' / ' - '
    3. No article    ("Star Wars Andor")
    4. Prefix        ("Star Wars")
    5. Short form    (first 3 words, for titles ≥ 5 words)
  All variants across all TMDB titles merged + deduplicated, capped at 6.

Filter behaviour:
  _parse_and_filter enriches the filter set with subtitle parts from every
  TMDB title, so "Andor.S01E05.1080p" passes when the only title is
  "Star Wars : Andor" (French).
"""

from __future__ import annotations

import asyncio
import re
import time
import unicodedata
from typing import List, Union

from RTN import parse

from stream_fusion.logging_config import logger
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series
from stream_fusion.utils.nostr.nostr_client import fetch_events
from stream_fusion.utils.nostr.nostr_result import NostrNip35Result
from stream_fusion.utils.torrent.torrent_item import TorrentItem

# ── NIP-35 category labels ────────────────────────────────────────────────────
_MOVIE_CATS  = ["u2p.cat:2183", "u2p.cat:2178", "u2p.cat:2181"]   # Film, Anim film, Docu
_SERIES_CATS = ["u2p.cat:2184", "u2p.cat:2179", "u2p.cat:2182"]   # Série, Anim série, TV
_ALL_VIDEO   = ["u2p.pcat:2145"]                                   # parent Film/Video

# ── Dotted-acronym detection: S.H.I.E.L.D.  N.C.I.S.  C.S.I. ───────────────
# Matches letter(dot-letter){2+} with optional trailing dot — minimum 3 letters.
_ACRONYM_RE = re.compile(r'\b[A-Za-z](?:\.[A-Za-z]){2,}\.?')

# ── Thresholds ────────────────────────────────────────────────────────────────
_MIN_RESULTS_MOVIE  = 5
_MIN_RESULTS_SERIES = 3

# ── Query limits ──────────────────────────────────────────────────────────────
# Nostr relays cap responses at 100 events regardless of the requested limit.
# Broad coverage comes from running many variants×relays in parallel:
#   6 variants × 3-4 relays = 18-24 concurrent queries × 100 = ~1 800 raw events
#   → deduplicated by event ID before local filtering.
_LIMIT_MOVIE  = 100
_LIMIT_SERIES = 100

# ── Variant caps ─────────────────────────────────────────────────────────────
_MAX_VARIANTS = 8     # 8 variants × 3-4 relays = 24-32 parallel WS queries/phase

# ── Series episode / pack detection ──────────────────────────────────────────
_SERIES_COMPLETE_RE = re.compile(
    r"\b(INTEGRALE|INTEGRAL|INTÉGRALE|COMPLET|COMPLETE|SAISON\s+COMPL[EÈ]TE|PACK)\b",
    re.IGNORECASE,
)
_ANY_EPISODE_MARKER_RE = re.compile(
    r"\bS\d{1,2}E\d{1,2}\b|\b\d{1,2}x\d{1,2}\b",
    re.IGNORECASE,
)

# ── In-memory relay health cache (shared across all service instances) ────────
_relay_healthy: list[str] | None = None
_relay_checked_at: float = 0.0


class UtopeerService:

    def __init__(self, config: dict):
        self.config = config

    # ── Public API ────────────────────────────────────────────────────────────

    async def search(self, media: Union[Movie, Series]) -> List[TorrentItem]:
        if isinstance(media, Movie):
            return await self._search_movie(media)
        elif isinstance(media, Series):
            return await self._search_series(media)
        raise TypeError("Only Movie and Series types are supported.")

    # ── Health check ──────────────────────────────────────────────────────────

    async def _get_healthy_relays(self) -> list[str]:
        """Return list of reachable relay URLs (in-memory cache, TTL from settings)."""
        from stream_fusion.settings import settings

        global _relay_healthy, _relay_checked_at
        ttl = settings.utopeer_relay_cache_ttl
        if _relay_healthy is not None and (time.monotonic() - _relay_checked_at) < ttl:
            return _relay_healthy

        relay_urls = settings.utopeer_relay_urls
        checks = await asyncio.gather(
            *[_ping_relay(url, settings.utopeer_ping_timeout) for url in relay_urls],
            return_exceptions=True,
        )
        healthy = [url for url, ok in zip(relay_urls, checks) if ok is True]
        _relay_healthy = healthy or relay_urls   # fallback: try all if none respond
        _relay_checked_at = time.monotonic()
        logger.debug(f"[utopeer] healthy relays: {_relay_healthy}")
        return _relay_healthy

    # ── Movie search ──────────────────────────────────────────────────────────

    async def _search_movie(self, media: Movie) -> List[TorrentItem]:
        titles = _unique_titles(getattr(media, "titles", []) or [])
        if not titles:
            return []

        year = getattr(media, "year", None)

        # Build query variants from ALL TMDB titles (primary + alt languages)
        variants: list[str] = []
        seen_v: set[str] = set()

        def add_v(v: str) -> None:
            v = re.sub(r"\s+", " ", v).strip()
            if v and v.lower() not in seen_v:
                seen_v.add(v.lower())
                variants.append(v)

        # Year-qualified form first (most specific) — from primary cleaned title
        if year:
            add_v(f"{_clean_title(titles[0])} {year}")

        for t in titles:
            for v in _make_title_variants(t):
                add_v(v)

        variants = variants[:_MAX_VARIANTS]

        logger.debug(
            f"[utopeer] movie '{titles[0]}' — {len(variants)} variants: {variants}"
        )

        relays = await self._get_healthy_relays()

        # Phase 1 — strict category
        events = await self._parallel_query(
            relays, variants, {"#l": _MOVIE_CATS}, _LIMIT_MOVIE
        )
        items = self._parse_and_filter(events, titles, "movie")
        if len(items) >= _MIN_RESULTS_MOVIE:
            return self._sort_and_cap(items)

        # Phase 2 — broad category
        events2 = await self._parallel_query(
            relays, variants, {"#l": _ALL_VIDEO}, _LIMIT_MOVIE
        )
        items = self._parse_and_filter(events + events2, titles, "movie")
        if len(items) >= _MIN_RESULTS_MOVIE:
            return self._sort_and_cap(items)

        # Phase 3 — no category, most distinctive single variant
        phase3_v = _most_distinctive_variant(variants)
        events3 = await self._parallel_query(relays, [phase3_v], {}, _LIMIT_MOVIE)
        return self._sort_and_cap(
            self._parse_and_filter(events + events2 + events3, titles, "movie")
        )

    # ── Series search ─────────────────────────────────────────────────────────

    async def _search_series(self, media: Series) -> List[TorrentItem]:
        titles = _unique_titles(getattr(media, "titles", []) or [])
        if not titles:
            return []

        season_num  = media.get_season_number()
        episode_num = media.get_episode_number()

        # Build TITLE-ONLY variants — no SxxExx appended.
        # We search broad and filter locally to maximise recall.
        variants: list[str] = []
        seen_v: set[str] = set()

        def add_v(v: str) -> None:
            v = re.sub(r"\s+", " ", v).strip()
            if v and v.lower() not in seen_v:
                seen_v.add(v.lower())
                variants.append(v)

        for t in titles:
            for v in _make_title_variants(t):
                add_v(v)

        variants = variants[:_MAX_VARIANTS]

        logger.debug(
            f"[utopeer] series '{titles[0]}' S{season_num:02d}E{episode_num:02d} "
            f"— {len(variants)} variants: {variants}"
        )

        relays = await self._get_healthy_relays()

        # Phase 1 — strict category, higher limit (200) for local filtering
        events = await self._parallel_query(
            relays, variants, {"#l": _SERIES_CATS}, _LIMIT_SERIES
        )
        items = self._parse_and_filter(events, titles, "series")
        filtered = _filter_series_by_episode(items, season_num, episode_num)
        if len(filtered) >= _MIN_RESULTS_SERIES:
            return self._sort_and_cap(filtered, limit=100)

        # Phase 2 — broad category
        events2 = await self._parallel_query(
            relays, variants, {"#l": _ALL_VIDEO}, _LIMIT_SERIES
        )
        items = self._parse_and_filter(events + events2, titles, "series")
        filtered = _filter_series_by_episode(items, season_num, episode_num)
        if len(filtered) >= _MIN_RESULTS_SERIES:
            return self._sort_and_cap(filtered, limit=100)

        # Phase 3 — no category, most distinctive variant
        phase3_v = _most_distinctive_variant(variants)
        events3 = await self._parallel_query(relays, [phase3_v], {}, _LIMIT_SERIES)
        items = self._parse_and_filter(events + events2 + events3, titles, "series")
        filtered = _filter_series_by_episode(items, season_num, episode_num)

        result = self._sort_and_cap(filtered, limit=100)
        logger.debug(
            f"[utopeer] {len(result)} final results for "
            f"'{titles[0]}' S{season_num:02d}E{episode_num:02d}"
        )
        return result

    # ── Relay query helpers ───────────────────────────────────────────────────

    async def _parallel_query(
        self,
        relays: list[str],
        title_variants: list[str],
        extra_filter: dict,
        limit: int = _LIMIT_MOVIE,
    ) -> list[dict]:
        """Fire all (relay × variant) queries in parallel, return merged deduped events."""
        from stream_fusion.settings import settings

        tasks = [
            _query_relay(
                relay,
                {"kinds": [2003], "search": variant, "limit": limit, **extra_filter},
                settings.utopeer_relay_timeout,
            )
            for relay in relays
            for variant in title_variants
        ]
        pages = await asyncio.gather(*tasks, return_exceptions=True)

        seen_ids: set[str] = set()
        merged: list[dict] = []
        for page in pages:
            if isinstance(page, Exception):
                continue
            for event in page:
                eid = event.get("id")
                if eid and eid not in seen_ids:
                    seen_ids.add(eid)
                    merged.append(event)
        return merged

    # ── Parsing + title pre-filter ────────────────────────────────────────────

    def _parse_and_filter(
        self,
        raw_events: list[dict],
        titles: list[str],
        media_type: str | None,
    ) -> list[TorrentItem]:
        """Convert NIP-35 events to TorrentItems and apply title pre-filter.

        The filter set is enriched beyond the raw TMDB titles:
          - Cleaned (punctuation-free) form of every TMDB title
          - Subtitle part (after ':' / ' - ') of every TMDB title
          - No-leading-article variant of every cleaned title

        This lets a torrent named "Andor.S01E05.1080p" pass when the only
        TMDB title is "Star Wars : Andor" (French).
        """
        filter_set: list[str] = []
        seen_f: set[str] = set()

        def add_f(raw: str) -> None:
            norm = _normalize_for_filter(raw)
            if norm and norm not in seen_f:
                seen_f.add(norm)
                filter_set.append(norm)

        for t in titles:
            cleaned = _clean_title(t)
            add_f(cleaned)
            add_f(_strip_leading_article(cleaned))

            # Original title normalized: "Marvel's Agents of S.H.I.E.L.D."
            # → "marvels agents of s h i e l d"  (dots become spaces)
            # Needed because _clean_title keeps acronym dots, but normalize_for_filter
            # converts them to spaces — and the torrent name uses dots as separators too.
            add_f(t)

            # Standalone collapsed-acronym tokens ("SHIELD", "NCIS", "CSI").
            # Critical for cross-language matching: a French TMDB title like
            # "Marvel : Les Agents du S.H.I.E.L.D." produces a filter string
            # "marvel les agents du s h i e l d" that does NOT contain "of",
            # so English torrent names "marvels agents of s h i e l d s04e01"
            # would be rejected.  Adding "shield" as a standalone filter token
            # ensures the substring check hits regardless of surrounding words.
            for acr in _ACRONYM_RE.findall(cleaned):
                collapsed = acr.replace(".", "")
                if len(collapsed) >= 3:
                    add_f(collapsed)    # "SHIELD" → normalized "shield"

            _, subtitle = _extract_colon_parts(t)
            if subtitle:
                sub_clean = _clean_title(subtitle)
                add_f(sub_clean)
                add_f(_strip_leading_article(sub_clean))
                add_f(subtitle)   # original subtitle normalized (same reasoning)

        seen_hashes: set[str] = set()
        items: list[TorrentItem] = []

        for event in raw_events:
            result = NostrNip35Result.from_event(event)
            if result is None:
                continue
            if result.info_hash in seen_hashes:
                continue
            seen_hashes.add(result.info_hash)

            norm_name = _normalize_for_filter(result.raw_title)
            if not any(f in norm_name for f in filter_set):
                continue

            items.append(result.convert_to_torrent_item(media_type))

        return items

    # ── Common post-processing ────────────────────────────────────────────────

    def _sort_and_cap(
        self,
        items: list[TorrentItem],
        limit: int = 100,
    ) -> list[TorrentItem]:
        """Remove negative-seeder junk, sort by seeders desc, cap at limit."""
        items = [i for i in items if i.seeders >= 0]
        return sorted(items, key=lambda i: i.seeders, reverse=True)[:limit]


# ── Module-level helpers ──────────────────────────────────────────────────────

async def _ping_relay(url: str, timeout: int = 2) -> bool:
    """Return True if the relay accepts a WebSocket connection within timeout."""
    try:
        import websockets
        async with websockets.connect(url, open_timeout=timeout, close_timeout=1):
            return True
    except Exception:
        return False


async def _query_relay(relay_url: str, filter_dict: dict, timeout: int = 8) -> list[dict]:
    """Collect all events from a single relay query into a list."""
    events: list[dict] = []
    try:
        async for event in fetch_events(relay_url, filter_dict, timeout=timeout):
            events.append(event)
    except Exception as exc:
        logger.warning(f"[utopeer] relay {relay_url} query failed: {exc}")
    return events


def _filter_series_by_episode(
    items: list[TorrentItem],
    season_num: int,
    episode_num: int,
) -> list[TorrentItem]:
    """
    Filter a list of TorrentItems to keep only those relevant to the
    requested season and episode.

    Classification buckets (priority order):

    1. Exact episode match  — S{sn}E{en} confirmed by RTN or regex
    2. Season pack          — correct season, no episode code
                              (includes "S19.COMPLETE", "Saison 19", …)
                              also includes multi-season ranges that cover our season
                              e.g. "S01-S07" when looking for S03
    3. Complete/intégrale   — full-series pack with NO specific season marker
                              (INTEGRALE, COMPLET alone, PACK without season…)
    4. Unclassified         — RTN can't parse AND no regex markers → cautious fallback

    Rejection cases:
    - Season range that explicitly EXCLUDES our season ("S01-S03" when looking for S05)
    - Any season marker for the WRONG season
    - Any SxxExx / NxNN marker for a DIFFERENT episode
    - Complete/intégrale keyword paired with a WRONG season marker (e.g. "S18.COMPLETE")

    Return value: buckets 1+2+3 concatenated, or bucket 4 if 1+2+3 are all empty.
    """
    exact_matches: list[TorrentItem] = []
    season_packs:  list[TorrentItem] = []
    total_packs:   list[TorrentItem] = []
    unclassified:  list[TorrentItem] = []

    for item in items:
        raw = item.raw_title or ""

        # ── Step 1 : Multi-season range detection (highest priority) ─────────
        # "S01-S07", "S1–S7", "Saisons 1 à 7" etc.
        # Must run BEFORE RTN and regex checks because RTN may only see the
        # boundary seasons (S01 and S07) and miss everything in between.
        range_result = _season_range_match(raw, season_num)
        if range_result is True:
            season_packs.append(item)
            continue
        elif range_result is False:
            continue   # range explicitly excludes our season

        # ── Step 2 : RTN parsed data (handles most standard releases) ─────────
        pd = item.parsed_data
        if pd is None:
            try:
                pd = parse(raw)
                item.parsed_data = pd
            except Exception:
                pd = None

        if pd is not None and pd.seasons:
            # Wrong season → reject
            if season_num not in pd.seasons:
                continue
            # Correct season, no episode → season pack
            if not pd.episodes:
                season_packs.append(item)
                continue
            # Correct season + correct episode → exact match
            if episode_num in pd.episodes:
                exact_matches.append(item)
            # else: correct season but wrong episode → reject
            continue

        # ── Step 3 : Pure regex fallback (RTN found no season info) ──────────
        has_complete   = bool(_SERIES_COMPLETE_RE.search(raw))
        has_exact      = _matches_exact_episode_regex(raw, season_num, episode_num)
        has_our_season = _contains_season_pack_regex(raw, season_num)
        has_any_season = _contains_any_season_marker_regex(raw)
        has_any_ep     = bool(_ANY_EPISODE_MARKER_RE.search(raw))

        if has_exact:
            exact_matches.append(item)

        elif has_our_season:
            # Correct season (with or without COMPLETE/PACK keyword) → season pack
            season_packs.append(item)

        elif has_complete:
            if has_any_season:
                # COMPLETE/PACK + a wrong-season marker (e.g. "S18.COMPLETE") → reject
                pass
            else:
                # INTEGRALE / COMPLET with no season qualifier → full-series pack
                total_packs.append(item)

        elif has_any_ep or has_any_season:
            # Has a marker but not ours → reject
            pass

        else:
            unclassified.append(item)

    classified = exact_matches + season_packs + total_packs
    if classified:
        logger.debug(
            f"[utopeer] episode filter: "
            f"{len(exact_matches)} exact + {len(season_packs)} season packs + "
            f"{len(total_packs)} intégrales = {len(classified)} kept "
            f"(from {len(items)} items, S{season_num:02d}E{episode_num:02d})"
        )
        return classified

    logger.debug(
        f"[utopeer] episode filter: nothing classified, "
        f"returning {len(unclassified)} unclassified as fallback"
    )
    return unclassified


def _season_range_match(title: str, season: int) -> bool | None:
    """
    Detect a multi-season range in a torrent title and check if season is included.

    Handled formats:
      S01-S07   S1-S7   S01–S07          (dash / en-dash between Sxx markers)
      Saisons 1 à 7    Saisons 1-7       (French)
      Seasons 1-7                        (English)

    Returns:
      True  — range found AND target season is within [start..end]
      False — range found AND target season is OUTSIDE [start..end]
      None  — no range pattern found (caller should fall through to other checks)
    """
    # "S01-S07", "S1–S7", "S01 - S07", …
    m = re.search(
        r'\bS(\d{1,2})\s*[-–]\s*S(\d{1,2})\b',
        title, re.IGNORECASE,
    )
    if m:
        start, end = int(m.group(1)), int(m.group(2))
        return start <= season <= end

    # "Saisons 1 à 7", "Saisons.1.à.6", "Saisons 1-7", "Seasons.1-5", …
    # Use [\s.]+ / [\s.]* to handle both space-separated and dot-separated titles.
    m = re.search(
        r'\bSAISONS?[\s.]+(\d{1,2})[\s.]*[àa\-–][\s.]*(\d{1,2})\b'
        r'|\bSEASONS?[\s.]+(\d{1,2})[\s.]*[-–][\s.]*(\d{1,2})\b',
        title, re.IGNORECASE,
    )
    if m:
        g = m.groups()
        # First pattern uses groups 1&2, second uses 3&4
        start = int(g[0] if g[0] is not None else g[2])
        end   = int(g[1] if g[1] is not None else g[3])
        return start <= season <= end

    return None


def _matches_exact_episode_regex(title: str, season: int, episode: int) -> bool:
    """True if title contains SxxExx (or NxNN) for exactly this season+episode."""
    patterns = [
        rf"\bS{season:02d}E{episode:02d}\b",
        rf"\bS{season}E{episode}\b",
        rf"\b{season:02d}x{episode:02d}\b",
        rf"\b{season}x{episode}\b",
    ]
    return any(re.search(p, title, re.IGNORECASE) for p in patterns)


def _contains_season_pack_regex(title: str, season: int) -> bool:
    """
    True if title contains a marker for exactly THIS season with no episode code.

    Handles common separators between keyword and number (space, dot, underscore).
    Examples matched for season=19:
      "S19", "S19.COMPLETE", "Saison.19", "SAISON 19", "Saison_19"
    """
    season_marker = re.search(
        rf"\bS{season:02d}\b"
        rf"|\bS{season}\b"
        rf"|\bSAISON[\s._]*{season:02d}\b"
        rf"|\bSAISON[\s._]*{season}\b",
        title, re.IGNORECASE,
    )
    if not season_marker:
        return False
    # Reject if an episode code immediately follows (e.g. "S19E03")
    episode_after = re.search(r"E\d{1,2}\b", title[season_marker.end():], re.IGNORECASE)
    return episode_after is None


def _contains_any_season_marker_regex(title: str) -> bool:
    """True if the title contains ANY season-like marker (Sxx, Saison xx, etc.)."""
    return bool(re.search(
        r"\bS\d{1,2}\b|\bSAISON[\s._]*\d{1,2}\b",
        title, re.IGNORECASE,
    ))


def _extract_colon_parts(title: str) -> tuple[str | None, str | None]:
    """
    Split a 'Franchise : Subtitle' title into (prefix, subtitle).

    Priority: colon split, then spaced-dash split.

    Examples:
      "Star Wars : Andor"                     → ("Star Wars", "Andor")
      "Avatar : La Voie de l'eau"             → ("Avatar", "La Voie de l'eau")
      "Mission : Impossible - Dead Reckoning" → ("Mission : Impossible", "Dead Reckoning")
      "Inception"                             → (None, None)
    """
    m = re.search(r'\s*:\s*(.+)$', title)
    if m:
        subtitle = m.group(1).strip()
        prefix   = title[:m.start()].strip()
        if len(subtitle) >= 3 and len(prefix) >= 2:
            return prefix, subtitle

    m = re.search(r'\s+-\s+(.+)$', title)
    if m:
        subtitle = m.group(1).strip()
        prefix   = title[:m.start()].strip()
        if len(subtitle) >= 3 and len(prefix) >= 2:
            return prefix, subtitle

    return None, None


def _strip_leading_article(title: str) -> str:
    """Remove a leading French/English article from an already-cleaned title."""
    return re.sub(
        r"^(?:les|le|la|the|une|un|des|an\s|a\s|l\s+|d\s+)\s*",
        "", title, flags=re.IGNORECASE,
    ).strip()


def _make_title_variants(title: str) -> list[str]:
    """
    Generate ordered search variants from a single media title.

    Core order (most → least specific):
      1. Full cleaned title          ("Marvels Agents of S.H.I.E.L.D.")
      2. Subtitle after ':' / ' - '  ("Andor", "No Way Home", …)
      3. No leading article          ("Agents du S.H.I.E.L.D.", …)
      4. Prefix before ':' / ' - '   ("Star Wars", "Avatar", …)
      5. Short form (first 3 words)   when ≥ 5 words

    Dotted-acronym supplements (appended after the core variants):
      When the title contains a dotted acronym (S.H.I.E.L.D., N.C.I.S., C.S.I.):
      6. Title with acronym collapsed  ("Marvels Agents of SHIELD")
      7. Collapsed acronym alone       ("SHIELD")       — high-recall fallback
      8. Original title (pre-cleaning) ("Marvel's Agents of S.H.I.E.L.D.")
         Useful for relays that tokenise dots natively.

    These extras push recall for acronym-heavy shows where the relay may tokenise
    "S.H.I.E.L.D." differently depending on its FTS implementation.
    """
    seen: set[str] = set()
    result: list[str] = []

    def add(v: str) -> None:
        v = re.sub(r"\s+", " ", v).strip()
        if v and v.lower() not in seen:
            seen.add(v.lower())
            result.append(v)

    cleaned = _clean_title(title)
    add(cleaned)

    prefix_raw, subtitle_raw = _extract_colon_parts(title)
    subtitle_clean = _clean_title(subtitle_raw) if subtitle_raw else None
    prefix_clean   = _clean_title(prefix_raw)   if prefix_raw   else None

    if subtitle_clean:
        add(subtitle_clean)
        add(_strip_leading_article(subtitle_clean))

    # ── Dotted-acronym supplements — before prefix/article variants ───────────
    # Priority: standalone acronym ("SHIELD") beats the franchise prefix ("Marvel")
    # because it is far more distinctive for relay NIP-50 search.
    # Inserted here so acronym variants land in the first slots even when the
    # title has a colon split (e.g. "Marvel : Les Agents du S.H.I.E.L.D.").
    acronym_hits = _ACRONYM_RE.findall(cleaned)
    if acronym_hits:
        collapsed_title = _ACRONYM_RE.sub(
            lambda m: m.group(0).replace(".", ""), cleaned
        ).strip()
        add(collapsed_title)                        # "Marvels Agents of SHIELD"
        for acr in acronym_hits:
            collapsed = acr.replace(".", "")
            if len(collapsed) >= 3:
                add(collapsed)                      # "SHIELD" — high-recall key term

    add(_strip_leading_article(cleaned))

    if prefix_clean:
        add(prefix_clean)
        add(_strip_leading_article(prefix_clean))

    words = cleaned.split()
    if len(words) >= 5:
        add(" ".join(words[:3]))

    # Original title as final fallback (relay may handle special chars natively)
    if acronym_hits:
        add(title)

    return result


def _most_distinctive_variant(variants: list[str]) -> str:
    """
    Pick the most search-effective single variant for Phase 3 (no category filter).

      1. 2-3 word variants ≥ 8 chars (concise but specific):
           "No Way Home", "Impossible Dead Reckoning", "Andor S01"
      2. Single-word variants ≥ 5 chars:
           "Andor", "Inception"
      3. Last variant as final fallback
    """
    multi = [v for v in variants if 2 <= len(v.split()) <= 3 and len(v) >= 8]
    if multi:
        return min(multi, key=len)
    single = [v for v in variants if len(v.split()) == 1 and len(v) >= 5]
    if single:
        return min(single, key=len)
    return variants[-1] if variants else ""


def _clean_title(title: str) -> str:
    """
    Return a punctuation-free, accent-normalised, search-friendly title.

      - Trailing "(year)" stripped
      - " : " / ": " → space
      - Isolated dashes → space  (but "Spider-Man" preserved)
      - l'/d' elision → "l "/"d "
      - Remaining apostrophes removed  ("Grey's" → "Greys")
      - Unicode accents stripped  (é→e, à→a …)
      - Repeated spaces collapsed
    """
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()
    t = re.sub(r"\s*:\s*", " ", t)
    t = re.sub(r"(?<!\w)-(?!\w)|\s-\s", " ", t)
    t = re.sub(r"\b([ldLD])['\u2019\u2018`]\s*", r"\1 ", t)
    t = re.sub(r"[!?''\u2019\u2018\"´`]", "", t)
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t).strip()


def _normalize_for_filter(text: str) -> str:
    """
    Aggressive normalization for substring title matching.

    Lowercase + strip accents + remove apostrophes (no space) +
    replace remaining non-alphanumeric with space + collapse whitespace.

    "Spider-Man : No Way Home" ↔ "Spider-Man.No.Way.Home.FRENCH…"
    "Grey's Anatomy"           ↔ "Greys.Anatomy…"
    """
    t = unicodedata.normalize("NFD", text.lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = re.sub(r"['\u2019\u2018`]", "", t)
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _unique_titles(titles: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for t in titles:
        t = " ".join(t.split()).strip()
        if t and t.lower() not in seen:
            seen.add(t.lower())
            result.append(t)
    return result
