"""
NIP-35 Nostr event → TorrentItem converter.

Parses kind-2003 events from u2p relays and converts them to
TorrentItem objects. Only the info_hash (tag "x") is used — no magnet.
"""

from __future__ import annotations

from RTN import parse

from stream_fusion.utils.torrent.torrent_item import TorrentItem

INDEXER_NAME = "u2p - Cache"

# Maps u2p category labels to TorrentItem.type values.
CAT_TYPE_MAP: dict[str, str | None] = {
    "u2p.cat:2178": "movie",   # Animation film
    "u2p.cat:2179": "series",  # Animation série
    "u2p.cat:2181": None,      # Documentaire
    "u2p.cat:2182": "series",  # Émission TV
    "u2p.cat:2183": "movie",   # Film
    "u2p.cat:2184": "series",  # Série TV
    "u2p.cat:2185": None,      # Spectacle
    "u2p.cat:2186": None,      # Sport
}


class NostrNip35Result:
    """Structured representation of a NIP-35 torrent event."""

    __slots__ = (
        "event_id", "pubkey", "created_at",
        "raw_title", "info_hash", "size_bytes",
        "trackers", "files", "seeders", "torrent_type",
        "ygg_id", "link",
    )

    def __init__(
        self,
        event_id: str,
        pubkey: str,
        created_at: int,
        raw_title: str,
        info_hash: str,
        size_bytes: int,
        trackers: list[str],
        files: list[dict],
        seeders: int,
        torrent_type: str | None,
        ygg_id: str | None,
    ) -> None:
        self.event_id = event_id
        self.pubkey = pubkey
        self.created_at = created_at
        self.raw_title = raw_title
        self.info_hash = info_hash
        self.size_bytes = size_bytes
        self.trackers = trackers
        self.files = files
        self.seeders = seeders
        self.torrent_type = torrent_type
        self.ygg_id = ygg_id
        self.link = f"https://ygg.gratis/#/e/{event_id}" if event_id else ""

    # ── Factory ──────────────────────────────────────────────────────────────

    @classmethod
    def from_event(cls, event: dict) -> "NostrNip35Result | None":
        """Parse a raw NIP-35 event dict. Returns None if info_hash is missing."""
        tags = event.get("tags", [])

        tag_first: dict[str, str] = {}
        l_values: list[str] = []
        trackers: list[str] = []
        file_entries: list[dict] = []
        ygg_id: str | None = None

        for tag in tags:
            if not tag:
                continue
            name = tag[0]

            if name == "l" and len(tag) > 1:
                l_values.append(tag[1])

            elif name == "tracker" and len(tag) > 1:
                trackers.append(tag[1])

            elif name == "file" and len(tag) > 1:
                parts = tag[1].split(";", 1)
                file_entries.append({
                    "name": parts[0],
                    "size": int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None,
                })

            elif name == "i" and len(tag) > 1:
                val = tag[1]
                if val.startswith("ygg:"):
                    ygg_id = val[4:]

            elif name not in tag_first and len(tag) > 1:
                tag_first[name] = tag[1]

        info_hash = tag_first.get("x", "").lower().strip()
        if not info_hash:
            return None

        raw_title = tag_first.get("title", "").strip()
        if not raw_title:
            return None

        size_raw = tag_first.get("size", "0")
        size_bytes = int(size_raw) if size_raw.isdigit() else 0

        seeders = 0
        for lv in l_values:
            if lv.startswith("u2p.seed:"):
                try:
                    seeders = int(lv.split(":", 1)[1])
                except ValueError:
                    pass
                break

        torrent_type: str | None = None
        for lv in l_values:
            if lv in CAT_TYPE_MAP:
                torrent_type = CAT_TYPE_MAP[lv]
                break

        return cls(
            event_id=event.get("id", ""),
            pubkey=event.get("pubkey", ""),
            created_at=event.get("created_at", 0),
            raw_title=raw_title,
            info_hash=info_hash,
            size_bytes=size_bytes,
            trackers=trackers,
            files=file_entries,
            seeders=seeders,
            torrent_type=torrent_type,
            ygg_id=ygg_id,
        )

    # ── Conversion ───────────────────────────────────────────────────────────

    def _build_magnet(self) -> str:
        """
        Build an anonymous magnet URI from the info_hash + display name.
        No tracker is added — the debrid service looks up the hash directly.
        If the event provided trackers (via NIP-35 'tracker' tags) they are
        appended so that direct-torrent clients can still use them.
        """
        import urllib.parse as _up
        magnet = f"magnet:?xt=urn:btih:{self.info_hash}"
        if self.raw_title:
            magnet += f"&dn={_up.quote(self.raw_title, safe='')}"
        for tr in self.trackers:
            magnet += f"&tr={_up.quote(tr, safe=':/@')}"
        return magnet

    def convert_to_torrent_item(self, media_type: str | None = None) -> TorrentItem:
        """Convert to a TorrentItem. media_type overrides the event category if provided."""
        parsed_data = parse(self.raw_title)

        languages: list[str] = []
        if parsed_data and hasattr(parsed_data, "language") and parsed_data.language:
            lang = parsed_data.language
            languages = list(lang) if isinstance(lang, (list, set)) else [lang]
        if not languages:
            languages = ["fr"]

        effective_type = media_type or self.torrent_type

        # Build magnet URI so torrent_service routes through __process_magnet
        # rather than trying to HTTP-fetch the event page URL.
        magnet = self._build_magnet()

        item = TorrentItem(
            raw_title=self.raw_title,
            size=self.size_bytes,
            magnet=magnet,
            info_hash=self.info_hash,
            link=magnet,   # starts with "magnet:" → __process_magnet is called
            seeders=self.seeders,
            languages=languages,
            indexer=INDEXER_NAME,
            privacy="public",
            type=effective_type,
            parsed_data=parsed_data,
        )
        item.trackers = self.trackers
        item.files = self.files if self.files else None
        return item
