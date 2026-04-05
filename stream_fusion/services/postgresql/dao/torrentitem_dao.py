from typing import List, Optional
from fastapi import Depends
from sqlalchemy import select, func, update, exists, literal_column
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone, timedelta

from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.services.postgresql.models.torrentitem_model import TorrentItemModel
from stream_fusion.services.postgresql.models.mismatch_model import TmdbMismatchModel
from stream_fusion.logging_config import logger
from stream_fusion.utils.torrent.torrent_item import TorrentItem

# Lazy import to avoid circular dependencies — only imported when propagation is needed
def _get_group_dao_class():
    from stream_fusion.services.postgresql.dao.torrentgroup_dao import TorrentGroupDAO
    return TorrentGroupDAO

class TorrentItemDAO:

    def __init__(self, session: AsyncSession = Depends(get_db_session)) -> None:
        self.session = session

    async def create_torrent_item(self, torrent_item: TorrentItem, id: str) -> TorrentItemModel:
        async with self.session.begin():
            try:
                new_item = TorrentItemModel.from_torrent_item(torrent_item)
                new_item.id = id
                self.session.add(new_item)
                await self.session.flush()
                await self.session.refresh(new_item)
                logger.trace(f"TorrentItemDAO: Created new TorrentItem: {new_item.id}")
                return new_item
            except Exception as e:
                if "duplicate key value violates unique constraint" not in str(e):
                    logger.error(f"TorrentItemDAO: Error creating TorrentItem: {str(e)}")

    async def get_all_torrent_items(self, limit: int, offset: int) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).limit(limit).offset(offset)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.trace(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems: {str(e)}")

    async def get_torrent_item_by_id(self, item_id: str) -> Optional[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == item_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()
                if db_item:
                    logger.trace(f"TorrentItemDAO: Retrieved TorrentItem: {item_id}")
                    return db_item
                else:
                    logger.trace(f"TorrentItemDAO: TorrentItem not found: {item_id}")
                    return None
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItem {item_id}: {str(e)}")
                return None

    async def update_torrent_item(self, item_id: str, torrent_item: TorrentItem) -> TorrentItemModel:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == item_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()

                if not db_item:
                    logger.warning(f"TorrentItemDAO: TorrentItem not found for update: {item_id}")
                    return None

                for key, value in torrent_item.__dict__.items():
                    if key == 'size' and value is not None:
                        try:
                            value = int(value)
                        except (ValueError, TypeError):
                            logger.warning(f"TorrentItemDAO: Invalid size value '{value}' for item {item_id}, skipping")
                            continue
                    setattr(db_item, key, value)

                db_item.updated_at = int(datetime.now(timezone.utc).timestamp())
                await self.session.flush()
                await self.session.refresh(db_item)
                logger.trace(f"TorrentItemDAO: Updated TorrentItem: {item_id}")
                return db_item
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error updating TorrentItem {item_id}: {str(e)}")
                return None

    async def delete_torrent_item(self, item_id: str) -> bool:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == item_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()

                if db_item:
                    await self.session.delete(db_item)
                    logger.trace(f"TorrentItemDAO: Deleted TorrentItem: {item_id}")
                    return True
                else:
                    logger.warning(f"TorrentItemDAO: TorrentItem not found for deletion: {item_id}")
                    return False
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error deleting TorrentItem {item_id}: {str(e)}")
                return False

    async def get_torrent_items_by_info_hash(self, info_hash: str) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.info_hash == info_hash)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.trace(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems with info_hash: {info_hash}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems by info_hash {info_hash}: {str(e)}")
                return None

    async def get_group_ids_by_ids(self, item_ids: List[str]) -> dict:
        """Return {item_id: group_id} for the given item IDs (single batch query)."""
        if not item_ids:
            return {}
        async with self.session.begin():
            try:
                result = await self.session.execute(
                    select(TorrentItemModel.id, TorrentItemModel.group_id)
                    .where(TorrentItemModel.id.in_(item_ids))
                )
                return {row[0]: row[1] for row in result.fetchall()}
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error in get_group_ids_by_ids: {e}")
                return {}

    async def get_torrent_items_by_indexer(self, indexer: str) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.indexer == indexer)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.trace(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems from indexer: {indexer}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems by indexer {indexer}: {str(e)}")
                return None

    async def is_torrent_item_cached(self, item_id: str) -> bool:
        async with self.session.begin():
            try:
                query = select(func.count()).where(TorrentItemModel.id == item_id)
                result = await self.session.execute(query)
                count = result.scalar_one()
                is_cached = count > 0
                logger.trace(f"TorrentItemDAO: TorrentItem {item_id} {'is' if is_cached else 'is not'} in cache")
                return is_cached
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error checking if TorrentItem {item_id} is cached: {str(e)}")
                return None

    async def get_torrent_items_by_type(self, item_type: str) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.type == item_type)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.trace(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems of type: {item_type}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems by type {item_type}: {str(e)}")
                return None

    async def get_torrent_items_by_availability(self, available: bool) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.availability == available)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.trace(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems with availability: {available}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems by availability {available}: {str(e)}")
                return None

    async def get_batch_by_hashes(self, hashes: list) -> dict:
        """Return {info_hash: metadata_dict} for the given hashes. Missing hashes are omitted."""
        if not hashes:
            return {}
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.info_hash.in_(hashes))
                result = await self.session.execute(query)
                rows = result.scalars().all()
                logger.trace(f"TorrentItemDAO: get_batch_by_hashes — {len(rows)}/{len(hashes)} found")
                return {
                    row.info_hash: {
                        "raw_title": row.raw_title,
                        "size": row.size,
                        "file_name": row.file_name,
                        "files": row.files,
                        "languages": row.languages,
                        "seeders": row.seeders,
                        "parsed_data": row.parsed_data,
                        "full_index": row.full_index,
                    }
                    for row in rows
                }
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error in get_batch_by_hashes: {str(e)}")
                return {}

    async def search_by_info_hash(self, info_hash: str) -> Optional[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.info_hash == info_hash).limit(1)
                result = await self.session.execute(query)
                item = result.scalar_one_or_none()
                if item:
                    logger.trace(f"TorrentItemDAO: Found torrent with info_hash: {info_hash}")
                return item
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error searching by info_hash {info_hash}: {str(e)}")
                return None

    async def search_by_tmdb_id(self, tmdb_id: int) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                # Exclude items that have been flagged as mismatches for this TMDB ID
                mismatch_subq = (
                    select(TmdbMismatchModel.id)
                    .where(TmdbMismatchModel.info_hash == TorrentItemModel.info_hash)
                    .where(TmdbMismatchModel.tmdb_id == tmdb_id)
                    .correlate(TorrentItemModel)
                )
                query = (
                    select(TorrentItemModel)
                    .where(TorrentItemModel.tmdb_id == tmdb_id)
                    .where(~exists(mismatch_subq))
                )
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.trace(f"TorrentItemDAO: Found {len(items)} torrents for TMDB ID: {tmdb_id}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error searching by TMDB ID {tmdb_id}: {str(e)}")
                return []

    async def update_torrent_file_path(self, torrent_id: str, file_path: str) -> bool:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == torrent_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()

                if not db_item:
                    logger.warning(f"TorrentItemDAO: TorrentItem not found for file path update: {torrent_id}")
                    return False

                db_item.torrent_file_path = file_path
                db_item.updated_at = int(datetime.now(timezone.utc).timestamp())
                await self.session.flush()
                await self.session.refresh(db_item)
                logger.trace(f"TorrentItemDAO: Updated torrent_file_path for {torrent_id}: {file_path}")
                return True
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error updating torrent_file_path for {torrent_id}: {str(e)}")
                return False

    async def update_torrent_file_path_and_tmdb_id(self, torrent_id: str, file_path: str, tmdb_id: Optional[int]) -> bool:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == torrent_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()

                if not db_item:
                    logger.warning(f"TorrentItemDAO: TorrentItem not found for update: {torrent_id}")
                    return False

                db_item.torrent_file_path = file_path
                if tmdb_id:
                    db_item.tmdb_id = tmdb_id
                db_item.updated_at = int(datetime.now(timezone.utc).timestamp())
                await self.session.flush()
                await self.session.refresh(db_item)
                logger.trace(f"TorrentItemDAO: Updated torrent_file_path ({file_path}) and TMDB ID ({tmdb_id}) for {torrent_id}")
                return True
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error updating torrent_file_path and tmdb_id for {torrent_id}: {str(e)}")
                return False

    async def update_tmdb_id_by_info_hash(self, info_hash: str, tmdb_id: int) -> int:
        """Update tmdb_id for a specific torrent identified by its info_hash.

        Preferred over update_tmdb_id_by_raw_title when info_hash is available,
        as info_hash is unique per torrent and avoids title-collision risks.
        Skips the update if (info_hash, tmdb_id) is a known mismatch.

        Each call runs inside a savepoint (nested transaction) so that a
        deadlock on one row does not poison the outer transaction for all
        subsequent calls in the same session.
        """
        try:
            mismatch_subq = (
                select(TmdbMismatchModel.id)
                .where(TmdbMismatchModel.info_hash == info_hash)
                .where(TmdbMismatchModel.tmdb_id == tmdb_id)
            )
            stmt = (
                update(TorrentItemModel)
                .where(TorrentItemModel.info_hash == info_hash)
                .where(TorrentItemModel.tmdb_id.is_(None))
                .where(~exists(mismatch_subq))
                .values(
                    tmdb_id=tmdb_id,
                    updated_at=int(datetime.now(timezone.utc).timestamp())
                )
            )
            # Savepoint: if this UPDATE deadlocks, only this savepoint is rolled
            # back — the outer transaction remains intact and the next call succeeds.
            async with self.session.begin_nested():
                result = await self.session.execute(stmt)
                row_count = result.rowcount

            if row_count:
                logger.trace(f"TorrentItemDAO: Updated {row_count} torrents with info_hash '{info_hash}' to tmdb_id {tmdb_id}")
                await self._propagate_tmdb_to_groups_by_hash(info_hash, tmdb_id)
            else:
                logger.trace(f"TorrentItemDAO: Skipped tmdb_id assignment for '{info_hash}' (already set or known mismatch)")
            return row_count
        except Exception as e:
            logger.error(f"TorrentItemDAO: Error updating tmdb_id for info_hash '{info_hash}': {str(e)}")
            return 0

    async def bulk_update_tmdb_id_by_info_hashes(
        self,
        info_hashes: list[str],
        tmdb_id: int,
    ) -> int:
        """Single-statement bulk UPDATE for a batch of info_hashes.

        Preferred over calling update_tmdb_id_by_info_hash in a loop when
        several hashes are known upfront: ONE statement acquires and releases
        all row locks atomically, which prevents deadlocks between concurrent
        requests that update overlapping hash sets.
        """
        if not info_hashes:
            return 0
        try:
            # Correlated mismatch subquery — filters out known bad assignments per row
            mismatch_subq = (
                select(TmdbMismatchModel.id)
                .where(TmdbMismatchModel.info_hash == TorrentItemModel.info_hash)
                .where(TmdbMismatchModel.tmdb_id == tmdb_id)
            )
            stmt = (
                update(TorrentItemModel)
                .where(TorrentItemModel.info_hash.in_(info_hashes))
                .where(TorrentItemModel.tmdb_id.is_(None))
                .where(~exists(mismatch_subq))
                .values(
                    tmdb_id=tmdb_id,
                    updated_at=int(datetime.now(timezone.utc).timestamp()),
                )
            )
            async with self.session.begin_nested():
                result = await self.session.execute(stmt)
                row_count = result.rowcount

            if row_count:
                logger.trace(
                    f"TorrentItemDAO: Bulk-updated {row_count} torrents "
                    f"({len(info_hashes)} hashes) to tmdb_id {tmdb_id}"
                )
                for ih in info_hashes:
                    await self._propagate_tmdb_to_groups_by_hash(ih, tmdb_id)
            return row_count
        except Exception as e:
            logger.error(
                f"TorrentItemDAO: Bulk tmdb_id update failed for {len(info_hashes)} hashes: {str(e)}"
            )
            return 0

    async def update_tmdb_id_by_raw_title(self, raw_title: str, tmdb_id: int, indexer: str = None) -> int:
        """Update tmdb_id for torrents matching raw_title, propagating to all siblings with the same info_hash.

        Fallback for when info_hash is not available on the calling item. Two-step process:
        1. Collect info_hashes of rows matching raw_title (+indexer safety filter), excluding known mismatches.
        2. Update ALL rows sharing those hashes (propagates to other indexers' copies of the same torrent).
        3. Rows without info_hash: update directly by raw_title+indexer (no propagation possible).

        Runs inside a savepoint so a deadlock only rolls back this call, not the outer transaction.
        """
        try:
            now_ts = int(datetime.now(timezone.utc).timestamp())
            total_rows = 0
            matched_hashes: list[str] = []
            safe_hashes: list[str] = []

            async with self.session.begin_nested():
                # ── Step 1: collect info_hashes of matching rows ──
                hash_conditions = [
                    TorrentItemModel.raw_title == raw_title,
                    TorrentItemModel.tmdb_id.is_(None),
                    TorrentItemModel.info_hash.isnot(None),
                ]
                if indexer:
                    hash_conditions.append(TorrentItemModel.indexer == indexer)

                hash_result = await self.session.execute(
                    select(TorrentItemModel.info_hash).where(*hash_conditions)
                )
                matched_hashes = [row[0] for row in hash_result.fetchall()]

                if matched_hashes:
                    # Filter out hashes that are known mismatches for this tmdb_id
                    mismatch_result = await self.session.execute(
                        select(TmdbMismatchModel.info_hash)
                        .where(TmdbMismatchModel.info_hash.in_(matched_hashes))
                        .where(TmdbMismatchModel.tmdb_id == tmdb_id)
                    )
                    mismatch_hashes = {row[0] for row in mismatch_result.fetchall()}
                    safe_hashes = [h for h in matched_hashes if h not in mismatch_hashes]

                    if safe_hashes:
                        # ── Step 2: propagate to ALL rows with those hashes (all indexers) ──
                        result = await self.session.execute(
                            update(TorrentItemModel)
                            .where(TorrentItemModel.info_hash.in_(safe_hashes))
                            .where(TorrentItemModel.tmdb_id.is_(None))
                            .values(tmdb_id=tmdb_id, updated_at=now_ts)
                        )
                        total_rows += result.rowcount

                # ── Step 3: rows without info_hash — direct update, no propagation possible ──
                no_hash_conditions = [
                    TorrentItemModel.raw_title == raw_title,
                    TorrentItemModel.tmdb_id.is_(None),
                    TorrentItemModel.info_hash.is_(None),
                ]
                if indexer:
                    no_hash_conditions.append(TorrentItemModel.indexer == indexer)

                result = await self.session.execute(
                    update(TorrentItemModel)
                    .where(*no_hash_conditions)
                    .values(tmdb_id=tmdb_id, updated_at=now_ts)
                )
                total_rows += result.rowcount

            # savepoint committed — now log and propagate outside the savepoint
            if total_rows:
                sibling_info = f", propagated via {len(matched_hashes)} hash(es)" if matched_hashes else ""
                logger.trace(
                    f"TorrentItemDAO: Updated {total_rows} torrents with raw_title '{raw_title}'"
                    f" (indexer={indexer}{sibling_info}) to tmdb_id {tmdb_id}"
                )
                for info_hash in safe_hashes:
                    await self._propagate_tmdb_to_groups_by_hash(info_hash, tmdb_id)
            else:
                logger.trace(
                    f"TorrentItemDAO: Skipped tmdb_id for raw_title '{raw_title}'"
                    f" (already set, no match, or known mismatch)"
                )
            return total_rows

        except Exception as e:
            logger.error(f"TorrentItemDAO: Error updating tmdb_id for raw_title '{raw_title}': {str(e)}")
            return 0

    async def touch_items_by_info_hash(self, info_hashes: list) -> None:
        """Refresh updated_at for a batch of items to reset the cache-first TTL.

        Called after a live search so that the next request sees the Postgres
        cache as fresh and doesn't trigger another live search immediately.
        """
        if not info_hashes:
            return
        # Sort for consistent row-locking order across concurrent requests.
        sorted_hashes = sorted(info_hashes)
        for attempt in range(2):
            try:
                stmt = (
                    update(TorrentItemModel)
                    .where(TorrentItemModel.info_hash.in_(sorted_hashes))
                    .values(updated_at=int(datetime.now(timezone.utc).timestamp()))
                )
                await self.session.execute(stmt)
                await self.session.flush()
                logger.trace(f"TorrentItemDAO: Touched updated_at for {len(sorted_hashes)} items (TTL refresh)")
                return
            except Exception as e:
                if "DeadlockDetectedError" in str(e):
                    if attempt == 0:
                        logger.debug("TorrentItemDAO: Deadlock on touch, retrying after rollback")
                        try:
                            await self.session.rollback()
                        except Exception:
                            pass
                        continue
                    logger.warning(
                        f"TorrentItemDAO: Deadlock on touch after retry — TTL refresh skipped for {len(sorted_hashes)} items"
                    )
                else:
                    logger.error(f"TorrentItemDAO: Error touching items by info_hash: {str(e)}")

    async def get_latest_tmdb_ids(self, item_type: str, limit: int = 50) -> List[int]:
        async with self.session.begin():
            try:
                query = select(
                    TorrentItemModel.tmdb_id,
                    func.min(TorrentItemModel.created_at).label('first_seen')
                ).where(
                    TorrentItemModel.type == item_type,
                    TorrentItemModel.tmdb_id.isnot(None),
                    TorrentItemModel.indexer == "Yggtorrent - API",
                    TorrentItemModel.languages.any('fr')
                ).group_by(
                    TorrentItemModel.tmdb_id
                ).order_by(
                    func.min(TorrentItemModel.created_at).desc()
                ).limit(limit)

                result = await self.session.execute(query)
                rows = result.fetchall()
                tmdb_ids = [row.tmdb_id for row in rows]
                logger.trace(f"TorrentItemDAO: Retrieved {len(tmdb_ids)} latest TMDB IDs (FR/MULTI) for {item_type}")
                return tmdb_ids
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error getting latest TMDB IDs for {item_type}: {str(e)}")
                return []

    async def get_recently_added_tmdb_ids(self, item_type: str, limit: int = 50) -> List[int]:
        async with self.session.begin():
            try:
                query = select(
                    TorrentItemModel.tmdb_id,
                    func.max(TorrentItemModel.created_at).label('last_added')
                ).where(
                    TorrentItemModel.type == item_type,
                    TorrentItemModel.tmdb_id.isnot(None),
                    TorrentItemModel.indexer == "Yggtorrent - API"
                ).group_by(
                    TorrentItemModel.tmdb_id
                ).order_by(
                    func.max(TorrentItemModel.created_at).desc()
                ).limit(limit)

                result = await self.session.execute(query)
                rows = result.fetchall()
                tmdb_ids = [row.tmdb_id for row in rows]
                logger.trace(f"TorrentItemDAO: Retrieved {len(tmdb_ids)} recently added TMDB IDs for {item_type}")
                return tmdb_ids
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error getting recently added TMDB IDs for {item_type}: {str(e)}")
                return []

    async def get_series_with_new_episodes(self, recent_days: int = 7, limit: int = 50) -> List[int]:
        async with self.session.begin():
            try:
                from sqlalchemy.sql.expression import literal_column

                cutoff_timestamp = int((datetime.now(timezone.utc) - timedelta(days=recent_days)).timestamp())

                subquery = select(
                    TorrentItemModel.tmdb_id,
                    literal_column("parsed_data->>'seasons'").label('season_json'),
                    literal_column("parsed_data->>'episodes'").label('episode_json'),
                    func.min(TorrentItemModel.created_at).label('first_seen')
                ).where(
                    TorrentItemModel.type == 'series',
                    TorrentItemModel.tmdb_id.isnot(None),
                    TorrentItemModel.parsed_data.isnot(None)
                ).group_by(
                    TorrentItemModel.tmdb_id,
                    literal_column("parsed_data->>'seasons'"),
                    literal_column("parsed_data->>'episodes'")
                ).having(
                    func.min(TorrentItemModel.created_at) >= cutoff_timestamp
                ).subquery()

                query = select(
                    subquery.c.tmdb_id,
                    func.max(subquery.c.first_seen).label('latest_new_episode')
                ).group_by(
                    subquery.c.tmdb_id
                ).order_by(
                    func.max(subquery.c.first_seen).desc()
                ).limit(limit)

                result = await self.session.execute(query)
                rows = result.fetchall()
                tmdb_ids = [row.tmdb_id for row in rows]
                logger.trace(f"TorrentItemDAO: Retrieved {len(tmdb_ids)} series with new episodes (last {recent_days} days)")
                return tmdb_ids
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error getting series with new episodes: {str(e)}")
                return []

    async def filter_existing_tmdb_ids(self, tmdb_ids: List[int], item_type: str, recent_days: Optional[int] = None, sort_by_added: bool = False, return_episode_info: bool = False):
        if not tmdb_ids:
            return []

        async with self.session.begin():
            try:
                conditions = [
                    TorrentItemModel.tmdb_id.in_(tmdb_ids),
                    TorrentItemModel.type == item_type,
                    TorrentItemModel.indexer == "Yggtorrent - API",
                ]

                if recent_days is not None:
                    cutoff_timestamp = int((datetime.now(timezone.utc) - timedelta(days=recent_days)).timestamp())

                    if item_type == "series":
                        from sqlalchemy.sql.expression import literal_column

                        subquery = select(
                            TorrentItemModel.tmdb_id,
                            literal_column("parsed_data->>'seasons'").label('season_json'),
                            literal_column("parsed_data->>'episodes'").label('episode_json'),
                            func.min(TorrentItemModel.created_at).label('first_seen')
                        ).where(
                            TorrentItemModel.tmdb_id.in_(tmdb_ids),
                            TorrentItemModel.type == item_type,
                            TorrentItemModel.parsed_data.isnot(None)
                        ).group_by(
                            TorrentItemModel.tmdb_id,
                            literal_column("parsed_data->>'seasons'"),
                            literal_column("parsed_data->>'episodes'")
                        ).having(
                            func.min(TorrentItemModel.created_at) >= cutoff_timestamp
                        ).subquery()

                        query = select(subquery.c.tmdb_id.distinct())
                    else:
                        conditions.append(TorrentItemModel.created_at >= cutoff_timestamp)
                        query = select(TorrentItemModel.tmdb_id.distinct()).where(*conditions)
                else:
                    from sqlalchemy import or_
                    conditions.append(or_(TorrentItemModel.languages.any('fr'), TorrentItemModel.languages.any('multi')))
                    if sort_by_added:
                        vostfr_conditions = conditions + [
                            ~TorrentItemModel.raw_title.ilike('%VOSTFR%'),
                            ~TorrentItemModel.raw_title.ilike('%FANSUB%'),
                            ~TorrentItemModel.raw_title.ilike('%SUBFRENCH%'),
                        ]
                        if item_type == "series":
                            from sqlalchemy.sql.expression import literal_column

                            subquery = select(
                                TorrentItemModel.tmdb_id,
                                literal_column("parsed_data->>'seasons'").label('season_json'),
                                literal_column("parsed_data->>'episodes'").label('episode_json'),
                                func.min(TorrentItemModel.created_at).label('first_seen')
                            ).where(
                                *vostfr_conditions,
                                TorrentItemModel.parsed_data.isnot(None)
                            ).group_by(
                                TorrentItemModel.tmdb_id,
                                literal_column("parsed_data->>'seasons'"),
                                literal_column("parsed_data->>'episodes'")
                            ).subquery()

                            cutoff_30d = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())

                            if return_episode_info:
                                query = select(
                                    subquery.c.tmdb_id,
                                    subquery.c.season_json,
                                    subquery.c.episode_json,
                                    subquery.c.first_seen
                                ).where(
                                    subquery.c.first_seen >= cutoff_30d
                                ).distinct(
                                    subquery.c.tmdb_id
                                ).order_by(
                                    subquery.c.tmdb_id,
                                    subquery.c.first_seen.desc()
                                )
                            else:
                                query = select(
                                    subquery.c.tmdb_id,
                                    func.max(subquery.c.first_seen).label('latest_new_episode')
                                ).group_by(
                                    subquery.c.tmdb_id
                                ).having(
                                    func.max(subquery.c.first_seen) >= cutoff_30d
                                ).order_by(
                                    func.max(subquery.c.first_seen).desc()
                                )
                        else:
                            query = select(
                                TorrentItemModel.tmdb_id,
                                func.min(TorrentItemModel.created_at).label('first_seen')
                            ).where(*vostfr_conditions).group_by(
                                TorrentItemModel.tmdb_id
                            ).order_by(
                                func.min(TorrentItemModel.created_at).desc()
                            )
                    else:
                        query = select(TorrentItemModel.tmdb_id.distinct()).where(*conditions)

                result = await self.session.execute(query)
                rows = result.fetchall()

                if return_episode_info and item_type == "series" and sort_by_added:
                    episode_data = []
                    for row in rows:
                        episode_data.append({
                            'tmdb_id': row[0],
                            'season': row[1],
                            'episode': row[2],
                            'first_seen': row[3]
                        })
                    episode_data.sort(key=lambda x: x['first_seen'], reverse=True)
                    logger.trace(f"TorrentItemDAO: Filtered {len(tmdb_ids)} TMDB IDs to {len(episode_data)} with episode info for {item_type}")
                    return episode_data
                elif sort_by_added and recent_days is None:
                    filtered_ids = [row[0] for row in rows]
                else:
                    existing_ids = {row[0] for row in rows}
                    filtered_ids = [tid for tid in tmdb_ids if tid in existing_ids]

                recent_info = f" (recent {recent_days}d, by episode)" if recent_days and item_type == "series" else (f" (recent {recent_days}d)" if recent_days else "")
                sort_info = ", sorted by added date" if sort_by_added else ""
                logger.trace(f"TorrentItemDAO: Filtered {len(tmdb_ids)} TMDB IDs to {len(filtered_ids)} existing (FR/MULTI{recent_info}{sort_info}) for {item_type}")
                return filtered_ids
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error filtering TMDB IDs for {item_type}: {str(e)}")
                return []

    async def search_unmatched(self, query: str = None, limit: int = 200) -> List[TorrentItemModel]:
        """Return torrents with tmdb_id=NULL, grouped by indexer for visual grouping.

        If a query string is provided, each word is matched against raw_title (ILIKE).
        Results are ordered by indexer then raw_title — the admin selects manually.
        """
        async with self.session.begin():
            try:
                conditions = [TorrentItemModel.tmdb_id.is_(None)]
                if query:
                    for word in query.split():
                        conditions.append(TorrentItemModel.raw_title.ilike(f"%{word}%"))
                stmt = (
                    select(TorrentItemModel)
                    .where(*conditions)
                    .order_by(
                        TorrentItemModel.indexer.asc(),
                        TorrentItemModel.raw_title.asc(),
                    )
                    .limit(limit)
                )
                result = await self.session.execute(stmt)
                items = result.scalars().all()
                logger.trace(f"TorrentItemDAO: Found {len(items)} unmatched torrents (query={query!r})")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error searching unmatched torrents: {str(e)}")
                return []

    async def assign_tmdb_by_info_hashes(self, info_hashes: list[str], tmdb_id: int) -> int:
        """Assign a tmdb_id to a list of torrents explicitly selected by the admin.

        Does not check tmdb_id IS NULL — the admin may intentionally re-assign
        a torrent that was previously mismatched and then cleaned.
        """
        if not info_hashes:
            return 0
        try:
            stmt = (
                update(TorrentItemModel)
                .where(TorrentItemModel.info_hash.in_(info_hashes))
                .values(
                    tmdb_id=tmdb_id,
                    updated_at=int(datetime.now(timezone.utc).timestamp()),
                )
            )
            result = await self.session.execute(stmt)
            await self.session.flush()
            row_count = result.rowcount
            logger.trace(
                f"TorrentItemDAO: Admin assigned tmdb_id={tmdb_id} to {row_count} torrents"
            )
            return row_count
        except Exception as e:
            logger.error(f"TorrentItemDAO: Error in assign_tmdb_by_info_hashes: {str(e)}")
            return 0

    # ------------------------------------------------------------------
    # Similarity search (used by auto-grouping in TorrentService)
    # ------------------------------------------------------------------

    async def find_similar_by_size(
        self,
        size: int,
        max_diff: int = 524288,  # 512 KB absolute tolerance
        exclude_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[TorrentItemModel]:
        """Return items whose size is within *max_diff* bytes of *size*.

        Default tolerance is 512 KB — enough to cover the presence/absence of
        an NFO file while being far too small to confuse different encodes.
        """
        if size <= 0:
            return []
        min_size = max(0, size - max_diff)
        max_size = size + max_diff
        async with self.session.begin():
            try:
                conditions = [
                    TorrentItemModel.size.between(min_size, max_size),
                    TorrentItemModel.size > 0,
                ]
                if exclude_id:
                    conditions.append(TorrentItemModel.id != exclude_id)
                query = select(TorrentItemModel).where(*conditions).limit(limit)
                result = await self.session.execute(query)
                items = list(result.scalars().all())
                logger.trace(
                    f"TorrentItemDAO: find_similar_by_size({size}, ±{max_diff} bytes) "
                    f"→ {len(items)} candidates"
                )
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error in find_similar_by_size: {e}")
                return []

    # ------------------------------------------------------------------
    # Group-aware TMDB propagation helpers
    # ------------------------------------------------------------------

    async def _propagate_tmdb_to_groups_by_hash(self, info_hash: str, tmdb_id: int) -> None:
        """If the item(s) with info_hash belong to a group, propagate tmdb_id to the group."""
        try:
            rows = await self.session.execute(
                select(TorrentItemModel.group_id)
                .where(TorrentItemModel.info_hash == info_hash)
                .where(TorrentItemModel.group_id.isnot(None))
            )
            group_ids = list({r[0] for r in rows.fetchall()})
            if not group_ids:
                return
            GroupDAO = _get_group_dao_class()
            group_dao = GroupDAO(self.session)
            for gid in group_ids:
                await group_dao.propagate_tmdb_within_group(gid)
        except Exception as e:
            logger.warning(f"TorrentItemDAO: _propagate_tmdb_to_groups_by_hash error: {e}")
