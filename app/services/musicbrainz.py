"""
LinerNotes MusicBrainz API Client
Searches artists, fetches genres and release groups, caches in DB.
"""
import json
import asyncio
import logging
import urllib.parse
from datetime import datetime, timezone, timedelta
import aiohttp

from app.config import MUSICBRAINZ_API, MUSICBRAINZ_USER_AGENT, MUSICBRAINZ_RATE_LIMIT

logger = logging.getLogger("linernotes.musicbrainz")

MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]

# Rate limiter — MB allows 1 req/sec
_last_request_time = 0


async def _rate_limit():
    """Ensure we don't exceed MB rate limits."""
    global _last_request_time
    now = asyncio.get_event_loop().time()
    elapsed = now - _last_request_time
    if elapsed < MUSICBRAINZ_RATE_LIMIT:
        await asyncio.sleep(MUSICBRAINZ_RATE_LIMIT - elapsed)
    _last_request_time = asyncio.get_event_loop().time()


async def _mb_request(url: str, session: aiohttp.ClientSession) -> dict:
    """Make a MusicBrainz API request with retry logic."""
    headers = {
        'User-Agent': MUSICBRAINZ_USER_AGENT,
        'Accept': 'application/json',
    }

    for attempt in range(MAX_RETRIES):
        await _rate_limit()
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 503:
                    # Rate limited — back off
                    delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                    logger.warning("MB rate limited, retrying in %ds", delay)
                    await asyncio.sleep(delay)
                    continue
                elif resp.status == 404:
                    return {'error': 'not found', 'status': 404}
                else:
                    text = await resp.text()
                    logger.warning("MB request failed (%d): %s", resp.status, text[:200])
                    return {'error': f'HTTP {resp.status}', 'status': resp.status}
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                logger.warning("MB request error, retry %d/%d in %ds: %s",
                             attempt + 1, MAX_RETRIES, delay, e)
                await asyncio.sleep(delay)
            else:
                return {'error': str(e)}

    return {'error': 'max retries exceeded'}


async def search_artist(name: str, session: aiohttp.ClientSession) -> tuple:
    """
    Search MusicBrainz for an artist by name.
    Returns (mbid, mb_name, score) or (None, None, 0) on failure.
    """
    query = urllib.parse.urlencode({
        'query': f'artist:"{name}"',
        'fmt': 'json',
        'limit': '5'
    })
    url = f"{MUSICBRAINZ_API}/artist?{query}"
    data = await _mb_request(url, session)

    if 'error' in data:
        logger.warning("Search failed for '%s': %s", name, data['error'])
        return None, None, 0

    artists = data.get('artists', [])
    if not artists:
        logger.warning("No MB results for '%s'", name)
        return None, None, 0

    best = artists[0]
    score = best.get('score', 0)
    mb_name = best.get('name', '')
    mbid = best.get('id', '')

    if score < 80:
        logger.warning("Low confidence match for '%s': '%s' (score: %d)", name, mb_name, score)
        return None, None, score

    return mbid, mb_name, score


async def lookup_artist(mbid: str, session: aiohttp.ClientSession) -> tuple:
    """
    Lookup an artist by MBID with genres and release groups.
    Returns (artist_data, None) or (None, error_string).
    """
    url = f"{MUSICBRAINZ_API}/artist/{mbid}?inc=genres+release-groups&fmt=json"
    data = await _mb_request(url, session)

    if 'error' in data:
        return None, data['error']

    return data, None


def extract_year_from_first_release(rg: dict) -> str:
    """Extract year from release group's first-release-date."""
    date_str = rg.get('first-release-date', '')
    if date_str and len(date_str) >= 4:
        return date_str[:4]
    return ''


async def sync_artist_from_mb(db, artist_id: int, artist_name: str,
                                session: aiohttp.ClientSession) -> dict:
    """
    Full MB sync for a single artist:
    1. Search for MBID
    2. Lookup genres + release groups
    3. Update artist record
    4. Upsert MB release groups (including ones not in library)

    Returns status dict.
    """
    from app.services.genre_resolver import pick_artist_genre

    result = {
        'status': 'ok',
        'mbid': None,
        'genre': None,
        'release_groups_found': 0,
        'matched_albums': 0,
        'new_mb_albums': 0,
    }

    # Step 1: Search
    mbid, mb_name, score = await search_artist(artist_name, session)
    if not mbid:
        result['status'] = f'search failed (score: {score})'
        await db.execute(
            "UPDATE artists SET last_synced = datetime('now'), updated_at = datetime('now') WHERE id = ?",
            (artist_id,)
        )
        await db.commit()
        return result

    result['mbid'] = mbid

    # Step 2: Lookup
    artist_data, err = await lookup_artist(mbid, session)
    if not artist_data:
        result['status'] = f'lookup failed: {err}'
        await db.execute(
            "UPDATE artists SET mbid = ?, last_synced = datetime('now'), updated_at = datetime('now') WHERE id = ?",
            (mbid, artist_id)
        )
        await db.commit()
        return result

    # Step 3: Extract and resolve genres
    mb_genres = [g.get('name', '') for g in artist_data.get('genres', [])]
    winner, weights, unmapped = pick_artist_genre(mb_genres)
    result['genre'] = winner

    # Update artist record
    await db.execute(
        """UPDATE artists SET
           mbid = ?,
           resolved_genre = ?,
           mb_genres_raw = ?,
           genre_weights = ?,
           last_synced = datetime('now'),
           updated_at = datetime('now')
           WHERE id = ?""",
        (mbid, winner, json.dumps(mb_genres), json.dumps(weights), artist_id)
    )

    # Step 4: Process release groups
    release_groups = artist_data.get('release-groups', [])
    result['release_groups_found'] = len(release_groups)

    # Get existing library albums for matching
    cursor = await db.execute(
        "SELECT id, folder_name FROM albums WHERE artist_id = ? AND in_library = 1",
        (artist_id,)
    )
    library_albums = {row['folder_name']: row['id'] for row in await cursor.fetchall()}

    # Get existing MB-only albums to avoid duplicates
    cursor = await db.execute(
        "SELECT id, mb_rgid FROM albums WHERE artist_id = ? AND in_library = 0",
        (artist_id,)
    )
    existing_mb_albums = {row['mb_rgid']: row['id'] for row in await cursor.fetchall()}

    for rg in release_groups:
        rg_title = rg.get('title', '')
        rg_id = rg.get('id', '')
        primary_type = rg.get('primary-type', '')
        secondary_types = rg.get('secondary-types', [])
        year = extract_year_from_first_release(rg)
        is_live = 'Live' in secondary_types
        is_acoustic = _detect_acoustic(rg_title)

        # Try to match to a library album
        matched_library_id = _match_to_library(rg_title, library_albums)

        if matched_library_id:
            # Update existing library album with MB data
            result['matched_albums'] += 1
            await db.execute(
                """UPDATE albums SET
                   mb_title = ?, mb_rgid = ?, primary_type = ?,
                   secondary_types = ?, is_live = ?, is_acoustic = ?,
                   year = ?, match_score = 100,
                   updated_at = datetime('now')
                   WHERE id = ?""",
                (rg_title, rg_id, primary_type,
                 json.dumps(secondary_types), int(is_live), int(is_acoustic),
                 year, matched_library_id)
            )
        elif rg_id not in existing_mb_albums:
            # Insert as MB-only album (not in library)
            result['new_mb_albums'] += 1
            await db.execute(
                """INSERT INTO albums
                   (artist_id, folder_name, mb_title, mb_rgid, primary_type,
                    secondary_types, in_library, is_live, is_acoustic, year,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, datetime('now'), datetime('now'))""",
                (artist_id, rg_title, rg_title, rg_id, primary_type,
                 json.dumps(secondary_types), int(is_live), int(is_acoustic), year)
            )
        else:
            # Update existing MB-only album
            await db.execute(
                """UPDATE albums SET
                   mb_title = ?, primary_type = ?, secondary_types = ?,
                   is_live = ?, is_acoustic = ?, year = ?,
                   updated_at = datetime('now')
                   WHERE id = ?""",
                (rg_title, primary_type, json.dumps(secondary_types),
                 int(is_live), int(is_acoustic), year,
                 existing_mb_albums[rg_id])
            )

    await db.commit()
    return result


async def sync_all_artists(db, progress_callback=None, force: bool = False,
                           cache_days: int = 30) -> dict:
    """
    Sync all artists with MusicBrainz.
    Skips artists synced within cache_days unless force=True.
    """
    stats = {
        'total': 0,
        'synced': 0,
        'skipped': 0,
        'errors': 0,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'finished_at': None,
    }

    # Get all artists
    if force:
        cursor = await db.execute("SELECT id, name FROM artists ORDER BY name")
    else:
        # Skip recently synced
        cutoff = (datetime.now(timezone.utc) - timedelta(days=cache_days)).isoformat()
        cursor = await db.execute(
            """SELECT id, name FROM artists
               WHERE last_synced IS NULL OR last_synced < ?
               ORDER BY name""",
            (cutoff,)
        )

    artists = await cursor.fetchall()
    stats['total'] = len(artists)

    if not artists:
        logger.info("No artists need MB sync")
        stats['finished_at'] = datetime.now(timezone.utc).isoformat()
        return stats

    logger.info("Syncing %d artists with MusicBrainz...", len(artists))

    async with aiohttp.ClientSession() as session:
        for idx, artist in enumerate(artists):
            artist_id = artist['id']
            artist_name = artist['name']

            if progress_callback:
                progress_callback(idx + 1, len(artists), artist_name)

            try:
                result = await sync_artist_from_mb(db, artist_id, artist_name, session)
                if result['status'] == 'ok':
                    stats['synced'] += 1
                    logger.debug("Synced: %s → %s (%d RGs, %d matched)",
                               artist_name, result['genre'],
                               result['release_groups_found'],
                               result['matched_albums'])
                else:
                    stats['errors'] += 1
                    logger.warning("Sync issue for %s: %s", artist_name, result['status'])
            except Exception as e:
                stats['errors'] += 1
                logger.error("Sync failed for %s: %s", artist_name, e)

    stats['finished_at'] = datetime.now(timezone.utc).isoformat()
    logger.info("MB sync complete: %d synced, %d errors, %d skipped",
               stats['synced'], stats['errors'], stats['skipped'])
    return stats


def _normalize_title(title: str) -> str:
    """Normalize an album title for matching."""
    import re
    t = title.lower().strip()
    t = re.sub(r'\s*[\(\[](?:deluxe|remaster|expanded|bonus|special|anniversary|edition).*?[\)\]]', '', t)
    t = re.sub(r'[^\w\s]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def _match_to_library(mb_title: str, library_albums: dict) -> int | None:
    """
    Try to match an MB release group title to a library album folder.
    Returns album_id if matched, None otherwise.
    """
    mb_norm = _normalize_title(mb_title)

    for folder_name, album_id in library_albums.items():
        lib_norm = _normalize_title(folder_name)

        # Exact match
        if mb_norm == lib_norm:
            return album_id

        # Containment match (one contains the other)
        if mb_norm and lib_norm:
            if mb_norm in lib_norm or lib_norm in mb_norm:
                # Require reasonable similarity
                shorter = min(len(mb_norm), len(lib_norm))
                longer = max(len(mb_norm), len(lib_norm))
                if shorter / longer > 0.6:
                    return album_id

    return None


def _detect_acoustic(title: str) -> bool:
    """Check if a title indicates acoustic content."""
    import re
    patterns = [
        re.compile(r'\bacoustic\b', re.IGNORECASE),
        re.compile(r'\bunplugged\b', re.IGNORECASE),
        re.compile(r'\bstripped\b', re.IGNORECASE),
    ]
    return any(p.search(title) for p in patterns)
