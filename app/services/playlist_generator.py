"""
LinerNotes Playlist Generator

Playlist types:
  - Decade playlists → .nsp smart playlists (Navidrome auto-imports, auto-refreshes)
  - Genre playlists  → API-synced to Navidrome with exclusion tracking
  - Live playlist    → API-synced (from albums.is_live)
  - Acoustic playlist→ API-synced (from albums.is_acoustic)

Sync flow for API-managed playlists:
  1. Build track list from LinerNotes DB
  2. Incrementally add new tracks to playlist_tracks (skip excluded)
  3. Build Navidrome path→song_id mapping
  4. Detect manual removals in Navidrome → add to playlist_exclusions
  5. Push new tracks to Navidrome playlist
"""
import os
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger("linernotes.playlist_gen")

DECADES = {
    '1960s': (1960, 1969),
    '1970s': (1970, 1979),
    '1980s': (1980, 1989),
    '1990s': (1990, 1999),
    '2000s': (2000, 2009),
    '2010s': (2010, 2019),
    '2020s': (2020, 2029),
}

# Special playlists derived from album flags
SPECIAL_PLAYLISTS = {
    'Live': 'is_live',
    'Acoustic': 'is_acoustic',
}


async def generate_playlists(db, music_path: str, playlist_path: str,
                              navidrome_url: str = None,
                              navidrome_user: str = None,
                              navidrome_pass: str = None,
                              progress_callback=None) -> dict:
    """
    Generate all playlists:
    1. Decade .nsp smart playlists (written once, Navidrome auto-refreshes)
    2. Genre playlists (API-synced, exclusion-tracked)
    3. Live + Acoustic playlists (API-synced, exclusion-tracked)
    """
    stats = {
        'genre_playlists': 0,
        'special_playlists': 0,
        'decade_playlists': 0,
        'tracks_added': 0,
        'tracks_excluded': 0,
        'tracks_already_in': 0,
        'playlists_synced': 0,
        'nsp_written': 0,
        'errors': 0,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'finished_at': None,
    }

    # ── 1. Decade smart playlists (.nsp) ──────────────────────────────
    if playlist_path:
        nsp_count = await _write_decade_nsp_files(playlist_path)
        stats['decade_playlists'] = nsp_count
        stats['nsp_written'] = nsp_count
        logger.info("Wrote %d decade .nsp files", nsp_count)

    # ── 2. Genre playlists ────────────────────────────────────────────
    cursor = await db.execute("""
        SELECT t.id as track_id, t.filename, t.title,
               a.name as artist_name, al.folder_name as album_folder,
               al.year, t.disc_number, t.track_number,
               COALESCE(a.manual_override, a.resolved_genre) as genre
        FROM tracks t
        JOIN albums al ON t.album_id = al.id
        JOIN artists a ON al.artist_id = a.id
        WHERE al.in_library = 1
          AND COALESCE(a.manual_override, a.resolved_genre) IS NOT NULL
        ORDER BY a.name, al.year, t.disc_number, t.track_number
    """)
    all_tracks = await cursor.fetchall()
    logger.info("Generating playlists from %d tracks", len(all_tracks))

    # Bucket by genre
    genre_buckets = {}
    for track in all_tracks:
        genre = track['genre']
        genre_buckets.setdefault(genre, []).append(track)

    # ── 3. Special playlists (Live, Acoustic) ─────────────────────────
    special_buckets = {}
    for playlist_name, db_column in SPECIAL_PLAYLISTS.items():
        cursor = await db.execute(f"""
            SELECT t.id as track_id, t.filename, t.title,
                   a.name as artist_name, al.folder_name as album_folder,
                   al.year, t.disc_number, t.track_number,
                   COALESCE(a.manual_override, a.resolved_genre) as genre
            FROM tracks t
            JOIN albums al ON t.album_id = al.id
            JOIN artists a ON al.artist_id = a.id
            WHERE al.in_library = 1
              AND al.{db_column} = 1
            ORDER BY a.name, al.year, t.disc_number, t.track_number
        """)
        tracks = await cursor.fetchall()
        if tracks:
            special_buckets[playlist_name] = tracks

    # Count total for progress
    total_playlists = len(genre_buckets) + len(special_buckets)
    current = 0

    # Snapshot existing playlist_tracks BEFORE adding new ones.
    # Only these should be checked for Navidrome removal detection —
    # newly added tracks haven't been synced yet, so their absence
    # from a Navidrome playlist is expected, not a manual removal.
    cursor = await db.execute(
        "SELECT playlist_name, track_id FROM playlist_tracks"
    )
    pre_existing = {}
    for row in await cursor.fetchall():
        pre_existing.setdefault(row['playlist_name'], set()).add(row['track_id'])

    # Process genre playlists
    for genre, tracks in sorted(genre_buckets.items()):
        current += 1
        if progress_callback:
            progress_callback(current, total_playlists, genre)

        result = await _update_playlist(db, genre, tracks)
        stats['genre_playlists'] += 1
        stats['tracks_added'] += result['added']
        stats['tracks_excluded'] += result['excluded']
        stats['tracks_already_in'] += result['already_in']

        await _log_action(db, genre, 'generate', result['total'],
                          {'added': result['added'], 'excluded': result['excluded']})

    # Process special playlists
    for name, tracks in sorted(special_buckets.items()):
        current += 1
        if progress_callback:
            progress_callback(current, total_playlists, name)

        result = await _update_playlist(db, name, tracks)
        stats['special_playlists'] += 1
        stats['tracks_added'] += result['added']
        stats['tracks_excluded'] += result['excluded']
        stats['tracks_already_in'] += result['already_in']

        await _log_action(db, name, 'generate', result['total'],
                          {'added': result['added'], 'excluded': result['excluded']})

    await db.commit()

    # ── 4. Navidrome sync ─────────────────────────────────────────────
    if navidrome_url and navidrome_user and navidrome_pass:
        sync_result = await _sync_to_navidrome(
            db, navidrome_url, navidrome_user, navidrome_pass,
            pre_existing_tracks=pre_existing
        )
        stats['playlists_synced'] = sync_result.get('synced', 0)
        stats['errors'] += sync_result.get('errors', 0)

    stats['finished_at'] = datetime.now(timezone.utc).isoformat()
    logger.info(
        "Playlist generation complete: %d genre, %d special, %d decade (.nsp), "
        "%d added, %d excluded",
        stats['genre_playlists'], stats['special_playlists'],
        stats['decade_playlists'], stats['tracks_added'], stats['tracks_excluded']
    )
    return stats


# ═══════════════════════════════════════════════════════════════════════
# DECADE SMART PLAYLISTS (.nsp)
# ═══════════════════════════════════════════════════════════════════════

async def _write_decade_nsp_files(playlist_path: str) -> int:
    """
    Write Navidrome Smart Playlist (.nsp) files for each decade.
    These are JSON query definitions — Navidrome auto-imports and
    auto-refreshes them. Idempotent: safe to overwrite each run.
    """
    os.makedirs(playlist_path, exist_ok=True)
    count = 0

    for decade_name, (year_start, year_end) in DECADES.items():
        nsp = {
            "name": decade_name,
            "comment": f"LinerNotes: all tracks from the {decade_name}",
            "all": [
                {"inTheRange": {"year": [year_start, year_end]}}
            ],
            "sort": "artist",
            "order": "asc"
        }

        filepath = os.path.join(playlist_path, f"{decade_name}.nsp")
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(nsp, f, indent=2)
        count += 1
        logger.debug("Wrote .nsp: %s", filepath)

    return count


# ═══════════════════════════════════════════════════════════════════════
# INCREMENTAL PLAYLIST UPDATE (genre, live, acoustic)
# ═══════════════════════════════════════════════════════════════════════

async def _update_playlist(db, playlist_name: str,
                           qualifying_tracks: list) -> dict:
    """
    Incrementally update a playlist in playlist_tracks.
    - Skip tracks already present
    - Skip tracks in playlist_exclusions (manually removed)
    - Add everything else
    """
    result = {'added': 0, 'excluded': 0, 'already_in': 0, 'total': 0}

    cursor = await db.execute(
        "SELECT track_id FROM playlist_tracks WHERE playlist_name = ?",
        (playlist_name,)
    )
    existing_ids = {row['track_id'] for row in await cursor.fetchall()}

    cursor = await db.execute(
        "SELECT track_id FROM playlist_exclusions WHERE playlist_name = ?",
        (playlist_name,)
    )
    excluded_ids = {row['track_id'] for row in await cursor.fetchall()}

    for track in qualifying_tracks:
        track_id = track['track_id']

        if track_id in excluded_ids:
            result['excluded'] += 1
            continue

        if track_id in existing_ids:
            result['already_in'] += 1
            continue

        await db.execute(
            """INSERT OR IGNORE INTO playlist_tracks
               (playlist_name, track_id, added_at)
               VALUES (?, ?, datetime('now'))""",
            (playlist_name, track_id)
        )
        result['added'] += 1

    result['total'] = result['already_in'] + result['added']
    return result


# ═══════════════════════════════════════════════════════════════════════
# NAVIDROME SYNC (API-managed playlists only, not .nsp)
# ═══════════════════════════════════════════════════════════════════════

async def _build_path_maps(db) -> tuple:
    """
    Build bidirectional path mappings for all tracks.
    Relative path = "Artist Name/Album Folder/filename.ext"
    matches Navidrome's path format relative to music root.
    """
    cursor = await db.execute("""
        SELECT t.id as track_id, t.filename,
               a.name as artist_name, al.folder_name as album_folder
        FROM tracks t
        JOIN albums al ON t.album_id = al.id
        JOIN artists a ON al.artist_id = a.id
        WHERE al.in_library = 1
    """)
    rows = await cursor.fetchall()

    path_to_track = {}
    track_to_path = {}
    for row in rows:
        rel_path = f"{row['artist_name']}/{row['album_folder']}/{row['filename']}"
        path_to_track[rel_path] = row['track_id']
        track_to_path[row['track_id']] = rel_path

    return path_to_track, track_to_path


async def _build_nd_song_cache(base_url: str, username: str, password: str,
                                artist_names: list) -> dict:
    """
    Build a Navidrome path → song_id cache by searching for each artist.
    Lets us map our tracks to Navidrome song IDs for playlist operations.
    """
    from app.services.navidrome import search_songs

    nd_path_to_id = {}
    searched = set()

    for artist in artist_names:
        if artist in searched:
            continue
        searched.add(artist)

        songs = await search_songs(base_url, username, password,
                                   artist, count=500)
        for song in songs:
            path = song.get('path', '')
            song_id = song.get('id', '')
            if path and song_id:
                nd_path_to_id[path] = song_id

    logger.info("Built Navidrome song cache: %d songs from %d artists",
                len(nd_path_to_id), len(searched))
    return nd_path_to_id


async def _sync_to_navidrome(db, base_url: str, username: str,
                             password: str,
                             pre_existing_tracks: dict = None) -> dict:
    """
    Sync API-managed playlists to Navidrome:
    1. Build path mappings (our DB ↔ Navidrome song IDs)
    2. For each playlist in playlist_tracks:
       a. New playlist → create in Navidrome with song IDs
       b. Existing playlist → detect removals → add to exclusions
       c. Push new tracks to Navidrome playlist

    pre_existing_tracks: {playlist_name: set(track_ids)} — tracks that were
    in playlist_tracks BEFORE this run. Only these are checked for removal
    detection. Newly added tracks are expected to be absent from Navidrome.
    """
    from app.services.navidrome import (
        get_playlists, get_playlist, create_playlist, update_playlist
    )

    result = {'synced': 0, 'errors': 0, 'removals_detected': 0}

    path_to_track, track_to_path = await _build_path_maps(db)

    # Get all distinct playlist names from playlist_tracks
    cursor = await db.execute(
        "SELECT DISTINCT playlist_name FROM playlist_tracks"
    )
    ln_playlists = [row['playlist_name'] for row in await cursor.fetchall()]
    if not ln_playlists:
        return result

    # Get unique artist names for song cache
    cursor = await db.execute("""
        SELECT DISTINCT a.name
        FROM playlist_tracks pt
        JOIN tracks t ON pt.track_id = t.id
        JOIN albums al ON t.album_id = al.id
        JOIN artists a ON al.artist_id = a.id
    """)
    artist_names = [row['name'] for row in await cursor.fetchall()]

    # Build Navidrome path→song_id cache
    nd_path_to_id = await _build_nd_song_cache(
        base_url, username, password, artist_names
    )

    # Get existing Navidrome playlists
    nd_playlists = await get_playlists(base_url, username, password)
    nd_by_name = {p['name']: p for p in nd_playlists}

    for playlist_name in ln_playlists:
        try:
            # Get our track IDs for this playlist
            cursor = await db.execute(
                "SELECT track_id FROM playlist_tracks WHERE playlist_name = ?",
                (playlist_name,)
            )
            our_track_ids = {row['track_id'] for row in await cursor.fetchall()}

            # Build path map for this playlist
            our_paths = {}
            for tid in our_track_ids:
                path = track_to_path.get(tid)
                if path:
                    our_paths[path] = tid

            if playlist_name in nd_by_name:
                # ── Existing playlist: detect removals + add new ──
                nd_playlist_data = await get_playlist(
                    base_url, username, password,
                    nd_by_name[playlist_name]['id']
                )

                if 'error' in nd_playlist_data:
                    result['errors'] += 1
                    continue

                nd_entries = nd_playlist_data.get('entry', [])
                if isinstance(nd_entries, dict):
                    nd_entries = [nd_entries]

                nd_paths = {e.get('path', ''): e.get('id', '')
                            for e in nd_entries if e.get('path')}

                # Detect removals: track was previously synced (in pre_existing)
                # + exists in Navidrome library, but NOT in this Navidrome
                # playlist = user manually removed it.
                # Only check pre-existing tracks — newly added ones haven't
                # been synced yet so their absence is expected.
                previously_synced = pre_existing_tracks.get(playlist_name, set()) \
                    if pre_existing_tracks else our_track_ids

                for path, track_id in our_paths.items():
                    if track_id not in previously_synced:
                        continue  # New this run, skip removal check
                    if path not in nd_paths and path in nd_path_to_id:
                        await db.execute(
                            """INSERT OR IGNORE INTO playlist_exclusions
                               (playlist_name, track_id, excluded_at)
                               VALUES (?, ?, datetime('now'))""",
                            (playlist_name, track_id)
                        )
                        await db.execute(
                            """DELETE FROM playlist_tracks
                               WHERE playlist_name = ? AND track_id = ?""",
                            (playlist_name, track_id)
                        )
                        result['removals_detected'] += 1
                        logger.info(
                            "Detected removal: '%s' track_id=%d (%s)",
                            playlist_name, track_id, path
                        )

                # Add new tracks not yet in Navidrome playlist
                new_song_ids = []
                for path, track_id in our_paths.items():
                    if path not in nd_paths:
                        nd_song_id = nd_path_to_id.get(path)
                        if nd_song_id:
                            new_song_ids.append(nd_song_id)

                if new_song_ids:
                    await update_playlist(
                        base_url, username, password,
                        nd_by_name[playlist_name]['id'],
                        song_ids_to_add=new_song_ids
                    )
                    logger.info("Added %d tracks to Navidrome '%s'",
                                len(new_song_ids), playlist_name)

                result['synced'] += 1

            else:
                # ── New playlist: create with all song IDs ──
                song_ids = []
                for path in our_paths:
                    nd_song_id = nd_path_to_id.get(path)
                    if nd_song_id:
                        song_ids.append(nd_song_id)

                playlist_id = await create_playlist(
                    base_url, username, password,
                    playlist_name, song_ids=song_ids if song_ids else None
                )
                if playlist_id:
                    result['synced'] += 1
                    logger.info("Created Navidrome playlist '%s' (%d tracks)",
                                playlist_name, len(song_ids))
                else:
                    result['errors'] += 1

            await _log_action(db, playlist_name, 'navidrome_sync', 0, None)

        except Exception as e:
            result['errors'] += 1
            logger.error("Navidrome sync failed for '%s': %s", playlist_name, e)

    await db.commit()

    if result['removals_detected']:
        logger.info("Detected %d manual removals across all playlists",
                     result['removals_detected'])

    return result


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

async def _log_action(db, playlist_name: str, action: str,
                      track_count: int, details: dict = None):
    """Write a playlist_log entry."""
    await db.execute(
        """INSERT INTO playlist_log
           (playlist_name, action, track_count, details, status, created_at)
           VALUES (?, ?, ?, ?, 'ok', datetime('now'))""",
        (playlist_name, action, track_count,
         json.dumps(details) if details else None)
    )
