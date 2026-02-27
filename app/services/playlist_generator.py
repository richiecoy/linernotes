"""
LinerNotes Playlist Generator

Playlist types:
  - Decade playlists → .nsp smart playlists (Navidrome auto-imports, auto-refreshes)
  - Genre playlists  → .m3u files in music root (Navidrome auto-imports on scan)
  - Live playlist    → .m3u file
  - Acoustic playlist→ .m3u file

All playlists tracked in playlist_tracks for LinerNotes' own records.
Exclusion tracking: compare Navidrome playlist contents via API to detect
manual removals, then exclude those tracks from future M3U writes.

Navidrome sync flow:
  1. On first run: M3U files created, Navidrome imports them
  2. On subsequent runs: check Navidrome playlists for removals → exclusions
  3. Rewrite M3U with current playlist_tracks (excluding removed)
  4. Navidrome picks up changes on next library scan
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
    1. Decade .nsp smart playlists (Navidrome auto-refreshes)
    2. Genre playlists → playlist_tracks + M3U files
    3. Live + Acoustic → playlist_tracks + M3U files
    4. Navidrome removal detection (if configured)
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
        'm3u_written': 0,
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
               t.disc_number, t.track_number, t.duration_seconds,
               a.name as artist_name, al.folder_name as album_folder,
               al.year,
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

    genre_buckets = {}
    for track in all_tracks:
        genre = track['genre']
        genre_buckets.setdefault(genre, []).append(track)

    # ── 3. Special playlists (Live, Acoustic) ─────────────────────────
    special_buckets = {}
    for playlist_name, db_column in SPECIAL_PLAYLISTS.items():
        cursor = await db.execute(f"""
            SELECT t.id as track_id, t.filename, t.title,
                   t.disc_number, t.track_number, t.duration_seconds,
                   a.name as artist_name, al.folder_name as album_folder,
                   al.year,
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

    # ── 4. Navidrome removal detection (before adding new tracks) ─────
    if navidrome_url and navidrome_user and navidrome_pass:
        removal_result = await _detect_navidrome_removals(
            db, navidrome_url, navidrome_user, navidrome_pass
        )
        stats['playlists_synced'] = removal_result.get('checked', 0)
        if removal_result.get('removals', 0):
            logger.info("Detected %d manual removals", removal_result['removals'])

    # ── 5. Update playlist_tracks + write M3U files ───────────────────
    total_playlists = len(genre_buckets) + len(special_buckets)
    current = 0

    for genre, tracks in sorted(genre_buckets.items()):
        current += 1
        if progress_callback:
            progress_callback(current, total_playlists, genre)

        result = await _update_playlist(db, genre, tracks)
        stats['genre_playlists'] += 1
        stats['tracks_added'] += result['added']
        stats['tracks_excluded'] += result['excluded']
        stats['tracks_already_in'] += result['already_in']

        if playlist_path:
            wrote = await _write_m3u(db, genre, music_path, playlist_path)
            if wrote:
                stats['m3u_written'] += 1

        await _log_action(db, genre, 'generate', result['total'],
                          {'added': result['added'], 'excluded': result['excluded']})

    for name, tracks in sorted(special_buckets.items()):
        current += 1
        if progress_callback:
            progress_callback(current, total_playlists, name)

        result = await _update_playlist(db, name, tracks)
        stats['special_playlists'] += 1
        stats['tracks_added'] += result['added']
        stats['tracks_excluded'] += result['excluded']
        stats['tracks_already_in'] += result['already_in']

        if playlist_path:
            wrote = await _write_m3u(db, name, music_path, playlist_path)
            if wrote:
                stats['m3u_written'] += 1

        await _log_action(db, name, 'generate', result['total'],
                          {'added': result['added'], 'excluded': result['excluded']})

    await db.commit()

    stats['finished_at'] = datetime.now(timezone.utc).isoformat()
    logger.info(
        "Playlist generation complete: %d genre, %d special, %d decade (.nsp), "
        "%d m3u, %d added, %d excluded",
        stats['genre_playlists'], stats['special_playlists'],
        stats['decade_playlists'], stats['m3u_written'],
        stats['tracks_added'], stats['tracks_excluded']
    )
    return stats


# ═══════════════════════════════════════════════════════════════════════
# DECADE SMART PLAYLISTS (.nsp)
# ═══════════════════════════════════════════════════════════════════════

async def _write_decade_nsp_files(playlist_path: str) -> int:
    """
    Write Navidrome Smart Playlist (.nsp) files for each decade.
    Idempotent: safe to overwrite each run.
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
# INCREMENTAL PLAYLIST UPDATE
# ═══════════════════════════════════════════════════════════════════════

async def _update_playlist(db, playlist_name: str,
                           qualifying_tracks: list) -> dict:
    """
    Incrementally update a playlist in playlist_tracks.
    Skip tracks already present or in exclusions.
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
# M3U FILE WRITING
# ═══════════════════════════════════════════════════════════════════════

async def _write_m3u(db, playlist_name: str, music_path: str,
                     playlist_path: str) -> bool:
    """
    Write an M3U playlist file using real filesystem paths.
    Navidrome auto-imports these from the music/playlist directory.

    Paths are absolute: /music/Artist/Album (Year)/filename.mp3
    This matches what Navidrome sees on its filesystem.
    """
    try:
        os.makedirs(playlist_path, exist_ok=True)

        cursor = await db.execute("""
            SELECT t.filename, t.title, t.duration_seconds,
                   a.name as artist_name, al.folder_name as album_folder
            FROM playlist_tracks pt
            JOIN tracks t ON pt.track_id = t.id
            JOIN albums al ON t.album_id = al.id
            JOIN artists a ON al.artist_id = a.id
            WHERE pt.playlist_name = ?
            ORDER BY a.name, al.year, t.disc_number, t.track_number
        """, (playlist_name,))
        tracks = await cursor.fetchall()

        if not tracks:
            return False

        filepath = os.path.join(playlist_path, f"{playlist_name}.m3u")

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('#EXTM3U\n')
            f.write(f'#PLAYLIST:{playlist_name}\n')
            for track in tracks:
                duration = int(track['duration_seconds'] or 0)
                artist = track['artist_name']
                title = track['title'] or track['filename']
                # Absolute path as Navidrome sees it
                abs_path = os.path.join(
                    music_path,
                    track['artist_name'],
                    track['album_folder'],
                    track['filename']
                )
                f.write(f'#EXTINF:{duration},{artist} - {title}\n')
                f.write(f'{abs_path}\n')

        logger.debug("Wrote M3U: %s (%d tracks)", filepath, len(tracks))
        return True

    except Exception as e:
        logger.error("Failed to write M3U for '%s': %s", playlist_name, e)
        return False


# ═══════════════════════════════════════════════════════════════════════
# NAVIDROME REMOVAL DETECTION
# ═══════════════════════════════════════════════════════════════════════

async def _detect_navidrome_removals(db, base_url: str, username: str,
                                     password: str) -> dict:
    """
    Compare Navidrome playlist contents against our playlist_tracks.
    Tracks in our DB but removed from Navidrome = user exclusion.

    Matching strategy: build a set of filesystem paths from Navidrome's
    playlist entries and compare against our known paths.
    Navidrome returns real filesystem paths in getPlaylist entries.
    """
    from app.services.navidrome import get_playlists, get_playlist

    result = {'checked': 0, 'removals': 0, 'errors': 0}

    # Get our playlists
    cursor = await db.execute(
        "SELECT DISTINCT playlist_name FROM playlist_tracks"
    )
    ln_playlists = [row['playlist_name'] for row in await cursor.fetchall()]
    if not ln_playlists:
        return result

    # Build our track_id → filesystem path map
    cursor = await db.execute("""
        SELECT t.id as track_id, t.filename,
               a.name as artist_name, al.folder_name as album_folder
        FROM tracks t
        JOIN albums al ON t.album_id = al.id
        JOIN artists a ON al.artist_id = a.id
        WHERE al.in_library = 1
    """)
    track_to_path = {}
    for row in await cursor.fetchall():
        # Path fragment: "Artist/Album/filename" — no leading /music/
        path = f"{row['artist_name']}/{row['album_folder']}/{row['filename']}"
        track_to_path[row['track_id']] = path

    # Get Navidrome playlists
    nd_playlists = await get_playlists(base_url, username, password)
    nd_by_name = {p['name']: p for p in nd_playlists}

    for playlist_name in ln_playlists:
        if playlist_name not in nd_by_name:
            continue  # Not yet in Navidrome, skip

        try:
            nd_playlist = await get_playlist(
                base_url, username, password,
                nd_by_name[playlist_name]['id']
            )

            if 'error' in nd_playlist:
                result['errors'] += 1
                continue

            # Get paths from Navidrome playlist entries
            nd_entries = nd_playlist.get('entry', [])
            if isinstance(nd_entries, dict):
                nd_entries = [nd_entries]

            # Navidrome paths are absolute: /music/Artist/Album/file.mp3
            # Normalize by stripping any leading path prefix to get
            # just "Artist/Album/file.mp3"
            nd_paths = set()
            for entry in nd_entries:
                path = entry.get('path', '')
                if path:
                    # Strip leading /music/ or whatever the mount is
                    # We just need the relative part: Artist/Album/file
                    parts = path.split('/')
                    # Find the artist-level start (skip mount prefix)
                    # The path from M3U shows /music/Artist/Album/file
                    # So strip everything before the artist dir
                    if len(parts) >= 3:
                        # Take last 3 parts: Artist/Album/file
                        rel = '/'.join(parts[-3:])
                        nd_paths.add(rel)

            # Get our tracks for this playlist
            cursor = await db.execute(
                "SELECT track_id FROM playlist_tracks WHERE playlist_name = ?",
                (playlist_name,)
            )
            our_track_ids = [row['track_id'] for row in await cursor.fetchall()]

            for track_id in our_track_ids:
                our_path = track_to_path.get(track_id)
                if not our_path:
                    continue

                if our_path not in nd_paths:
                    # Track is in our DB but not in Navidrome playlist
                    # = user manually removed it
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
                    result['removals'] += 1
                    logger.info(
                        "Detected removal: '%s' track_id=%d (%s)",
                        playlist_name, track_id, our_path
                    )

            result['checked'] += 1

        except Exception as e:
            result['errors'] += 1
            logger.error("Removal check failed for '%s': %s", playlist_name, e)

    await db.commit()
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
