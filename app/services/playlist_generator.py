"""
LinerNotes Playlist Generator

Playlist types:
  - Genre: .nsp smart playlists (favorites only, Navidrome evaluates)
  - Decade: .nsp smart playlists (all tracks in range, Navidrome evaluates)
  - Live: M3U (from LinerNotes DB is_live flag)
  - Acoustic: M3U (from LinerNotes DB is_acoustic flag)

Genre/decade playlists are fully managed by Navidrome's smart playlist
engine. LinerNotes just writes the rule files.

Live/acoustic playlists use M3U with exclusion support managed in the
LinerNotes UI.
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

SPECIAL_PLAYLISTS = {
    'Live': 'is_live',
    'Acoustic': 'is_acoustic',
}


async def generate_playlists(db, music_path: str, playlist_path: str,
                              progress_callback=None) -> dict:
    stats = {
        'genre_playlists': 0,
        'decade_playlists': 0,
        'special_playlists': 0,
        'nsp_written': 0,
        'm3u_written': 0,
        'tracks_added': 0,
        'tracks_excluded': 0,
        'tracks_already_in': 0,
        'errors': 0,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'finished_at': None,
    }

    os.makedirs(playlist_path, exist_ok=True)

    # ── Cleanup: remove old genre/decade M3U files and DB entries ─────
    await _cleanup_migrated_playlists(db, playlist_path)

    # ── Genre smart playlists (.nsp) ──────────────────────────────────
    cursor = await db.execute("""
        SELECT DISTINCT COALESCE(a.manual_override, a.resolved_genre) as genre
        FROM artists a
        JOIN albums al ON al.artist_id = a.id
        WHERE al.in_library = 1
          AND COALESCE(a.manual_override, a.resolved_genre) IS NOT NULL
        ORDER BY genre
    """)
    genres = [row['genre'] for row in await cursor.fetchall()]

    # ── Decade smart playlists (.nsp) ─────────────────────────────────
    cursor = await db.execute("""
        SELECT DISTINCT al.year
        FROM albums al
        WHERE al.in_library = 1 AND al.year IS NOT NULL
    """)
    years_present = set()
    for row in await cursor.fetchall():
        try:
            years_present.add(int(str(row['year'])[:4]))
        except (ValueError, TypeError):
            pass

    active_decades = {}
    for decade_name, (start, end) in DECADES.items():
        if any(start <= y <= end for y in years_present):
            active_decades[decade_name] = (start, end)

    # ── Special playlists (Live, Acoustic) - M3U ──────────────────────
    special_buckets = {}
    for playlist_name, db_column in SPECIAL_PLAYLISTS.items():
        cursor = await db.execute(f"""
            SELECT t.id as track_id, t.filename, t.title,
                   t.disc_number, t.track_number, t.duration_seconds,
                   a.name as artist_name, al.folder_name as album_folder,
                   al.year
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

    # ── Progress tracking ─────────────────────────────────────────────
    total = len(genres) + len(active_decades) + len(special_buckets)
    current = 0

    # ── Write genre .nsp files ────────────────────────────────────────
    for genre in genres:
        current += 1
        if progress_callback:
            progress_callback(current, total, genre)

        nsp = {
            "name": genre,
            "comment": f"Favorited {genre} tracks",
            "all": [
                {"is": {"genre": genre}},
                {"is": {"loved": True}}
            ],
            "sort": "artist,year,discNumber,trackNumber",
            "order": "asc"
        }

        wrote = _write_nsp(playlist_path, genre, nsp)
        if wrote:
            stats['nsp_written'] += 1
            stats['genre_playlists'] += 1
            await _log_action(db, genre, 'generate_nsp', 0,
                              {'type': 'genre', 'favorited': True})

    # ── Write decade .nsp files ───────────────────────────────────────
    for decade_name, (start, end) in sorted(active_decades.items()):
        current += 1
        if progress_callback:
            progress_callback(current, total, decade_name)

        nsp = {
            "name": decade_name,
            "comment": f"All tracks from {start}-{end}",
            "all": [
                {"inTheRange": {"year": [start, end]}}
            ],
            "sort": "artist,year,discNumber,trackNumber",
            "order": "asc"
        }

        wrote = _write_nsp(playlist_path, decade_name, nsp)
        if wrote:
            stats['nsp_written'] += 1
            stats['decade_playlists'] += 1
            await _log_action(db, decade_name, 'generate_nsp', 0,
                              {'type': 'decade', 'range': [start, end]})

    # ── Write special M3U playlists ───────────────────────────────────
    for name, tracks in sorted(special_buckets.items()):
        current += 1
        if progress_callback:
            progress_callback(current, total, name)

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
        "Playlist generation complete: %d genre nsp, %d decade nsp, "
        "%d special m3u, %d added, %d excluded",
        stats['genre_playlists'], stats['decade_playlists'],
        stats['special_playlists'],
        stats['tracks_added'], stats['tracks_excluded']
    )
    return stats


async def _cleanup_migrated_playlists(db, playlist_path: str):
    """Remove old genre/decade M3U files and their DB entries.
    These are now handled as .nsp smart playlists."""
    cursor = await db.execute("SELECT DISTINCT playlist_name FROM playlist_tracks")
    cleaned = 0
    for row in await cursor.fetchall():
        name = row['playlist_name']
        if name in ('Live', 'Acoustic'):
            continue
        # Everything else is old genre or decade — clean up
        await db.execute("DELETE FROM playlist_tracks WHERE playlist_name = ?", (name,))
        await db.execute("DELETE FROM playlist_exclusions WHERE playlist_name = ?", (name,))
        m3u_path = os.path.join(playlist_path, f"{name}.m3u")
        if os.path.exists(m3u_path):
            os.remove(m3u_path)
            logger.info("Removed migrated M3U: %s", m3u_path)
        cleaned += 1

    if cleaned:
        await db.commit()
        logger.info("Cleaned up %d old M3U playlists migrated to NSP", cleaned)


def _write_nsp(playlist_path: str, name: str, nsp_data: dict) -> bool:
    """Write a Navidrome smart playlist (.nsp) file."""
    try:
        filepath = os.path.join(playlist_path, f"{name}.nsp")
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(nsp_data, f, indent=2)
        logger.debug("Wrote NSP: %s", filepath)
        return True
    except Exception as e:
        logger.error("Failed to write NSP for '%s': %s", name, e)
        return False


async def _update_playlist(db, playlist_name: str,
                           qualifying_tracks: list) -> dict:
    """Incrementally update an M3U-backed playlist with exclusion support."""
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


async def _write_m3u(db, playlist_name: str, music_path: str,
                     playlist_path: str) -> bool:
    """Write an M3U playlist file for special playlists."""
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


async def _log_action(db, playlist_name: str, action: str,
                      track_count: int, details: dict = None):
    await db.execute(
        """INSERT INTO playlist_log
           (playlist_name, action, track_count, details, status, created_at)
           VALUES (?, ?, ?, ?, 'ok', datetime('now'))""",
        (playlist_name, action, track_count,
         json.dumps(details) if details else None)
    )
