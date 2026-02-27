"""
LinerNotes Playlist Generator

All playlists are M3U files written to the playlist path.
Navidrome auto-imports on library scan.

Playlist types:
  - Genre: one per resolved genre
  - Decade: one per decade, excludes live/acoustic albums
  - Live: all tracks from live albums
  - Acoustic: all tracks from acoustic albums

Exclusions managed in LinerNotes UI. Excluded tracks are
omitted from playlist_tracks and M3U output.
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
                              navidrome_url: str = None,
                              navidrome_user: str = None,
                              navidrome_pass: str = None,
                              progress_callback=None) -> dict:
    stats = {
        'genre_playlists': 0,
        'special_playlists': 0,
        'decade_playlists': 0,
        'tracks_added': 0,
        'tracks_excluded': 0,
        'tracks_already_in': 0,
        'm3u_written': 0,
        'errors': 0,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'finished_at': None,
    }

    # ── Genre tracks ──────────────────────────────────────────────────
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
        genre_buckets.setdefault(track['genre'], []).append(track)

    # ── Special playlists (Live, Acoustic) ────────────────────────────
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

    # ── Decade tracks (exclude live + acoustic) ───────────────────────
    cursor = await db.execute("""
        SELECT t.id as track_id, t.filename, t.title,
               t.disc_number, t.track_number, t.duration_seconds,
               a.name as artist_name, al.folder_name as album_folder,
               al.year
        FROM tracks t
        JOIN albums al ON t.album_id = al.id
        JOIN artists a ON al.artist_id = a.id
        WHERE al.in_library = 1
          AND al.is_live = 0
          AND al.is_acoustic = 0
        ORDER BY a.name, al.year, t.disc_number, t.track_number
    """)
    decade_buckets = {}
    for track in await cursor.fetchall():
        year_str = track['year']
        if not year_str:
            continue
        try:
            year = int(str(year_str)[:4])
            for decade_name, (start, end) in DECADES.items():
                if start <= year <= end:
                    decade_buckets.setdefault(decade_name, []).append(track)
                    break
        except (ValueError, TypeError):
            pass

    # ── Process all playlists ─────────────────────────────────────────
    all_buckets = []
    for name, tracks in sorted(genre_buckets.items()):
        all_buckets.append(('genre', name, tracks))
    for name, tracks in sorted(special_buckets.items()):
        all_buckets.append(('special', name, tracks))
    for name, tracks in sorted(decade_buckets.items()):
        all_buckets.append(('decade', name, tracks))

    total = len(all_buckets)
    for i, (ptype, name, tracks) in enumerate(all_buckets):
        if progress_callback:
            progress_callback(i + 1, total, name)

        result = await _update_playlist(db, name, tracks)
        stats[f'{ptype}_playlists'] += 1
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
        "Playlist generation complete: %d genre, %d special, %d decade, "
        "%d m3u, %d added, %d excluded",
        stats['genre_playlists'], stats['special_playlists'],
        stats['decade_playlists'], stats['m3u_written'],
        stats['tracks_added'], stats['tracks_excluded']
    )
    return stats


async def _update_playlist(db, playlist_name: str,
                           qualifying_tracks: list) -> dict:
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
