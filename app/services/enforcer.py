"""
LinerNotes Metadata Enforcer
Reads actual genre tags from audio files and compares against
resolved artist genres. Optionally writes corrections via mutagen.
Never trusts the DB for current tag state — always reads from disk.
"""
import os
import json
import logging
from datetime import datetime, timezone
from mutagen import File as MutagenFile

logger = logging.getLogger("linernotes.enforcer")


def read_genre_tag(filepath: str) -> str | None:
    """Read the genre tag from an audio file."""
    try:
        audio = MutagenFile(filepath, easy=True)
        if audio is None:
            return None
        genre = audio.get('genre')
        if genre:
            return genre[0] if isinstance(genre, list) else str(genre)
        return None
    except Exception as e:
        logger.warning("Failed to read genre from %s: %s", filepath, e)
        return None


def write_genre_tag(filepath: str, genre: str) -> bool:
    """Write a genre tag to an audio file. Returns True on success."""
    try:
        audio = MutagenFile(filepath, easy=True)
        if audio is None:
            return False
        audio['genre'] = genre
        audio.save()
        return True
    except Exception as e:
        logger.error("Failed to write genre to %s: %s", filepath, e)
        return False


async def run_enforcer(db, music_path: str, dry_run: bool = True,
                       progress_callback=None) -> dict:
    """
    Scan all tracks, compare genre tags to resolved artist genres.
    If dry_run is False, write corrections to files.

    Returns stats dict.
    """
    stats = {
        'tracks_checked': 0,
        'tracks_correct': 0,
        'tracks_mismatched': 0,
        'tracks_updated': 0,
        'tracks_failed': 0,
        'tracks_no_genre': 0,
        'dry_run': dry_run,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'finished_at': None,
    }

    # Get all artists with their effective genre
    cursor = await db.execute("""
        SELECT id, name,
               COALESCE(manual_override, resolved_genre) as effective_genre
        FROM artists
        WHERE COALESCE(manual_override, resolved_genre) IS NOT NULL
    """)
    artists = await cursor.fetchall()

    total_artists = len(artists)
    logger.info("Enforcer starting: %d artists with resolved genres (dry_run=%s)",
                total_artists, dry_run)

    for artist_idx, artist in enumerate(artists):
        artist_id = artist['id']
        artist_name = artist['name']
        effective_genre = artist['effective_genre']

        if progress_callback:
            progress_callback(artist_idx + 1, total_artists, artist_name)

        # Get all library albums for this artist
        cursor = await db.execute("""
            SELECT a.id, a.folder_name, a.is_live, a.is_acoustic
            FROM albums a
            WHERE a.artist_id = ? AND a.in_library = 1
        """, (artist_id,))
        albums = await cursor.fetchall()

        for album in albums:
            album_id = album['id']
            folder_name = album['folder_name']

            # Build expected genre tag
            expected_genre = effective_genre

            # Get tracks for this album
            cursor = await db.execute("""
                SELECT id, filename
                FROM tracks WHERE album_id = ?
            """, (album_id,))
            tracks = await cursor.fetchall()

            for track in tracks:
                stats['tracks_checked'] += 1
                track_id = track['id']
                filename = track['filename']

                filepath = os.path.join(music_path, artist_name, folder_name, filename)

                # Read actual genre tag from the file, not the DB
                if os.path.isfile(filepath):
                    current_tag = read_genre_tag(filepath)
                else:
                    stats['tracks_failed'] += 1
                    logger.warning("File not found: %s", filepath)
                    await db.execute(
                        """INSERT INTO enforcer_log
                           (track_id, artist_name, album_name, track_filename,
                            old_value, new_value, status, error_message, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, 'failed', 'File not found', datetime('now'))""",
                        (track_id, artist_name, folder_name, filename,
                         None, expected_genre)
                    )
                    continue

                # Compare actual file tag to expected
                if current_tag == expected_genre:
                    stats['tracks_correct'] += 1
                    await db.execute(
                        """UPDATE tracks SET
                           current_genre_tag = ?, expected_genre_tag = ?, needs_update = 0,
                           last_checked = datetime('now'), updated_at = datetime('now')
                           WHERE id = ?""",
                        (current_tag, expected_genre, track_id)
                    )
                    continue

                stats['tracks_mismatched'] += 1

                if not effective_genre:
                    stats['tracks_no_genre'] += 1
                    continue

                if dry_run:
                    # Mark as needing update, sync current_genre_tag to actual file
                    await db.execute(
                        """UPDATE tracks SET
                           current_genre_tag = ?, expected_genre_tag = ?, needs_update = 1,
                           last_checked = datetime('now'), updated_at = datetime('now')
                           WHERE id = ?""",
                        (current_tag, expected_genre, track_id)
                    )
                    # Log the planned change
                    await db.execute(
                        """INSERT INTO enforcer_log
                           (track_id, artist_name, album_name, track_filename,
                            old_value, new_value, status, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, 'pending', datetime('now'))""",
                        (track_id, artist_name, folder_name, filename,
                         current_tag, expected_genre)
                    )
                else:
                    # Actually write the tag (file existence already confirmed)
                    success = write_genre_tag(filepath, expected_genre)
                    if success:
                        stats['tracks_updated'] += 1
                        await db.execute(
                            """UPDATE tracks SET
                               current_genre_tag = ?, expected_genre_tag = ?,
                               needs_update = 0, last_checked = datetime('now'),
                               updated_at = datetime('now')
                               WHERE id = ?""",
                            (expected_genre, expected_genre, track_id)
                        )
                        await db.execute(
                            """INSERT INTO enforcer_log
                               (track_id, artist_name, album_name, track_filename,
                                old_value, new_value, status, created_at)
                               VALUES (?, ?, ?, ?, ?, ?, 'applied', datetime('now'))""",
                            (track_id, artist_name, folder_name, filename,
                             current_tag, expected_genre)
                        )
                    else:
                        stats['tracks_failed'] += 1
                        await db.execute(
                            """INSERT INTO enforcer_log
                               (track_id, artist_name, album_name, track_filename,
                                old_value, new_value, status, error_message, created_at)
                               VALUES (?, ?, ?, ?, ?, ?, 'failed', 'Write failed', datetime('now'))""",
                            (track_id, artist_name, folder_name, filename,
                             current_tag, expected_genre)
                        )

        await db.commit()

    stats['finished_at'] = datetime.now(timezone.utc).isoformat()

    logger.info(
        "Enforcer complete: %d checked, %d correct, %d mismatched, %d updated, %d failed (dry_run=%s)",
        stats['tracks_checked'], stats['tracks_correct'], stats['tracks_mismatched'],
        stats['tracks_updated'], stats['tracks_failed'], dry_run
    )

    return stats
