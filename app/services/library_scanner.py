"""
LinerNotes Library Scanner
Walks the music directory, reads audio file metadata via mutagen,
and indexes artists/albums/tracks into the database.
"""
import os
import re
import json
import logging
import urllib.parse
from datetime import datetime, timezone
from mutagen import File as MutagenFile
from mutagen.id3 import ID3
from mutagen.flac import FLAC
from mutagen.mp4 import MP4

logger = logging.getLogger("linernotes.scanner")

AUDIO_EXTENSIONS = {'.mp3', '.flac', '.m4a', '.ogg', '.wma', '.opus', '.aac', '.wav', '.alac'}


def get_sort_name(name: str) -> str:
    """Generate a sort name (strip leading 'The ', 'A ', 'An ', case-insensitive)."""
    lower = name.lower()
    for prefix in ('the ', 'a ', 'an '):
        if lower.startswith(prefix):
            return name[len(prefix):].strip().lower()
    return name.lower()


def read_audio_metadata(filepath: str) -> dict:
    """
    Read metadata from an audio file using mutagen.
    Returns dict with title, track_number, disc_number, duration, genre, format.
    """
    meta = {
        'title': None,
        'track_number': None,
        'disc_number': None,
        'duration_seconds': None,
        'genre': None,
        'file_format': None,
    }

    ext = os.path.splitext(filepath)[1].lower()
    meta['file_format'] = ext.lstrip('.')

    try:
        audio = MutagenFile(filepath, easy=True)
        if audio is None:
            return meta

        # Duration
        if hasattr(audio, 'info') and audio.info:
            meta['duration_seconds'] = round(audio.info.length, 1) if audio.info.length else None

        # Easy tags (works for most formats with easy=True)
        if hasattr(audio, 'tags') and audio.tags:
            tags = audio.tags if isinstance(audio.tags, dict) else audio

            # Title
            title = tags.get('title')
            if title:
                meta['title'] = title[0] if isinstance(title, list) else str(title)

            # Track number
            tracknumber = tags.get('tracknumber')
            if tracknumber:
                tn = tracknumber[0] if isinstance(tracknumber, list) else str(tracknumber)
                # Handle "3/12" format
                if '/' in str(tn):
                    tn = str(tn).split('/')[0]
                try:
                    meta['track_number'] = int(tn)
                except (ValueError, TypeError):
                    pass

            # Disc number
            discnumber = tags.get('discnumber')
            if discnumber:
                dn = discnumber[0] if isinstance(discnumber, list) else str(discnumber)
                if '/' in str(dn):
                    dn = str(dn).split('/')[0]
                try:
                    meta['disc_number'] = int(dn)
                except (ValueError, TypeError):
                    pass

            # Genre
            genre = tags.get('genre')
            if genre:
                meta['genre'] = genre[0] if isinstance(genre, list) else str(genre)

    except Exception as e:
        logger.warning("Failed to read metadata from %s: %s", filepath, e)

    # Fallback title from filename
    if not meta['title']:
        basename = os.path.splitext(os.path.basename(filepath))[0]
        # Strip leading track numbers like "01 - ", "01. ", "1-"
        cleaned = re.sub(r'^\d{1,3}[\s.\-_]+', '', basename).strip()
        meta['title'] = cleaned if cleaned else basename

    return meta


async def scan_library(db, music_path: str, progress_callback=None) -> dict:
    """
    Scan the music library and index into the database.

    Directory structure expected: /music/ArtistName/AlbumName/tracks.ext

    Returns dict with scan statistics.
    """
    stats = {
        'artists_found': 0,
        'artists_new': 0,
        'albums_found': 0,
        'albums_new': 0,
        'tracks_found': 0,
        'tracks_new': 0,
        'tracks_updated': 0,
        'artists_removed': 0,
        'albums_removed': 0,
        'errors': 0,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'finished_at': None,
    }

    if not os.path.exists(music_path):
        logger.error("Music library path does not exist: %s", music_path)
        stats['errors'] = 1
        return stats

    # Get all existing artists for quick lookup
    cursor = await db.execute("SELECT id, name FROM artists")
    existing_artists = {row['name']: row['id'] for row in await cursor.fetchall()}

    # Walk artist directories
    try:
        artist_dirs = sorted([
            d for d in os.listdir(music_path)
            if os.path.isdir(os.path.join(music_path, d)) and not d.startswith('.')
        ])
    except PermissionError as e:
        logger.error("Cannot read music directory: %s", e)
        stats['errors'] = 1
        return stats

    total_artists = len(artist_dirs)
    logger.info("Found %d artist directories in %s", total_artists, music_path)

    for artist_idx, artist_name in enumerate(artist_dirs):
        artist_path = os.path.join(music_path, artist_name)
        stats['artists_found'] += 1

        if progress_callback:
            progress_callback(artist_idx + 1, total_artists, artist_name)

        # Upsert artist
        # Check for artist thumbnail
        thumb_url = None
        for img_name in ('folder.jpg', 'folder.png', 'artist.jpg', 'artist.png'):
            img_path = os.path.join(artist_path, img_name)
            if os.path.isfile(img_path):
                thumb_url = f"/artist-image/{urllib.parse.quote(artist_name)}"
                break

        if artist_name in existing_artists:
            artist_id = existing_artists[artist_name]
            # Update sort name and thumb in case they changed
            sort_name = get_sort_name(artist_name)
            await db.execute(
                """UPDATE artists SET sort_name = ?, thumb_url = ?, updated_at = datetime('now')
                   WHERE id = ?""",
                (sort_name, thumb_url, artist_id)
            )
        else:
            sort_name = get_sort_name(artist_name)
            await db.execute(
                """INSERT INTO artists (name, sort_name, thumb_url, created_at, updated_at)
                   VALUES (?, ?, ?, datetime('now'), datetime('now'))""",
                (artist_name, sort_name, thumb_url)
            )
            cursor = await db.execute("SELECT last_insert_rowid()")
            artist_id = (await cursor.fetchone())[0]
            existing_artists[artist_name] = artist_id
            stats['artists_new'] += 1
            logger.debug("New artist: %s (id=%d)", artist_name, artist_id)

        # Get existing albums for this artist
        cursor = await db.execute(
            "SELECT id, folder_name FROM albums WHERE artist_id = ? AND in_library = 1",
            (artist_id,)
        )
        existing_albums = {row['folder_name']: row['id'] for row in await cursor.fetchall()}

        # Walk album directories
        try:
            album_dirs = sorted([
                d for d in os.listdir(artist_path)
                if os.path.isdir(os.path.join(artist_path, d)) and not d.startswith('.')
            ])
        except PermissionError:
            logger.warning("Cannot read artist directory: %s", artist_path)
            stats['errors'] += 1
            continue

        for album_name in album_dirs:
            album_path = os.path.join(artist_path, album_name)

            # Check for audio files
            audio_files = []
            try:
                for f in os.listdir(album_path):
                    if os.path.splitext(f)[1].lower() in AUDIO_EXTENSIONS:
                        audio_files.append(f)
            except PermissionError:
                logger.warning("Cannot read album directory: %s", album_path)
                stats['errors'] += 1
                continue

            if not audio_files:
                continue

            stats['albums_found'] += 1

            # Upsert album
            if album_name in existing_albums:
                album_id = existing_albums[album_name]
            else:
                await db.execute(
                    """INSERT INTO albums (artist_id, folder_name, in_library, created_at, updated_at)
                       VALUES (?, ?, 1, datetime('now'), datetime('now'))""",
                    (artist_id, album_name)
                )
                cursor = await db.execute("SELECT last_insert_rowid()")
                album_id = (await cursor.fetchone())[0]
                existing_albums[album_name] = album_id
                stats['albums_new'] += 1
                logger.debug("  New album: %s (id=%d)", album_name, album_id)

            # Get existing tracks for this album
            cursor = await db.execute(
                "SELECT id, filename FROM tracks WHERE album_id = ?",
                (album_id,)
            )
            existing_tracks = {row['filename']: row['id'] for row in await cursor.fetchall()}

            # Process each audio file
            for audio_file in sorted(audio_files):
                stats['tracks_found'] += 1
                filepath = os.path.join(album_path, audio_file)

                # Read metadata
                try:
                    meta = read_audio_metadata(filepath)
                except Exception as e:
                    logger.warning("Error reading %s: %s", filepath, e)
                    stats['errors'] += 1
                    continue

                if audio_file in existing_tracks:
                    # Update existing track if genre changed
                    track_id = existing_tracks[audio_file]
                    await db.execute(
                        """UPDATE tracks SET
                           title = ?, track_number = ?, disc_number = ?,
                           duration_seconds = ?, file_format = ?,
                           current_genre_tag = ?,
                           updated_at = datetime('now')
                           WHERE id = ?""",
                        (meta['title'], meta['track_number'], meta['disc_number'],
                         meta['duration_seconds'], meta['file_format'],
                         meta['genre'], track_id)
                    )
                    stats['tracks_updated'] += 1
                else:
                    # Insert new track
                    await db.execute(
                        """INSERT INTO tracks
                           (album_id, filename, title, track_number, disc_number,
                            duration_seconds, file_format, current_genre_tag,
                            created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
                        (album_id, audio_file, meta['title'], meta['track_number'],
                         meta['disc_number'], meta['duration_seconds'],
                         meta['file_format'], meta['genre'])
                    )
                    stats['tracks_new'] += 1

        # Commit after each artist to avoid huge transactions
        await db.commit()

    stats['finished_at'] = datetime.now(timezone.utc).isoformat()

    # Cleanup: remove artists/albums no longer on disk
    cursor = await db.execute("SELECT id, name FROM artists")
    all_db_artists = await cursor.fetchall()
    artists_removed = 0
    albums_removed = 0

    for db_artist in all_db_artists:
        artist_path = os.path.join(music_path, db_artist['name'])
        if not os.path.isdir(artist_path):
            # Artist folder gone — remove artist and all their library albums/tracks
            await db.execute("DELETE FROM tracks WHERE album_id IN (SELECT id FROM albums WHERE artist_id = ?)", (db_artist['id'],))
            await db.execute("DELETE FROM albums WHERE artist_id = ?", (db_artist['id'],))
            await db.execute("DELETE FROM artists WHERE id = ?", (db_artist['id'],))
            artists_removed += 1
            logger.info("Removed stale artist: %s", db_artist['name'])
        else:
            # Check for removed albums
            cursor2 = await db.execute(
                "SELECT id, folder_name FROM albums WHERE artist_id = ? AND in_library = 1",
                (db_artist['id'],)
            )
            db_albums = await cursor2.fetchall()
            for db_album in db_albums:
                album_path = os.path.join(artist_path, db_album['folder_name'])
                if not os.path.isdir(album_path):
                    await db.execute("DELETE FROM tracks WHERE album_id = ?", (db_album['id'],))
                    await db.execute("DELETE FROM albums WHERE id = ?", (db_album['id'],))
                    albums_removed += 1
                    logger.info("Removed stale album: %s - %s", db_artist['name'], db_album['folder_name'])

    if artists_removed or albums_removed:
        await db.commit()

    stats['artists_removed'] = artists_removed
    stats['albums_removed'] = albums_removed

    logger.info(
        "Scan complete: %d artists (%d new, %d removed), %d albums (%d new, %d removed), %d tracks (%d new, %d updated), %d errors",
        stats['artists_found'], stats['artists_new'], stats['artists_removed'],
        stats['albums_found'], stats['albums_new'], stats['albums_removed'],
        stats['tracks_found'], stats['tracks_new'], stats['tracks_updated'],
        stats['errors']
    )

    return stats
