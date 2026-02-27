"""
LinerNotes Artist Routes
"""
import json
import os
import logging
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from app.database import get_db, get_setting

logger = logging.getLogger("linernotes.artists")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/artist-image/{artist_name}")
async def artist_image(artist_name: str):
    """Serve artist folder.jpg from the music library."""
    library_path = await get_setting("library_path", "/music")
    artist_path = os.path.join(library_path, artist_name)

    for img_name in ('folder.jpg', 'folder.png', 'artist.jpg', 'artist.png'):
        img_path = os.path.join(artist_path, img_name)
        if os.path.isfile(img_path):
            return FileResponse(img_path)

    return HTMLResponse(status_code=404)


@router.get("/", response_class=HTMLResponse)
async def artist_index(request: Request, q: str = "", genre: str = ""):
    """Artist index page — browse and search all artists."""
    db = await get_db()
    try:
        # Build query with optional filters
        query = "SELECT * FROM artists WHERE 1=1"
        params = []

        if q:
            query += " AND name LIKE ?"
            params.append(f"%{q}%")

        if genre:
            query += " AND (resolved_genre = ? OR manual_override = ?)"
            params.extend([genre, genre])

        query += " ORDER BY sort_name ASC, name ASC"

        cursor = await db.execute(query, params)
        artists = await cursor.fetchall()

        # Get genre counts for filter sidebar
        genre_cursor = await db.execute("""
            SELECT COALESCE(manual_override, resolved_genre, 'Unresolved') as genre,
                   COUNT(*) as count
            FROM artists
            GROUP BY genre
            ORDER BY count DESC
        """)
        genre_counts = await genre_cursor.fetchall()

        # Get album counts per artist
        album_cursor = await db.execute("""
            SELECT artist_id, COUNT(*) as count
            FROM albums
            WHERE in_library = 1
            GROUP BY artist_id
        """)
        album_counts = {row["artist_id"]: row["count"] for row in await album_cursor.fetchall()}

        # Stats
        stats_cursor = await db.execute("SELECT COUNT(*) as count FROM artists")
        total_artists = (await stats_cursor.fetchone())["count"]

        stats_cursor = await db.execute("SELECT COUNT(*) as count FROM albums WHERE in_library = 1")
        total_albums = (await stats_cursor.fetchone())["count"]

        stats_cursor = await db.execute("SELECT COUNT(*) as count FROM tracks")
        total_tracks = (await stats_cursor.fetchone())["count"]

        return templates.TemplateResponse("index.html", {
            "request": request,
            "artists": artists,
            "genre_counts": genre_counts,
            "album_counts": album_counts,
            "total_artists": total_artists,
            "total_albums": total_albums,
            "total_tracks": total_tracks,
            "search_query": q,
            "selected_genre": genre,
        })
    finally:
        await db.close()


@router.get("/artist/{artist_id}", response_class=HTMLResponse)
async def artist_detail(request: Request, artist_id: int):
    """Artist detail page — genres, discography, metadata status."""
    db = await get_db()
    try:
        # Get artist
        cursor = await db.execute("SELECT * FROM artists WHERE id = ?", (artist_id,))
        artist = await cursor.fetchone()

        if not artist:
            return templates.TemplateResponse("error.html", {
                "request": request,
                "message": "Artist not found",
            }, status_code=404)

        # Get all albums (both in library and from MB)
        cursor = await db.execute("""
            SELECT * FROM albums
            WHERE artist_id = ?
            ORDER BY in_library DESC, year ASC, folder_name ASC
        """, (artist_id,))
        albums = await cursor.fetchall()

        # Get track counts and update status per album
        album_stats = {}
        for album in albums:
            if album["in_library"]:
                tc = await db.execute(
                    "SELECT COUNT(*) as total, SUM(needs_update) as pending FROM tracks WHERE album_id = ?",
                    (album["id"],)
                )
                row = await tc.fetchone()
                album_stats[album["id"]] = {
                    "track_count": row["total"],
                    "pending_updates": row["pending"] or 0,
                }

        # Parse MB genres for display
        mb_genres = []
        if artist["mb_genres_raw"]:
            try:
                mb_genres = json.loads(artist["mb_genres_raw"])
            except json.JSONDecodeError:
                pass

        genre_weights = {}
        if artist["genre_weights"]:
            try:
                genre_weights = json.loads(artist["genre_weights"])
            except json.JSONDecodeError:
                pass

        # Parse secondary types for each album (stored as JSON)
        albums_processed = []
        for album in albums:
            album_dict = dict(album)
            try:
                st = album_dict.get('secondary_types', '[]')
                album_dict['secondary_types_list'] = json.loads(st) if st else []
            except (json.JSONDecodeError, TypeError):
                album_dict['secondary_types_list'] = []
            albums_processed.append(album_dict)

        return templates.TemplateResponse("artist.html", {
            "request": request,
            "artist": artist,
            "albums": albums_processed,
            "album_stats": album_stats,
            "mb_genres": mb_genres,
            "genre_weights": genre_weights,
        })
    finally:
        await db.close()


@router.post("/artist/{artist_id}/override")
async def set_genre_override(artist_id: int, genre: str = Form(...)):
    """Set or clear a manual genre override for an artist."""
    db = await get_db()
    try:
        override = genre.strip() if genre.strip() else None
        await db.execute(
            "UPDATE artists SET manual_override = ?, updated_at = datetime('now') WHERE id = ?",
            (override, artist_id)
        )
        await db.commit()
        logger.info("Genre override set for artist %d: %s", artist_id, override)
    finally:
        await db.close()

    return RedirectResponse(url=f"/artist/{artist_id}", status_code=303)
