"""
LinerNotes Playlist Routes
"""
import asyncio
import logging
import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from app.database import get_db, get_setting

logger = logging.getLogger("linernotes.playlists")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# Track generator state
_gen_state = {
    "running": False,
    "progress": 0,
    "total": 0,
    "current_playlist": "",
    "last_result": None,
}


@router.get("/playlists", response_class=HTMLResponse)
async def playlists_page(request: Request):
    """Playlist management page."""
    db = await get_db()
    try:
        # Get playlist summary
        cursor = await db.execute("""
            SELECT playlist_name, COUNT(*) as track_count
            FROM playlist_tracks
            GROUP BY playlist_name
            ORDER BY playlist_name
        """)
        playlists = await cursor.fetchall()

        # Get exclusion counts
        cursor = await db.execute("""
            SELECT playlist_name, COUNT(*) as count
            FROM playlist_exclusions
            GROUP BY playlist_name
        """)
        exclusions = {row["playlist_name"]: row["count"] for row in await cursor.fetchall()}

        # Get recent log entries
        cursor = await db.execute("""
            SELECT * FROM playlist_log
            ORDER BY created_at DESC
            LIMIT 50
        """)
        logs = await cursor.fetchall()

        # Stats
        cursor = await db.execute("SELECT COUNT(*) as count FROM playlist_tracks")
        total_assignments = (await cursor.fetchone())["count"]

        cursor = await db.execute(
            "SELECT COUNT(DISTINCT playlist_name) as count FROM playlist_tracks"
        )
        total_playlists = (await cursor.fetchone())["count"]

        return templates.TemplateResponse("playlists.html", {
            "request": request,
            "playlists": playlists,
            "exclusions": exclusions,
            "logs": logs,
            "total_playlists": total_playlists,
            "total_assignments": total_assignments,
            "gen_state": _gen_state,
        })
    finally:
        await db.close()


@router.post("/playlists/generate")
async def trigger_playlist_generation():
    """Manually trigger playlist generation."""
    if _gen_state["running"]:
        return JSONResponse(
            {"status": "already_running", "message": "Generator is already running"},
            status_code=409
        )

    asyncio.create_task(_run_generator())
    return JSONResponse({"status": "started", "message": "Playlist generation started"})


@router.get("/playlists/status")
async def generator_status():
    """Get current generator status."""
    return JSONResponse({
        "running": _gen_state["running"],
        "progress": _gen_state["progress"],
        "total": _gen_state["total"],
        "current_playlist": _gen_state["current_playlist"],
        "last_result": _gen_state["last_result"],
    })


@router.post("/playlists/{playlist_name}/exclude/{track_id}")
async def exclude_track(playlist_name: str, track_id: int):
    """Exclude a track from a playlist."""
    db = await get_db()
    try:
        await db.execute(
            """INSERT OR IGNORE INTO playlist_exclusions
               (playlist_name, track_id, excluded_at)
               VALUES (?, ?, datetime('now'))""",
            (playlist_name, track_id)
        )
        # Remove from playlist
        await db.execute(
            "DELETE FROM playlist_tracks WHERE playlist_name = ? AND track_id = ?",
            (playlist_name, track_id)
        )
        await db.commit()
        return JSONResponse({"status": "ok"})
    finally:
        await db.close()


@router.post("/playlists/{playlist_name}/include/{track_id}")
async def include_track(playlist_name: str, track_id: int):
    """Remove a track exclusion."""
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM playlist_exclusions WHERE playlist_name = ? AND track_id = ?",
            (playlist_name, track_id)
        )
        await db.commit()
        return JSONResponse({"status": "ok"})
    finally:
        await db.close()


@router.get("/playlists/{playlist_name}", response_class=HTMLResponse)
async def playlist_detail(request: Request, playlist_name: str):
    """View tracks in a specific playlist."""
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT t.id as track_id, t.title, t.filename, t.duration_seconds,
                   a.name as artist_name, al.folder_name as album_folder, al.year
            FROM playlist_tracks pt
            JOIN tracks t ON pt.track_id = t.id
            JOIN albums al ON t.album_id = al.id
            JOIN artists a ON al.artist_id = a.id
            WHERE pt.playlist_name = ?
            ORDER BY a.name, al.year, t.disc_number, t.track_number
        """, (playlist_name,))
        tracks = await cursor.fetchall()

        # Get exclusions for this playlist
        cursor = await db.execute("""
            SELECT pe.track_id, t.title, t.filename,
                   a.name as artist_name, al.folder_name as album_folder
            FROM playlist_exclusions pe
            JOIN tracks t ON pe.track_id = t.id
            JOIN albums al ON t.album_id = al.id
            JOIN artists a ON al.artist_id = a.id
            WHERE pe.playlist_name = ?
            ORDER BY a.name, t.title
        """, (playlist_name,))
        exclusions = await cursor.fetchall()

        return templates.TemplateResponse("playlist_detail.html", {
            "request": request,
            "playlist_name": playlist_name,
            "tracks": tracks,
            "exclusions": exclusions,
        })
    finally:
        await db.close()


async def _run_generator():
    """Execute the playlist generator with progress tracking."""
    from app.services.playlist_generator import generate_playlists

    _gen_state["running"] = True
    _gen_state["progress"] = 0
    _gen_state["total"] = 0
    _gen_state["current_playlist"] = ""

    def progress_cb(current, total, playlist_name):
        _gen_state["progress"] = current
        _gen_state["total"] = total
        _gen_state["current_playlist"] = playlist_name

    try:
        music_path = await get_setting("library_path", "/music")
        playlist_path = await get_setting("playlist_path", "/playlists")

        db = await get_db()
        try:
            stats = await generate_playlists(
                db, music_path, playlist_path,
                progress_callback=progress_cb,
            )
            _gen_state["last_result"] = stats
            logger.info("Playlist generation complete: %s", stats)
        finally:
            await db.close()
    except Exception as e:
        logger.error("Playlist generation failed: %s", e)
        _gen_state["last_result"] = {"error": str(e)}
    finally:
        _gen_state["running"] = False
