"""
LinerNotes Playlist Routes
"""
import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from app.database import get_db

logger = logging.getLogger("linernotes.playlists")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


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

        return templates.TemplateResponse("playlists.html", {
            "request": request,
            "playlists": playlists,
            "exclusions": exclusions,
            "logs": logs,
        })
    finally:
        await db.close()


@router.post("/playlists/generate")
async def trigger_playlist_generation():
    """Manually trigger playlist generation."""
    logger.info("Manual playlist generation requested")
    # Will be wired to actual generator in phase 7
    return JSONResponse({"status": "started", "message": "Playlist generation triggered"})
