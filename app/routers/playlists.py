"""
LinerNotes Playlist Routes
"""
import asyncio
import json
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

DECADE_NAMES = ['1960s','1970s','1980s','1990s','2000s','2010s','2020s']


def _scan_nsp_files(playlist_path: str) -> list:
    """Scan playlist directory for .nsp files and parse them."""
    nsp_playlists = []
    if not playlist_path or not os.path.isdir(playlist_path):
        return nsp_playlists

    for fname in sorted(os.listdir(playlist_path)):
        if not fname.endswith('.nsp'):
            continue
        filepath = os.path.join(playlist_path, fname)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            nsp_playlists.append({
                'name': data.get('name', fname.replace('.nsp', '')),
                'comment': data.get('comment', ''),
                'filename': fname,
                'rules': data,
            })
        except Exception as e:
            logger.warning("Failed to read NSP %s: %s", fname, e)

    return nsp_playlists


@router.get("/playlists", response_class=HTMLResponse)
async def playlists_page(request: Request):
    """Playlist management page."""
    db = await get_db()
    try:
        playlist_path = await get_setting("playlist_path", "/playlists")

        # M3U playlists (Live, Acoustic) — from DB
        cursor = await db.execute("""
            SELECT playlist_name, COUNT(*) as track_count
            FROM playlist_tracks
            GROUP BY playlist_name
            ORDER BY playlist_name
        """)
        m3u_playlists = await cursor.fetchall()

        # Exclusion counts for M3U playlists
        cursor = await db.execute("""
            SELECT playlist_name, COUNT(*) as count
            FROM playlist_exclusions
            GROUP BY playlist_name
        """)
        exclusions = {row["playlist_name"]: row["count"] for row in await cursor.fetchall()}

        # NSP playlists (genre, decade) — from filesystem
        nsp_playlists = _scan_nsp_files(playlist_path)

        # Split NSP into genre vs decade
        genre_nsp = []
        decade_nsp = []
        for nsp in nsp_playlists:
            if nsp['name'] in DECADE_NAMES:
                decade_nsp.append(nsp)
            else:
                genre_nsp.append(nsp)

        # Recent log entries
        cursor = await db.execute("""
            SELECT * FROM playlist_log
            ORDER BY created_at DESC
            LIMIT 50
        """)
        logs = await cursor.fetchall()

        # Stats
        total_nsp = len(nsp_playlists)
        total_m3u = len(m3u_playlists)

        return templates.TemplateResponse("playlists.html", {
            "request": request,
            "genre_nsp": genre_nsp,
            "decade_nsp": decade_nsp,
            "m3u_playlists": m3u_playlists,
            "exclusions": exclusions,
            "logs": logs,
            "total_nsp": total_nsp,
            "total_m3u": total_m3u,
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
    """Exclude a track from an M3U playlist."""
    db = await get_db()
    try:
        await db.execute(
            """INSERT OR IGNORE INTO playlist_exclusions
               (playlist_name, track_id, excluded_at)
               VALUES (?, ?, datetime('now'))""",
            (playlist_name, track_id)
        )
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
    """Remove a track exclusion from an M3U playlist."""
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
    """View tracks in a specific M3U playlist."""
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
