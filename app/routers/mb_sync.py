"""
LinerNotes MusicBrainz Sync Routes
"""
import asyncio
import logging
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.database import get_db, get_setting

logger = logging.getLogger("linernotes.mb_sync_route")
router = APIRouter()

# Track sync state
_sync_state = {
    "running": False,
    "progress": 0,
    "total": 0,
    "current_artist": "",
    "last_result": None,
}


@router.post("/mb/sync")
async def trigger_mb_sync(force: bool = False):
    """Manually trigger a MusicBrainz sync."""
    if _sync_state["running"]:
        return JSONResponse(
            {"status": "already_running", "message": "MB sync is already in progress"},
            status_code=409
        )

    asyncio.create_task(_run_sync(force=force))
    return JSONResponse({"status": "started", "message": "MusicBrainz sync started"})


@router.get("/mb/status")
async def mb_sync_status():
    """Get current MB sync status."""
    return JSONResponse({
        "running": _sync_state["running"],
        "progress": _sync_state["progress"],
        "total": _sync_state["total"],
        "current_artist": _sync_state["current_artist"],
        "last_result": _sync_state["last_result"],
    })


async def _run_sync(force: bool = False):
    """Execute the MB sync with progress tracking."""
    from app.services.musicbrainz import sync_all_artists

    _sync_state["running"] = True
    _sync_state["progress"] = 0
    _sync_state["total"] = 0
    _sync_state["current_artist"] = ""

    def progress_cb(current, total, artist_name):
        _sync_state["progress"] = current
        _sync_state["total"] = total
        _sync_state["current_artist"] = artist_name

    try:
        cache_days = int(await get_setting("mb_cache_days", "30"))
        db = await get_db()
        try:
            stats = await sync_all_artists(
                db, progress_callback=progress_cb,
                force=force, cache_days=cache_days
            )
            _sync_state["last_result"] = stats
            logger.info("MB sync complete: %s", stats)
        finally:
            await db.close()
    except Exception as e:
        logger.error("MB sync failed: %s", e)
        _sync_state["last_result"] = {"error": str(e)}
    finally:
        _sync_state["running"] = False
