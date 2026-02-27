"""
LinerNotes Library Scanner Routes
"""
import asyncio
import logging
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from app.database import get_db, get_setting

logger = logging.getLogger("linernotes.scanner_route")
router = APIRouter()

# Track scan state
_scan_state = {
    "running": False,
    "progress": 0,
    "total": 0,
    "current_artist": "",
    "last_result": None,
}


@router.post("/scan/run")
async def trigger_scan():
    """Manually trigger a library scan."""
    if _scan_state["running"]:
        return JSONResponse(
            {"status": "already_running", "message": "A scan is already in progress"},
            status_code=409
        )

    # Run in background
    asyncio.create_task(_run_scan())
    return JSONResponse({"status": "started", "message": "Library scan started"})


@router.get("/scan/status")
async def scan_status():
    """Get current scan status."""
    return JSONResponse({
        "running": _scan_state["running"],
        "progress": _scan_state["progress"],
        "total": _scan_state["total"],
        "current_artist": _scan_state["current_artist"],
        "last_result": _scan_state["last_result"],
    })


async def _run_scan():
    """Execute the library scan with progress tracking."""
    from app.services.library_scanner import scan_library

    _scan_state["running"] = True
    _scan_state["progress"] = 0
    _scan_state["total"] = 0
    _scan_state["current_artist"] = ""

    def progress_cb(current, total, artist_name):
        _scan_state["progress"] = current
        _scan_state["total"] = total
        _scan_state["current_artist"] = artist_name

    try:
        music_path = await get_setting("library_path", "/music")
        db = await get_db()
        try:
            stats = await scan_library(db, music_path, progress_callback=progress_cb)
            _scan_state["last_result"] = stats
            logger.info("Manual scan complete: %s", stats)
        finally:
            await db.close()
    except Exception as e:
        logger.error("Manual scan failed: %s", e)
        _scan_state["last_result"] = {"error": str(e)}
    finally:
        _scan_state["running"] = False
