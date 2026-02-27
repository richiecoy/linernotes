"""
LinerNotes Metadata Enforcer Routes
"""
import asyncio
import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from app.database import get_db, get_setting

logger = logging.getLogger("linernotes.enforcer")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# Track enforcer state
_enforcer_state = {
    "running": False,
    "progress": 0,
    "total": 0,
    "current_artist": "",
    "last_result": None,
}


@router.get("/enforcer", response_class=HTMLResponse)
async def enforcer_page(request: Request):
    """Enforcer status and log page."""
    db = await get_db()
    try:
        dry_run = await get_setting("enforcer_dry_run", "true")

        # Get recent enforcer log entries
        cursor = await db.execute("""
            SELECT * FROM enforcer_log
            ORDER BY created_at DESC
            LIMIT 100
        """)
        logs = await cursor.fetchall()

        # Get pending update counts
        cursor = await db.execute("""
            SELECT COUNT(*) as count FROM tracks WHERE needs_update = 1
        """)
        pending = (await cursor.fetchone())["count"]

        # Get total tracks checked
        cursor = await db.execute("""
            SELECT COUNT(*) as count FROM tracks WHERE last_checked IS NOT NULL
        """)
        checked = (await cursor.fetchone())["count"]

        # Get total correct
        cursor = await db.execute("""
            SELECT COUNT(*) as count FROM tracks
            WHERE last_checked IS NOT NULL AND needs_update = 0
        """)
        correct = (await cursor.fetchone())["count"]

        return templates.TemplateResponse("enforcer.html", {
            "request": request,
            "logs": logs,
            "pending_updates": pending,
            "tracks_checked": checked,
            "tracks_correct": correct,
            "dry_run": dry_run == "true",
            "enforcer_state": _enforcer_state,
        })
    finally:
        await db.close()


@router.post("/enforcer/run")
async def trigger_enforcer(apply: bool = False):
    """Manually trigger the metadata enforcer. Pass apply=true to write changes."""
    if _enforcer_state["running"]:
        return JSONResponse(
            {"status": "already_running", "message": "Enforcer is already running"},
            status_code=409
        )

    asyncio.create_task(_run_enforcer(dry_run=not apply))
    mode = "APPLY" if apply else "DRY RUN"
    return JSONResponse({"status": "started", "message": f"Enforcer started ({mode})"})


@router.get("/enforcer/status")
async def enforcer_status():
    """Get current enforcer status."""
    return JSONResponse({
        "running": _enforcer_state["running"],
        "progress": _enforcer_state["progress"],
        "total": _enforcer_state["total"],
        "current_artist": _enforcer_state["current_artist"],
        "last_result": _enforcer_state["last_result"],
    })


async def _run_enforcer(dry_run: bool = True):
    """Execute the enforcer with progress tracking."""
    from app.services.enforcer import run_enforcer

    _enforcer_state["running"] = True
    _enforcer_state["progress"] = 0
    _enforcer_state["total"] = 0
    _enforcer_state["current_artist"] = ""

    def progress_cb(current, total, artist_name):
        _enforcer_state["progress"] = current
        _enforcer_state["total"] = total
        _enforcer_state["current_artist"] = artist_name

    try:
        # If not explicitly overriding, check the setting
        if dry_run:
            setting = await get_setting("enforcer_dry_run", "true")
            dry_run = setting == "true"

        music_path = await get_setting("library_path", "/music")
        db = await get_db()
        try:
            # Clear old log entries before new run
            await db.execute("DELETE FROM enforcer_log")
            await db.commit()

            stats = await run_enforcer(db, music_path, dry_run=dry_run,
                                       progress_callback=progress_cb)
            _enforcer_state["last_result"] = stats
            logger.info("Enforcer complete: %s", stats)
        finally:
            await db.close()
    except Exception as e:
        logger.error("Enforcer failed: %s", e)
        _enforcer_state["last_result"] = {"error": str(e)}
    finally:
        _enforcer_state["running"] = False
