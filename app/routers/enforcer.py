"""
LinerNotes Metadata Enforcer Routes
"""
import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from app.database import get_db, get_setting

logger = logging.getLogger("linernotes.enforcer")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


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

        return templates.TemplateResponse("enforcer.html", {
            "request": request,
            "logs": logs,
            "pending_updates": pending,
            "tracks_checked": checked,
            "dry_run": dry_run == "true",
        })
    finally:
        await db.close()


@router.post("/enforcer/run")
async def trigger_enforcer():
    """Manually trigger the metadata enforcer."""
    logger.info("Manual enforcer trigger requested")
    # Will be wired to actual enforcer in phase 6
    return JSONResponse({"status": "started", "message": "Enforcer job triggered"})
