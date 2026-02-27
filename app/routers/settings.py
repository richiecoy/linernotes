"""
LinerNotes Settings Routes
"""
import logging
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.database import get_all_settings, set_setting
from app.scheduler import get_job_status

logger = logging.getLogger("linernotes.settings")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, saved: bool = False):
    """Settings page."""
    settings = await get_all_settings()
    jobs = get_job_status()

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "settings": settings,
        "jobs": jobs,
        "saved": saved,
    })


@router.post("/settings")
async def save_settings(
    request: Request,
    library_path: str = Form(""),
    scan_schedule_hours: str = Form("24"),
    enforcer_schedule_hours: str = Form("24"),
    playlist_schedule_hours: str = Form("24"),
    enforcer_dry_run: str = Form("false"),
    mb_cache_days: str = Form("30"),
    navidrome_url: str = Form(""),
    navidrome_username: str = Form(""),
    navidrome_password: str = Form(""),
):
    """Save settings."""
    fields = {
        "library_path": library_path,
        "scan_schedule_hours": scan_schedule_hours,
        "enforcer_schedule_hours": enforcer_schedule_hours,
        "playlist_schedule_hours": playlist_schedule_hours,
        "enforcer_dry_run": enforcer_dry_run,
        "mb_cache_days": mb_cache_days,
        "navidrome_url": navidrome_url,
        "navidrome_username": navidrome_username,
        "navidrome_password": navidrome_password,
    }

    for key, value in fields.items():
        await set_setting(key, value.strip())

    logger.info("Settings updated")

    # Reschedule jobs with new intervals
    from app.scheduler import scheduler, run_library_scan, run_metadata_enforcer, run_playlist_generator
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler.reschedule_job("library_scan",
        trigger=IntervalTrigger(hours=int(scan_schedule_hours or 24)))
    scheduler.reschedule_job("metadata_enforcer",
        trigger=IntervalTrigger(hours=int(enforcer_schedule_hours or 24)))
    scheduler.reschedule_job("playlist_generator",
        trigger=IntervalTrigger(hours=int(playlist_schedule_hours or 24)))

    return RedirectResponse(url="/settings?saved=true", status_code=303)
