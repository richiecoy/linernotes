"""
LinerNotes Scheduler
"""
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger("linernotes.scheduler")

scheduler = AsyncIOScheduler()


async def init_scheduler():
    """Initialize and start the scheduler with jobs from settings."""
    from app.database import get_setting

    # Get schedule times (HH:MM format)
    scan_time = await get_setting("scan_schedule_time", "02:00")
    enforcer_time = await get_setting("enforcer_schedule_time", "03:00")
    playlist_time = await get_setting("playlist_schedule_time", "04:00")

    scan_h, scan_m = _parse_time(scan_time)
    enforcer_h, enforcer_m = _parse_time(enforcer_time)
    playlist_h, playlist_m = _parse_time(playlist_time)

    # Library scan job
    scheduler.add_job(
        run_library_scan,
        trigger=CronTrigger(hour=scan_h, minute=scan_m),
        id="library_scan",
        name="Library Scan",
        replace_existing=True,
    )

    # Metadata enforcer job
    scheduler.add_job(
        run_metadata_enforcer,
        trigger=CronTrigger(hour=enforcer_h, minute=enforcer_m),
        id="metadata_enforcer",
        name="Metadata Enforcer",
        replace_existing=True,
    )

    # Playlist generator job
    scheduler.add_job(
        run_playlist_generator,
        trigger=CronTrigger(hour=playlist_h, minute=playlist_m),
        id="playlist_generator",
        name="Playlist Generator",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started: scan=%s, enforcer=%s, playlists=%s",
                scan_time, enforcer_time, playlist_time)


def _parse_time(time_str: str) -> tuple:
    """Parse HH:MM string into (hour, minute) ints."""
    try:
        parts = time_str.strip().split(":")
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return 2, 0  # default 2:00 AM


async def run_library_scan():
    """Scheduled library scan job."""
    logger.info("Scheduled library scan starting...")
    from app.database import get_db, get_setting
    from app.services.library_scanner import scan_library

    music_path = await get_setting("library_path", "/music")
    db = await get_db()
    try:
        stats = await scan_library(db, music_path)
        logger.info("Scheduled scan complete: %s", stats)
    except Exception as e:
        logger.error("Scheduled scan failed: %s", e)
    finally:
        await db.close()


async def run_metadata_enforcer():
    """Scheduled metadata enforcer job."""
    logger.info("Scheduled metadata enforcer starting...")
    from app.database import get_db, get_setting
    from app.services.enforcer import run_enforcer

    music_path = await get_setting("library_path", "/music")
    dry_run = (await get_setting("enforcer_dry_run", "true")) == "true"
    db = await get_db()
    try:
        stats = await run_enforcer(db, music_path, dry_run=dry_run)
        logger.info("Scheduled enforcer complete: %s", stats)
    except Exception as e:
        logger.error("Scheduled enforcer failed: %s", e)
    finally:
        await db.close()


async def run_playlist_generator():
    """Scheduled playlist generator job."""
    logger.info("Scheduled playlist generator starting...")
    from app.database import get_db, get_setting
    from app.services.playlist_generator import generate_playlists

    music_path = await get_setting("library_path", "/music")
    playlist_path = await get_setting("playlist_path", "/playlists")
    nd_url = await get_setting("navidrome_url", "")
    nd_user = await get_setting("navidrome_username", "")
    nd_pass = await get_setting("navidrome_password", "")

    db = await get_db()
    try:
        stats = await generate_playlists(
            db, music_path, playlist_path,
            navidrome_url=nd_url if nd_url else None,
            navidrome_user=nd_user if nd_user else None,
            navidrome_pass=nd_pass if nd_pass else None,
        )
        logger.info("Scheduled playlist generation complete: %s", stats)
    except Exception as e:
        logger.error("Scheduled playlist generation failed: %s", e)
    finally:
        await db.close()


def get_job_status():
    """Get status of all scheduled jobs."""
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": next_run.isoformat() if next_run else "Not scheduled",
        })
    return jobs
