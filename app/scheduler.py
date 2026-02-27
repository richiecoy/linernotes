"""
LinerNotes Scheduler
"""
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger("linernotes.scheduler")

scheduler = AsyncIOScheduler()


async def init_scheduler():
    """Initialize and start the scheduler with jobs from settings."""
    from app.database import get_setting

    # Get intervals from settings
    scan_hours = int(await get_setting("scan_schedule_hours", "24"))
    enforcer_hours = int(await get_setting("enforcer_schedule_hours", "24"))
    playlist_hours = int(await get_setting("playlist_schedule_hours", "24"))

    # Library scan job
    scheduler.add_job(
        run_library_scan,
        trigger=IntervalTrigger(hours=scan_hours),
        id="library_scan",
        name="Library Scan",
        replace_existing=True,
    )

    # Metadata enforcer job
    scheduler.add_job(
        run_metadata_enforcer,
        trigger=IntervalTrigger(hours=enforcer_hours),
        id="metadata_enforcer",
        name="Metadata Enforcer",
        replace_existing=True,
    )

    # Playlist generator job
    scheduler.add_job(
        run_playlist_generator,
        trigger=IntervalTrigger(hours=playlist_hours),
        id="playlist_generator",
        name="Playlist Generator",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started with scan=%dh, enforcer=%dh, playlists=%dh",
                scan_hours, enforcer_hours, playlist_hours)


async def run_library_scan():
    """Scheduled library scan job."""
    logger.info("Scheduled library scan starting...")
    # Will be wired to library_scanner service in phase 2
    logger.info("Library scan placeholder complete")


async def run_metadata_enforcer():
    """Scheduled metadata enforcer job."""
    logger.info("Scheduled metadata enforcer starting...")
    # Will be wired to metadata_enforcer service in phase 6
    logger.info("Metadata enforcer placeholder complete")


async def run_playlist_generator():
    """Scheduled playlist generator job."""
    logger.info("Scheduled playlist generator starting...")
    # Will be wired to playlist_generator service in phase 7
    logger.info("Playlist generator placeholder complete")


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
