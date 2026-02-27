"""
LinerNotes — Your personal MusicBrainz for your music library
"""
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app import config
from app.database import init_db
from app.scheduler import init_scheduler
from app.routers import artists, settings, enforcer, playlists, scanner

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("linernotes")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("LinerNotes v%s starting up...", config.APP_VERSION)

    # Ensure data directory exists
    os.makedirs(config.DATA_DIR, exist_ok=True)

    # Initialize database
    await init_db()
    logger.info("Database initialized at %s", config.DB_PATH)

    # Start scheduler
    await init_scheduler()
    logger.info("Scheduler started")

    logger.info("LinerNotes ready on port %d", config.APP_PORT)

    yield

    # Shutdown
    from app.scheduler import scheduler
    scheduler.shutdown()
    logger.info("LinerNotes shutting down")


# Create app
app = FastAPI(
    title=config.APP_NAME,
    version=config.APP_VERSION,
    lifespan=lifespan,
)

# Static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Make config available to templates
templates = Jinja2Templates(directory="app/templates")
templates.env.globals["config"] = config

# Also inject config into router templates
for router_module in [artists, settings, enforcer, playlists, scanner]:
    if hasattr(router_module, 'templates'):
        router_module.templates.env.globals["config"] = config

# Register routers
app.include_router(artists.router)
app.include_router(settings.router)
app.include_router(enforcer.router)
app.include_router(playlists.router)
app.include_router(scanner.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=config.APP_HOST,
        port=config.APP_PORT,
        reload=False,
    )
