"""
LinerNotes Database Layer
"""
import aiosqlite
from app.config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    sort_name TEXT,
    mbid TEXT,
    resolved_genre TEXT,
    manual_override TEXT,
    mb_genres_raw TEXT,          -- JSON array of raw MB genre strings
    genre_weights TEXT,          -- JSON dict of {category: weight}
    bio TEXT,
    thumb_url TEXT,
    last_synced TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS albums (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id INTEGER NOT NULL,
    folder_name TEXT NOT NULL,
    mb_title TEXT,
    mb_rgid TEXT,                -- MusicBrainz release group ID
    primary_type TEXT,           -- Album, Single, EP
    secondary_types TEXT,        -- JSON array: Live, Compilation, etc.
    in_library INTEGER DEFAULT 1,
    is_live INTEGER DEFAULT 0,
    is_acoustic INTEGER DEFAULT 0,
    match_score REAL DEFAULT 0,
    year TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE,
    UNIQUE(artist_id, folder_name)
);

CREATE TABLE IF NOT EXISTS tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    album_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    title TEXT,
    track_number INTEGER,
    disc_number INTEGER,
    duration_seconds REAL,
    file_format TEXT,            -- mp3, flac, m4a, etc.
    current_genre_tag TEXT,
    expected_genre_tag TEXT,
    needs_update INTEGER DEFAULT 0,
    last_checked TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (album_id) REFERENCES albums(id) ON DELETE CASCADE,
    UNIQUE(album_id, filename)
);

CREATE TABLE IF NOT EXISTS enforcer_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER,
    artist_name TEXT,
    album_name TEXT,
    track_filename TEXT,
    old_value TEXT,
    new_value TEXT,
    status TEXT,                 -- updated, skipped, error, dry_run
    error_message TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS playlist_tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    playlist_name TEXT NOT NULL,
    track_id INTEGER NOT NULL,
    added_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE,
    UNIQUE(playlist_name, track_id)
);

CREATE TABLE IF NOT EXISTS playlist_exclusions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    playlist_name TEXT NOT NULL,
    track_id INTEGER NOT NULL,
    excluded_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE,
    UNIQUE(playlist_name, track_id)
);

CREATE TABLE IF NOT EXISTS playlist_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    playlist_name TEXT,
    action TEXT,                 -- generated, updated, tracks_added, tracks_excluded
    track_count INTEGER,
    details TEXT,
    status TEXT,                 -- success, error
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_albums_artist ON albums(artist_id);
CREATE INDEX IF NOT EXISTS idx_tracks_album ON tracks(album_id);
CREATE INDEX IF NOT EXISTS idx_tracks_needs_update ON tracks(needs_update);
CREATE INDEX IF NOT EXISTS idx_enforcer_log_created ON enforcer_log(created_at);
CREATE INDEX IF NOT EXISTS idx_playlist_tracks_playlist ON playlist_tracks(playlist_name);
CREATE INDEX IF NOT EXISTS idx_playlist_exclusions_playlist ON playlist_exclusions(playlist_name);
"""

# Default settings
DEFAULT_SETTINGS = {
    "library_path": "/music",
    "playlist_path": "/playlists",
    "scan_schedule_time": "02:00",
    "enforcer_schedule_time": "03:00",
    "playlist_schedule_time": "04:00",
    "enforcer_dry_run": "true",
    "mb_cache_days": "30",
}


async def get_db() -> aiosqlite.Connection:
    """Get a database connection."""
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def init_db():
    """Initialize database with schema and default settings."""
    db = await get_db()
    try:
        await db.executescript(SCHEMA)

        # Insert default settings if not present
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, value)
            )
        await db.commit()
    finally:
        await db.close()


async def get_setting(key: str, default: str = "") -> str:
    """Get a setting value."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row["value"] if row else default
    finally:
        await db.close()


async def set_setting(key: str, value: str):
    """Set a setting value."""
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value)
        )
        await db.commit()
    finally:
        await db.close()


async def get_all_settings() -> dict:
    """Get all settings as a dict."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT key, value FROM settings")
        rows = await cursor.fetchall()
        return {row["key"]: row["value"] for row in rows}
    finally:
        await db.close()
