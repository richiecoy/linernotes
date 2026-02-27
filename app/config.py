"""
LinerNotes Configuration
"""
import os

# Paths
DATA_DIR = os.environ.get("LINERNOTES_DATA", "/data")
DB_PATH = os.path.join(DATA_DIR, "linernotes.db")

# Music library path (mounted volume)
MUSIC_LIBRARY_PATH = os.environ.get("MUSIC_LIBRARY_PATH", "/music")

# Playlist output path
PLAYLIST_PATH = os.environ.get("PLAYLIST_PATH", "/playlists")

# MusicBrainz
MUSICBRAINZ_API = "https://musicbrainz.org/ws/2"
MUSICBRAINZ_USER_AGENT = "LinerNotes/1.0 (https://github.com/richicoy/linernotes)"
MUSICBRAINZ_RATE_LIMIT = 1.1  # seconds between requests

# App
APP_NAME = "LinerNotes"
APP_VERSION = "1.0.0"
APP_HOST = "0.0.0.0"
APP_PORT = int(os.environ.get("LINERNOTES_PORT", "8299"))
