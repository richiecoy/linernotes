"""
LinerNotes Navidrome API Client
Uses the Subsonic API for playlist management.
"""
import hashlib
import secrets
import logging
import aiohttp

logger = logging.getLogger("linernotes.navidrome")


def _auth_params(username: str, password: str) -> dict:
    """Generate Subsonic API auth parameters using token-based auth."""
    salt = secrets.token_hex(8)
    token = hashlib.md5((password + salt).encode()).hexdigest()
    return {
        'u': username,
        't': token,
        's': salt,
        'v': '1.16.1',
        'c': 'LinerNotes',
        'f': 'json',
    }


async def _api_request(base_url: str, endpoint: str,
                       username: str, password: str,
                       params: dict = None) -> dict:
    """Make a Subsonic API request."""
    url = f"{base_url.rstrip('/')}/rest/{endpoint}"
    auth = _auth_params(username, password)
    if params:
        auth.update(params)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=auth,
                                   timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    return {'error': f'HTTP {resp.status}'}
                data = await resp.json()
                sr = data.get('subsonic-response', {})
                if sr.get('status') != 'ok':
                    return {'error': sr.get('error', {}).get('message', 'Unknown error')}
                return sr
    except Exception as e:
        logger.error("Navidrome API error: %s", e)
        return {'error': str(e)}


async def test_connection(base_url: str, username: str, password: str) -> tuple:
    """Test the Navidrome connection. Returns (success, message)."""
    data = await _api_request(base_url, 'ping', username, password)
    if 'error' in data:
        return False, data['error']
    return True, 'Connected'


async def get_playlists(base_url: str, username: str, password: str) -> list:
    """Get all playlists from Navidrome."""
    data = await _api_request(base_url, 'getPlaylists', username, password)
    if 'error' in data:
        logger.error("Failed to get playlists: %s", data['error'])
        return []
    playlists = data.get('playlists', {}).get('playlist', [])
    # Normalize single item to list
    if isinstance(playlists, dict):
        playlists = [playlists]
    return playlists


async def get_playlist(base_url: str, username: str, password: str,
                       playlist_id: str) -> dict:
    """Get a playlist with its tracks from Navidrome."""
    data = await _api_request(base_url, 'getPlaylist', username, password,
                              {'id': playlist_id})
    if 'error' in data:
        return {'error': data['error']}
    return data.get('playlist', {})


async def create_playlist(base_url: str, username: str, password: str,
                          name: str, song_ids: list = None) -> str | None:
    """Create a new playlist. Returns playlist ID or None."""
    params = {'name': name}
    if song_ids:
        params['songId'] = song_ids

    data = await _api_request(base_url, 'createPlaylist', username, password, params)
    if 'error' in data:
        logger.error("Failed to create playlist '%s': %s", name, data['error'])
        return None

    playlist = data.get('playlist', {})
    return playlist.get('id')


async def update_playlist(base_url: str, username: str, password: str,
                          playlist_id: str, song_ids_to_add: list = None,
                          song_indexes_to_remove: list = None) -> bool:
    """Update a playlist by adding/removing songs. Returns success."""
    params = {'playlistId': playlist_id}
    if song_ids_to_add:
        params['songIdToAdd'] = song_ids_to_add
    if song_indexes_to_remove:
        params['songIndexToRemove'] = song_indexes_to_remove

    data = await _api_request(base_url, 'updatePlaylist', username, password, params)
    if 'error' in data:
        logger.error("Failed to update playlist: %s", data['error'])
        return False
    return True


async def search_songs(base_url: str, username: str, password: str,
                       query: str, count: int = 50) -> list:
    """Search for songs by query."""
    data = await _api_request(base_url, 'search3', username, password,
                              {'query': query, 'songCount': count,
                               'artistCount': 0, 'albumCount': 0})
    if 'error' in data:
        return []
    results = data.get('searchResult3', {})
    songs = results.get('song', [])
    if isinstance(songs, dict):
        songs = [songs]
    return songs


async def get_songs_by_genre(base_url: str, username: str, password: str,
                             genre: str, count: int = 500, offset: int = 0) -> list:
    """Get songs filtered by genre."""
    data = await _api_request(base_url, 'getSongsByGenre', username, password,
                              {'genre': genre, 'count': count, 'offset': offset})
    if 'error' in data:
        return []
    songs = data.get('songsByGenre', {}).get('song', [])
    if isinstance(songs, dict):
        songs = [songs]
    return songs
