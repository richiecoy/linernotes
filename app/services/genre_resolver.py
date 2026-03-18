"""
LinerNotes Genre Resolver
Maps MusicBrainz genres to 15 master categories.
Handles manual overrides and genre weight calculation.
"""
import re
import logging
from collections import defaultdict

logger = logging.getLogger("linernotes.genre_resolver")

# ============================================================================
# 14 MASTER GENRES
# ============================================================================
MASTER_GENRES = [
    'Alternative', 'Rock', 'Soft Rock', 'Metal', 'Punk', 'Pop', 'Hip Hop', 'R&B',
    'Electronic', 'Country', 'Folk', 'Jazz', 'Classical', 'Reggae', 'Soundtrack',
]

# Order matters — more specific patterns before general ones
GENRE_MAP = [
    # --- Metal (includes industrial, nu metal) ---
    (r'\bheavy metal\b', 'Metal'),
    (r'\bdoom metal\b', 'Metal'),
    (r'\bgroove metal\b', 'Metal'),
    (r'\bgothic metal\b', 'Metal'),
    (r'\bmelodic metalcore\b', 'Metal'),
    (r'\bpower metal\b', 'Metal'),
    (r'\bthrash metal\b', 'Metal'),
    (r'\bdeath metal\b', 'Metal'),
    (r'\bblack metal\b', 'Metal'),
    (r'\bspeed metal\b', 'Metal'),
    (r'\bsymphonic metal\b', 'Metal'),
    (r'\bfolk metal\b', 'Metal'),
    (r'\bprogressive metal\b', 'Metal'),
    (r'\bindustrial metal\b', 'Metal'),
    (r'\bindustrial rock\b', 'Metal'),
    (r'\bindustrial\b', 'Metal'),
    (r'\belectro-industrial\b', 'Metal'),
    (r'\bnu metal\b', 'Metal'),
    (r'\balternative metal\b', 'Metal'),
    (r'\bmetalcore\b', 'Metal'),
    (r'\bmetal\b', 'Metal'),

    # --- Punk ---
    (r'\bpunk rock\b', 'Punk'),
    (r'\bhardcore punk\b', 'Punk'),
    (r'\bpost-punk\b', 'Punk'),
    (r'\bpop punk\b', 'Punk'),
    (r'\bskate punk\b', 'Punk'),
    (r'\bpunk\b', 'Punk'),

    # --- Alternative (includes grunge, indie) ---
    (r'\bgrunge\b', 'Alternative'),
    (r'\bpost-grunge\b', 'Alternative'),
    (r'\balternative rock\b', 'Alternative'),
    (r'\balt\.?\s*rock\b', 'Alternative'),
    (r'\bfunk metal\b', 'Alternative'),
    (r'\brap rock\b', 'Alternative'),
    (r'\bnew wave\b', 'Alternative'),
    (r'\bbritpop\b', 'Alternative'),
    (r'\bexperimental rock\b', 'Alternative'),
    (r'\bexperimental\b', 'Alternative'),
    (r'\bavant-garde\b', 'Alternative'),
    (r'\bshoegaze\b', 'Alternative'),
    (r'\bmusica alternativa\b', 'Alternative'),
    (r'\bindie rock\b', 'Alternative'),
    (r'\bindie\b', 'Alternative'),
    (r'\balternative\b', 'Alternative'),

    # --- Soft Rock (melodic, radio-friendly) ---
    (r'\bsoft rock\b', 'Soft Rock'),
    (r'\bpop rock\b', 'Soft Rock'),
    (r'\bpiano rock\b', 'Soft Rock'),
    (r'\badult contemporary\b', 'Soft Rock'),
    (r'\byacht rock\b', 'Soft Rock'),
    (r'\bheartland rock\b', 'Soft Rock'),
    (r'\bfolk rock\b', 'Soft Rock'),
    (r'\bcountry rock\b', 'Soft Rock'),
    (r'\bsinger-songwriter\b', 'Soft Rock'),

    # --- Rock (absorbs classic, hard, prog, blues rock) ---
    (r'\bhard rock\b', 'Rock'),
    (r'\bclassic rock\b', 'Rock'),
    (r'\bprogressive rock\b', 'Rock'),
    (r'\bprog rock\b', 'Rock'),
    (r'\bsymphonic prog\b', 'Rock'),
    (r'\bspace rock\b', 'Rock'),
    (r'\bart rock\b', 'Rock'),
    (r'\bpsychedelic rock\b', 'Rock'),
    (r'\bpsychedelic pop\b', 'Rock'),
    (r'\barena rock\b', 'Rock'),
    (r'\bglam rock\b', 'Rock'),
    (r'\bglam\b', 'Rock'),
    (r'\baor\b', 'Rock'),
    (r'\bglam metal\b', 'Rock'),
    (r'\bpop metal\b', 'Rock'),
    (r'\bblues rock\b', 'Rock'),
    (r'\bsouthern rock\b', 'Rock'),
    (r'\brock and roll\b', 'Rock'),
    (r'\brock\b', 'Rock'),

    # --- Pop ---
    (r'\bdance-pop\b', 'Pop'),
    (r'\balternative pop\b', 'Pop'),
    (r'\bartpop\b', 'Pop'),
    (r'\bindie pop\b', 'Pop'),
    (r'\bfolk pop\b', 'Pop'),
    (r'\bballad\b', 'Pop'),
    (r'\bpop\b', 'Pop'),

    # --- Hip Hop ---
    (r'\bhip[\s-]?hop\b', 'Hip Hop'),
    (r'\brap\b', 'Hip Hop'),
    (r'\bboom bap\b', 'Hip Hop'),
    (r'\bgangsta\b', 'Hip Hop'),
    (r'\bg-funk\b', 'Hip Hop'),
    (r'\bturntablism\b', 'Hip Hop'),
    (r'\bhorrorcore\b', 'Hip Hop'),
    (r'\bconscious hip hop\b', 'Hip Hop'),
    (r'\breggaeton\b', 'Hip Hop'),
    (r'\btrap\b', 'Hip Hop'),
    (r'\bwest coast hip hop\b', 'Hip Hop'),
    (r'\beast coast hip hop\b', 'Hip Hop'),
    (r'\bhardcore hip hop\b', 'Hip Hop'),
    (r'\bpop rap\b', 'Hip Hop'),

    # --- R&B (absorbs soul & funk) ---
    (r'\br&b\b', 'R&B'),
    (r'\br ?n ?b\b', 'R&B'),
    (r'\bsoul\b', 'R&B'),
    (r'\bfunk\b', 'R&B'),
    (r'\bblue-eyed soul\b', 'R&B'),
    (r'\bneo[- ]?soul\b', 'R&B'),
    (r'\bmotown\b', 'R&B'),

    # --- Electronic (absorbs dance, ambient, synthpop) ---
    (r'\bdubstep\b', 'Electronic'),
    (r'\belectro\b', 'Electronic'),
    (r'\belectronica\b', 'Electronic'),
    (r'\belectronic\b', 'Electronic'),
    (r'\bsynthpop\b', 'Electronic'),
    (r'\bsynth-pop\b', 'Electronic'),
    (r'\bsynth\b', 'Electronic'),
    (r'\btechno\b', 'Electronic'),
    (r'\bhouse\b', 'Electronic'),
    (r'\bacid house\b', 'Electronic'),
    (r'\bbreakbeat\b', 'Electronic'),
    (r'\bdrum and bass\b', 'Electronic'),
    (r'\bdub\b', 'Electronic'),
    (r'\bdowntempo\b', 'Electronic'),
    (r'\bdance\b', 'Electronic'),
    (r'\bdisco\b', 'Electronic'),
    (r'\bambient\b', 'Electronic'),
    (r'\btrance\b', 'Electronic'),
    (r'\bedm\b', 'Electronic'),

    # --- Country ---
    (r'\bcountry\b', 'Country'),
    (r'\bamericana\b', 'Country'),
    (r'\bbluegrass\b', 'Country'),

    # --- Blues → Rock ---
    (r'\belectric blues\b', 'Rock'),
    (r'\bblues\b', 'Rock'),

    # --- Folk ---
    (r'\bfolk\b', 'Folk'),

    # --- Jazz ---
    (r'\bacid jazz\b', 'Jazz'),
    (r'\bjazz\b', 'Jazz'),

    # --- Classical ---
    (r'\bclassical\b', 'Classical'),
    (r'\bmodern classical\b', 'Classical'),
    (r'\borchestral\b', 'Classical'),
    (r'\bopera\b', 'Classical'),

    # --- Reggae ---
    (r'\bregga\b', 'Reggae'),
    (r'\bska\b', 'Reggae'),
    (r'\bdancehall\b', 'Reggae'),

    # --- Soundtrack ---
    (r'\bsoundtrack\b', 'Soundtrack'),
    (r'\bscore\b', 'Soundtrack'),

    # --- Misc ---
    (r'\bspoken word\b', 'Soundtrack'),
    (r'\bnon-music\b', 'Soundtrack'),
]

# Compile patterns once
GENRE_MAP_COMPILED = [(re.compile(p, re.IGNORECASE), m) for p, m in GENRE_MAP]


def normalize_genre(raw_genre: str) -> str | None:
    """Map a single raw MusicBrainz genre string to a master category."""
    if not raw_genre:
        return None
    for pattern, master in GENRE_MAP_COMPILED:
        if pattern.search(raw_genre):
            return master
    return None


def pick_artist_genre(mb_genres: list[str]) -> tuple:
    """
    Given a list of MB artist genre strings, normalize each and pick the winner.
    MB artist genres are listed by relevance — first genre gets highest weight.

    Returns (winner, weights_dict, unmapped_list)
    """
    if not mb_genres:
        return None, {}, []

    category_weight = defaultdict(int)
    unmapped = []

    for i, genre_name in enumerate(mb_genres):
        weight = len(mb_genres) - i  # First listed = highest weight
        master = normalize_genre(genre_name)
        if master:
            category_weight[master] += weight
        else:
            unmapped.append(genre_name)

    if not category_weight:
        return None, {}, unmapped

    winner = max(category_weight, key=category_weight.get)
    return winner, dict(category_weight), unmapped


def get_effective_genre(artist_row) -> str:
    """Get the effective genre for an artist (manual override takes priority)."""
    if artist_row['manual_override']:
        return artist_row['manual_override']
    if artist_row['resolved_genre']:
        return artist_row['resolved_genre']
    return 'Unresolved'


def build_genre_tag(genre: str, is_live: bool = False, is_acoustic: bool = False) -> str:
    """Build the full genre tag string including secondary tags."""
    parts = [genre]
    if is_live:
        parts.append('Live')
    if is_acoustic:
        parts.append('Acoustic')
    return '; '.join(parts)
