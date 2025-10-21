import re
import json
import subprocess
import logging
import unicodedata
from typing import Optional, Tuple
from difflib import SequenceMatcher
import sqlite3

DB_PATH = "artists.db"  # Path to your local SQLite file

def get_artist_channel(artist: str):
    """Return cached YouTube channel if it exists."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT youtube_channel FROM artists WHERE name = ?", (artist,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] else None

def set_artist_channel(artist: str, url: str):
    """Save or update YouTube channel link for artist."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE artists SET youtube_channel = ? WHERE name = ?", (url, artist))
    conn.commit()
    conn.close()


LOGGER = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# TEXT NORMALIZATION
# ─────────────────────────────────────────────

def normalize_name(s: str) -> str:
    """Normalize artist/channel names for loose comparison."""
    s = unicodedata.normalize("NFKC", s.lower())
    s = re.sub(r"(?i)\b(vevo|topic|official|music|channel)\b", "", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s.strip()

# ─────────────────────────────────────────────
# CHANNEL CACHE (DB-INTEGRATED)
# ─────────────────────────────────────────────

def find_or_cache_artist_channel(artist: str) -> str:
    """
    Find and cache the artist's actual YouTube channel handle.
    - Compares against display names (e.g. 'PLAT.') for matching
    - Caches the canonical handle (e.g. 'https://www.youtube.com/@plat.mp3')
    - Rejects weak matches
    """
    cached = get_artist_channel(artist)
    if cached:
        return cached

    artist_norm = normalize_name(artist)
    best_url, best_similarity = "", 0.0

    cmd = [
        "yt-dlp",
        f"ytsearch20:{artist} official channel",
        "--dump-json",
        "--quiet",
        "--ignore-errors",
        "--no-warnings",
        "--format", "bestaudio/best",
        "--extractor-args", "youtube:player_client=web",
    ]

    try:
        output = subprocess.check_output(cmd, text=True, errors="ignore", stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        LOGGER.warning("Failed to search YouTube for %s", artist)
        return ""

    for line in output.splitlines():
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        display_name = data.get("channel") or data.get("uploader") or data.get("title") or ""
        channel_url = (
            data.get("channel_url")
            or data.get("uploader_url")
            or data.get("url")
            or ""
        )
        if not display_name or not channel_url or "youtube.com" not in channel_url:
            continue

        display_norm = normalize_name(display_name)
        similarity = SequenceMatcher(None, artist_norm, display_norm).ratio()

        # Skip mismatched lengths or very low similarity
        if abs(len(artist_norm) - len(display_norm)) > 3:
            continue
        if similarity < 0.6:
            continue

        # Bonus for near-exact match or official/handle-based names
        if artist_norm == display_norm:
            similarity += 0.3
        elif artist_norm in display_norm or display_norm in artist_norm:
            similarity += 0.2
        if "official" in display_name.lower():
            similarity += 0.1

        # Prefer URLs with @handle over /channel/ID
        if "/@" in channel_url.lower():
            similarity += 0.15

        # Prefer official artist channels
        if "officialartistchannel" in channel_url.lower():
            similarity += 0.25

        if similarity > best_similarity:
            best_similarity = similarity
            best_url = channel_url

    # Normalize to full URL
    if best_url and not best_url.startswith("https://"):
        best_url = f"https://www.youtube.com{best_url}"

    if best_url:
        set_artist_channel(artist, best_url)
        LOGGER.info("✅ Cached artist '%s' → %s (%.2f)", artist, best_url, best_similarity)
        return best_url

    LOGGER.warning("⚠️ No suitable channel found for %s", artist)
    return ""

# ─────────────────────────────────────────────
# YOUTUBE ALBUM CACHE
# ─────────────────────────────────────────────

YOUTUBE_CACHE: dict[str, list[dict[str, str]]] = {}

def cache_youtube_album_search(artist: str, album: str):
    """
    Try to cache results from the artist's channel.
    If no valid channel found, fallback to a global search.
    """
    artist_key = artist.lower()
    if artist_key in YOUTUBE_CACHE:
        LOGGER.info("✅ Using cached YouTube results for %s", artist)
        return

    # Get or find artist channel
    channel_url = get_artist_channel(artist)
    if not channel_url:
        channel_url = find_or_cache_artist_channel(artist)

    # Fallback to global search if still missing
    if channel_url:
        search_url = f"{channel_url}/search?query={album.replace(' ', '+')}"
    else:
        LOGGER.warning("🌐 Falling back to global YouTube search for %s - %s", artist, album)
        query = f"{artist} {album}".replace(" ", "+")
        search_url = f"https://www.youtube.com/results?search_query={query}"

    cmd = [
        "yt-dlp",
        "--flat-playlist", "--quiet", "--no-warnings",
        "--print", "%(title)s|%(uploader)s|%(webpage_url)s",
        search_url,
    ]

    try:
        output = subprocess.check_output(cmd, text=True, errors="ignore", timeout=12)
    except subprocess.TimeoutExpired:
        LOGGER.warning("⏰ Timeout fetching search results for %s - %s", artist, album)
        return
    except subprocess.CalledProcessError:
        LOGGER.warning("⚠️ Failed to fetch search results for %s - %s", artist, album)
        return

    entries = []
    for line in output.splitlines():
        parts = line.split("|")
        if len(parts) < 3:
            continue
        title, uploader, url = [p.strip() for p in parts[:3]]
        entries.append({"title": title, "uploader": uploader, "url": url})

    YOUTUBE_CACHE[artist_key] = entries
    LOGGER.info("💾 Cached %d YouTube results for %s - %s", len(entries), artist, album)

# ─────────────────────────────────────────────
# SONG MATCHING (CACHE-BASED)
# ─────────────────────────────────────────────

def find_best_from_cache(artist: str, song: str) -> Optional[str]:
    """Return best cached match for a song."""
    artist_key = artist.lower()
    if artist_key not in YOUTUBE_CACHE:
        LOGGER.warning("⚠️ No cached results for %s yet. Run cache_youtube_album_search() first.", artist)
        return None

    ignore_phrases = ("live", "visualiser", "shorts", "behind", "acoustic", "performance")
    prefer_phrases = ("lyric", "official audio", "audio")

    candidates = YOUTUBE_CACHE[artist_key]
    best_url, best_score = None, -1.0

    for entry in candidates:
        title = entry["title"].lower()
        if any(p in title for p in ignore_phrases):
            continue

        score = SequenceMatcher(None, song.lower(), title).ratio()
        if any(p in title for p in prefer_phrases):
            score += 0.2
        if score > best_score:
            best_score = score
            best_url = entry["url"]

    if best_url:
        LOGGER.info("🎵 Matched '%s' → %s (score=%.2f)", song, best_url, best_score)
    else:
        LOGGER.warning("❌ No match found for '%s'", song)
    return best_url

# ─────────────────────────────────────────────
# FALLBACK SINGLE-TRACK SEARCH
# ─────────────────────────────────────────────

def search_youtube_for_song(
    name: str,
    artist: str,
    album: Optional[str] = None,
    channel_url: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Fast search function:
      - If channel_url is provided, search that channel directly
      - Otherwise use cached album results or fallback ytsearch
    """
    LOGGER.info("🧭 Searching YouTube for '%s' by '%s'", name, artist)
    if not name:
        return None, None

    # ─────────────────────────────────────────────
    # 1️⃣ Direct channel search (preferred)
    # ─────────────────────────────────────────────
    if channel_url:
        query_url = f"{channel_url}/search?query={name.replace(' ', '+')}"
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--quiet",
            "--no-warnings",
            "--print", "%(title)s | %(uploader)s | %(webpage_url)s",
            query_url,
        ]
        try:
            output = subprocess.check_output(cmd, text=True, errors="ignore", timeout=12)
        except subprocess.TimeoutExpired:
            LOGGER.warning("⏰ Timeout searching channel %s for %s", channel_url, name)
            output = ""
        except subprocess.CalledProcessError:
            LOGGER.warning("⚠️ yt-dlp failed for %s", channel_url)
            output = ""

        best_url, best_score = None, 0.0
        song_norm = normalize_name(name)

        for line in output.splitlines():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 3:
                continue
            title, uploader, url = parts[:3]
            title_norm = normalize_name(title)
            score = SequenceMatcher(None, song_norm, title_norm).ratio()

            # Boosts
            if song_norm in title_norm or title_norm in song_norm:
                score += 0.25
            if any(k in title_norm for k in ["lyric", "audio", "official"]):
                score += 0.1
            if any(k in title_norm for k in ["live", "remix", "cover"]):
                score -= 0.05

            if score > best_score:
                best_score = score
                best_url = url

        if best_url and best_score >= 0.40:
            LOGGER.info("🎵 Found match for %s on channel → %s (%.2f)", name, best_url, best_score)
            return best_url, channel_url
        else:
            LOGGER.info("❌ No confident match for %s on channel %s", name, channel_url)

    # ─────────────────────────────────────────────
    # 2️⃣ Fallback: cached album search (same as before)
    # ─────────────────────────────────────────────
    if artist.lower() not in YOUTUBE_CACHE and album:
        LOGGER.debug("📦 Caching album results for %s - %s", artist, album)
        cache_youtube_album_search(artist, album)

    cached_url = find_best_from_cache(artist, name)
    if cached_url:
        return cached_url, channel_url or None

    # ─────────────────────────────────────────────
    # 3️⃣ Global fallback ytsearch
    # ─────────────────────────────────────────────
    query = f"{artist} {name}"
    cmd = ["yt-dlp", "--quiet", "--no-warnings", "--get-id", f"ytsearch5:{query}"]

    try:
        output = subprocess.check_output(cmd, text=True, errors="ignore", timeout=10)
        ids = [line.strip() for line in output.splitlines() if line.strip()]
        if ids:
            best_url = f"https://www.youtube.com/watch?v={ids[0]}"
            LOGGER.info("🎵 Global fallback match for %s - %s → %s", artist, name, best_url)
            return best_url, None
    except subprocess.TimeoutExpired:
        LOGGER.warning("⏰ Timeout for fallback search %s - %s", artist, name)
    except subprocess.CalledProcessError:
        LOGGER.warning("❌ yt-dlp error during fallback search for %s - %s", artist, name)

    LOGGER.warning("❌ No YouTube match found for %s - %s", artist, name)
    return None, None


# ─────────────────────────────────────────────
# MANUAL TEST ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # Usage examples:
    #   py -m backend.services.youtube_searcher "song" "artist" "album"
    #   py -m backend.services.youtube_searcher --playlist "My Playlist"
    if len(sys.argv) == 3 and sys.argv[1] == "--playlist":
        playlist_name = sys.argv[2]
        search_playlist_from_db(playlist_name)
        sys.exit(0)

    if len(sys.argv) < 3:
        print("Usage:")
        print("  python -m backend.services.youtube_searcher '<song>' '<artist>' [album]")
        print("  python -m backend.services.youtube_searcher --playlist '<playlist name>'")
        sys.exit(1)

    song = sys.argv[1]
    artist = sys.argv[2]
    album = sys.argv[3] if len(sys.argv) > 3 else None
    url, channel = search_youtube_for_song(song, artist, album)
    print(f"\n✅ Done:\n  URL = {url}\n  Channel = {channel}")
