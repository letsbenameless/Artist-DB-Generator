import csv
import sqlite3
import subprocess
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from difflib import SequenceMatcher

DB_PATH = "artists.db"
OUTPUT_FILE = Path("missing_channel_matches_v2.csv")
MAX_WORKERS = 8

# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────
def normalize(s: str) -> str:
    """Normalize titles for loose comparison."""
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\b(official|video|audio|lyric|lyrics|music|visualizer|mv)\b", "", s)
    return s.strip()

# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def ensure_auto_verified_column():
    """Add auto_verified column if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(artists)")
    cols = [c[1] for c in cur.fetchall()]
    if "auto_verified" not in cols:
        cur.execute("ALTER TABLE artists ADD COLUMN auto_verified INTEGER DEFAULT 0")
        print("🆕 Added 'auto_verified' column to artists table.")
        conn.commit()
    conn.close()

def get_artists_with_channels():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, song_name, youtube_channel
        FROM artists
        WHERE youtube_channel IS NOT NULL
          AND TRIM(youtube_channel) <> ''
          AND song_name IS NOT NULL
          AND TRIM(song_name) <> ''
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def update_auto_verified(artist_name: str, channel_url: str, status: int):
    """Set auto_verified flag in the DB for a specific artist and channel."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        UPDATE artists
        SET auto_verified = ?
        WHERE name = ? AND youtube_channel = ?
    """, (status, artist_name, channel_url))
    conn.commit()
    conn.close()

# ─────────────────────────────────────────────
# YOUTUBE SEARCH
# ─────────────────────────────────────────────
def search_channel_for_song(youtube_channel: str, song_name: str) -> str | None:
    """Search the artist's YouTube channel using yt-dlp."""
    query_url = f"{youtube_channel}/search?query={song_name.replace(' ', '+')}"
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
        print(f"⏰ Timeout searching {youtube_channel}")
        return None
    except subprocess.CalledProcessError:
        print(f"⚠️ yt-dlp failed for {youtube_channel}")
        return None

    best_match = None
    best_score = 0.0
    song_norm = normalize(song_name)

    for line in output.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 3:
            continue
        title, uploader, url = parts[:3]

        title_norm = normalize(title)
        if not title_norm:
            continue

        score = SequenceMatcher(None, song_norm, title_norm).ratio()
        if any(k in title_norm for k in ["lyric", "audio"]):
            score += 0.1
        if any(k in title_norm for k in ["live", "remix", "cover", "performance"]):
            score -= 0.1

        if score > best_score:
            best_score = score
            best_match = url

    return best_match if best_score >= 0.4 else None

# ─────────────────────────────────────────────
# WORKER
# ─────────────────────────────────────────────
def verify_artist(row):
    _, artist, song, channel = row
    url = search_channel_for_song(channel, song)
    return (artist, song, channel, bool(url))

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def verify_all_channels():
    ensure_auto_verified_column()
    artists = get_artists_with_channels()
    total = len(artists)
    print(f"🔍 Verifying {total} artist channels using yt-dlp search queries...\n")

    missing = []
    lock = Lock()

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["artist_name", "song_name", "youtube_channel"])

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(verify_artist, a): a for a in artists}

            for i, f in enumerate(as_completed(futures), start=1):
                artist, song, channel, found = f.result()
                progress = f"[{i}/{total}] {artist} – {song}"

                if found:
                    print(f"{progress} ✅")
                    update_auto_verified(artist, channel, 1)
                else:
                    print(f"{progress} ❌")
                    update_auto_verified(artist, channel, 0)
                    with lock:
                        writer.writerow([artist, song, channel])
                        missing.append((artist, song, channel))

    print("\n─────────────────────────────")
    print(f"✅ Verified {total - len(missing)} channels")
    print(f"❌ {len(missing)} missing matches")
    print(f"📁 Saved results to: {OUTPUT_FILE.resolve()}\n")

# ─────────────────────────────────────────────
if __name__ == "__main__":
    verify_all_channels()
