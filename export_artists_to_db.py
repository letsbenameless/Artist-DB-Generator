import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sys
import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
DB_PATH = "artists.db"

# ─────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
    """)
    conn.commit()
    return conn, cur

# ─────────────────────────────────────────────
# SPOTIFY CLIENT
# ─────────────────────────────────────────────
def get_spotify_client():
    auth_manager = SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET
    )
    return spotipy.Spotify(auth_manager=auth_manager)

# ─────────────────────────────────────────────
# MAIN EXPORT FUNCTION
# ─────────────────────────────────────────────
def export_artists(playlist_url: str):
    sp = get_spotify_client()
    conn, cur = init_db()

    # Handle both full URLs and bare IDs
    playlist_id = playlist_url.split("/")[-1].split("?")[0]

    print(f"🎧 Fetching playlist: {playlist_id} ...")
    results = sp.playlist_tracks(playlist_id, fields="items.track.artists,next,total")

    all_artists = set()

    # Paginate through all tracks
    while results:
        for item in results["items"]:
            track = item.get("track")
            if not track:
                continue
            for artist in track["artists"]:
                all_artists.add(artist["name"].strip())

        if results.get("next"):
            results = sp.next(results)
        else:
            break

    print(f"🎵 Found {len(all_artists)} unique artists in playlist.")

    added = 0
    for artist in sorted(all_artists):
        try:
            cur.execute("INSERT OR IGNORE INTO artists (name) VALUES (?)", (artist,))
            if cur.rowcount:
                added += 1
        except Exception as e:
            print(f"⚠️  Failed to insert {artist}: {e}")

    conn.commit()
    conn.close()

    percentage = (added / len(all_artists)) * 100 if all_artists else 0
    print(f"✅ Added {added} new artists ({percentage:.1f}% of playlist).")

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_artists_to_db.py <playlist_url_or_id>")
        sys.exit(1)

    export_artists(sys.argv[1])
