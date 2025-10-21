import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import sys
import os
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
DB_PATH = "artists.db"

# ─────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Ensure base table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
    """)
    conn.commit()

    # Add new columns if missing
    cur.execute("PRAGMA table_info(artists);")
    columns = [col[1] for col in cur.fetchall()]

    if "song_name" not in columns:
        print("🧱 Adding missing column: song_name ...")
        cur.execute("ALTER TABLE artists ADD COLUMN song_name TEXT;")
        conn.commit()

    if "channel_url" not in columns:
        cur.execute("ALTER TABLE artists ADD COLUMN channel_url TEXT;")
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
# EXPORT ARTISTS FROM PLAYLISTS
# ─────────────────────────────────────────────
def export_artists_from_playlists(playlist_urls):
    sp = get_spotify_client()
    conn, cur = init_db()

    all_artists = {}

    for playlist_url in playlist_urls:
        playlist_id = playlist_url.split("/")[-1].split("?")[0]
        print(f"🎧 Fetching playlist: {playlist_id} ...")

        try:
            results = sp.playlist_tracks(playlist_id, fields="items.track.artists,items.track.name,next,total")
        except Exception as e:
            print(f"⚠️  Failed to fetch playlist {playlist_url}: {e}")
            continue

        # Paginate through tracks
        while results:
            for item in results.get("items", []):
                track = item.get("track")
                if not track:
                    continue
                track_name = track.get("name", "").strip()
                for artist in track.get("artists", []):
                    artist_name = artist.get("name", "").strip()
                    # Keep first song we encounter for each unique artist
                    if artist_name and artist_name not in all_artists:
                        all_artists[artist_name] = track_name
            if results.get("next"):
                results = sp.next(results)
            else:
                break

    print(f"\n🎵 Found {len(all_artists)} unique artists across all playlists.\n")

    added_artists = 0
    added_song_names = 0

    for artist, song_name in sorted(all_artists.items()):
        try:
            cur.execute("INSERT OR IGNORE INTO artists (name) VALUES (?)", (artist,))
            if cur.rowcount:
                added_artists += 1

            cur.execute("""
                UPDATE artists
                SET song_name = ?
                WHERE name = ? AND (song_name IS NULL OR song_name = '')
            """, (song_name, artist))
            if cur.rowcount:
                added_song_names += 1

        except Exception as e:
            print(f"⚠️  Failed to insert/update {artist}: {e}")

    conn.commit()
    conn.close()

    total_updates = added_artists + added_song_names

    print("─────────────────────────────")
    print(f"🎧 Summary:")
    print(f"   • {len(all_artists)} unique artists processed")
    print(f"   • {added_artists} new artists added")
    print(f"   • {added_song_names} new song names added")
    print(f"✅ {total_updates} total database updates\n")

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_artists_to_db.py <spotify_playlist_url_1> [<spotify_playlist_url_2> ...]")
        sys.exit(1)

    export_artists_from_playlists(sys.argv[1:])
