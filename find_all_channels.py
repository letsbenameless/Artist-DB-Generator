import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from youtube_searcher import find_or_cache_artist_channel

DB_PATH = "artists.db"
MAX_WORKERS = 8  # safe upper limit (try 6–10 depending on CPU and connection)

def get_pending_artists():
    """Return a list of artists without channel links."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM artists
        WHERE youtube_channel IS NULL OR youtube_channel = ''
    """)
    artists = [row[0] for row in cur.fetchall()]
    conn.close()
    return artists

def update_channel_in_db(artist: str, url: str):
    """Thread-safe DB update."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE artists SET youtube_channel = ? WHERE name = ?", (url, artist))
    conn.commit()
    conn.close()

def process_artist(artist: str):
    """Thread worker: find a YouTube channel for the artist."""
    url = find_or_cache_artist_channel(artist)
    return artist, url

def main():
    try:
        pending_artists = get_pending_artists()
        total_pending = len(pending_artists)
        if total_pending == 0:
            print("✅ All artists already have channel links!")
            return

        print(f"🔍 Found {total_pending} artists missing channels.")
        print(f"⚙️ Running with {MAX_WORKERS} threads.\n")

        start_time = time.time()
        found = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_artist, artist): artist for artist in pending_artists}

            for i, future in enumerate(as_completed(futures), start=1):
                artist = futures[future]
                try:
                    artist, url = future.result()
                    if url:
                        update_channel_in_db(artist, url)
                        found += 1
                        print(f"[{i}/{total_pending}] ✅ {artist} → {url}")
                    else:
                        print(f"[{i}/{total_pending}] ❌ {artist}")
                except Exception as e:
                    print(f"[{i}/{total_pending}] ⚠️ {artist} failed: {e}")

                # ETA display
                elapsed = time.time() - start_time
                avg_time = elapsed / i
                remaining = total_pending - i
                eta = remaining * avg_time / 60
                print(f"    ⏱️ ETA: {eta:.1f} min remaining")

        elapsed_total = (time.time() - start_time) / 60
        print(f"\n✅ Completed: {found}/{total_pending} channels found ({found/total_pending*100:.1f}%)")
        print(f"⏳ Total time: {elapsed_total:.1f} minutes")

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user — stopping threads and closing cleanly.")
        # ThreadPoolExecutor automatically joins threads here
        return

if __name__ == "__main__":
    main()
