from flask import Flask, render_template_string, request, jsonify
import sqlite3
from pathlib import Path
import csv
import re
import requests
import subprocess

DB_PATH = Path("artists.db")
CSV_PATH = Path("missing_channel_matches.csv")

app = Flask(__name__)

# ─────────────────────────────────────────────
# Ensure verified + auto_verified columns exist
# ─────────────────────────────────────────────
def ensure_verified_columns():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(artists)")
    cols = [c[1] for c in cur.fetchall()]
    if "verified" not in cols:
        cur.execute("ALTER TABLE artists ADD COLUMN verified INTEGER DEFAULT 0")
        print("🆕 Added 'verified' column.")
    if "auto_verified" not in cols:
        cur.execute("ALTER TABLE artists ADD COLUMN auto_verified INTEGER DEFAULT 0")
        print("🆕 Added 'auto_verified' column.")
    conn.commit()
    conn.close()

# ─────────────────────────────────────────────
# CSV ordering map
# ─────────────────────────────────────────────
def get_csv_index_map():
    index_map = {}
    if not CSV_PATH.exists():
        return index_map
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            url = row.get("youtube_channel", "").strip()
            if url:
                index_map[url] = i
    return index_map

# ─────────────────────────────────────────────
# Channel metadata + video fetching
# ─────────────────────────────────────────────
_channel_cache = {}

def fetch_channel_metadata(url: str):
    """Scrape YouTube channel banner, avatar, display name, handle, subs, and top videos."""
    if url in _channel_cache:
        return _channel_cache[url]

    meta = {"banner": None, "avatar": None, "display_name": None,
            "handle": None, "subs": None, "videos": []}

    try:
        # Basic metadata via HTML scrape
        html = requests.get(url, timeout=10).text

        m = re.search(r'https://yt3\.googleusercontent\.com/[A-Za-z0-9_\-]+[^\"]+', html)
        if m: meta["banner"] = m.group(0)

        m2 = re.search(r'"avatar":\{"thumbnails":\[{"url":"(https://yt3\.googleusercontent\.com/[^\"]+)"', html)
        if m2: meta["avatar"] = m2.group(1)

        m3 = re.search(r'"channelMetadataRenderer":\{"title":"([^"]+)"', html)
        if m3:
            meta["display_name"] = m3.group(1)
        else:
            m_fallback = re.search(r'"title":"([^"]+ - YouTube)"', html)
            if m_fallback:
                meta["display_name"] = m_fallback.group(1).replace(" - YouTube", "").strip()

        m4 = re.search(r'"handle":"([^"]+)"', html)
        if m4: meta["handle"] = m4.group(1)

        m5 = re.search(r'"subscriberCountText":\{"simpleText":"([^"]+)"', html)
        if m5: meta["subs"] = m5.group(1)

        # ─ Fetch top 5 video titles using yt-dlp
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--quiet",
            "--no-warnings",
            "--print", "%(title)s | %(webpage_url)s",
            f"{url}/videos",
        ]
        try:
            output = subprocess.check_output(cmd, text=True, errors="ignore", timeout=10)
            lines = [line.strip() for line in output.splitlines() if line.strip()]
            meta["videos"] = []
            for line in lines[:5]:
                parts = [p.strip() for p in line.split("|")]
                if len(parts) == 2:
                    title, video_url = parts
                    meta["videos"].append({"title": title, "url": video_url})
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout getting videos for {url}")
        except subprocess.CalledProcessError:
            print(f"⚠️ yt-dlp failed fetching videos for {url}")

    except Exception as e:
        print(f"⚠️ Failed metadata fetch for {url}: {e}")

    _channel_cache[url] = meta
    return meta

# ─────────────────────────────────────────────
# Get next unverified artist
# ─────────────────────────────────────────────
def get_next_unverified():
    ensure_verified_columns()
    index_map = get_csv_index_map()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, song_name, youtube_channel, verified
        FROM artists
        WHERE youtube_channel IS NOT NULL
        AND TRIM(youtube_channel) <> ''
        AND song_name IS NOT NULL
        AND TRIM(song_name) <> ''
        AND (verified IS NULL OR verified = 0)
        ORDER BY name ASC
    """)
    rows = cur.fetchall()
    conn.close()

    sorted_rows = []
    for (id_, name, song, channel, verified) in rows:
        csv_index = index_map.get(channel, 999999)
        if verified == 0 or verified is None:
            sorted_rows.append((csv_index, id_, name, song, channel, verified))

    if not sorted_rows:
        return None

    sorted_rows.sort(key=lambda x: x[0])
    csv_index, id_, name, song, channel, verified = sorted_rows[0]
    meta = fetch_channel_metadata(channel)
    return {"csv_index": csv_index, "id": id_, "name": name, "song": song,
            "channel": channel, "verified": verified, **meta}

# ─────────────────────────────────────────────
# API: verify one and load next
# ─────────────────────────────────────────────
@app.route("/verify", methods=["POST"])
def verify():
    data = request.get_json()
    artist_id = data.get("id")
    status = data.get("verified")

    # Treat "No" as -1 instead of 0
    if status == 0:
        status = -1

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE artists SET verified = ? WHERE id = ?", (status, artist_id))
    conn.commit()
    conn.close()

    next_artist = get_next_unverified()
    return jsonify(success=True, next=next_artist)

# ─────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────
@app.route("/")
def index():
    artist = get_next_unverified()
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>YouTube Channel Verifier</title>
        <style>
            body { font-family: Arial, sans-serif; background: #0f0f0f; color: #eee;
                   margin: 0; padding: 0; text-align: center; }
            .card { background: #1b1b1b; border-radius: 10px; padding: 16px;
                    width: 70%; margin: 40px auto; box-shadow: 0 0 10px rgba(0,0,0,0.4); }
            .banner { width: 100%; height: 120px; object-fit: cover; border-radius: 8px; }
            .avatar { width: 80px; height: 80px; border-radius: 50%;
                      margin-top: -40px; border: 4px solid #1b1b1b; }
            .meta h2 { margin: 8px 0 2px; color: #1e90ff; }
            .meta p { margin: 2px; color: #aaa; }
            .videos { text-align: left; margin-top: 10px; }
            .videos h4 { color: #1e90ff; margin-bottom: 4px; }
            .videos a { color: #eee; text-decoration: none; display: block; margin: 2px 0; }
            .videos a:hover { color: #1e90ff; }
            .buttons { margin-top: 12px; }
            button {
                border: none; padding: 10px 18px; border-radius: 6px;
                font-size: 15px; font-weight: bold; cursor: pointer;
                margin: 6px; color: white;
            }
            .yes { background: #2ecc71; }
            .no { background: #e74c3c; }
            .done { color: #aaa; margin-top: 50px; }
            a { color: #1e90ff; text-decoration: none; }
        </style>
    </head>
    <body>
        <h1>🎧 YouTube Channel Verifier</h1>

        {% if artist %}
            <div class="card" id="card">
                {% if artist.banner %}
                    <img class="banner" src="{{artist.banner}}" alt="banner">
                {% endif %}
                {% if artist.avatar %}
                    <img class="avatar" src="{{artist.avatar}}" alt="avatar">
                {% endif %}
                <div class="meta">
                    <h2>{{artist.display_name or artist.name}}</h2>
                    {% if artist.handle %}<p>@{{artist.handle}}</p>{% endif %}
                    {% if artist.subs %}<p>{{artist.subs}}</p>{% endif %}
                    <!-- <p><b>Song:</b> ${a.song}</p> -->
                    <p style="color:#bbb; font-size:0.95em;"><b>Artist (from CSV):</b> {{artist.name}}</p>

                    {% if artist.display_name and artist.name and artist.display_name|lower != artist.name|lower %}
                        <p style="color:#e67e22;"><i>⚠️ YouTube name differs from CSV artist</i></p>
                    {% endif %}

                    <div class="videos">
                        {% if artist.videos %}
                            <h4>Top 5 Videos:</h4>
                            {% for v in artist.videos %}
                                <a href="{{v.url}}" target="_blank">🎬 {{v.title}}</a>
                            {% endfor %}
                        {% else %}
                            <p style="color:#777;">No videos found</p>
                        {% endif %}
                    </div>

                    <p><a href="{{artist.channel}}" target="_blank">{{artist.channel}}</a></p>
                    <p><small>CSV index: {{artist.csv_index}}</small></p>
                </div>
                <div class="buttons">
                    <button class="yes" onclick="verify({{artist.id}}, 1)">✅ Yes</button>
                    <button class="no" onclick="verify({{artist.id}}, 0)">❌ No</button>
                </div>
            </div>
        {% else %}
            <p class="done">✅ All channels verified!</p>
        {% endif %}

        <script>
        async function verify(id, value) {
            const res = await fetch("/verify", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({id: id, verified: value})
            });
            const data = await res.json();
            if (data.next && Object.keys(data.next).length) {
                loadNext(data.next);
            } else {
                document.body.innerHTML = '<h2 style="margin-top:40px;">✅ All channels verified!</h2>';
            }
        }

        function loadNext(a) {
            const card = document.getElementById("card");
            const vids = a.videos && a.videos.length
                ? a.videos.map(v => `<a href='${v.url}' target='_blank'>🎬 ${v.title}</a>`).join("")
                : "<p style='color:#777;'>No videos found</p>";

            card.innerHTML = `
                ${a.banner ? `<img class="banner" src="${a.banner}" alt="banner">` : ""}
                ${a.avatar ? `<img class="avatar" src="${a.avatar}" alt="avatar">` : ""}
                <div class="meta">
                    <h2>${a.display_name || a.name}</h2>
                    ${a.handle ? `<p>@${a.handle}</p>` : ""}
                    ${a.subs ? `<p>${a.subs}</p>` : ""}
                    <!-- <p><b>Song:</b> ${a.song}</p> -->
                    <p style="color:#bbb; font-size:0.95em;"><b>Artist (from CSV):</b> ${a.name}</p>
                    ${(a.display_name && a.name && a.display_name.toLowerCase() !== a.name.toLowerCase())
                        ? `<p style='color:#e67e22;'><i>⚠️ YouTube name differs from CSV artist</i></p>` : ""}
                    <div class="videos">
                        <h4>Top 5 Videos:</h4>${vids}
                    </div>
                    <p><a href="${a.channel}" target="_blank">${a.channel}</a></p>
                    <p><small>CSV index: ${a.csv_index}</small></p>
                </div>
                <div class="buttons">
                    <button class="yes" onclick="verify(${a.id}, 1)">✅ Yes</button>
                    <button class="no" onclick="verify(${a.id}, 0)">❌ No</button>
                </div>`;
        }
        </script>
    </body>
    </html>
    """
    return render_template_string(html, artist=artist)

# ─────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=5000)
