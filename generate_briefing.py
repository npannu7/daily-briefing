import os
import glob
import time
import datetime
import email.utils
import xml.etree.ElementTree as ET
import feedparser
import requests
import asyncio
import edge_tts

# ----------------- CONFIGURATION -----------------
# Replace with your actual GitHub username and repository name
GITHUB_USERNAME = os.getenv("GITHUB_REPOSITORY_OWNER", "your-username")
REPO_NAME = os.getenv("GITHUB_REPOSITORY", "your-username/daily-briefing").split("/")[-1]
BASE_URL = f"https://{GITHUB_USERNAME}.github.io/{REPO_NAME}"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
AUDIO_DIR = "audio"
PODCAST_FILE = "podcast.xml"
RETENTION_DAYS = 7
VOICE = "en-US-ChristopherNeural"  # Authoritative, natural broadcast voice

# Primary, peer-reviewed, and institutional RSS feeds
FEEDS = [
    "https://api.quantamagazine.org/feed/",              # Settled Math, Physics & Computer Science
    "https://www.nasa.gov/news-release/feed/",           # Confirmed Space Missions & Operations
    "https://www.esa.int/rssfeed/Our_Activities/Space_News",
    "https://www.nature.com/nature.rss",                 # Completed research milestones
]

# ----------------- 1. PRUNE OLD FILES -----------------
def cleanup_old_episodes(audio_dir=AUDIO_DIR, max_days=RETENTION_DAYS):
    os.makedirs(audio_dir, exist_ok=True)
    cutoff = time.time() - (max_days * 86400)
    for path in glob.glob(os.path.join(audio_dir, "*.mp3")):
        if os.path.getmtime(path) < cutoff:
            print(f"Deleting expired episode: {path}")
            os.remove(path)

# ----------------- 2. FETCH RAW STEM FEEDS -----------------
def fetch_feed_data():
    raw_articles = []
    for feed_url in FEEDS:
        try:
            parsed = feedparser.parse(feed_url)
            for entry in parsed.entries[:5]:  # Take top 5 latest per feed
                title = entry.get("title", "")
                summary = entry.get("summary", entry.get("description", ""))
                raw_articles.append(f"Title: {title}\nSummary: {summary}\n")
        except Exception as e:
            print(f"Failed to fetch {feed_url}: {e}")
    return "\n---\n".join(raw_articles)

# ----------------- 3. GEMINI SCRIPT SYNTHESIS (CASCADE) -----------------
def generate_broadcast_script(raw_content):
    prompt = f"""
You are a senior science and engineering correspondent for a factual, long-form audio broadcast.
Synthesize the provided STEM raw items into a continuous, highly detailed ~2,800 to 3,000 word spoken radio script (roughly 20 minutes when read aloud).

STRICT EDITORIAL RULES:
1. HARD EXCLUSION: Completely discard and skip any story involving mouse models, animal studies, in-vitro laboratory assays, theoretical molecules, or early-stage preclinical ideas. If an item lacks a completed real-world endpoint (e.g., full human Phase III completion, actual space hardware launched/concluded, settled mathematical theorem, or operational industrial system), omit it entirely. Do not mention hope or potential.
2. HISTORICAL RETROSPECTIVE: When reporting on an engineering, space, or scientific project completed today, trace its trajectory. Detail what was anticipated when it was first conceived 10 to 20 years ago, what mechanical or computational compromises occurred over the decades, and what concrete capability is delivered right now.
3. TONE & STYLE: Crisp, direct, analytical BBC/NPR World Service style. Use short, impactful sentences built for the ear. Avoid conversational greetings, filler, sound-effect cues, and sign-offs. Jump straight into the first story.

RAW MATERIAL:
{raw_content}
"""

    # Ranked hierarchy from highest reasoning quality to lightweight baseline
    model_cascade = [
        "gemini-3.1-pro-preview",
        "gemini-3.8-flash",
        "gemini-3.7-flash",
        "gemini-3.6-flash",
        "gemini-3.5-flash",
        "gemini-3.5-flash-lite",
        "gemini-3.1-flash-lite",
        "gemini-3-flash-preview",
    ]

    for model in model_cascade:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 8192
            }
        }

        print(f"Attempting generation with: {model}...")
        for attempt in range(1, 3):
            try:
                res = requests.post(url, json=payload, timeout=120)

                if res.status_code == 200:
                    data = res.json()
                    candidates = data.get("candidates", [])
                    if candidates and "content" in candidates[0]:
                        print(f"Successfully generated script via {model}.")
                        return candidates[0]["content"]["parts"][0]["text"]

                # If server is overloaded (503/500) or rate-limited (429), pause and retry
                if res.status_code in (500, 503, 429):
                    wait = attempt * 8
                    print(f"{model} returned HTTP {res.status_code}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"{model} unavailable (HTTP {res.status_code}). Falling back to next tier...")
                    break

            except requests.exceptions.RequestException as err:
                print(f"Connection issue on {model} (Attempt {attempt}): {err}")
                time.sleep(5)

    raise RuntimeError("All Gemini candidate models in the fallback chain were unavailable.")

# ----------------- 4. TTS AUDIO GENERATION -----------------
async def text_to_audio(script_text, output_path):
    communicate = edge_tts.Communicate(script_text, voice=VOICE, rate="+0%")
    await communicate.save(output_path)

# ----------------- 5. UPDATE RSS FEED -----------------
def update_podcast_rss(new_audio_file, briefing_title):
    audio_files = sorted(glob.glob(os.path.join(AUDIO_DIR, "*.mp3")), key=os.path.getmtime, reverse=True)
    
    rss = ET.Element("rss", {
        "version": "2.0",
        "xmlns:itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"
    })
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "Daily STEM Executive Briefing"
    ET.SubElement(channel, "link").text = BASE_URL
    ET.SubElement(channel, "description").text = "Automated non-speculative STEM briefing."
    ET.SubElement(channel, "language").text = "en-us"

    for file_path in audio_files:
        filename = os.path.basename(file_path)
        file_url = f"{BASE_URL}/{AUDIO_DIR}/{filename}"
        file_size = str(os.path.getsize(file_path))
        mod_time = email.utils.formatdate(os.path.getmtime(file_path))

        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = filename.replace(".mp3", "").replace("_", " ").title()
        ET.SubElement(item, "pubDate").text = mod_time
        ET.SubElement(item, "guid").text = file_url
        ET.SubElement(item, "enclosure", {
            "url": file_url,
            "length": file_size,
            "type": "audio/mpeg"
        })
        ET.SubElement(item, "itunes:duration").text = "1200"

    tree = ET.ElementTree(rss)
    tree.write(PODCAST_FILE, encoding="utf-8", xml_declaration=True)

# ----------------- MAIN PIPELINE -----------------
def main():
    print("Pruning old episodes...")
    cleanup_old_episodes()

    print("Fetching raw feeds...")
    raw_data = fetch_feed_data()

    print("Synthesizing script with Gemini...")
    script_text = generate_broadcast_script(raw_data)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    audio_filename = f"briefing_{timestamp}.mp3"
    audio_path = os.path.join(AUDIO_DIR, audio_filename)

    print(f"Generating audio: {audio_path}...")
    asyncio.run(text_to_audio(script_text, audio_path))

    print("Updating podcast.xml...")
    update_podcast_rss(audio_filename, f"STEM Briefing - {timestamp}")
    print("Complete.")

if __name__ == "__main__":
    main()
