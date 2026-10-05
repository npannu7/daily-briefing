import os
import glob
import time
import datetime
import email.utils
import xml.etree.ElementTree as ET
import requests
import asyncio
import edge_tts

# ----------------- CONFIGURATION -----------------
GITHUB_USERNAME = os.getenv("GITHUB_REPOSITORY_OWNER", "your-username")
REPO_NAME = os.getenv("GITHUB_REPOSITORY", "your-username/daily-briefing").split("/")[-1]
BASE_URL = f"https://{GITHUB_USERNAME}.github.io/{REPO_NAME}"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
AUDIO_DIR = "audio"
PODCAST_FILE = "podcast.xml"
RETENTION_DAYS = 7
VOICE = "en-US-ChristopherNeural"

# ----------------- 1. PRUNE OLD FILES -----------------
def cleanup_old_episodes(audio_dir=AUDIO_DIR, max_days=RETENTION_DAYS):
    os.makedirs(audio_dir, exist_ok=True)
    cutoff = time.time() - (max_days * 86400)
    for path in glob.glob(os.path.join(audio_dir, "*.mp3")):
        if os.path.getmtime(path) < cutoff:
            print(f"Deleting expired episode: {path}")
            os.remove(path)

# ----------------- 2. SEARCH & SYNTHESIZE SCRIPT -----------------
def generate_broadcast_script():
    today_str = datetime.date.today().strftime("%B %d, %Y")
    
    prompt = f"""
Today is {today_str}.
Act as a senior science and engineering investigative broadcaster. Conduct live Google searches to find the most significant STEM milestones completed or deployed in the past 48 hours.

Select exactly 4 distinct completed milestones across:
- Aerospace / Space Exploration (actual hardware launched, docked, landed, or mission concluded)
- Mathematics / Computing (formally published/settled theorems, operational industrial silicon, deployed open models)
- Medicine / Public Health (completed human Phase 3/4 trial results, approved therapies, or major public health containment campaigns)
- Physical Sciences / Energy (grid-connected power, completed particle physics data runs, or verified material manufacturing)

EDITORIAL FILTER:
1. HARD EXCLUSION: Do NOT cover mouse/animal models, in-vitro lab assays, theoretical compounds, conceptual designs, or speculative "could pave the way" promises. If it is not a finished, real-world deployment or verified result, ignore it.
2. HISTORICAL RETROSPECTIVE (Mandatory): For each of the 4 stories, do a web search on its origin:
   - When this initiative was first conceived/funded 10 to 20 years ago, what was the original thesis and timeline?
   - What mechanical, computational, or political roadblocks caused delays or design pivots along the way?
   - What concrete capability, hardware, or proof was delivered today?

LENGTH & FORMAT REQUIREMENTS:
- Write a long-form spoken radio script of roughly 2,800 to 3,200 words (~700 words per story) so it lasts ~20 minutes when read aloud.
- Write in a dense, crisp, authoritative broadcast style (BBC World Service / NPR style).
- Do NOT include markdown bolding, section titles, episode intros, greeting chit-chat, or sign-offs. Jump straight into the first sentence of story 1.
"""

    model_cascade = [
        "gemini-3.1-pro-preview",
        "gemini-3.8-flash",
        "gemini-3.7-flash",
        "gemini-3.6-flash",
        "gemini-3.5-flash",
    ]

    for model in model_cascade:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "tools": [{"google_search": {}}],  # Enables real-time web search grounding
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 8192
            }
        }

        print(f"Querying {model} with live Google Search...")
        for attempt in range(1, 3):
            try:
                res = requests.post(url, json=payload, timeout=180)
                if res.status_code == 200:
                    data = res.json()
                    candidates = data.get("candidates", [])
                    if candidates and "content" in candidates[0]:
                        text = candidates[0]["content"]["parts"][0]["text"]
                        word_count = len(text.split())
                        print(f"Generated {word_count} words via {model}.")
                        return text
                
                if res.status_code in (500, 503, 429):
                    wait = attempt * 10
                    print(f"{model} returned {res.status_code}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"{model} status {res.status_code}. Falling back...")
                    break
            except Exception as e:
                print(f"Error on {model}: {e}")
                time.sleep(5)

    raise RuntimeError("All models failed to generate content.")

# ----------------- 3. TTS GENERATION -----------------
async def text_to_audio(script_text, output_path):
    communicate = edge_tts.Communicate(script_text, voice=VOICE, rate="+0%")
    await communicate.save(output_path)

# ----------------- 4. RSS FEED UPDATE -----------------
def update_podcast_rss(new_audio_file):
    audio_files = sorted(glob.glob(os.path.join(AUDIO_DIR, "*.mp3")), key=os.path.getmtime, reverse=True)
    
    rss = ET.Element("rss", {
        "version": "2.0",
        "xmlns:itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"
    })
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "Daily STEM Retrospective Briefing"
    ET.SubElement(channel, "link").text = BASE_URL
    ET.SubElement(channel, "description").text = "Non-speculative STEM journalism tracing completed milestones from origin to completion."
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

    tree = ET.ElementTree(rss)
    tree.write(PODCAST_FILE, encoding="utf-8", xml_declaration=True)

# ----------------- MAIN PIPELINE -----------------
def main():
    print("1. Cleaning old briefings...")
    cleanup_old_episodes()

    print("2. Searching Google and generating 20-minute script...")
    script_text = generate_broadcast_script()

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    audio_filename = f"briefing_{timestamp}.mp3"
    audio_path = os.path.join(AUDIO_DIR, audio_filename)

    print(f"3. Synthesizing audio to {audio_path}...")
    asyncio.run(text_to_audio(script_text, audio_path))

    print("4. Updating podcast.xml feed...")
    update_podcast_rss(audio_filename)
    print("Done! Briefing generated and feed updated.")

if __name__ == "__main__":
    main()
