#!/usr/bin/env python3
"""
Daily STEM Briefing - generates a ~20 minute audio episode and publishes it
to a private podcast feed.

Pipeline
  1. Decide the edition (morning / evening) in India time; skip if it already exists.
  2. Read real, dated news items from the RSS feeds listed in feeds.txt.
  3. Gemini (JSON mode) selects only COMPLETED, verified outcomes - no preclinical,
     no proposals, no "could revolutionise" stories, and nothing covered recently.
  4. Gemini (with Google Search) writes each story separately: what happened,
     where it started 10-20 years ago, what changed on the way, what was delivered.
  5. edge-tts converts the script to MP3 in small chunks (with retries).
  6. MP3 is uploaded as an asset of the GitHub Release "episodes" (keeps the
     git repository small); assets older than RETENTION_DAYS are deleted.
  7. episodes.json and podcast.xml are rewritten; the workflow commits them.

Environment variables (set by the workflow)
  GEMINI_API_KEY   required
  GEMINI_MODELS    optional, comma-separated model list (best first)
  EDITION          auto | morning | evening   (default auto)
  FORCE            true to regenerate an edition that already exists
  DRY_RUN          true to skip the GitHub release upload (local testing)
"""

import asyncio
import datetime as dt
import email.utils
import html
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

import edge_tts
import feedparser
import requests

# ============================ CONFIGURATION ============================
ROOT = Path(__file__).resolve().parent
TZ = ZoneInfo("Asia/Kolkata")

OWNER = os.getenv("GITHUB_REPOSITORY_OWNER", "your-username")
REPO_FULL = os.getenv("GITHUB_REPOSITORY", f"{OWNER}/daily-briefing")
REPO = REPO_FULL.split("/")[-1]
PAGES_URL = f"https://{OWNER.lower()}.github.io/{REPO}"
RELEASE_TAG = "episodes"
AUDIO_BASE_URL = f"https://github.com/{REPO_FULL}/releases/download/{RELEASE_TAG}"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
# Free-tier text models, best first. Override with repo variable GEMINI_MODELS.
DEFAULT_MODELS = "gemini-3.8-flash,gemini-3.7-flash,gemini-3.5-flash-lite,gemini-3.1-flash-lite"
MODELS = [m.strip() for m in (os.getenv("GEMINI_MODELS") or DEFAULT_MODELS).split(",") if m.strip()]

VOICE = os.getenv("TTS_VOICE") or "en-US-ChristopherNeural"
TTS_RATE = os.getenv("TTS_RATE") or "+0%"
EDGE_BYTES_PER_SEC = 6000  # edge-tts default output = 48 kbit/s mono MP3

TARGET_WORDS = 2800        # ~20 minutes at ~145 words/min
RETENTION_DAYS = 7         # audio kept for 7 days
HISTORY_DAYS = 21          # headlines remembered to avoid repeats

FEEDS_FILE = ROOT / "feeds.txt"
EPISODES_FILE = ROOT / "episodes.json"
PODCAST_FILE = ROOT / "podcast.xml"
BUILD_DIR = ROOT / "build"  # temporary, git-ignored

PODCAST_TITLE = "Daily STEM Briefing"
PODCAST_DESC = ("Completed, verified milestones in AI, space, mathematics, biology and "
                "disease control - traced from the original promise to what was actually delivered.")

PRONUNCIATIONS = {
    r"\bPhase III\b": "Phase 3",
    r"\bPhase II\b": "Phase 2",
    r"\bPhase IV\b": "Phase 4",
    r"\bCRISPR\b": "crisper",
    r"\bet al\.": "and colleagues",
    r"\be\.g\.": "for example",
    r"\bi\.e\.": "that is",
    r"\bvs\.": "versus",
    r"\bkm/h\b": "kilometres per hour",
    r"\bkm/s\b": "kilometres per second",
}


def log(msg):
    print(msg, flush=True)


def env_true(name):
    return os.getenv(name, "false").strip().lower() in ("1", "true", "yes")


# ============================ GEMINI ============================
def call_gemini(prompt, use_search=False, json_mode=False, min_words=0):
    """Try each model in MODELS. Returns (text, grounding_metadata)."""
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not set")

    gen_cfg = {"temperature": 0.3, "maxOutputTokens": 16384}
    if json_mode:
        gen_cfg["responseMimeType"] = "application/json"
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": gen_cfg}
    if use_search:
        payload["tools"] = [{"google_search": {}}]
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}

    for model in MODELS:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        for attempt in range(1, 4):
            try:
                r = requests.post(url, json=payload, headers=headers, timeout=300)
            except requests.RequestException as e:
                log(f"  {model}: network error ({e}); retrying")
                time.sleep(15 * attempt)
                continue

            if r.status_code == 200:
                data = r.json()
                cand = (data.get("candidates") or [{}])[0]
                parts = cand.get("content", {}).get("parts", [])
                text = "".join(p.get("text", "") for p in parts if not p.get("thought")).strip()
                finish = cand.get("finishReason")
                words = len(text.split())
                log(f"  {model}: {words} words, finish={finish}")
                if text and words >= min_words:
                    return text, cand.get("groundingMetadata", {}) or {}
                break  # empty / too short / blocked -> next model

            body = r.text[:300].replace("\n", " ")
            if r.status_code == 429 and "PerDay" in r.text:
                log(f"  {model}: daily quota used up -> next model")
                break
            if r.status_code in (429, 500, 502, 503, 504):
                wait = r.headers.get("Retry-After")
                wait = int(wait) if wait and wait.isdigit() else 20 * attempt
                log(f"  {model}: HTTP {r.status_code}; waiting {wait}s (attempt {attempt}/3)")
                time.sleep(wait)
                continue
            log(f"  {model}: HTTP {r.status_code} {body} -> next model")
            break  # 400/403/404: model not available on this key

    raise RuntimeError("All Gemini models failed. See log above.")


def parse_json(text):
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, flags=re.S)
        if m:
            return json.loads(m.group(0))
        raise


# ============================ STATE ============================
def load_state():
    if EPISODES_FILE.exists():
        data = json.loads(EPISODES_FILE.read_text(encoding="utf-8"))
        data.setdefault("episodes", [])
        data.setdefault("history", [])
        return data
    return {"episodes": [], "history": []}


def save_state(state):
    EPISODES_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


# ============================ 1. FEEDS ============================
def read_feed_list():
    feeds = []
    for line in FEEDS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line:
            name, url = [x.strip() for x in line.split("|", 1)]
        else:
            name, url = line, line
        feeds.append((name, url))
    return feeds


def strip_html(s, limit=500):
    s = html.unescape(re.sub(r"<[^>]+>", " ", s or ""))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:limit]


def fetch_items():
    items = []
    ua = {"User-Agent": "Mozilla/5.0 (DailySTEMBriefing; +https://github.com)"}
    for name, url in read_feed_list():
        try:
            r = requests.get(url, headers=ua, timeout=25)
            r.raise_for_status()
            parsed = feedparser.parse(r.content)
            n = 0
            for e in parsed.entries[:25]:
                t = e.get("published_parsed") or e.get("updated_parsed")
                if not t:
                    continue
                when = dt.datetime(*t[:6], tzinfo=dt.timezone.utc)
                items.append({
                    "source": name,
                    "title": strip_html(e.get("title", ""), 200),
                    "link": e.get("link", ""),
                    "date": when,
                    "summary": strip_html(e.get("summary") or e.get("description", "")),
                })
                n += 1
            log(f"  OK   {name}: {n} items")
        except Exception as ex:  # one bad feed must not stop the run
            log(f"  FAIL {name}: {ex}")
    return items


def candidate_items(items, history, max_age_hours):
    now = dt.datetime.now(dt.timezone.utc)
    seen_links = {h.get("link") for h in history}
    seen_titles = {h.get("headline", "").lower() for h in history}
    out, titles = [], set()
    for it in sorted(items, key=lambda x: x["date"], reverse=True):
        if now - it["date"] > dt.timedelta(hours=max_age_hours):
            continue
        key = it["title"].lower()
        if it["link"] in seen_links or key in seen_titles or key in titles:
            continue
        titles.add(key)
        out.append(it)
    return out[:80]


# ============================ 2. SELECT ============================
RULES = """ACCEPT only items that report a COMPLETED, real-world, verifiable outcome, for example:
- a spacecraft or rocket that actually launched, docked, landed, returned data, or ended its mission
- a regulatory approval, or final results of a completed Phase 3 / Phase 4 human trial
- a disease-control result: an outbreak declared over, an elimination certified, a vaccination campaign completed with numbers
- a mathematical proof that is published or accepted by experts
- an AI or computing system that is actually released or deployed, with measured results
- a facility, instrument or machine that has been commissioned and is operating

REJECT completely:
- animal studies, mouse models, in-vitro / cell studies, preclinical or early-phase (Phase 1/2) trials
- proposals, plans, concepts, funding announcements, roadmaps, predictions
- stories whose main point is that something "could", "may" or "might" lead somewhere
- opinion pieces, interviews, explainers, product marketing, events and awards"""


def select_stories(cands, edition):
    if edition == "morning":
        brief = ("Pick up to 4 items for a news briefing. Prefer variety across: space, AI/computing, "
                 "mathematics, biology/medicine, disease control. Never pick two items about the same event.")
    else:
        brief = ("Pick the 1 or 2 items that make the best long-form story: a completed outcome with a "
                 "documented history of 10-20 years (original promise, delays, redesigns, final delivery).")

    listing = "\n".join(
        f"[{i}] ({c['source']}, {c['date']:%Y-%m-%d}) {c['title']} :: {c['summary']}"
        for i, c in enumerate(cands)
    )
    prompt = f"""You are the editor of a strict, factual STEM news broadcast.

{RULES}

{brief}
If no item qualifies, return an empty list. Quality over quantity.

Return JSON only, in this shape:
{{"stories": [{{"id": <number in brackets>, "headline": "<short spoken headline>", "domain": "<space|ai|math|biology|medicine|disease-control|physics|other>"}}]}}

ITEMS:
{listing}
"""
    text, _ = call_gemini(prompt, json_mode=True)
    picked = []
    for s in parse_json(text).get("stories", []):
        try:
            c = dict(cands[int(s["id"])])
        except (KeyError, ValueError, IndexError, TypeError):
            continue
        c["headline"] = s.get("headline") or c["title"]
        c["domain"] = s.get("domain", "other")
        picked.append(c)
    return picked[:4] if edition == "morning" else picked[:2]


# ============================ 3. WRITE ============================
def write_story(story, words, today_str):
    prompt = f"""Today is {today_str}. You are a senior science correspondent writing for radio
(BBC World Service quality: precise, crisp, well-constructed sentences, written for the ear).

Use Google Search to verify this story and research its history.

STORY
Headline: {story['headline']}
Source: {story['source']} ({story['date']:%d %B %Y})
Link: {story['link']}
Summary: {story['summary']}

Write about {words} words of continuous spoken prose covering, in this order:
1. What was completed or delivered, with concrete facts: dates, numbers, names, organisations.
2. The origin: when and by whom it was first proposed or funded (ideally 10-20 years ago, or as far
   back as the record goes), and what was promised or expected at the time - goals, timeline, cost.
3. The path: delays, failures, redesigns, cost changes and decisions along the way.
4. Promise versus delivery: what was achieved compared with the original expectation, and the known
   limitations today - stated as facts.

STRICT RULES
- Report only what has happened. No predictions, no "could revolutionise", "paves the way",
  "offers hope", "in the future". Do not speculate about next steps.
- If a fact cannot be verified, leave it out. Never invent numbers, quotes or dates.
- If your research shows this is NOT a completed, real-world outcome (e.g. it is preclinical, early-phase,
  or only a plan), reply with the single word SKIP and nothing else.
- Plain text only: no headings, no bullet points, no markdown, no citation numbers, no URLs.
- Expand abbreviations the first time they appear. Write numbers the way a presenter would say them.
- No greeting, no sign-off, no "in this story". Start with the first fact.
"""
    text, meta = call_gemini(prompt, use_search=True, min_words=1)
    if text.strip().upper().startswith("SKIP") and len(text.split()) < 10:
        return None, []
    if len(text.split()) < words * 0.35:
        log(f"  story too short ({len(text.split())} words) - dropped")
        return None, []
    sources = []
    for ch in meta.get("groundingChunks", []) or []:
        web = ch.get("web") or {}
        if web.get("uri"):
            sources.append({"title": web.get("title") or "source", "uri": web["uri"]})
    return text, sources[:4]


# ============================ 4. AUDIO ============================
def clean_for_tts(t):
    t = re.sub(r"```.*?```", " ", t, flags=re.S)
    t = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", t)          # markdown links
    t = re.sub(r"\[\d+(?:[,\s\-]+\d+)*\]", "", t)             # [1], [2, 3]
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"^\s*#+\s*", "", t, flags=re.M)               # headings
    t = re.sub(r"^\s*(?:[-*•]|\d+\.)\s+", "", t, flags=re.M)  # bullets
    t = t.replace("**", "").replace("__", "").replace("*", "").replace("`", "")
    for pat, rep in PRONUNCIATIONS.items():
        t = re.sub(pat, rep, t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def chunk_text(text, limit=2500):
    chunks, cur = [], ""
    for para in [p.strip() for p in text.split("\n") if p.strip()]:
        pieces = [para] if len(para) <= limit else re.split(r"(?<=[.!?])\s+", para)
        for piece in pieces:
            if cur and len(cur) + len(piece) + 1 > limit:
                chunks.append(cur)
                cur = piece
            else:
                cur = f"{cur}\n{piece}" if cur else piece
    if cur:
        chunks.append(cur)
    return chunks


async def tts_chunk(text):
    for attempt in range(1, 5):
        try:
            buf = bytearray()
            comm = edge_tts.Communicate(text, VOICE, rate=TTS_RATE)
            async for part in comm.stream():
                if part["type"] == "audio":
                    buf.extend(part["data"])
            if buf:
                return bytes(buf)
            raise RuntimeError("empty audio")
        except Exception as e:
            log(f"  TTS attempt {attempt}/4 failed: {e}")
            await asyncio.sleep(10 * attempt)
    raise RuntimeError("edge-tts failed repeatedly (try: pip install -U edge-tts)")


async def build_audio(segments, out_path):
    """segments: list of (label, text). Returns list of (label, start_seconds)."""
    audio = bytearray()
    marks = []
    for label, text in segments:
        marks.append((label, len(audio) / EDGE_BYTES_PER_SEC))
        for chunk in chunk_text(clean_for_tts(text)):
            audio.extend(await tts_chunk(chunk))
    out_path.write_bytes(bytes(audio))
    return marks


# ============================ 5. PUBLISH ============================
def gh(*args, check=True):
    res = subprocess.run(["gh", *args], capture_output=True, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {res.stderr.strip()}")
    return res


def ensure_release():
    if gh("release", "view", RELEASE_TAG, check=False).returncode != 0:
        gh("release", "create", RELEASE_TAG, "--title", "Briefing audio",
           "--notes", "Audio files for the podcast feed. Managed automatically.", "--latest=false")


def sync_release_assets(keep_files):
    res = gh("release", "view", RELEASE_TAG, "--json", "assets", check=False)
    if res.returncode != 0:
        return
    for a in json.loads(res.stdout).get("assets", []):
        if a["name"] not in keep_files:
            log(f"  deleting old audio asset {a['name']}")
            gh("release", "delete-asset", RELEASE_TAG, a["name"], "--yes", check=False)


def fmt_ts(sec):
    sec = int(sec)
    return f"{sec // 60:02d}:{sec % 60:02d}"


def build_rss(episodes):
    cover = next((n for n in ("cover.jpg", "cover.png") if (ROOT / n).exists()), None)
    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" '
           'xmlns:atom="http://www.w3.org/2005/Atom">',
           "<channel>",
           f"<title>{escape(PODCAST_TITLE)}</title>",
           f"<link>{escape(PAGES_URL)}</link>",
           f"<description>{escape(PODCAST_DESC)}</description>",
           "<language>en</language>",
           f"<itunes:author>{escape(PODCAST_TITLE)}</itunes:author>",
           "<itunes:explicit>false</itunes:explicit>",
           f'<atom:link href="{escape(PAGES_URL)}/podcast.xml" rel="self" type="application/rss+xml"/>',
           f"<lastBuildDate>{email.utils.formatdate(usegmt=True)}</lastBuildDate>"]
    if cover:
        img = f"{PAGES_URL}/{cover}"
        out.append(f'<itunes:image href="{escape(img)}"/>')
        out.append(f"<image><url>{escape(img)}</url><title>{escape(PODCAST_TITLE)}</title>"
                   f"<link>{escape(PAGES_URL)}</link></image>")
    for ep in sorted(episodes, key=lambda e: e["published"], reverse=True):
        url = f"{AUDIO_BASE_URL}/{ep['file']}"
        out += ["<item>",
                f"<title>{escape(ep['title'])}</title>",
                f"<description>{escape(ep['description'])}</description>",
                f"<pubDate>{ep['pubDate']}</pubDate>",
                f'<guid isPermaLink="false">{escape(ep["file"])}</guid>',
                f'<enclosure url="{escape(url)}" length="{ep["size"]}" type="audio/mpeg"/>',
                f"<itunes:duration>{ep['duration']}</itunes:duration>",
                "<itunes:episodeType>full</itunes:episodeType>",
                "</item>"]
    out += ["</channel>", "</rss>"]
    PODCAST_FILE.write_text("\n".join(out) + "\n", encoding="utf-8")


# ============================ MAIN ============================
def main():
    now = dt.datetime.now(TZ)
    edition = os.getenv("EDITION", "auto").strip().lower()
    if edition not in ("morning", "evening"):
        edition = "morning" if now.hour < 12 else "evening"
    date_key = now.strftime("%Y-%m-%d")
    force, dry = env_true("FORCE"), env_true("DRY_RUN")
    log(f"Edition: {edition} {date_key} (force={force}, dry_run={dry}) | models: {', '.join(MODELS)}")

    state = load_state()
    same_slot = [e for e in state["episodes"] if e["date"] == date_key and e["edition"] == edition]
    if same_slot and not force:
        log("This edition already exists - nothing to do (backup run).")
        return

    log("1/5 Reading feeds")
    items = fetch_items()
    if not items:
        raise RuntimeError("No feed could be read. Check feeds.txt and the log above.")

    log("2/5 Selecting completed, verified stories")
    stories = []
    for hours in (72, 24 * 7):  # widen the window if the last 3 days are thin
        cands = candidate_items(items, state["history"], hours)
        log(f"  {len(cands)} candidates within {hours} h")
        if cands:
            stories = select_stories(cands, edition)
        if stories:
            break
        time.sleep(5)
    if not stories:
        raise RuntimeError("No qualifying stories found this run.")
    for s in stories:
        log(f"  + [{s['domain']}] {s['headline']}  ({s['source']})")

    log("3/5 Writing stories (with Google Search)")
    per_story = max(500, min(TARGET_WORDS // len(stories), 2800))
    today_str = now.strftime("%A %d %B %Y")
    written = []
    for s in stories:
        time.sleep(6)  # stay under free-tier requests-per-minute
        log(f"  writing: {s['headline']}")
        text, sources = write_story(s, per_story, today_str)
        if text:
            written.append((s, text, sources))
        else:
            log("  skipped after research")
    if not written:
        raise RuntimeError("Every selected story was rejected during research.")

    log("4/5 Creating audio")
    BUILD_DIR.mkdir(exist_ok=True)
    stamp = now.strftime("%H%M")
    filename = f"briefing_{date_key}_{edition}_{stamp}.mp3"
    mp3 = BUILD_DIR / filename
    pretty_date = f"{now:%A}, {now.day} {now:%B %Y}"
    segments = [("Intro", f"Daily STEM Briefing. {edition.title()} edition. {pretty_date}.")]
    for i, (s, text, _) in enumerate(written, 1):
        lead = f"Story {i}. {s['headline']}." if len(written) > 1 else f"{s['headline']}."
        segments.append((s["headline"], f"{lead}\n\n{text}"))
    segments.append(("End", "That is the end of this briefing."))
    marks = asyncio.run(build_audio(segments, mp3))
    size = mp3.stat().st_size
    duration = int(size / EDGE_BYTES_PER_SEC)
    log(f"  {filename}: {size / 1e6:.1f} MB, {fmt_ts(duration)}")

    notes = [f"{edition.title()} edition - {pretty_date}", ""]
    mark_map = dict(marks)
    for s, _, sources in written:
        notes.append(f"{fmt_ts(mark_map.get(s['headline'], 0))}  {s['headline']}")
        notes.append(f"   Source: {s['source']} - {s['link']}")
        for src in sources:
            notes.append(f"   Ref: {src['title']} - {src['uri']}")
        notes.append("")

    log("5/5 Publishing")
    episode = {
        "file": filename,
        "date": date_key,
        "edition": edition,
        "title": f"{edition.title()} Edition - {now.day} {now:%b %Y}",
        "published": now.isoformat(),
        "pubDate": email.utils.format_datetime(now),
        "size": size,
        "duration": duration,
        "description": "\n".join(notes).strip(),
        "headlines": [s["headline"] for s, _, _ in written],
    }
    cutoff = (now - dt.timedelta(days=RETENTION_DAYS)).isoformat()
    keep = [e for e in state["episodes"]
            if e["published"] >= cutoff and not (e["date"] == date_key and e["edition"] == edition)]
    keep.append(episode)

    hist_cutoff = (now - dt.timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    history = [h for h in state["history"] if h.get("date", "") >= hist_cutoff]
    history += [{"date": date_key, "headline": s["headline"], "link": s["link"]} for s, _, _ in written]

    if dry:
        log(f"  DRY_RUN: audio left at {mp3}; release upload skipped")
    else:
        ensure_release()
        gh("release", "upload", RELEASE_TAG, str(mp3), "--clobber")
        sync_release_assets({e["file"] for e in keep})

    state = {"episodes": keep, "history": history}
    save_state(state)
    build_rss(keep)
    log(f"Done. Feed: {PAGES_URL}/podcast.xml")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"ERROR: {exc}")
        sys.exit(1)
