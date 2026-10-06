#!/usr/bin/env python3
"""
The AI Briefing - a two-host, 20+ minute podcast about what is actually happening in AI.

Pipeline (one run = one episode)
  1. COLLECT  - no AI calls: Hugging Face Daily Papers, trending new open models,
                lab / analyst blogs (feeds.txt) and AI stories from Hacker News.
  2. EDIT     - one LLM call picks the segments for this edition, using the memory of
                past episodes so ongoing stories are continued, not repeated.
  3. RESEARCH - full text is fetched only for the chosen items (articles, model cards,
                paper abstracts) so the writers work from real detail.
  4. WRITE    - each segment is written separately as a dialogue between two hosts.
                Extra segments are added until the script is long enough (20+ min).
  5. GREET    - a personal greeting that links today to earlier episodes, a short
                close, and a summary saved as memory for future episodes.
  6. VOICE    - Gemini 3.8 Flash TTS (two speakers). If its free quota runs out or it
                fails, the whole episode is voiced with free Edge voices instead.
  7. PUBLISH  - MP3 goes to the GitHub Release "episodes"; podcast.xml is rebuilt.

LLM providers: Gemini models first, then free OpenRouter models as backup.

Environment variables (set by the workflow)
  GEMINI_API_KEY        required
  OPENROUTER_API_KEY    optional backup writer
  GEMINI_MODELS         optional, comma list of Gemini text models (best first)
  OPENROUTER_MODELS     optional, comma list; default = auto-pick free models
  GEMINI_TTS_MODEL      optional, default gemini-3.8-flash-tts
  TTS_ENGINE            auto | gemini | edge   (default auto)
  EDITION               auto | morning | evening
  FORCE                 true = remake an edition that already exists
  DRY_RUN               true = no upload (local testing)
"""

import asyncio
import base64
import datetime as dt
import email.utils
import html
import io
import json
import os
import re
import subprocess
import sys
import time
import wave
from pathlib import Path
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

import edge_tts
import feedparser
import requests

try:
    import trafilatura
except ImportError:  # optional: better article extraction
    trafilatura = None

# =============================== SETTINGS ===============================
LISTENER = "Nikhil"
SHOW_TITLE = "The AI Briefing"
SHOW_DESC = ("A two-host daily briefing on what is actually happening in AI: industry moves, new open "
             "models, new research, and what we are learning about how LLMs work - how things are "
             "built and how they evolved, without hype.")

# Hosts: name, Gemini voice, Edge voice, delivery style
HOST_A = {"name": "Maya", "gemini": "Kore", "edge": "en-US-AvaNeural",
          "style": "warm, clear and measured radio presenter; natural pace"}
HOST_B = {"name": "Leo", "gemini": "Charon", "edge": "en-US-AndrewNeural",
          "style": "thoughtful technical analyst; precise, curious, conversational"}

MIN_WORDS = 3300           # hard floor for the script (~22 min at ~150 words/min)
TARGET_WORDS = 3800        # what the editor plans for
MIN_AUDIO_SEC = 20 * 60
RETENTION_DAYS = 7         # audio kept in the release
MEMORY_DAYS = 60           # episode summaries kept for continuity
SEEN_DAYS = 21             # links remembered to avoid repeating a story

TZ = ZoneInfo("Asia/Kolkata")
OWNER = os.getenv("GITHUB_REPOSITORY_OWNER", "your-username")
REPO_FULL = os.getenv("GITHUB_REPOSITORY", f"{OWNER}/daily-briefing")
REPO = REPO_FULL.split("/")[-1]
PAGES_URL = f"https://{OWNER.lower()}.github.io/{REPO}"
RELEASE_TAG = "episodes"
AUDIO_BASE_URL = f"https://github.com/{REPO_FULL}/releases/download/{RELEASE_TAG}"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
DEFAULT_GEMINI = "gemini-3.8-flash,gemini-3.7-flash,gemini-3.5-flash-lite,gemini-3.1-flash-lite"
GEMINI_MODELS = [m.strip() for m in (os.getenv("GEMINI_MODELS") or DEFAULT_GEMINI).split(",") if m.strip()]
GEMINI_TTS_MODEL = os.getenv("GEMINI_TTS_MODEL") or "gemini-3.8-flash-tts"
TTS_ENGINE = (os.getenv("TTS_ENGINE") or "auto").lower()

ROOT = Path(__file__).resolve().parent
FEEDS_FILE = ROOT / "feeds.txt"
EPISODES_FILE = ROOT / "episodes.json"
PODCAST_FILE = ROOT / "podcast.xml"
BUILD_DIR = ROOT / "build"

RATE = 24000               # PCM sample rate used for all audio
BYTES_PER_SEC = RATE * 2   # 16-bit mono
UA = {"User-Agent": "Mozilla/5.0 (AI-Briefing podcast bot)"}

AI_WORDS = re.compile(
    r"\b(AI|A\.I\.|LLMs?|GPT|Claude|Gemini|Llama|Mistral|Qwen|DeepSeek|OpenAI|Anthropic|DeepMind|"
    r"transformer|neural|machine learning|model weights|open[- ]weights?|inference|fine-?tun|"
    r"diffusion|agents?|reasoning model|benchmark|tokens?|GPU|Nvidia|interpretability|RLHF)\b", re.I)


def log(msg):
    print(msg, flush=True)


def gh_note(kind, title, msg):
    """GitHub annotation - shows on the run's Summary page."""
    msg = str(msg).replace("\n", " ")[:900]
    print(f"::{kind} title={title}::{msg}", flush=True)


def env_true(name):
    return os.getenv(name, "false").strip().lower() in ("1", "true", "yes")


def words(text):
    return len(text.split())


def strip_html(s, limit=None):
    s = html.unescape(re.sub(r"<[^>]+>", " ", s or ""))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:limit] if limit else s


# =============================== LLM ===============================
def _gemini_stream(model, prompt, json_mode):
    cfg = {"temperature": 0.6, "maxOutputTokens": 16384}
    if json_mode:
        cfg["responseMimeType"] = "application/json"
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model}:streamGenerateContent?alt=sse")
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": cfg}
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}
    with requests.post(url, json=payload, headers=headers, stream=True, timeout=(20, 180)) as r:
        if r.status_code != 200:
            return r.status_code, "", r.text[:300].replace("\n", " ")
        parts = []
        for line in r.iter_lines(decode_unicode=True):
            if not line or not line.startswith("data:"):
                continue
            chunk = json.loads(line[5:].strip())
            cand = (chunk.get("candidates") or [{}])[0]
            for p in cand.get("content", {}).get("parts", []):
                if p.get("text") and not p.get("thought"):
                    parts.append(p["text"])
        return 200, "".join(parts).strip(), ""


_OR_MODELS = None


def openrouter_models():
    """Free OpenRouter models, best guess first (override with OPENROUTER_MODELS)."""
    global _OR_MODELS
    if _OR_MODELS is not None:
        return _OR_MODELS
    override = os.getenv("OPENROUTER_MODELS", "").strip()
    if override:
        _OR_MODELS = [m.strip() for m in override.split(",") if m.strip()]
        return _OR_MODELS
    _OR_MODELS = []
    try:
        data = requests.get("https://openrouter.ai/api/v1/models", headers=UA, timeout=30).json()["data"]
        free = [m for m in data if m.get("id", "").endswith(":free")
                and str(m.get("pricing", {}).get("prompt", "1")) in ("0", "0.0")
                and (m.get("context_length") or 0) >= 64000
                and "text" in (m.get("architecture", {}).get("output_modalities") or ["text"])]
        pref = ["deepseek", "qwen", "kimi", "moonshot", "glm", "z-ai", "llama", "gemma", "mistral", "gpt-oss"]

        def rank(m):
            mid = m["id"].lower()
            fam = next((i for i, p in enumerate(pref) if p in mid), len(pref))
            return (fam, -(m.get("context_length") or 0))
        _OR_MODELS = [m["id"] for m in sorted(free, key=rank)][:4]
    except Exception as e:
        log(f"  could not list OpenRouter models: {e}")
    if _OR_MODELS:
        log(f"  OpenRouter free models: {', '.join(_OR_MODELS)}")
    return _OR_MODELS


def _openrouter(model, prompt, json_mode):
    body = {"model": model, "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.6, "max_tokens": 12000}
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    r = requests.post("https://openrouter.ai/api/v1/chat/completions", json=body, timeout=(20, 400),
                      headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}",
                               "HTTP-Referer": PAGES_URL, "X-Title": SHOW_TITLE})
    if r.status_code != 200:
        return r.status_code, "", r.text[:300].replace("\n", " ")
    data = r.json()
    if "error" in data:
        return 502, "", str(data["error"])[:300]
    msg = (data.get("choices") or [{}])[0].get("message", {}) or {}
    text = msg.get("content") or ""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()
    return 200, text, ""


def llm(prompt, json_mode=False, min_words=0, label=""):
    """Gemini models first, then free OpenRouter models. Returns text."""
    providers = [("gemini", m) for m in GEMINI_MODELS] if GEMINI_API_KEY else []
    if OPENROUTER_API_KEY:
        providers += [("openrouter", m) for m in openrouter_models()]
    if not providers:
        raise RuntimeError("No LLM provider configured (GEMINI_API_KEY / OPENROUTER_API_KEY)")
    errors = []
    for prov, model in providers:
        for attempt in (1, 2):
            t0 = time.time()
            try:
                fn = _gemini_stream if prov == "gemini" else _openrouter
                status, text, err = fn(model, prompt, json_mode)
            except (requests.RequestException, ValueError, KeyError) as e:
                log(f"    {model}: connection problem ({str(e)[:120]})")
                errors.append(f"{model}: connection")
                time.sleep(8)
                continue
            if status == 200:
                n = words(text)
                log(f"    {label} {model}: {n} words in {time.time() - t0:.0f}s")
                if text and n >= min_words:
                    return text
                errors.append(f"{model}: short ({n} words)")
                break
            if status == 429:
                log(f"    {model}: 429 quota/rate -> next | {err[:160]}")
                errors.append(f"{model}: 429")
                break
            if status in (500, 502, 503, 504) and attempt == 1:
                log(f"    {model}: HTTP {status}; retry in 15s")
                time.sleep(15)
                continue
            log(f"    {model}: HTTP {status} -> next | {err[:160]}")
            errors.append(f"{model}: HTTP {status}")
            break
    raise RuntimeError(f"All LLM providers failed ({label}) -> " + " | ".join(errors[-8:]))


def parse_json(text):
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, flags=re.S)
        if m:
            return json.loads(m.group(0))
        raise


# =============================== STATE ===============================
def load_state():
    if EPISODES_FILE.exists():
        data = json.loads(EPISODES_FILE.read_text(encoding="utf-8"))
    else:
        data = {}
    data.setdefault("episodes", [])   # audio currently in the feed
    data.setdefault("memory", [])     # summaries of past episodes (longer retention)
    data.setdefault("seen", [])       # links already covered
    return data


def save_state(state):
    EPISODES_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def memory_text(state, n=8):
    mem = sorted(state["memory"], key=lambda m: m["published"])[-n:]
    if not mem:
        return "(This is the first episode - there are no past episodes yet.)"
    out = []
    for m in mem:
        out.append(f"- {m['label']}: {m['summary']}")
        if m.get("threads"):
            out.append(f"  Open threads: {'; '.join(m['threads'])}")
    return "\n".join(out)


# =============================== 1. COLLECT ===============================
def get_json(url, timeout=30):
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return r.json()


def collect_hf_papers(days=3):
    items = []
    today = dt.datetime.now(dt.timezone.utc).date()
    for d in range(days):
        day = today - dt.timedelta(days=d)
        try:
            data = get_json(f"https://huggingface.co/api/daily_papers?date={day:%Y-%m-%d}")
        except Exception as e:
            log(f"  FAIL HF papers {day}: {e}")
            continue
        for row in data:
            p = row.get("paper", row)
            pid = p.get("id") or row.get("id")
            if not pid:
                continue
            items.append({
                "kind": "paper", "source": "Hugging Face Daily Papers",
                "title": strip_html(p.get("title") or row.get("title", ""), 220),
                "link": f"https://arxiv.org/abs/{pid}",
                "date": day.isoformat(),
                "metric": f"{p.get('upvotes', row.get('upvotes', 0))} upvotes",
                "score": int(p.get("upvotes", row.get("upvotes", 0)) or 0),
                "summary": strip_html(p.get("ai_summary") or p.get("summary") or row.get("summary", ""), 600),
                "fulltext": strip_html(p.get("summary") or row.get("summary", "")),
            })
    items.sort(key=lambda x: x["score"], reverse=True)
    log(f"  OK   HF papers: {len(items)} (keeping top 25)")
    return items[:25]


def collect_hf_models(days=14):
    items = []
    try:
        data = get_json("https://huggingface.co/api/models?sort=trendingScore&direction=-1&limit=60")
    except Exception as e:
        log(f"  FAIL HF trending models: {e}")
        return items
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    for m in data:
        created = m.get("createdAt") or ""
        try:
            when = dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
        except ValueError:
            continue
        if when < cutoff:
            continue  # only genuinely new releases
        items.append({
            "kind": "open-model", "source": "Hugging Face trending",
            "title": m["id"], "link": f"https://huggingface.co/{m['id']}",
            "date": when.date().isoformat(),
            "metric": f"{m.get('likes', 0)} likes, {m.get('downloads', 0)} downloads, "
                      f"task {m.get('pipeline_tag') or 'n/a'}",
            "score": int(m.get("likes", 0) or 0),
            "summary": f"New open model on Hugging Face ({m.get('pipeline_tag') or 'unknown task'}).",
            "card": f"https://huggingface.co/{m['id']}/raw/main/README.md",
        })
    log(f"  OK   HF new trending models: {len(items)}")
    return items[:20]


def collect_feeds(hours=72):
    items = []
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)
    for line in FEEDS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "|" not in line:
            continue
        name, url = [x.strip() for x in line.split("|", 1)]
        try:
            r = requests.get(url, headers=UA, timeout=25)
            r.raise_for_status()
            feed = feedparser.parse(r.content)
            n = 0
            for e in feed.entries[:30]:
                t = e.get("published_parsed") or e.get("updated_parsed")
                if not t:
                    continue
                when = dt.datetime(*t[:6], tzinfo=dt.timezone.utc)
                if when < cutoff:
                    continue
                full = ""
                if e.get("content"):
                    full = strip_html(e["content"][0].get("value", ""))
                summary = strip_html(e.get("summary") or e.get("description", ""))
                items.append({
                    "kind": "article", "source": name, "title": strip_html(e.get("title", ""), 220),
                    "link": e.get("link", ""), "date": when.date().isoformat(), "metric": "",
                    "score": 0, "summary": summary[:600], "fulltext": full or summary,
                })
                n += 1
            log(f"  OK   {name}: {n} recent")
        except Exception as ex:
            log(f"  FAIL {name}: {ex}")
            gh_note("warning", "Feed failed", f"{name}: {str(ex)[:150]}")
    return items


def collect_hn(hours=48):
    items = []
    since = int(time.time() - hours * 3600)
    try:
        data = get_json("https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=200"
                        f"&numericFilters=created_at_i>{since},points>60")
    except Exception as e:
        log(f"  FAIL Hacker News: {e}")
        return items
    for h in data.get("hits", []):
        title = h.get("title") or ""
        if not AI_WORDS.search(title) or not h.get("url"):
            continue
        items.append({
            "kind": "industry", "source": "Hacker News", "title": title[:220], "link": h["url"],
            "date": (h.get("created_at") or "")[:10], "metric": f"{h.get('points', 0)} points",
            "score": int(h.get("points", 0) or 0), "summary": "", "fulltext": "",
        })
    items.sort(key=lambda x: x["score"], reverse=True)
    log(f"  OK   Hacker News AI stories: {len(items)} (keeping top 20)")
    return items[:20]


def collect(state):
    seen = {s["link"] for s in state["seen"]}
    all_items = collect_hf_papers() + collect_hf_models() + collect_feeds() + collect_hn()
    out, titles = [], set()
    for it in all_items:
        key = it["title"].lower()
        if not it["link"] or it["link"] in seen or key in titles:
            continue
        titles.add(key)
        out.append(it)
    for i, it in enumerate(out):
        it["id"] = i
    return out


# =============================== 2. EDIT ===============================
EDITORIAL = f"""EDITORIAL STANDARD (applies to everything):
- Cover what has actually happened: released models, published papers, shipped products,
  measured results, real deals and decisions. No predictions, no "this will change everything",
  no "revolutionary", "game-changer", "unlocks", "paves the way".
- Explain HOW things work: architecture, training data and recipe, compute, evaluation method.
- Explain how it EVOLVED: what earlier work it builds on, what was tried before, what changed.
- Treat benchmarks with care: say what they measure, their known weaknesses, and whether
  results are self-reported or independently reproduced.
- Separate claims (what a company or paper says) from evidence (what was measured or shown).
- Your own training knowledge ends around early 2025. For anything after that, rely ONLY on the
  SOURCE material and PAST EPISODES given to you. Never invent numbers, names, dates or quotes.
  If a detail is not in the sources and you are not certain of it, leave it out."""


def plan_episode(cands, edition, state):
    if edition == "morning":
        shape = f"""MORNING EDITION - news and new work. Plan 5 to 6 segments, about {TARGET_WORDS} words total:
- 1 to 2 segments: industry (releases, deals, policy, infrastructure) - only with real substance
- 1 to 2 segments: new open models (what it is, architecture, size, licence, how it was trained)
- 1 to 2 segments: new research papers (problem, method, results, limits)
- 1 segment: what we are learning about how LLMs work (interpretability, evaluation, behaviour)
Skip a category if nothing in the list is worth it, and give the time to stronger items."""
    else:
        shape = f"""EVENING EDITION - depth. Plan 4 to 5 segments, about {TARGET_WORDS} words total:
- 1 DEEP DIVE (1600-2000 words, may span 2 segments): pick one technique, model family or line of
  research from the list and tell how it evolved - the earlier work it builds on, what was
  tried before, what changed technically, and what the new result actually shows.
- 2 to 3 shorter segments: other notable items, and follow-ups on open threads from past episodes."""

    listing = "\n".join(
        f"[{c['id']}] ({c['kind']}; {c['source']}; {c['date']}; {c['metric']}) {c['title']}"
        + (f" :: {c['summary'][:260]}" if c["summary"] else "")
        for c in cands
    )
    prompt = f"""You are the editor of "{SHOW_TITLE}", a daily two-host podcast for a technically curious
professional who wants substance, not hype.

{EDITORIAL}

{shape}

PAST EPISODES (most recent last) - continue open threads where today's items connect, and do
not repeat a story unless there is genuinely new information:
{memory_text(state)}

CANDIDATE ITEMS:
{listing}

Return JSON only:
{{"theme": "<one line tying today's episode together>",
  "segments": [{{"title": "<short spoken title>", "category": "<industry|open-models|research|understanding|deep-dive|follow-up>",
                 "item_ids": [<ids from the list>], "angle": "<what to explain and why it matters>",
                 "connects_to": "<past episode this follows up, or empty>", "words": <number>}}]}}
"""
    plan = parse_json(llm(prompt, json_mode=True, label="editor"))
    segs = []
    for s in plan.get("segments", []):
        ids = [i for i in s.get("item_ids", []) if isinstance(i, int) and 0 <= i < len(cands)]
        if not ids:
            continue
        s["item_ids"] = ids
        s["words"] = int(s.get("words") or 650)
        segs.append(s)
    if not segs:
        raise RuntimeError("Editor returned no usable segments")
    return plan.get("theme", ""), segs


# =============================== 3. RESEARCH ===============================
def fetch_article(url, limit=7000):
    try:
        r = requests.get(url, headers=UA, timeout=25)
        r.raise_for_status()
        text = ""
        if trafilatura:
            text = trafilatura.extract(r.text, include_comments=False, include_tables=False) or ""
        if not text:
            text = strip_html(re.sub(r"(?is)<(script|style|nav|footer|header).*?</\1>", " ", r.text))
        return text[:limit]
    except Exception as e:
        return f"(could not fetch full text: {str(e)[:80]})"


def research(item):
    if item.get("_researched"):
        return item
    if item["kind"] == "open-model":
        try:
            r = requests.get(item["card"], headers=UA, timeout=25)
            card = r.text if r.status_code == 200 else ""
        except Exception:
            card = ""
        card = re.sub(r"^---.*?---", "", card, flags=re.S)  # drop YAML header
        item["fulltext"] = ("MODEL CARD:\n" + card[:7000]) if card else item["summary"]
    elif item["kind"] == "paper":
        item["fulltext"] = "ABSTRACT:\n" + (item.get("fulltext") or item["summary"])
    elif len(item.get("fulltext", "")) < 1500:
        item["fulltext"] = fetch_article(item["link"])
    item["_researched"] = True
    return item


def sources_block(items):
    blocks = []
    for it in items:
        blocks.append(f"SOURCE: {it['title']}\nFrom: {it['source']} ({it['date']}) {it['metric']}\n"
                      f"Link: {it['link']}\n{(it.get('fulltext') or it['summary'])[:7000]}")
    return "\n\n-----\n\n".join(blocks)


# =============================== 4. WRITE ===============================
A, B = HOST_A["name"], HOST_B["name"]
LINE_RE = re.compile(rf"^\s*\**\s*({A}|{B})\s*\**\s*:\s*(.+)$", re.I)


def write_segment(seg, items, state, position, total, theme):
    target = int(seg["words"] * 1.15)  # models tend to undershoot
    prompt = f"""You are writing one segment of "{SHOW_TITLE}", a two-host audio podcast.
Hosts:
- {A}: the anchor. Frames the story, keeps it moving, asks the questions a smart listener would ask.
- {B}: the technical analyst. Explains how it works, how it evolved, and what the evidence shows.

{EDITORIAL}

Today's theme: {theme}
This is segment {position} of {total}: "{seg['title']}" ({seg.get('category', '')})
Angle: {seg.get('angle', '')}
{('Follows up on a past episode: ' + seg['connects_to']) if seg.get('connects_to') else ''}

PAST EPISODES (for continuity; mention one naturally only if it genuinely connects):
{memory_text(state, 5)}

SOURCES:
{sources_block(items)}

WRITE {target} to {int(target * 1.2)} words of natural spoken dialogue (do not stop early - length matters).
FORMAT - every line starts with a speaker name and a colon, nothing else:
{A}: ...
{B}: ...
RULES
- Real conversation: short turns mixed with longer explanations; hosts build on each other,
  occasionally disagree or push for evidence. No reading lists of facts.
- Explain concrete mechanisms (e.g. architecture choices, training data, objective, eval setup)
  in plain words a smart non-specialist can follow while driving.
- Mention the source naturally ("in the paper's abstract...", "according to the model card...").
- No greeting and no sign-off (those are added separately). {"Start directly with the story." if position == 1 else "Open with a one-line transition, then the story."}
- No markdown, no stage directions, no sound effects, no URLs, no citation numbers.
- Write numbers and abbreviations the way a presenter would say them.
"""
    text = llm(prompt, min_words=150, label=f"segment {position}")
    return parse_dialogue(text)


def parse_dialogue(text):
    turns = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = LINE_RE.match(line)
        if m:
            spk = A if m.group(1).lower() == A.lower() else B
            turns.append([spk, m.group(2).strip()])
        elif turns and not line.startswith(("#", "[", "(")):
            turns[-1][1] += " " + line
    return [(s, clean_speech(t)) for s, t in turns if clean_speech(t)]


def clean_speech(t):
    t = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", t)
    t = re.sub(r"\[\d+(?:[,\s\-]+\d+)*\]", "", t)
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"\((?:laughs?|pause|music|sfx)[^)]*\)", "", t, flags=re.I)
    t = t.replace("**", "").replace("__", "").replace("*", "").replace("`", "").replace("#", "")
    t = re.sub(r"\bet al\.", "and colleagues", t)
    t = re.sub(r"\be\.g\.", "for example", t)
    t = re.sub(r"\bi\.e\.", "that is", t)
    return re.sub(r"\s+", " ", t).strip()


def turns_words(turns):
    return sum(words(t) for _, t in turns)


def write_bookends(state, segments_written, theme, edition, now):
    outline = "\n".join(f"- {s['title']}: {s.get('angle', '')}" for s, _ in segments_written)
    prompt = f"""You write the opening and closing for "{SHOW_TITLE}" (hosts {A} and {B}).
The listener is {LISTENER}; he listens in the car. Today is {now:%A, %d %B %Y}, {edition} edition.

TODAY'S SEGMENTS:
{outline}
Theme: {theme}

PAST EPISODES (most recent last):
{memory_text(state)}

Return JSON only:
{{"greeting": [["{A}", "..."], ["{B}", "..."], ...],
  "closing": [["{A}", "..."], ["{B}", "..."]],
  "summary": "<3-4 sentence factual summary of today's episode, for future episodes to refer back to>",
  "threads": ["<open story to watch for follow-ups>", "..."]}}

Greeting: 90-150 words, 4-6 turns. Greet {LISTENER} by name, naturally (not every turn).
Link today to past episodes where it truly connects (e.g. "on Thursday we looked at X - today
there is a follow-up"). If there are no past episodes, welcome him to the first episode.
Then preview today's segments briefly. Vary the wording - never a fixed formula.
Closing: 2-3 short turns, 30-60 words, no hype, no "stay tuned for the future of AI".
"""
    data = parse_json(llm(prompt, json_mode=True, label="greeting"))

    def norm(lst):
        out = []
        for row in lst or []:
            if isinstance(row, (list, tuple)) and len(row) == 2:
                spk = A if str(row[0]).strip().lower() == A.lower() else B
                txt = clean_speech(str(row[1]))
                if txt:
                    out.append((spk, txt))
        return out
    greeting = norm(data.get("greeting")) or [(A, f"Good {edition}, {LISTENER}. This is {SHOW_TITLE}.")]
    closing = norm(data.get("closing")) or [(A, "That's the briefing."), (B, "See you next time.")]
    return greeting, closing, data.get("summary", theme), [str(t) for t in data.get("threads", [])][:5]


# =============================== 6. VOICE ===============================
class TTSQuotaError(Exception):
    pass


def pcm_from_audio(raw):
    if raw[:4] == b"RIFF":
        with wave.open(io.BytesIO(raw)) as w:
            pcm, rate, ch, sw = w.readframes(w.getnframes()), w.getframerate(), w.getnchannels(), w.getsampwidth()
        if (rate, ch, sw) != (RATE, 1, 2):
            pcm = ffmpeg_to_pcm(raw)
        return pcm
    return raw  # headerless 24 kHz 16-bit mono


def ffmpeg_to_pcm(data):
    res = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", "pipe:0",
                          "-f", "s16le", "-ar", str(RATE), "-ac", "1", "pipe:1"],
                         input=data, capture_output=True)
    if res.returncode != 0:
        raise RuntimeError(f"ffmpeg decode failed: {res.stderr[:200]}")
    return res.stdout


def silence(sec):
    return b"\x00\x00" * int(RATE * sec)


def chunk_turns(turns, max_words=420):
    chunks, cur, n = [], [], 0
    for spk, text in turns:
        w = words(text)
        if cur and n + w > max_words:
            chunks.append(cur)
            cur, n = [], 0
        cur.append((spk, text))
        n += w
    if cur:
        chunks.append(cur)
    return chunks


def _extract_audio_b64(data):
    for step in reversed(data.get("steps", []) or []):
        for c in reversed(step.get("content", []) or []):
            if c.get("type") == "audio" and c.get("data"):
                return c["data"]
    for o in data.get("outputs", []) or []:  # alternative shape
        if o.get("type") == "audio" and o.get("data"):
            return o["data"]
    for cand in data.get("candidates", []) or []:  # legacy generateContent shape
        for p in cand.get("content", {}).get("parts", []):
            if p.get("inlineData", {}).get("data"):
                return p["inlineData"]["data"]
    return None


def gemini_tts_chunk(turns):
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}
    style = {A: HOST_A["style"], B: HOST_B["style"]}
    interactions = {
        "model": GEMINI_TTS_MODEL,
        "input": [{"type": "user_input", "content": [
            {"type": "text", "text": t,
             "annotations": [{"type": "speech_metadata", "speaker": s, "style": style[s]}]}
            for s, t in turns]}],
        "response_format": {"type": "audio"},
        "generation_config": {"speech_config": {"mode": "conversational", "speakers": [
            {"speaker": A, "voice": HOST_A["gemini"]}, {"speaker": B, "voice": HOST_B["gemini"]}]}},
    }
    legacy = {
        "contents": [{"parts": [{"text": "TTS the following conversation. "
                                         f"{A} is a {HOST_A['style']}; {B} is a {HOST_B['style']}.\n\n"
                                         + "\n".join(f"{s}: {t}" for s, t in turns)}]}],
        "generationConfig": {"responseModalities": ["AUDIO"], "speechConfig": {"multiSpeakerVoiceConfig": {
            "speakerVoiceConfigs": [
                {"speaker": A, "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": HOST_A["gemini"]}}},
                {"speaker": B, "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": HOST_B["gemini"]}}}]}}},
    }
    attempts = [("https://generativelanguage.googleapis.com/v1beta/interactions", interactions),
                (f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_TTS_MODEL}:generateContent",
                 legacy)]
    expected = turns_words(turns) / 2.6  # seconds at ~155 words/min
    last = ""
    for url, payload in attempts:
        for attempt in range(1, 4):
            try:
                r = requests.post(url, json=payload, headers=headers, timeout=(20, 300))
            except requests.RequestException as e:
                last = f"connection: {e}"
                time.sleep(10)
                continue
            if r.status_code == 200:
                b64 = _extract_audio_b64(r.json())
                if not b64:
                    last = "no audio in response"
                    break
                pcm = pcm_from_audio(base64.b64decode(b64))
                secs = len(pcm) / BYTES_PER_SEC
                if secs < expected * 0.55:
                    last = f"audio too short ({secs:.0f}s for ~{expected:.0f}s of text)"
                    log(f"    TTS {last}; retrying")
                    continue
                return pcm
            body = r.text[:300].replace("\n", " ")
            if r.status_code == 429:
                if "PerDay" in body or "per day" in body.lower() or attempt == 3:
                    raise TTSQuotaError(body)
                log("    TTS rate limit; waiting 65s")
                time.sleep(65)
                continue
            if r.status_code in (500, 502, 503, 504):
                last = f"HTTP {r.status_code}"
                time.sleep(20 * attempt)
                continue
            last = f"HTTP {r.status_code} {body[:200]}"
            break  # 400/404: try the other request format
    raise RuntimeError(f"Gemini TTS failed: {last}")


async def _edge_turn(text, voice):
    for attempt in range(1, 5):
        try:
            buf = bytearray()
            async for part in edge_tts.Communicate(text, voice).stream():
                if part["type"] == "audio":
                    buf.extend(part["data"])
            if buf:
                return bytes(buf)
            raise RuntimeError("empty audio")
        except Exception as e:
            log(f"    edge-tts attempt {attempt}/4 failed: {e}")
            await asyncio.sleep(8 * attempt)
    raise RuntimeError("edge-tts failed repeatedly")


def edge_tts_chunk(turns):
    voice = {A: HOST_A["edge"], B: HOST_B["edge"]}
    pcm = bytearray()
    for spk, text in turns:
        mp3 = asyncio.run(_edge_turn(text, voice[spk]))
        pcm += ffmpeg_to_pcm(mp3) + silence(0.25)
    return bytes(pcm)


def voice_episode(sections):
    """sections: list of (label, turns). Returns (pcm, marks, engine).
    Turns from all sections are packed into ~420-word chunks (fewer TTS requests);
    section start times are estimated from word position inside a chunk."""
    flat = [(label, spk, text) for label, turns in sections for spk, text in turns]
    chunks, cur, n = [], [], 0
    for row in flat:
        w = words(row[2])
        if cur and n + w > 420:
            chunks.append(cur)
            cur, n = [], 0
        cur.append(row)
        n += w
    if cur:
        chunks.append(cur)

    engines = ["gemini", "edge"] if TTS_ENGINE == "auto" else [TTS_ENGINE]
    for engine in engines:
        if engine == "gemini" and not GEMINI_API_KEY:
            continue
        try:
            pcm, marks, seen_labels = bytearray(), [], set()
            for i, chunk in enumerate(chunks, 1):
                turns = [(spk, text) for _, spk, text in chunk]
                log(f"  {engine} TTS chunk {i}/{len(chunks)} ({turns_words(turns)} words)")
                audio = gemini_tts_chunk(turns) if engine == "gemini" else edge_tts_chunk(turns)
                start, total_w, acc = len(pcm) / BYTES_PER_SEC, max(1, turns_words(turns)), 0
                for label, _, text in chunk:
                    if label not in seen_labels:
                        seen_labels.add(label)
                        marks.append((label, start + (acc / total_w) * len(audio) / BYTES_PER_SEC))
                    acc += words(text)
                pcm += audio + silence(0.4)
                if engine == "gemini":
                    time.sleep(8)  # gentle on free-tier per-minute limits
            return bytes(pcm), marks, engine
        except TTSQuotaError as e:
            log(f"  Gemini TTS quota reached -> switching whole episode to Edge voices | {str(e)[:160]}")
            gh_note("warning", "Gemini TTS quota reached", "This episode uses the free Edge voices.")
        except Exception as e:
            if engine == engines[-1]:
                raise
            log(f"  {engine} TTS failed ({e}) -> switching whole episode to Edge voices")
            gh_note("warning", "Gemini TTS failed", f"{str(e)[:300]} - this episode uses Edge voices.")
    raise RuntimeError("No TTS engine succeeded")


def encode_mp3(pcm, out_path):
    res = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "s16le",
                          "-ar", str(RATE), "-ac", "1", "-i", "pipe:0", "-codec:a", "libmp3lame",
                          "-b:a", "64k", str(out_path)], input=pcm, capture_output=True)
    if res.returncode != 0:
        raise RuntimeError(f"ffmpeg encode failed: {res.stderr[:200]}")


# =============================== 7. PUBLISH ===============================
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
            log(f"  deleting old audio {a['name']}")
            gh("release", "delete-asset", RELEASE_TAG, a["name"], "--yes", check=False)


def fmt_ts(sec):
    sec = int(sec)
    return f"{sec // 60:02d}:{sec % 60:02d}"


def build_rss(episodes):
    cover = next((n for n in ("cover.jpg", "cover.png") if (ROOT / n).exists()), None)
    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" '
           'xmlns:atom="http://www.w3.org/2005/Atom">', "<channel>",
           f"<title>{escape(SHOW_TITLE)}</title>", f"<link>{escape(PAGES_URL)}</link>",
           f"<description>{escape(SHOW_DESC)}</description>", "<language>en</language>",
           f"<itunes:author>{escape(SHOW_TITLE)}</itunes:author>", "<itunes:explicit>false</itunes:explicit>",
           f'<atom:link href="{escape(PAGES_URL)}/podcast.xml" rel="self" type="application/rss+xml"/>',
           f"<lastBuildDate>{email.utils.formatdate(usegmt=True)}</lastBuildDate>"]
    if cover:
        img = f"{PAGES_URL}/{cover}"
        out.append(f'<itunes:image href="{escape(img)}"/>')
        out.append(f"<image><url>{escape(img)}</url><title>{escape(SHOW_TITLE)}</title>"
                   f"<link>{escape(PAGES_URL)}</link></image>")
    for ep in sorted(episodes, key=lambda e: e["published"], reverse=True):
        url = f"{AUDIO_BASE_URL}/{ep['file']}"
        out += ["<item>", f"<title>{escape(ep['title'])}</title>",
                f"<description>{escape(ep['description'])}</description>",
                f"<pubDate>{ep['pubDate']}</pubDate>",
                f'<guid isPermaLink="false">{escape(ep["file"])}</guid>',
                f'<enclosure url="{escape(url)}" length="{ep["size"]}" type="audio/mpeg"/>',
                f"<itunes:duration>{ep['duration']}</itunes:duration>",
                "<itunes:episodeType>full</itunes:episodeType>", "</item>"]
    out += ["</channel>", "</rss>"]
    PODCAST_FILE.write_text("\n".join(out) + "\n", encoding="utf-8")


# =============================== MAIN ===============================
def main():
    now = dt.datetime.now(TZ)
    edition = os.getenv("EDITION", "auto").strip().lower()
    if edition not in ("morning", "evening"):
        edition = "morning" if now.hour < 12 else "evening"
    date_key = now.strftime("%Y-%m-%d")
    force, dry = env_true("FORCE"), env_true("DRY_RUN")
    log(f"{SHOW_TITLE} | {edition} {date_key} | force={force} dry_run={dry} | TTS={TTS_ENGINE}")

    state = load_state()
    if any(e["date"] == date_key and e["edition"] == edition for e in state["episodes"]) and not force:
        log("This edition already exists - nothing to do (backup run).")
        return

    log("1/7 Collecting sources")
    cands = collect(state)
    if len(cands) < 4:
        raise RuntimeError(f"Only {len(cands)} new items found - check feeds and the log.")
    by_id = {c["id"]: c for c in cands}

    log("2/7 Planning the episode")
    theme, plan = plan_episode(cands, edition, state)
    for s in plan:
        log(f"  - [{s.get('category')}] {s['title']} ({s['words']} words) items={s['item_ids']}")

    log("3/7 + 4/7 Researching and writing segments")
    written = []
    used = set()
    for i, seg in enumerate(plan, 1):
        items = [research(by_id[j]) for j in seg["item_ids"]]
        try:
            turns = write_segment(seg, items, state, i, len(plan), theme)
        except RuntimeError as e:
            log(f"  segment skipped: {e}")
            continue
        if turns_words(turns) < 120:
            log("  segment too short after parsing - skipped")
            continue
        written.append((seg, turns))
        used.update(seg["item_ids"])
        log(f"  running total: {sum(turns_words(t) for _, t in written)} words")

    # Top up until the episode is long enough
    extra = 0
    while sum(turns_words(t) for _, t in written) < MIN_WORDS and extra < 4:
        spare = sorted([c for c in cands if c["id"] not in used],
                       key=lambda c: (c["kind"] != "paper", -c["score"]))[:2]
        if not spare:
            break
        extra += 1
        seg = {"title": spare[0]["title"][:80], "category": "extra", "item_ids": [c["id"] for c in spare],
               "angle": "Explain what this is, how it works and how it fits with the rest of today's episode.",
               "words": min(850, max(500, MIN_WORDS - sum(turns_words(t) for _, t in written)))}
        log(f"  script short - adding extra segment: {seg['title']}")
        try:
            turns = write_segment(seg, [research(c) for c in spare], state, len(written) + 1,
                                  len(written) + 1, theme)
            written.append((seg, turns))
        except RuntimeError as e:
            log(f"  extra segment failed: {e}")
        used.update(seg["item_ids"])

    total_words = sum(turns_words(t) for _, t in written)
    if total_words < MIN_WORDS * 0.6:
        raise RuntimeError(f"Script too short ({total_words} words) - not publishing.")
    if total_words < MIN_WORDS:
        gh_note("warning", "Shorter script", f"{total_words} words (target {MIN_WORDS}).")

    log("5/7 Greeting, closing and memory")
    greeting, closing, summary, threads = write_bookends(state, written, theme, edition, now)

    log("6/7 Voicing")
    sections = [("Welcome", greeting)] + [(s["title"], t) for s, t in written] + [("Close", closing)]
    pcm, marks, engine = voice_episode(sections)
    BUILD_DIR.mkdir(exist_ok=True)
    filename = f"ai_{date_key}_{edition}_{now:%H%M}.mp3"
    mp3 = BUILD_DIR / filename
    encode_mp3(pcm, mp3)
    duration = int(len(pcm) / BYTES_PER_SEC)
    size = mp3.stat().st_size
    log(f"  {filename}: {fmt_ts(duration)}, {size / 1e6:.1f} MB, voices={engine}")
    if duration < MIN_AUDIO_SEC:
        gh_note("warning", "Shorter episode", f"{fmt_ts(duration)} (target 20:00+)")

    log("7/7 Publishing")
    pretty = f"{now:%A}, {now.day} {now:%B %Y}"
    notes = [f"{edition.title()} edition - {pretty}", theme, ""]
    for (label, start), (seg, _) in zip(marks[1:], written):
        notes.append(f"{fmt_ts(start)}  {label}")
        for j in seg["item_ids"]:
            notes.append(f"   {by_id[j]['source']}: {by_id[j]['link']}")
    notes.append("")
    notes.append(f"Voices: {'Gemini TTS' if engine == 'gemini' else 'Edge TTS'}")

    episode = {"file": filename, "date": date_key, "edition": edition,
               "title": f"{edition.title()} - {now.day} {now:%b}: {plan[0]['title']}"[:120],
               "published": now.isoformat(), "pubDate": email.utils.format_datetime(now),
               "size": size, "duration": duration, "description": "\n".join(notes).strip()}
    cutoff = (now - dt.timedelta(days=RETENTION_DAYS)).isoformat()
    keep = [e for e in state["episodes"] if e["published"] >= cutoff
            and not (e["date"] == date_key and e["edition"] == edition)]
    keep.append(episode)

    label = f"{now:%A %d %b} {edition}"
    mem_cut = (now - dt.timedelta(days=MEMORY_DAYS)).isoformat()
    memory = [m for m in state["memory"] if m["published"] >= mem_cut
              and not (m.get("date") == date_key and m.get("edition") == edition)]
    memory.append({"label": label, "date": date_key, "edition": edition, "published": now.isoformat(),
                   "summary": summary, "threads": threads,
                   "segments": [s["title"] for s, _ in written]})
    seen_cut = (now - dt.timedelta(days=SEEN_DAYS)).strftime("%Y-%m-%d")
    seen = [s for s in state["seen"] if s.get("date", "") >= seen_cut]
    seen += [{"date": date_key, "link": by_id[j]["link"]} for j in used]

    if dry:
        log(f"  DRY_RUN: audio at {mp3}; upload skipped")
    else:
        ensure_release()
        gh("release", "upload", RELEASE_TAG, str(mp3), "--clobber")
        sync_release_assets({e["file"] for e in keep})

    save_state({"episodes": keep, "memory": memory, "seen": seen})
    build_rss(keep)
    log(f"Done. Feed: {PAGES_URL}/podcast.xml")
    gh_note("notice", "Episode ready", f"{filename} - {fmt_ts(duration)} - {len(written)} segments - "
                                        f"{total_words} words - voices: {engine}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"ERROR: {exc}")
        gh_note("error", "Briefing failed", exc)
        sys.exit(1)
