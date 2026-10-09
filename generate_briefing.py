#!/usr/bin/env python3
"""
Weights & Measures - a two-host podcast about what the AI community is discussing,
checked against the evidence. One run = one episode.

Pipeline
  1. COLLECT  - Reddit threads from the subreddits in subreddits.txt. Preferred source is
                reddit/latest.json, uploaded by the office-PC collector (Reddit blocks cloud
                servers). Fallbacks: direct Reddit RSS, then Hacker News discussions.
  2. SCORE    - a cheap decision model rates every thread (relevant? substantial? level?
                needs fact-checking? hype?). Duplicates across subreddits are merged.
  3. TRACK    - threads are grouped into topics that build up over days. When a topic has
                come up on several days in several communities, the evening becomes a deep dive.
  4. PLAN     - morning: "the thread vs the evidence"; evening: follow-ups + a concept from
                scratch, or a deep dive.
  5. RESEARCH - the linked article / paper / repo behind each thread is fetched.
  6. WRITE    - two hosts (Maya, Leo) in dialogue, pitched at an engineer outside CS.
  7. GREET    - personal greeting linked to past episodes; summary saved as memory.
  8. VOICE    - Gemini TTS (free quota, reserved for the evening) -> Fish Audio free (OpenRouter)
                -> Edge voices.
  9. PUBLISH  - MP3 to the GitHub release, podcast.xml, quiz.json and transcript.

Writers: Gemini 3.8 Flash first (free key, then OpenRouter), Claude Haiku 5.5 as backup,
then free models. Helper jobs (topics, quiz) use free models first.
"""

import asyncio
import base64
import concurrent.futures as cf
import datetime as dt
import email.utils
import html
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
import wave
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

import edge_tts
import requests

try:
    import trafilatura
except ImportError:  # optional: better article extraction
    trafilatura = None

# =============================== SETTINGS ===============================
LISTENER = "Nikhil"
LISTENER_PROFILE = ("Nikhil has an M.Tech in mechanical engineering and works in medical-device quality. "
                    "He codes and follows AI as a hobby, reads research papers comfortably (not in computer "
                    "science), and follows AI discussions on Reddit.")
SHOW_TITLE = "Weights & Measures"
SHOW_DESC = ("AI, as it actually works. Two hosts take what the AI community on Reddit is discussing, "
             "check it against the evidence, and explain it for curious engineers - with deep dives "
             "when a story has built up over days.")

HOST_A = {"name": "Maya", "gemini": "Kore", "edge": "en-US-AvaNeural",
          "style": "warm, clear and measured radio presenter; natural pace"}
HOST_B = {"name": "Leo", "gemini": "Charon", "edge": "en-US-AndrewNeural",
          "style": "thoughtful technical analyst; precise, curious, conversational"}

MIN_WORDS = 3300
TARGET_WORDS = 3800
DEEP_DIVE_WORDS = 4600
MIN_AUDIO_SEC = 20 * 60
RETENTION_DAYS = 7
MEMORY_DAYS = 60
SEEN_DAYS = 21
TOPIC_DAYS = 30
SNAPSHOT_MAX_AGE_H = 30

TZ = ZoneInfo("Asia/Kolkata")
OWNER = os.getenv("GITHUB_REPOSITORY_OWNER", "your-username")
REPO_FULL = os.getenv("GITHUB_REPOSITORY", f"{OWNER}/daily-briefing")
REPO = REPO_FULL.split("/")[-1]
PAGES_URL = f"https://{OWNER.lower()}.github.io/{REPO}"
RELEASE_TAG = "episodes"
AUDIO_BASE_URL = f"https://github.com/{REPO_FULL}/releases/download/{RELEASE_TAG}"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
WRITER_CHAIN = os.getenv("WRITER_MODELS") or (
    "gemini:gemini-3.8-flash,openrouter:google/gemini-3.8-flash,openrouter:anthropic/claude-haiku-5.5,"
    "gemini:gemini-3.7-flash,gemini:gemini-3.5-flash-lite,openrouter:free")
HELPER_CHAIN = os.getenv("HELPER_MODELS") or (
    "openrouter:free,gemini:gemini-3.5-flash-lite,gemini:gemini-3.1-flash-lite,"
    "openrouter:anthropic/claude-haiku-5.5")
DECISION_MODELS = [m.strip() for m in (os.getenv("DECISION_MODELS") or
                   "openai/gpt-6-luna-decisions,~typesafe/jev-latest").split(",") if m.strip()]
OPENROUTER_DAILY_USD = float(os.getenv("OPENROUTER_DAILY_USD") or 0.40)   # spend cap for paid models

GEMINI_TTS_MODEL = os.getenv("GEMINI_TTS_MODEL") or "gemini-3.8-flash-tts"
TTS_ENGINE = (os.getenv("TTS_ENGINE") or "auto").lower()   # auto | gemini | fish | edge
GEMINI_TTS_DAILY = int(os.getenv("GEMINI_TTS_DAILY") or 10)
TTS_CHUNK_WORDS = int(os.getenv("TTS_CHUNK_WORDS") or 700)
FISH_MODEL = os.getenv("FISH_MODEL") or "fish-audio/s2.1-pro-free:free"
FISH_VOICES = [v for v in (os.getenv("FISH_VOICE_A", ""), os.getenv("FISH_VOICE_B", "")) if v]

ROOT = Path(__file__).resolve().parent
EPISODES_FILE = ROOT / "episodes.json"
PODCAST_FILE = ROOT / "podcast.xml"
QUIZ_FILE = ROOT / "quiz.json"
TRANSCRIPT_DIR = ROOT / "transcripts"
SNAPSHOT_FILE = ROOT / "reddit" / "latest.json"
BUILD_DIR = ROOT / "build"

RATE = 24000
BYTES_PER_SEC = RATE * 2
UA = {"User-Agent": "Mozilla/5.0 (WeightsAndMeasures podcast bot)"}
try:   # ffmpeg is no longer preinstalled on GitHub runners; imageio-ffmpeg ships a ready binary
    import imageio_ffmpeg
    FFMPEG = shutil.which("ffmpeg") or imageio_ffmpeg.get_ffmpeg_exe()
except ImportError:
    FFMPEG = "ffmpeg"
FEEDBACK_DAYS = 60

AI_WORDS = re.compile(
    r"\b(AI|LLMs?|GPT|Claude|Gemini|Llama|Mistral|Qwen|DeepSeek|OpenAI|Anthropic|DeepMind|"
    r"transformer|neural|machine learning|open[- ]weights?|inference|fine-?tun|diffusion|agents?|"
    r"reasoning|benchmark|tokens?|GPU|Nvidia|interpretability|RLHF|model)\b", re.I)


def log(msg):
    print(msg, flush=True)


def gh_note(kind, title, msg):
    msg = str(msg).replace("\n", " ")[:900]
    print(f"::{kind} title={title}::{msg}", flush=True)


def env_true(name):
    return os.getenv(name, "false").strip().lower() in ("1", "true", "yes")


def words(text):
    return len(text.split())


def fix_text(s):
    """Repair UTF-8 text that was decoded as Latin-1 somewhere upstream (e.g. 'â€”')."""
    if s and any(m in s for m in ("â€", "Ã", "Å", "â\x80")):
        try:
            return s.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return s.replace("â€”", "—").replace("â€“", "–").replace("â€™", "’").replace("â€œ", "“")
    return s


def strip_html(s, limit=None):
    s = html.unescape(re.sub(r"<[^>]+>", " ", s or ""))
    s = fix_text(re.sub(r"\s+", " ", s).strip())
    return s[:limit] if limit else s


# =============================== LLM LAYER ===============================
DEAD = set()          # models that hit a quota/hard error this run
SPEND = []            # [[iso_time, usd], ...] OpenRouter paid spend, last 24 h
USED_BY = Counter()   # which writer produced the script


def spent_today():
    cutoff = (dt.datetime.now(TZ) - dt.timedelta(hours=24)).isoformat()
    return sum(c for t, c in SPEND if t >= cutoff)


def nice_model(model):
    names = {"gemini-3.8-flash": "Gemini 3.8 Flash", "google/gemini-3.8-flash": "Gemini 3.8 Flash",
             "anthropic/claude-haiku-5.5": "Claude Haiku 5.5", "gemini-3.7-flash": "Gemini 3.7 Flash",
             "gemini-3.5-flash-lite": "Gemini 3.5 Flash-Lite", "gemini-3.1-flash-lite": "Gemini 3.1 Flash-Lite"}
    return names.get(model, model.split("/")[-1].replace(":free", " (free)"))


def _gemini_stream(model, prompt, json_mode):
    cfg = {"temperature": 0.6, "maxOutputTokens": 16384}
    if json_mode:
        cfg["responseMimeType"] = "application/json"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:streamGenerateContent?alt=sse"
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": cfg}
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}
    with requests.post(url, json=payload, headers=headers, stream=True, timeout=(20, 180)) as r:
        r.encoding = "utf-8"   # SSE has no charset header; without this, dashes became 'â€”'
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


_FREE = None


def free_models():
    global _FREE
    if _FREE is not None:
        return _FREE
    override = os.getenv("OPENROUTER_FREE_MODELS", "").strip()
    if override:
        _FREE = [m.strip() for m in override.split(",") if m.strip()]
        return _FREE
    _FREE = []
    try:
        data = requests.get("https://openrouter.ai/api/v1/models", headers=UA, timeout=30).json()["data"]
        free = [m for m in data if m.get("id", "").endswith(":free") and "inkling" not in m["id"]
                and (m.get("context_length") or 0) >= 64000
                and "text" in ((m.get("architecture") or {}).get("output_modalities") or ["text"])]
        pref = ["gemini", "deepseek", "qwen", "kimi", "glm", "gemma", "inkling", "llama", "mistral", "gpt-oss"]

        def rank(m):
            mid = m["id"].lower()
            return (next((i for i, p in enumerate(pref) if p in mid), len(pref)), -(m.get("context_length") or 0))
        _FREE = [m["id"] for m in sorted(free, key=rank)][:4]
    except Exception as e:
        log(f"  could not list OpenRouter models: {e}")
    log(f"  OpenRouter free models: {', '.join(_FREE) or 'none'}")
    return _FREE


def _openrouter(model, prompt, json_mode):
    paid = not model.endswith(":free")
    if paid and spent_today() >= OPENROUTER_DAILY_USD:
        return 402, "", f"daily OpenRouter budget ${OPENROUTER_DAILY_USD:.2f} reached"
    body = {"model": model, "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.6, "max_tokens": 12000, "usage": {"include": True}}
    if paid:
        body["reasoning"] = {"effort": "low"}
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
    cost = float((data.get("usage") or {}).get("cost") or 0)
    if cost:
        SPEND.append([dt.datetime.now(TZ).isoformat(), cost])
    msg = (data.get("choices") or [{}])[0].get("message", {}) or {}
    text = re.sub(r"<think>.*?</think>", "", msg.get("content") or "", flags=re.S).strip()
    return 200, text, ""


def expand_chain(chain):
    out = []
    for item in [c.strip() for c in chain.split(",") if c.strip()]:
        prov, _, model = item.partition(":")
        if prov == "gemini" and GEMINI_API_KEY:
            out.append(("gemini", model))
        elif prov == "openrouter" and OPENROUTER_API_KEY:
            out += [("openrouter", m) for m in free_models()] if model == "free" else [("openrouter", model)]
    return out


def llm(prompt, json_mode=False, min_words=0, label="", role="writer"):
    """Returns (text, model). role 'writer' = quality chain, 'helper' = free-first chain."""
    chain = expand_chain(WRITER_CHAIN if role == "writer" else HELPER_CHAIN)
    if not chain:
        raise RuntimeError("No LLM provider configured")
    errors = []
    for prov, model in chain:
        if model in DEAD:
            continue
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
                text = fix_text(text)
                n = words(text)
                log(f"    {label} {prov}:{model}: {n} words in {time.time() - t0:.0f}s")
                if text and n >= min_words:
                    return text, model
                errors.append(f"{model}: short ({n})")
                break
            if status in (429, 402, 401, 403, 404):
                log(f"    {model}: HTTP {status} -> skipping for this run | {err[:140]}")
                DEAD.add(model)
                errors.append(f"{model}: {status}")
                break
            if status in (500, 502, 503, 504) and attempt == 1:
                log(f"    {model}: HTTP {status}; retry in 15s")
                time.sleep(15)
                continue
            log(f"    {model}: HTTP {status} -> next | {err[:140]}")
            errors.append(f"{model}: HTTP {status}")
            break
    raise RuntimeError(f"All models failed ({label}) -> " + " | ".join(errors[-8:]))


def parse_json(text):
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", text, flags=re.S)
        if m:
            return json.loads(m.group(0))
        raise


def as_obj(data, key):
    """Models sometimes return the inner list instead of {key: [...]}; accept both."""
    if isinstance(data, list):
        return {key: data}
    return data if isinstance(data, dict) else {}


def decide(state_obj, questions):
    """OpenRouter Decisions API. Returns the answers dict, or None if unavailable."""
    if not OPENROUTER_API_KEY:
        return None
    for model in DECISION_MODELS:
        if model in DEAD:
            continue
        try:
            r = requests.post("https://openrouter.ai/api/alpha/decisions", timeout=(15, 90),
                              json={"model": model, "state": state_obj, "questions": questions},
                              headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}",
                                       "HTTP-Referer": PAGES_URL, "X-Title": SHOW_TITLE})
        except requests.RequestException:
            continue
        if r.status_code == 200:
            data = r.json()
            cost = float((data.get("usage") or {}).get("cost") or 0)
            if cost:
                SPEND.append([dt.datetime.now(TZ).isoformat(), cost])
            return data.get("answers")
        if r.status_code in (400, 401, 402, 403, 404):
            log(f"    decision model {model}: HTTP {r.status_code} -> skipping | {r.text[:160]}")
            DEAD.add(model)
    return None


# =============================== STATE ===============================
def load_state():
    data = json.loads(EPISODES_FILE.read_text(encoding="utf-8")) if EPISODES_FILE.exists() else {}
    for k in ("episodes", "memory", "seen", "feedback", "tts_log", "or_spend"):
        data.setdefault(k, [])
    data.setdefault("topics", {})
    return data


def save_state(state):
    EPISODES_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def gh(*args, check=True):
    res = subprocess.run(["gh", *args], capture_output=True, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {res.stderr.strip()}")
    return res


def ingest_feedback(state):
    res = gh("issue", "list", "--state", "open", "--limit", "50",
             "--json", "number,title,body,labels", check=False)
    if res.returncode != 0:
        log(f"  could not read feedback issues: {res.stderr.strip()[:200]}")
        return []
    done = []
    for iss in json.loads(res.stdout or "[]"):
        labels = {l.get("name") for l in iss.get("labels", [])}
        if "feedback" not in labels and not iss.get("title", "").lower().startswith("feedback"):
            continue
        m = re.search(r"```json\s*(\{.*?\})\s*```", iss.get("body") or "", flags=re.S)
        if not m:
            continue
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        for fb in data.get("feedback", [data]):
            fb["received"] = dt.datetime.now(TZ).isoformat()
            state["feedback"].append(fb)
        done.append(iss["number"])
        log(f"  feedback issue #{iss['number']}: {iss.get('title', '')}")
    return done


def pace_shift(state):
    recent = [f.get("ratings", {}).get("pace") for f in state["feedback"][-6:]]
    fast, slow = recent.count("too fast"), recent.count("too slow")
    return 1 if slow > fast else -1 if fast > slow else 0


def feedback_text(state, short=False):
    fb = state["feedback"][-12:]
    if not fb:
        return f"(No feedback from {LISTENER} yet.)"
    meta = {m.get("file"): m for m in state["memory"] if m.get("file")}
    more, less, notes, missed, depth, pace, by_writer = [], [], [], [], [], [], []
    for f in fb:
        for seg, v in (f.get("segments") or {}).items():
            (more if v == "more" else less if v == "less" else []).append(seg)
        r = f.get("ratings") or {}
        if r.get("depth"):
            depth.append(r["depth"])
        if r.get("pace"):
            pace.append(r["pace"])
        if f.get("comment"):
            notes.append(f"{f.get('date', '')} {f.get('edition', '')}: {f['comment'][:400]}")
        missed += (f.get("quiz") or {}).get("missed", [])
        m = meta.get(f.get("episode"))
        if m and r.get("overall"):
            by_writer.append(f"{m.get('writer', '?')}/{m.get('voices', '?')}: {r['overall']} stars")
    out = [f"LISTENER FEEDBACK from {LISTENER} (most recent last; follow it):"]
    if depth:
        out.append(f"- Depth votes: {', '.join(depth[-6:])}")
    if pace:
        out.append(f"- Pace votes: {', '.join(pace[-6:])}")
    if notes:
        out.append("- His comments: " + " | ".join(notes[-5:]))
    if not short:
        if more:
            out.append(f"- Wanted MORE like: {'; '.join(more[-8:])}")
        if less:
            out.append(f"- Wanted LESS like: {'; '.join(less[-8:])}")
        if missed:
            out.append("- Quiz questions he missed (explain these ideas more clearly): " + "; ".join(missed[-6:]))
        if by_writer:
            out.append("- Ratings by writer/voices: " + "; ".join(by_writer[-6:]))
    return "\n".join(out)


def memory_text(state, n=8):
    mem = sorted(state["memory"], key=lambda m: m["published"])[-n:]
    if not mem:
        return "(No past episodes in the new format yet.)"
    out = []
    for m in mem:
        out.append(f"- {m['label']}: {m['summary']}")
        if m.get("threads"):
            out.append(f"  Open threads: {'; '.join(m['threads'])}")
    return "\n".join(out)


# =============================== 1. COLLECT ===============================
def load_snapshot():
    if not SNAPSHOT_FILE.exists():
        return None, "no snapshot from the office PC yet"
    snap = json.loads(SNAPSHOT_FILE.read_text(encoding="utf-8"))
    when = dt.datetime.fromisoformat(snap["collected_at"].replace("Z", "+00:00"))
    age = (dt.datetime.now(dt.timezone.utc) - when).total_seconds() / 3600
    if age > SNAPSHOT_MAX_AGE_H:
        return None, f"office-PC snapshot is {age:.0f} h old"
    log(f"  office-PC snapshot: {len(snap['posts'])} posts, {age:.1f} h old")
    return snap, f"office-PC snapshot ({age:.0f} h old)"


def direct_reddit():
    """Try Reddit RSS from this server (usually blocked for cloud IPs)."""
    sys.path.insert(0, str(ROOT / "collector"))
    try:
        import reddit_collector as rc
        subs = rc.read_subreddits(REPO_FULL)
        snap = rc.collect(subs, pause=2, log=log, budget_s=600, comment_limit=10)   # max ~10 minutes
        if not snap["posts"]:
            raise RuntimeError("no posts returned")
        log(f"  direct Reddit: {len(snap['posts'])} posts")
        return snap
    except Exception as e:
        log(f"  direct Reddit not available here: {str(e)[:120]}")
        return None


def hn_threads(hours=36, keep=25):
    since = int(time.time() - hours * 3600)
    try:
        data = requests.get("https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=300"
                            f"&numericFilters=created_at_i>{since},points>50", headers=UA, timeout=30).json()
    except Exception as e:
        log(f"  Hacker News failed: {e}")
        return []
    hits = sorted([h for h in data.get("hits", []) if AI_WORDS.search(h.get("title") or "")],
                  key=lambda h: -(h.get("points") or 0))[:keep]
    posts = []
    for rank, h in enumerate(hits):
        comments = []
        try:
            item = requests.get(f"https://hn.algolia.com/api/v1/items/{h['objectID']}", headers=UA, timeout=30).json()
            for c in (item.get("children") or [])[:12]:
                t = strip_html(c.get("text") or "", 700)
                if t:
                    comments.append(t)
        except Exception:
            pass
        posts.append({"sub": "HackerNews", "weight": 2, "rank": rank, "id": h["objectID"],
                      "title": h.get("title", ""), "url": f"https://news.ycombinator.com/item?id={h['objectID']}",
                      "date": h.get("created_at", ""), "link": h.get("url") or "", "text": "",
                      "comments": comments})
    log(f"  Hacker News: {len(posts)} AI discussions")
    return posts


def norm_link(u):
    if not u:
        return ""
    p = urlparse(u)
    return (p.netloc.lower().removeprefix("www.") + p.path.rstrip("/")).lower()


def title_words(t):
    return {w for w in re.findall(r"[a-z0-9]+", t.lower()) if len(w) > 2}


def build_stories(posts, state):
    seen = {s["link"] for s in state["seen"]}
    stories = []
    for p in posts:
        if p["url"] in seen or (p.get("link") and p["link"] in seen):
            continue
        p = dict(p, title=fix_text(p["title"]), text=fix_text(p.get("text", "")),
                 comments=[fix_text(c) for c in p.get("comments", [])])
        nl, tw = norm_link(p.get("link")), title_words(p["title"])
        match = None
        for s in stories:
            if nl and nl == s["nlink"]:
                match = s
                break
            if len(tw) >= 4 and s["tw"] and len(tw & s["tw"]) / len(tw | s["tw"]) >= 0.7:
                match = s
                break
        if match:
            match["subs"].append(p["sub"])
            match["threads"].append(p["url"])
            match["comments"] += p["comments"]
            match["signal"] = max(match["signal"], p["weight"] / (1 + p["rank"])) + 0.3
            if len(p.get("text", "")) > len(match["text"]):
                match["text"] = p["text"]
            continue
        stories.append({"title": p["title"], "subs": [p["sub"]], "threads": [p["url"]], "link": p.get("link", ""),
                        "nlink": nl, "tw": tw, "text": p.get("text", ""), "comments": list(p["comments"]),
                        "date": p.get("date", ""), "signal": p["weight"] / (1 + p["rank"])})
    stories.sort(key=lambda s: -s["signal"])
    for i, s in enumerate(stories):
        s["id"] = i
        s["comments"] = s["comments"][:24]
    return stories


# =============================== 2. SCORE ===============================
SCORE_QUESTIONS = {
    "relevant": {"type": "noul", "instructions": "Is this thread about AI technology, research, products, the AI "
                 "industry or AI's effects on society, with something to discuss?",
                 "criteria": {"true": "News, a release, research, a technical question with real discussion, "
                              "a debate about AI's impact.", "false": "Memes, art or video showcases, prompt "
                              "sharing, personal tech support, giveaways, low-effort rants."}},
    "substance": {"type": "score", "instructions": "How much factual or technical substance does the post plus "
                  "comments contain?", "criteria": ["None", "Light", "Moderate", "Rich, informative discussion"]},
    "level": {"type": "choice", "instructions": "What background is needed to follow it?",
              "criteria": {"general": "An engineer outside computer science can follow it.",
                           "technical": "Needs some ML or coding background, but can be explained.",
                           "specialist": "Deep research detail only specialists would follow."}},
    "needs_check": {"type": "noul", "instructions": "Does the thread make factual claims (numbers, benchmarks, "
                    "capabilities, 'X replaced Y') that should be checked against a source?",
                    "criteria": {"true": "Yes, checkable claims.", "false": "No, mostly opinion or questions."}},
    "hype": {"type": "noul", "instructions": "Is the framing hype or speculation rather than what happened?",
             "criteria": {"true": "Hype, speculation, sensational framing.", "false": "Grounded."}},
}


def score_stories(stories, limit=70):
    top = stories[:limit]

    def one(s):
        st = {"title": s["title"], "communities": sorted(set(s["subs"])), "linked_site": urlparse(s["link"]).netloc,
              "post": s["text"][:1500], "top_comments": [c[:300] for c in s["comments"][:8]]}
        return s["id"], decide(st, SCORE_QUESTIONS)

    results = {}
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        for sid, ans in ex.map(one, top):
            if ans:
                results[sid] = ans
    if len(results) < len(top) * 0.5:
        log(f"  decision model scored only {len(results)}/{len(top)}; using a free model to score")
        results.update(llm_score(top))
    kept = []
    for s in top:
        a = results.get(s["id"])
        if not a:
            continue
        rel = (a.get("relevant") or {}).get("noul", 0)
        if rel < 0.5:
            continue
        sub = (a.get("substance") or {}).get("score", 1) / 3
        lvl = (a.get("level") or {}).get("choice", "technical")
        s.update(level=lvl, needs_check=(a.get("needs_check") or {}).get("noul", 0) >= 0.5,
                 hype=(a.get("hype") or {}).get("noul", 0) >= 0.5)
        lvl_bonus = {"general": 1.0, "technical": 0.75, "specialist": 0.25}.get(lvl, 0.6)
        s["score"] = 0.35 * min(s["signal"] / 3, 1) + 0.35 * sub + 0.2 * lvl_bonus + 0.1 * min(len(set(s["subs"])) / 3, 1)
        kept.append(s)
    kept.sort(key=lambda s: -s["score"])
    log(f"  scored {len(results)} threads, {len(kept)} relevant")
    return kept


def llm_score(stories):
    listing = "\n".join(f"[{s['id']}] ({', '.join(sorted(set(s['subs'])))}) {s['title']} :: {s['text'][:200]}"
                        for s in stories)
    prompt = f"""Rate these AI discussion threads. Return JSON only:
{{"items": [{{"id": <n>, "relevant": 0-1, "substance": 0-3, "level": "general|technical|specialist",
             "needs_check": 0-1, "hype": 0-1}}]}}
relevant = about AI tech/research/products/industry/impact (not memes, showcases, prompt sharing, tech support).
substance = factual/technical content. level = background needed (general = engineer outside CS can follow).
needs_check = makes checkable factual claims. hype = speculative or sensational framing.

{listing}"""
    try:
        text, _ = llm(prompt, json_mode=True, label="scoring", role="helper")
        out = {}
        for it in as_obj(parse_json(text), "items").get("items", []):
            out[int(it["id"])] = {"relevant": {"noul": float(it.get("relevant", 0))},
                                  "substance": {"score": float(it.get("substance", 1))},
                                  "level": {"choice": it.get("level", "technical")},
                                  "needs_check": {"noul": float(it.get("needs_check", 0))},
                                  "hype": {"noul": float(it.get("hype", 0))}}
        return out
    except Exception as e:
        log(f"  fallback scoring failed: {e}")
        return {s["id"]: {"relevant": {"noul": 1}, "substance": {"score": 1.5}} for s in stories}


# =============================== 3. TOPICS ===============================
def update_topics(state, stories, date_key):
    topics = state["topics"]
    recent = sorted(topics.items(), key=lambda kv: kv[1].get("last_seen", ""), reverse=True)[:40]
    known = "\n".join(f"- {k}: {v['name']}" for k, v in recent) or "(none yet)"
    listing = "\n".join(f"[{s['id']}] {s['title']}" for s in stories[:40])
    prompt = f"""Group these AI discussion threads into ongoing topics (stories or themes that can build up
over days, e.g. "open-weight reasoning models", "AI coding agents in real teams", "AI and jobs in India").
Reuse an existing topic key when a thread belongs to it; otherwise create a new short kebab-case key.
A thread may have no topic (one-off). Keep topics specific enough to make a 30-minute deep dive.

EXISTING TOPICS:
{known}

THREADS:
{listing}

Return JSON only: {{"assign": [{{"id": <thread id>, "topic": "<key>", "name": "<readable topic name>"}}]}}"""
    try:
        text, _ = llm(prompt, json_mode=True, label="topics", role="helper")
        assign = as_obj(parse_json(text), "assign").get("assign", [])
    except Exception as e:
        log(f"  topic tracking skipped: {e}")
        return
    by_id = {s["id"]: s for s in stories}
    for a in assign:
        s, key = by_id.get(a.get("id")), re.sub(r"[^a-z0-9-]", "", str(a.get("topic", "")).lower())[:60]
        if not s or not key:
            continue
        t = topics.setdefault(key, {"name": a.get("name") or key, "first_seen": date_key, "days": [], "subs": [],
                                    "mentions": 0, "refs": [], "deep_dive": ""})
        s["topic"] = key
        t["last_seen"] = date_key
        t["mentions"] += 1
        if date_key not in t["days"]:
            t["days"].append(date_key)
        t["subs"] = sorted(set(t["subs"]) | set(s["subs"]))
        t["refs"] = (t["refs"] + [{"date": date_key, "title": s["title"], "thread": s["threads"][0],
                                   "link": s["link"], "comment": (s["comments"] or [""])[0][:300]}])[-14:]
    cutoff = (dt.date.fromisoformat(date_key) - dt.timedelta(days=TOPIC_DAYS)).isoformat()
    for k in [k for k, v in topics.items() if v.get("last_seen", "") < cutoff]:
        del topics[k]
    log(f"  topics tracked: {len(topics)}")


def ready_topic(state, date_key):
    today = dt.date.fromisoformat(date_key)
    last_dd = max([v.get("deep_dive", "") for v in state["topics"].values()] + [""])
    if last_dd and (today - dt.date.fromisoformat(last_dd)).days < 2:
        return None
    best = None
    for key, t in state["topics"].items():
        if len(t["days"]) < 3 or len(t["subs"]) < 3 or t["mentions"] < 5:
            continue
        if t.get("deep_dive") and (today - dt.date.fromisoformat(t["deep_dive"])).days < 21:
            continue
        if (today - dt.date.fromisoformat(t["last_seen"])).days > 2:
            continue
        rank = (len(t["days"]), len(t["subs"]), t["mentions"])
        if not best or rank > best[0]:
            best = (rank, key)
    return best[1] if best else None


# =============================== 4. PLAN ===============================
EDITORIAL = f"""EDITORIAL STANDARD:
- The listener: {LISTENER_PROFILE}
- Pitch: a smart engineer outside computer science. Define every CS/ML term the first time, in plain words,
  ideally with an analogy from mechanical engineering, control systems, optimisation, materials, or
  manufacturing quality. One idea at a time; never stack jargon. Prefer the mechanism over the buzzword.
- Reddit is the starting point, not the authority. Posts and comments are often wrong, exaggerated or
  missing context. Separate: what the post claims / what the community argues / what the evidence shows.
- Paraphrase commenters; never use usernames ("one commenter who runs models at home argued...").
- No hype words ("revolutionary", "game-changer", "unlocks"); no predictions presented as facts.
- Your own training knowledge may be out of date. For anything recent, rely only on the SOURCES given.
  Never invent numbers, names, dates or quotes. If unsure, say it is unclear."""


def candidate_listing(stories, n=30):
    rows = []
    for s in stories[:n]:
        flags = ", ".join(x for x in (s.get("level", ""), "claims to check" if s.get("needs_check") else "",
                                      "hype" if s.get("hype") else "") if x)
        rows.append(f"[{s['id']}] ({', '.join(sorted(set(s['subs'])))}; {len(s['comments'])} comments; {flags}) "
                    f"{s['title']}" + (f" :: {s['text'][:220]}" if s["text"] else ""))
    return "\n".join(rows)


def plan_regular(stories, edition, state):
    if edition == "morning":
        shape = f"""MORNING EDITION - "the thread vs the evidence". About {TARGET_WORDS} words in total:
- 4 THREAD segments (format "thread", ~700 words each): the most worthwhile discussions. Prefer threads
  with real disagreement or checkable claims, and a spread of subjects (models/tools, research,
  industry, society). Avoid four stories about the same company.
- 1 QUICKFIRE segment (format "quickfire", ~500 words) covering 3-5 smaller threads in brief."""
    else:
        shape = f"""EVENING EDITION - calmer and deeper. About {TARGET_WORDS} words in total:
- 1 CONCEPT segment (format "concept", ~900 words): one idea that keeps coming up in these threads or past
  episodes (e.g. quantization, context windows, RLHF, mixture of experts, agents, tokenization), explained
  from first principles with engineering analogies, how it developed, and how it shows up in this week's
  threads. Put the term in "concept". item_ids = threads where it appears.
- 2-3 THREAD segments (format "thread", ~700 words): follow-ups on stories from past episodes, or the
  strongest new discussions."""
    prompt = f"""You are the editor of "{SHOW_TITLE}", a two-host podcast built from AI discussions on Reddit.

{EDITORIAL}

{shape}

PAST EPISODES (most recent last; follow developing stories, never repeat one without new information):
{memory_text(state)}

{feedback_text(state)}

CANDIDATE THREADS (best first; same story in several communities already merged):
{candidate_listing(stories)}

Return JSON only:
{{"theme": "<one line>",
  "segments": [{{"title": "<short spoken title>", "format": "thread|quickfire|concept", "concept": "<term or empty>",
                 "item_ids": [<ids>], "angle": "<what to explain and why it matters to {LISTENER}>",
                 "claims_to_check": ["<claim from the thread>", "..."], "connects_to": "<past episode or empty>",
                 "words": <number>}}]}}"""
    text, _ = llm(prompt, json_mode=True, label="editor")
    plan = as_obj(parse_json(text), "segments")
    ids = {s["id"] for s in stories}
    segs = []
    for s in plan.get("segments", []):
        s["item_ids"] = [i for i in s.get("item_ids", []) if isinstance(i, int) and i in ids]
        if s["item_ids"] or s.get("format") == "concept":
            s["words"] = int(s.get("words") or 700)
            segs.append(s)
    if not segs:
        raise RuntimeError("Editor returned no usable segments")
    return plan.get("theme", ""), segs


def plan_deep_dive(topic_key, state, stories):
    t = state["topics"][topic_key]
    today_ids = [s["id"] for s in stories if s.get("topic") == topic_key][:4]
    base = {"item_ids": today_ids, "format": "deepdive", "topic": topic_key, "connects_to": ""}
    parts = [
        ("Why everyone is talking about it", "Walk through how the discussion built up over the past days across "
         "communities: what people claimed, what they argued about, what changed.", 900),
        ("The foundations, from scratch", "Explain the underlying idea from first principles for an engineer "
         "outside CS: the problem it solves, the mechanism, analogies, and the key terms.", 1250),
        ("How we got here", "The history: earlier approaches, key papers or releases, what each step fixed and "
         "what it broke. Only use facts you are sure of or that are in the sources.", 1250),
        ("Reality check", "What the community gets right and wrong, the actual evidence today, the honest "
         "limitations, and open questions to watch - no predictions.", 1100),
    ]
    segs = [dict(base, title=f"{t['name']}: {title}", angle=angle, words=w) for title, angle, w in parts]
    return f"Deep dive: {t['name']}", segs


# =============================== 5. RESEARCH ===============================
SKIP_DOMAINS = ("i.redd.it", "v.redd.it", "reddit.com", "x.com", "twitter.com", "youtube.com", "youtu.be",
                "imgur.com", "instagram.com", "tiktok.com")


def fetch_article(url, limit=7000):
    if not url or any(d in urlparse(url).netloc for d in SKIP_DOMAINS):
        return ""
    if "arxiv.org/pdf/" in url:
        url = url.replace("/pdf/", "/abs/").removesuffix(".pdf")
    try:
        r = requests.get(url, headers=UA, timeout=25)
        r.raise_for_status()
        if "html" not in r.headers.get("content-type", "html"):
            return ""
        if not r.encoding or r.encoding.lower() == "iso-8859-1":
            r.encoding = r.apparent_encoding or "utf-8"
        text = trafilatura.extract(r.text, include_comments=False, include_tables=False) if trafilatura else ""
        if not text:
            text = strip_html(re.sub(r"(?is)<(script|style|nav|footer|header).*?</\1>", " ", r.text))
        return fix_text(text)[:limit]
    except Exception as e:
        return f"(could not fetch the linked page: {str(e)[:80]})"


def sources_block(items, topic=None):
    blocks = []
    for s in items:
        if "evidence" not in s:
            s["evidence"] = fetch_article(s.get("link"))
        comments = "\n".join(f"  - {c[:500]}" for c in s["comments"][:14])
        blocks.append(f"THREAD: {s['title']}\nCommunities: {', '.join(sorted(set(s['subs'])))}\n"
                      f"Post text: {s['text'][:2000] or '(link post, no text)'}\n"
                      f"Top comments:\n{comments or '  (none collected)'}\n"
                      f"Linked source: {s.get('link') or 'none'}\n"
                      f"Linked source text: {s['evidence'][:6000] or '(not available)'}")
    if topic:
        refs = "\n".join(f"- {r['date']}: {r['title']}" + (f" | comment: {r['comment']}" if r.get("comment") else "")
                         for r in topic["refs"])
        blocks.append(f"HOW THE TOPIC BUILT UP (threads over the past days, {len(topic['days'])} days, "
                      f"communities: {', '.join(topic['subs'])}):\n{refs}")
        for r in topic["refs"][-6:]:
            if r.get("link"):
                ev = fetch_article(r["link"], 4000)
                if ev:
                    blocks.append(f"SOURCE linked from an earlier thread ({r['title']}):\n{ev}")
    return "\n\n-----\n\n".join(blocks)


# =============================== 6. WRITE ===============================
A, B = HOST_A["name"], HOST_B["name"]
LINE_RE = re.compile(rf"^\s*\**\s*({A}|{B})\s*\**\s*:\s*(.+)$", re.I)
FORMATS = {
    "thread": f"""Structure (keep it conversational, not a checklist):
1. The post: what was posted and why people cared.
2. The thread: the strongest arguments on different sides, paraphrased.
3. The evidence: what the linked source actually says; check the claims listed above against it.
4. The verdict: does the claim hold up, partly hold up, not hold up, or is it too early to tell - and why.""",
    "quickfire": "Cover each thread briefly: one or two exchanges each - what it is, and one honest sentence on "
                 "whether it holds up. Clear verbal transitions between items.",
    "concept": "Teach the concept from first principles: the problem, the mechanism, an engineering analogy, how "
               "the idea developed, and how it appears in the listed threads. Check understanding with a quick "
               "recap at the end.",
    "deepdive": "This is one part of a multi-part deep dive. Go slower and deeper than a news segment, but keep "
                "the listener's level. Use analogies, recap key ideas, and connect to the earlier parts.",
}


def write_segment(seg, items, state, position, total, theme):
    target = int(seg["words"] * 1.15)
    fmt = seg.get("format", "thread")
    topic = state["topics"].get(seg.get("topic", "")) if fmt == "deepdive" else None
    claims = "; ".join(seg.get("claims_to_check") or []) or "(identify them from the thread)"
    prompt = f"""You are writing one segment of "{SHOW_TITLE}", a two-host audio podcast built from AI
discussions on Reddit, for {LISTENER} to hear while driving.
Hosts:
- {A}: the anchor. Brings the community's view and asks the questions a curious engineer would ask.
- {B}: the analyst. Explains how things work and weighs claims against the evidence.

{EDITORIAL}

Today's theme: {theme}
Segment {position} of {total}: "{seg['title']}"  (format: {fmt}{', concept: ' + seg['concept'] if seg.get('concept') else ''})
Angle: {seg.get('angle', '')}
Claims to check: {claims}
{('Follows up on: ' + seg['connects_to']) if seg.get('connects_to') else ''}
{FORMATS.get(fmt, FORMATS['thread'])}

PAST EPISODES (mention one only if it genuinely connects):
{memory_text(state, 5)}

{feedback_text(state, short=True)}

SOURCES:
{sources_block(items, topic)}

WRITE {target} to {int(target * 1.2)} words of natural spoken dialogue (do not stop early).
FORMAT - every line starts with a speaker name and a colon:
{A}: ...
{B}: ...
RULES: real conversation with short and longer turns; at most two numbers per turn; no greeting or sign-off;
{"start directly with the story" if position == 1 else "open with a one-line transition"}; no markdown,
stage directions, URLs or usernames; write numbers and abbreviations as a presenter would say them."""
    text, model = llm(prompt, min_words=150, label=f"segment {position}")
    USED_BY[nice_model(model)] += 1
    return parse_dialogue(text)


def parse_dialogue(text):
    turns = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = LINE_RE.match(line)
        if m:
            turns.append([A if m.group(1).lower() == A.lower() else B, m.group(2).strip()])
        elif turns and not line.startswith(("#", "[", "(")):
            turns[-1][1] += " " + line
    return [(s, clean_speech(t)) for s, t in turns if clean_speech(t)]


def clean_speech(t):
    t = fix_text(t)
    t = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", t)
    t = re.sub(r"\[\d+(?:[,\s\-]+\d+)*\]", "", t)
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"\bu/[A-Za-z0-9_-]+", "a commenter", t)
    t = re.sub(r"\((?:laughs?|pause|music|sfx)[^)]*\)", "", t, flags=re.I)
    t = t.replace("**", "").replace("__", "").replace("*", "").replace("`", "").replace("#", "")
    t = re.sub(r"\br/([A-Za-z0-9_]+)", r"the \1 subreddit", t)
    t = re.sub(r"\bet al\.", "and colleagues", t)
    t = re.sub(r"\be\.g\.", "for example", t)
    t = re.sub(r"\bi\.e\.", "that is", t)
    t = t.replace(" & ", " and ")
    return re.sub(r"\s+", " ", t).strip()


def turns_words(turns):
    return sum(words(t) for _, t in turns)


def write_bookends(state, segments_written, theme, edition, now, deep_dive, source_note):
    outline = "\n".join(f"- {s['title']}: {s.get('angle', '')}" for s, _ in segments_written)
    prompt = f"""You write the opening and closing for "{SHOW_TITLE}" (hosts {A} and {B}).
The listener is {LISTENER}; he listens in the car. Today is {now:%A, %d %B %Y}, {edition} edition.
{'This episode is a DEEP DIVE: ' + theme if deep_dive else 'Theme: ' + theme}
Where today's material came from: {source_note}

TODAY'S SEGMENTS:
{outline}

PAST EPISODES (most recent last):
{memory_text(state)}

{feedback_text(state)}

Return JSON only:
{{"greeting": [["{A}", "..."], ["{B}", "..."], ...],
  "closing": [["{A}", "..."], ["{B}", "..."]],
  "summary": "<3-4 sentence factual summary of today's episode for future episodes>",
  "threads": ["<story to watch for follow-ups>", "..."]}}

Greeting: 90-150 words, 4-6 turns. Greet {LISTENER} by name naturally. Link to past episodes only where it
truly connects. Briefly preview the segments. If there is recent feedback, say in one sentence what changed
because of it. {'For a deep dive, say why this topic is ready now (it built up over several days).' if deep_dive else ''}
Vary the wording every time. Closing: 2-3 short turns, 30-60 words, no hype."""
    text, _ = llm(prompt, json_mode=True, label="greeting")
    data = as_obj(parse_json(text), "greeting")

    def norm(lst):
        out = []
        for row in lst or []:
            if isinstance(row, (list, tuple)) and len(row) == 2:
                txt = clean_speech(str(row[1]))
                if txt:
                    out.append((A if str(row[0]).strip().lower() == A.lower() else B, txt))
        return out
    greeting = norm(data.get("greeting")) or [(A, f"Good {edition}, {LISTENER}. This is {SHOW_TITLE}.")]
    closing = norm(data.get("closing")) or [(A, "That's the show."), (B, "See you next time.")]
    return greeting, closing, data.get("summary", theme), [str(t) for t in data.get("threads", [])][:5]


# =============================== 7. VOICE ===============================
class TTSQuotaError(Exception):
    pass


PACE_NOTE, EDGE_RATE = "", "+0%"


def ffmpeg_to_pcm(data):
    res = subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-i", "pipe:0",
                          "-f", "s16le", "-ar", str(RATE), "-ac", "1", "pipe:1"], input=data, capture_output=True)
    if res.returncode != 0:
        raise RuntimeError(f"ffmpeg decode failed: {res.stderr[:200]}")
    return res.stdout


def pcm_from_audio(raw):
    if raw[:4] == b"RIFF":
        with wave.open(io.BytesIO(raw)) as w:
            pcm, fmt = w.readframes(w.getnframes()), (w.getframerate(), w.getnchannels(), w.getsampwidth())
        return pcm if fmt == (RATE, 1, 2) else ffmpeg_to_pcm(raw)
    return raw


def silence(sec):
    return b"\x00\x00" * int(RATE * sec)


def _extract_audio_b64(data):
    for step in reversed(data.get("steps", []) or []):
        for c in reversed(step.get("content", []) or []):
            if c.get("type") == "audio" and c.get("data"):
                return c["data"]
    for o in data.get("outputs", []) or []:
        if o.get("type") == "audio" and o.get("data"):
            return o["data"]
    for cand in data.get("candidates", []) or []:
        for p in cand.get("content", {}).get("parts", []):
            if p.get("inlineData", {}).get("data"):
                return p["inlineData"]["data"]
    return None


def gemini_tts_chunk(turns):
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}
    style = {A: HOST_A["style"] + PACE_NOTE, B: HOST_B["style"] + PACE_NOTE}
    interactions = {
        "model": GEMINI_TTS_MODEL,
        "input": [{"type": "user_input", "content": [
            {"type": "text", "text": t, "annotations": [{"type": "speech_metadata", "speaker": s, "style": style[s]}]}
            for s, t in turns]}],
        "response_format": {"type": "audio"},
        "generation_config": {"speech_config": {"mode": "conversational", "speakers": [
            {"speaker": A, "voice": HOST_A["gemini"]}, {"speaker": B, "voice": HOST_B["gemini"]}]}},
    }
    legacy = {
        "contents": [{"parts": [{"text": f"TTS the following conversation. {A} is a {style[A]}; {B} is a {style[B]}.\n\n"
                                         + "\n".join(f"{s}: {t}" for s, t in turns)}]}],
        "generationConfig": {"responseModalities": ["AUDIO"], "speechConfig": {"multiSpeakerVoiceConfig": {
            "speakerVoiceConfigs": [
                {"speaker": A, "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": HOST_A["gemini"]}}},
                {"speaker": B, "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": HOST_B["gemini"]}}}]}}},
    }
    attempts = [("https://generativelanguage.googleapis.com/v1beta/interactions", interactions),
                (f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_TTS_MODEL}:generateContent", legacy)]
    expected = turns_words(turns) / 2.6
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
                    last = f"audio too short ({secs:.0f}s for ~{expected:.0f}s)"
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
            break
    raise RuntimeError(f"Gemini TTS failed: {last}")


_FISH = None


def fish_voices():
    """Two Fish Audio voice ids: from FISH_VOICE_A/B, else from OpenRouter's supported_voices list."""
    global _FISH
    if _FISH is not None:
        return _FISH
    _FISH = FISH_VOICES if len(FISH_VOICES) == 2 else []
    if not _FISH and OPENROUTER_API_KEY:
        try:
            data = requests.get("https://openrouter.ai/api/v1/models?output_modalities=speech", timeout=30,
                                headers={**UA, "Authorization": f"Bearer {OPENROUTER_API_KEY}"}).json().get("data", [])
            m = next((x for x in data if x.get("id", "").startswith(FISH_MODEL.split(":")[0])), None)
            voices = (m or {}).get("supported_voices") or []
            norm = []
            for v in voices:
                if isinstance(v, str):
                    norm.append({"id": v, "text": v.lower()})
                elif isinstance(v, dict):
                    vid = v.get("id") or v.get("voice") or v.get("name")
                    norm.append({"id": vid, "text": json.dumps(v).lower()})
            en = [v for v in norm if v["id"] and ("en" in v["text"] or "english" in v["text"])] or norm
            fem = next((v["id"] for v in en if "female" in v["text"] or "woman" in v["text"]), None)
            male = next((v["id"] for v in en if re.search(r"\bmale\b|\bman\b", v["text"])), None)
            picks = [fem, male] if fem and male else [v["id"] for v in en[:2]]
            _FISH = [p for p in picks if p][:2]
            log(f"  Fish Audio voices: {_FISH or 'none found'} (set FISH_VOICE_A/B to choose)")
        except Exception as e:
            log(f"  Fish Audio voice list failed: {e}")
    return _FISH


def fish_tts_chunk(turns):
    voices = fish_voices()
    if len(voices) < 2:
        raise RuntimeError("no Fish Audio voices available")
    vmap = {A: voices[0], B: voices[1]}
    pcm = bytearray()
    for spk, text in turns:
        for attempt in range(1, 4):
            r = requests.post("https://openrouter.ai/api/v1/audio/speech", timeout=(20, 180),
                              json={"model": FISH_MODEL, "input": text, "voice": vmap[spk], "response_format": "mp3"},
                              headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}",
                                       "HTTP-Referer": PAGES_URL, "X-Title": SHOW_TITLE})
            if r.status_code == 200 and r.content:
                pcm += ffmpeg_to_pcm(r.content) + silence(0.25)
                break
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 3:
                time.sleep(30 if r.status_code == 429 else 10 * attempt)
                continue
            raise RuntimeError(f"Fish Audio HTTP {r.status_code}: {r.text[:160]}")
        time.sleep(3.2)  # free-model limit: 20 requests/minute
    return bytes(pcm)


async def _edge_turn(text, voice):
    for attempt in range(1, 5):
        try:
            buf = bytearray()
            async for part in edge_tts.Communicate(text, voice, rate=EDGE_RATE).stream():
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
        pcm += ffmpeg_to_pcm(asyncio.run(_edge_turn(text, voice[spk]))) + silence(0.25)
    return bytes(pcm)


ENGINE_NAMES = {"gemini": "Gemini TTS", "fish": "Fish Audio (free)", "edge": "Edge TTS"}


def voice_episode(sections, state, edition):
    flat = [(label, spk, text) for label, turns in sections for spk, text in turns]
    chunks, cur, n = [], [], 0
    for row in flat:
        w = words(row[2])
        if cur and n + w > TTS_CHUNK_WORDS:
            chunks.append(cur)
            cur, n = [], 0
        cur.append(row)
        n += w
    if cur:
        chunks.append(cur)

    engines = ["gemini", "fish", "edge"] if TTS_ENGINE == "auto" else [TTS_ENGINE]
    for engine in engines:
        if engine == "gemini":
            if not GEMINI_API_KEY:
                continue
            day_ago = (dt.datetime.now(TZ) - dt.timedelta(hours=24)).isoformat()
            state["tts_log"] = [t for t in state.get("tts_log", []) if t >= day_ago]
            used = len(state["tts_log"])
            reserve = len(chunks) if edition == "morning" else 0   # keep the best voices for the evening
            if TTS_ENGINE == "auto" and used + len(chunks) + reserve > GEMINI_TTS_DAILY:
                log(f"  Gemini TTS: {used} used in 24 h, need {len(chunks)}"
                    f"{f' + {reserve} kept for the evening' if reserve else ''} (limit {GEMINI_TTS_DAILY}) -> next voice")
                continue
        if engine == "fish" and not OPENROUTER_API_KEY:
            continue
        try:
            pcm, marks, seen_labels = bytearray(), [], set()
            for i, chunk in enumerate(chunks, 1):
                turns = [(spk, text) for _, spk, text in chunk]
                log(f"  {engine} TTS chunk {i}/{len(chunks)} ({turns_words(turns)} words)")
                if engine == "gemini":
                    audio = gemini_tts_chunk(turns)
                    state["tts_log"].append(dt.datetime.now(TZ).isoformat())
                elif engine == "fish":
                    audio = fish_tts_chunk(turns)
                else:
                    audio = edge_tts_chunk(turns)
                start, total_w, acc = len(pcm) / BYTES_PER_SEC, max(1, turns_words(turns)), 0
                for label, _, text in chunk:
                    if label not in seen_labels:
                        seen_labels.add(label)
                        marks.append((label, start + (acc / total_w) * len(audio) / BYTES_PER_SEC))
                    acc += words(text)
                pcm += audio + silence(0.4)
                if engine == "gemini":
                    time.sleep(8)
            return bytes(pcm), marks, engine
        except TTSQuotaError as e:
            state["tts_log"] = state.get("tts_log", []) + [dt.datetime.now(TZ).isoformat()] * GEMINI_TTS_DAILY
            log(f"  Gemini TTS quota reached -> next voice | {str(e)[:160]}")
        except Exception as e:
            if engine == engines[-1]:
                raise
            log(f"  {engine} TTS failed ({str(e)[:200]}) -> next voice")
            gh_note("warning", f"{ENGINE_NAMES[engine]} failed", f"{str(e)[:300]} - using the next voice option.")
    raise RuntimeError("No TTS engine succeeded")


def encode_mp3(pcm, out_path):
    res = subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-y", "-f", "s16le", "-ar", str(RATE),
                          "-ac", "1", "-i", "pipe:0", "-codec:a", "libmp3lame", "-b:a", "64k", str(out_path)],
                         input=pcm, capture_output=True)
    if res.returncode != 0:
        raise RuntimeError(f"ffmpeg encode failed: {res.stderr[:200]}")


# =============================== 8. QUIZ + PUBLISH ===============================
def make_quiz(written, theme):
    script = "\n\n".join(f"[SEGMENT: {s['title']}]\n" + "\n".join(f"{spk}: {t}" for spk, t in turns)
                         for s, turns in written)
    prompt = f"""Write a short quiz for {LISTENER} about today's episode of "{SHOW_TITLE}".
Theme: {theme}
Listener: {LISTENER_PROFILE}

EPISODE SCRIPT:
{script[:60000]}

Return JSON only:
{{"questions": [{{"q": "...", "options": ["...", "...", "...", "..."], "answer": <index 0-3>,
                 "explain": "<1-2 sentences>", "segment": "<segment title>"}}]}}
Rules: 4 questions, one per major segment. Test understanding of how things work, why a claim did or did not
hold up, and the concepts explained - not dates or trivia. Every answer must be stated in the script.
Plausible distractors; vary the position of the correct answer."""
    try:
        text, _ = llm(prompt, json_mode=True, label="quiz", role="helper")
        out = []
        for q in as_obj(parse_json(text), "questions").get("questions", [])[:5]:
            opts = [str(o) for o in q.get("options", [])][:4]
            ans = int(q.get("answer", -1))
            if q.get("q") and len(opts) >= 3 and 0 <= ans < len(opts):
                out.append({"q": str(q["q"]), "options": opts, "answer": ans,
                            "explain": str(q.get("explain", "")), "segment": str(q.get("segment", ""))})
        return out
    except Exception as e:
        log(f"  quiz skipped: {e}")
        return []


def write_quiz_files(keep, entry, transcript_text):
    TRANSCRIPT_DIR.mkdir(exist_ok=True)
    (TRANSCRIPT_DIR / (Path(entry["file"]).stem + ".txt")).write_text(transcript_text, encoding="utf-8")
    old = json.loads(QUIZ_FILE.read_text(encoding="utf-8")).get("episodes", []) if QUIZ_FILE.exists() else []
    files = {e["file"] for e in keep}
    eps = [e for e in old if e["file"] in files and e["file"] != entry["file"]] + [entry]
    eps.sort(key=lambda e: e["published"], reverse=True)
    QUIZ_FILE.write_text(json.dumps({"show": SHOW_TITLE, "listener": LISTENER, "repo": REPO_FULL, "episodes": eps},
                                    indent=1, ensure_ascii=False), encoding="utf-8")
    for t in TRANSCRIPT_DIR.glob("*.txt"):
        if t.stem + ".mp3" not in files:
            t.unlink()


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
    cover = next((n for n in ("artwork.jpg", "cover.jpg", "cover.png") if (ROOT / n).exists()), None)
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
    global PACE_NOTE, EDGE_RATE
    now = dt.datetime.now(TZ)
    edition = os.getenv("EDITION", "auto").strip().lower()
    if edition not in ("morning", "evening"):
        edition = "morning" if now.hour < 12 else "evening"
    date_key = now.strftime("%Y-%m-%d")
    force, dry = env_true("FORCE"), env_true("DRY_RUN")
    log(f"{SHOW_TITLE} | {edition} {date_key} | force={force} dry_run={dry} | TTS={TTS_ENGINE}")

    state = load_state()
    SPEND.extend(state.get("or_spend", []))
    if any(e["date"] == date_key and e["edition"] == edition for e in state["episodes"]) and not force:
        log("This edition already exists - nothing to do (backup run).")
        return

    close_issues = [] if dry else ingest_feedback(state)
    shift = pace_shift(state)
    if shift:
        PACE_NOTE = "; slightly slower, unhurried pace" if shift < 0 else "; slightly brisker pace"
        EDGE_RATE = "-8%" if shift < 0 else "+8%"

    log("1/8 Collecting discussions")
    snap, source_note = load_snapshot()
    posts = snap["posts"] if snap else []
    if not posts:
        log(f"  {source_note}")
        direct = direct_reddit()
        if direct and direct["posts"]:
            posts, source_note = direct["posts"], "Reddit (collected directly)"
    if len(posts) < 15:
        source_note = (source_note + " + Hacker News") if posts else f"Hacker News (no Reddit data: {source_note})"
        posts += hn_threads()
        if not snap:
            gh_note("warning", "No fresh Reddit data", "Using Hacker News discussions. Check the office-PC collector.")
    stories = build_stories(posts, state)
    if len(stories) < 5:
        raise RuntimeError(f"Only {len(stories)} new discussions found.")
    log(f"  {len(stories)} distinct stories from {len(posts)} threads ({source_note})")

    log("2/8 Scoring threads")
    stories = score_stories(stories)
    by_id = {s["id"]: s for s in stories}

    log("3/8 Tracking topics")
    update_topics(state, stories, date_key)
    deep = ready_topic(state, date_key) if edition == "evening" else None
    if env_true("DEEP_DIVE") and not deep and state["topics"]:   # manual "deep dive now" from the workflow
        deep = max(state["topics"], key=lambda k: (len(state["topics"][k]["days"]), state["topics"][k]["mentions"]))

    log("4/8 Planning")
    if deep:
        theme, plan = plan_deep_dive(deep, state, stories)
        log(f"  DEEP DIVE: {state['topics'][deep]['name']} ({len(state['topics'][deep]['days'])} days, "
            f"{len(state['topics'][deep]['subs'])} communities)")
        min_words = DEEP_DIVE_WORDS - 600
    else:
        theme, plan = plan_regular(stories, edition, state)
        min_words = MIN_WORDS
    for s in plan:
        log(f"  - [{s.get('format')}] {s['title']} ({s['words']} words) items={s['item_ids']}")

    log("5/8 + 6/8 Researching and writing")
    written, used = [], set()
    for i, seg in enumerate(plan, 1):
        items = [by_id[j] for j in seg["item_ids"] if j in by_id]
        try:
            turns = write_segment(seg, items, state, i, len(plan), theme)
        except RuntimeError as e:
            log(f"  segment skipped: {e}")
            continue
        if turns_words(turns) < 120:
            continue
        written.append((seg, turns))
        used.update(seg["item_ids"])
        log(f"  running total: {sum(turns_words(t) for _, t in written)} words")

    extra = 0
    while sum(turns_words(t) for _, t in written) < min_words and extra < 3:
        spare = [s for s in stories if s["id"] not in used][:1]
        if not spare:
            break
        extra += 1
        seg = {"title": spare[0]["title"][:80], "format": "thread", "item_ids": [spare[0]["id"]],
               "angle": "What the thread claims, what the community argues, and whether it holds up.",
               "words": min(850, max(500, min_words - sum(turns_words(t) for _, t in written)))}
        log(f"  script short - adding: {seg['title']}")
        try:
            written.append((seg, write_segment(seg, spare, state, len(written) + 1, len(written) + 1, theme)))
        except RuntimeError as e:
            log(f"  extra segment failed: {e}")
        used.update(seg["item_ids"])

    total_words = sum(turns_words(t) for _, t in written)
    if total_words < min_words * 0.6:
        raise RuntimeError(f"Script too short ({total_words} words) - not publishing.")

    log("7/8 Greeting and voices")
    greeting, closing, summary, threads = write_bookends(state, written, theme, edition, now, bool(deep), source_note)
    sections = [("Welcome", greeting)] + [(s["title"], t) for s, t in written] + [("Close", closing)]
    pcm, marks, engine = voice_episode(sections, state, edition)
    BUILD_DIR.mkdir(exist_ok=True)
    filename = f"ai_{date_key}_{edition}_{now:%H%M}.mp3"
    mp3 = BUILD_DIR / filename
    encode_mp3(pcm, mp3)
    duration, size = int(len(pcm) / BYTES_PER_SEC), mp3.stat().st_size
    writer = USED_BY.most_common(1)[0][0] if USED_BY else "?"
    log(f"  {filename}: {fmt_ts(duration)}, {size / 1e6:.1f} MB, writer={dict(USED_BY)}, voices={engine}")

    log("8/8 Publishing")
    pretty = f"{now:%A}, {now.day} {now:%B %Y}"
    notes = [f"{'Deep dive' if deep else edition.title() + ' edition'} - {pretty}", theme, ""]
    for (label, start), (seg, _) in zip(marks[1:], written):
        notes.append(f"{fmt_ts(start)}  {label}")
        for j in seg["item_ids"]:
            if j in by_id:
                s = by_id[j]
                notes.append(f"   {', '.join(sorted(set(s['subs'])))}: {s['threads'][0]}")
                if s.get("link"):
                    notes.append(f"   Source: {s['link']}")
    notes += ["", f"Quiz and feedback: {PAGES_URL}/quiz.html",
              f"Written by: {writer} · Voices: {ENGINE_NAMES[engine]} · Discussions: {source_note}"]
    kind = "Deep dive" if deep else edition.title()
    episode = {"file": filename, "date": date_key, "edition": edition,
               "title": f"{kind} - {now.day} {now:%b}: {(state['topics'][deep]['name'] if deep else plan[0]['title'])}"[:120],
               "published": now.isoformat(), "pubDate": email.utils.format_datetime(now),
               "size": size, "duration": duration, "description": "\n".join(notes).strip()}
    cutoff = (now - dt.timedelta(days=RETENTION_DAYS)).isoformat()
    keep = [e for e in state["episodes"] if e["published"] >= cutoff
            and not (e["date"] == date_key and e["edition"] == edition)] + [episode]

    label = f"{now:%A %d %b} {edition}{' deep dive' if deep else ''}"
    mem_cut = (now - dt.timedelta(days=MEMORY_DAYS)).isoformat()
    memory = [m for m in state["memory"] if m["published"] >= mem_cut
              and not (m.get("date") == date_key and m.get("edition") == edition)]
    memory.append({"label": label, "date": date_key, "edition": edition, "published": now.isoformat(),
                   "file": filename, "writer": writer, "voices": ENGINE_NAMES[engine],
                   "summary": summary, "threads": threads, "segments": [s["title"] for s, _ in written]})
    seen_cut = (now - dt.timedelta(days=SEEN_DAYS)).strftime("%Y-%m-%d")
    seen = [s for s in state["seen"] if s.get("date", "") >= seen_cut]
    for j in used:
        if j in by_id:
            seen += [{"date": date_key, "link": u} for u in by_id[j]["threads"]]
            if by_id[j].get("link"):
                seen.append({"date": date_key, "link": by_id[j]["link"]})
    if deep:
        state["topics"][deep]["deep_dive"] = date_key

    if dry:
        log(f"  DRY_RUN: audio at {mp3}; upload skipped")
    else:
        ensure_release()
        gh("release", "upload", RELEASE_TAG, str(mp3), "--clobber")
        sync_release_assets({e["file"] for e in keep})

    fb_cut = (now - dt.timedelta(days=FEEDBACK_DAYS)).isoformat()
    day_ago = (now - dt.timedelta(hours=24)).isoformat()
    save_state({"episodes": keep, "memory": memory, "seen": seen,
                "feedback": [f for f in state["feedback"] if f.get("received", "") >= fb_cut],
                "tts_log": state.get("tts_log", []), "or_spend": [s for s in SPEND if s[0] >= day_ago],
                "topics": state["topics"]})

    log("  writing quiz")
    quiz = make_quiz(written, theme)
    transcript = "\n\n".join(f"## {lbl}\n" + "\n".join(f"{spk}: {t}" for spk, t in turns) for lbl, turns in sections)
    write_quiz_files(keep, {
        "file": filename, "title": episode["title"], "label": label, "date": date_key, "edition": edition,
        "published": now.isoformat(), "duration": duration, "theme": theme, "writer": writer,
        "voices": ENGINE_NAMES[engine], "deep_dive": bool(deep),
        "segments": [{"title": s["title"], "category": s.get("format", "")} for s, _ in written],
        "quiz": quiz, "transcript": f"transcripts/{Path(filename).stem}.txt"}, transcript)
    build_rss(keep)
    for n in close_issues:
        gh("issue", "close", str(n), "--comment", "Thanks - recorded. It will shape the next episodes.", check=False)
    log(f"Done. Feed: {PAGES_URL}/podcast.xml | OpenRouter spend last 24 h: ${spent_today():.3f}")
    gh_note("notice", "Episode ready", f"{filename} - {fmt_ts(duration)} - {len(written)} segments - {total_words} words "
                                        f"- writer: {writer} - voices: {ENGINE_NAMES[engine]} - source: {source_note}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"ERROR: {exc}")
        gh_note("error", "Briefing failed", exc)
        sys.exit(1)
