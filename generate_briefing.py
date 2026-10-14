#!/usr/bin/env python3
"""
Weights & Measures - a two-host podcast about what the AI community is discussing,
checked against the evidence. One run = one episode.

Pipeline
  1. COLLECT  - Reddit threads from the subreddits in subreddits.txt, gathered hourly through the day by
                the Collect Reddit workflow (about one request a minute; comments re-read once a thread
                has matured). Also: reddit/latest.json from the office-PC collector if present.
                Fallbacks: direct Reddit RSS, then Hacker News discussions.
  2. SCORE    - one batched call to a free model rates every thread (relevant? substantial? level?
                needs fact-checking? hype?). Duplicates across subreddits are merged.
                CHECK: each thread is matched against feeds.txt, Hugging Face Daily Papers, the
                primary source it links to, and Hacker News.
  3. TRACK    - threads are grouped into topics that build up over days. When a topic has
                come up on several days in several communities, the evening becomes a deep dive.
  4. PLAN     - morning: "the thread vs the evidence"; evening: follow-ups + a concept from
                scratch, or a deep dive.
  5. RESEARCH - the linked article / paper / repo behind each thread is fetched.
  6. WRITE    - two hosts (Sarah, Laura) in dialogue, pitched at an engineer outside CS.
  7. GREET    - personal greeting linked to past episodes; summary saved as memory.
  8. VOICE    - Fish Audio free (OpenRouter) -> Gemini TTS (free key) -> Edge voices.
  9. PUBLISH  - MP3 to the GitHub release, podcast.xml, quiz.json (quiz + daily digest) and transcript.

Zero cost by default: Gemini free key, then OpenRouter free models, then GitHub Models. One writer
per episode where possible. OpenRouter's free daily request limit is counted, with a reserve for writing.
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
import threading
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
try:
    import feedparser
except ImportError:  # optional: trusted-source cross-check
    feedparser = None

# =============================== SETTINGS ===============================
LISTENER = "Nikhil"
LISTENER_PROFILE = ("Nikhil has an M.Tech in mechanical engineering and works in medical-device quality. "
                    "He codes and follows AI as a hobby, reads research papers comfortably (not in computer "
                    "science), follows AI discussions on Reddit, and is curious about science, biology and "
                    "technology beyond AI.")
SHOW_TITLE = "Weights & Measures"
SHOW_DESC = ("AI and science, as they actually work. Two hosts take what Reddit's AI and science communities "
             "are discussing, check it against the evidence, and explain it for curious engineers - two "
             "short episodes on weekdays, two long deep dives on Sundays.")

HOST_A = {"name": "Sarah", "gemini": "Leda", "edge": "en-US-AvaNeural",
          "style": "young woman in her late twenties; energetic, upbeat and quick, smiling as she talks, bright "
                   "and wide awake, never flat or sleepy",
          "persona": "late twenties, grew up online and lives in these communities; high-energy, quick, "
                     "enthusiastic and funny, talks in short lively sentences with real reactions, "
                     "knows the slang and the drama, gets excited by new tools - but is honest when she was wrong "
                     "and genuinely wants to understand how things work"}
HOST_B = {"name": "Laura", "gemini": "Gacrux", "edge": "en-US-JennyNeural",
          "style": "woman in her late forties; warm, grounded and unhurried, with dry humour",
          "persona": "late forties, an engineer who has lived through several hype cycles; warm, dry sense of "
                     "humour, asks what was actually measured and how, explains mechanisms patiently - but is "
                     "not cynical, and says so when something really is new"}

# Episode length. Fish voices read about 155 words a minute. A weekday episode is a few topics of about seven
# minutes each; Sunday makes two long deep dives instead (no commute), sized to the free daily budget.
EPISODE_MINUTES = float(os.getenv("EPISODE_MINUTES") or 20)
SUNDAY_MINUTES = float(os.getenv("SUNDAY_MINUTES") or 45)
WORDS_PER_MIN = 155
SEGMENT_WORDS = 1150          # one topic, about seven minutes
FACT_CHECK = os.getenv("FACT_CHECK", "true").strip().lower() not in ("0", "false", "no")
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
# Free only by default: the Gemini API key's free quota, then OpenRouter's free models, then GitHub Models.
# A paid model is used only if you add it to WRITER_MODELS / HELPER_MODELS and set OPENROUTER_DAILY_USD above 0.
WRITER_CHAIN = os.getenv("WRITER_MODELS") or (
    "gemini:gemini-3.8-flash,gemini:gemini-3.7-flash,openrouter:free,gemini:gemini-3.5-flash-lite,github:auto")
HELPER_CHAIN = os.getenv("HELPER_MODELS") or (
    "gemini:gemini-3.5-flash-lite,gemini:gemini-3.1-flash-lite,openrouter:free,github:auto")
# OpenRouter free models (checked 12 Oct 2026). "openrouter:free" in a chain expands to these, then to any other
# free model OpenRouter lists. OPENROUTER_FREE_MODELS replaces both lists.
FREE_WRITERS = ["nvidia/nemotron-3-ultra-550b-a55b:free", "nvidia/nemotron-3-super-120b-a12b:free",
                "google/gemma-4-31b-it:free"]
FREE_HELPERS = ["nvidia/nemotron-3-super-120b-a12b:free", "google/gemma-4-31b-it:free",
                "google/gemma-4-26b-a4b-it:free"]
# Free models without a JSON mode: asked for JSON in the prompt only.
NO_JSON_MODE = ("nemotron-3-ultra", "nemotron-3.5-lightning", "nano-omni", "inkling", "laguna", "north-mini", "ling-")
# Scoring: each thread is judged on its own by these models (free chat models answer the scoring questions as
# JSON; dedicated paid decision models use OpenRouter's Decisions API). Threads they miss are scored in a batch.
DECISION_MODELS = [m.strip() for m in (os.getenv("DECISION_MODELS") or
                   "nvidia/nemotron-3-super-120b-a12b:free,google/gemma-4-31b-it:free").split(",") if m.strip()]
SCORING = (os.getenv("SCORING") or "auto").lower()      # auto | batch | decisions
OPENROUTER_DAILY_USD = float(os.getenv("OPENROUTER_DAILY_USD") or 0)   # spend cap for paid models
# OpenRouter's free models (text and Fish voices) share one daily request limit that resets at 00:00 UTC:
# 1000 a day on accounts that have bought at least $10 of credits, 50 otherwise. Writing keeps a reserve.
OPENROUTER_FREE_DAILY = int(os.getenv("OPENROUTER_FREE_DAILY") or 1000)
FREE_WRITER_RESERVE = int(os.getenv("FREE_WRITER_RESERVE") or 40)
GITHUB_TOKEN = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN", "")

GEMINI_TTS_MODEL = os.getenv("GEMINI_TTS_MODEL") or "gemini-3.8-flash-tts"
TTS_ENGINE = (os.getenv("TTS_ENGINE") or "auto").lower()   # auto | gemini | fish | edge
GEMINI_TTS_DAILY = int(os.getenv("GEMINI_TTS_DAILY") or 10)
TTS_CHUNK_WORDS = int(os.getenv("TTS_CHUNK_WORDS") or 700)
TTS_MAX_CHUNK_WORDS = int(os.getenv("TTS_MAX_CHUNK_WORDS") or 1100)
FISH_MODEL = os.getenv("FISH_MODEL") or "fish-audio/s2.1-pro-free:free"
# Playback speed per host after Fish renders it (pitch unchanged): Sarah a touch brisker so she sounds awake.
FISH_TEMPO = [float(os.getenv("FISH_TEMPO_A") or 1.08), float(os.getenv("FISH_TEMPO_B") or 1.0)]
# Fish Audio library voices (via OpenRouter's free Fish model): Sarah, Laura
FISH_VOICES = [os.getenv("FISH_VOICE_A") or "933563129e564b19a115bedd57b7406a",
               os.getenv("FISH_VOICE_B") or "e3cd384158934cc9a01029cd7d278634"]

ROOT = Path(__file__).resolve().parent
EPISODES_FILE = ROOT / "episodes.json"
PODCAST_FILE = ROOT / "podcast.xml"
QUIZ_FILE = ROOT / "quiz.json"
TRANSCRIPT_DIR = ROOT / "transcripts"
SNAPSHOT_FILE = ROOT / "reddit" / "latest.json"
INTERESTS_FILE = ROOT / "interests.txt"
POOL_FILE = ROOT / "reddit" / "pool.json"
FEEDS_FILE = ROOT / "feeds.txt"
SUBREDDITS_FILE = ROOT / "subreddits.txt"
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
FREE_LOG = []         # [utc_iso, ...] OpenRouter free-model requests (text and Fish) - one shared daily limit
FREE_DAY_OUT = [""]   # UTC date on which OpenRouter said the free daily limit was used up
WRITER_PICK = []      # the writer that last succeeded; later writing tries it first so the hosts sound the same


def utc_day():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def free_used():
    return sum(1 for t in FREE_LOG if t.startswith(utc_day()))


def free_allowed(n=1, role="writer"):
    """Writing may use the whole free limit; scoring, quiz, digest and voices leave a reserve for it."""
    if FREE_DAY_OUT[0] == utc_day():
        return False
    reserve = 0 if role == "writer" else FREE_WRITER_RESERVE
    return free_used() + n <= OPENROUTER_FREE_DAILY - reserve


def note_free(n=1):
    FREE_LOG.extend([dt.datetime.now(dt.timezone.utc).isoformat()] * n)


def free_limit_hit(status, err):
    if status == 429 and re.search(r"per[- ]?day|daily", err or "", re.I):
        if FREE_DAY_OUT[0] != utc_day():
            log(f"    OpenRouter free daily limit reached ({free_used()} requests counted today)")
        FREE_DAY_OUT[0] = utc_day()
        return True
    return False


def spent_today():
    cutoff = (dt.datetime.now(TZ) - dt.timedelta(hours=24)).isoformat()
    return sum(c for t, c in SPEND if t >= cutoff)


def nice_model(model):
    names = {"gemini-3.8-flash": "Gemini 3.8 Flash", "google/gemini-3.8-flash": "Gemini 3.8 Flash",
             "anthropic/claude-haiku-5.5": "Claude Haiku 5.5", "gemini-3.7-flash": "Gemini 3.7 Flash",
             "gemini-3.5-flash-lite": "Gemini 3.5 Flash-Lite", "gemini-3.1-flash-lite": "Gemini 3.1 Flash-Lite"}
    return names.get(model, model.split("/")[-1].replace(":free", " (free)"))


def _gemini_stream(model, prompt, json_mode, role="writer"):
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
        parts, finish = [], ""
        for line in r.iter_lines(decode_unicode=True):
            if not line or not line.startswith("data:"):
                continue
            chunk = json.loads(line[5:].strip())
            cand = (chunk.get("candidates") or [{}])[0]
            finish = cand.get("finishReason") or finish
            for p in cand.get("content", {}).get("parts", []):
                if p.get("text") and not p.get("thought"):
                    parts.append(p["text"])
        if finish and finish != "STOP":
            log(f"    {model}: stopped early ({finish})")
        return 200, "".join(parts).strip(), ""


_FREE = None
_LISTED = None


def listed_free():
    """Other free text models OpenRouter lists today - a safety net when the chosen ones stop being free."""
    global _LISTED
    if _LISTED is not None:
        return _LISTED
    _LISTED = []
    try:
        data = requests.get("https://openrouter.ai/api/v1/models", headers=UA, timeout=30).json()["data"]
        free = [m for m in data if m.get("id", "").endswith(":free") and (m.get("context_length") or 0) >= 64000
                and not re.search(r"safety|lyria|code|inkling|liquid|omni", m["id"])
                and "text" in ((m.get("architecture") or {}).get("output_modalities") or ["text"])]
        pref = ["nemotron", "gemma", "deepseek", "qwen", "kimi", "glm", "dots", "apodex", "llama", "mistral"]

        def rank(m):
            mid = m["id"].lower()
            return (next((i for i, p in enumerate(pref) if p in mid), len(pref)), -(m.get("context_length") or 0))
        _LISTED = [m["id"] for m in sorted(free, key=rank)]
    except Exception as e:
        log(f"  could not list OpenRouter models: {e}")
    return _LISTED


def free_models(role="writer"):
    global _FREE
    if _FREE is None:
        _FREE = {}
    if role not in _FREE:
        override = [m.strip() for m in os.getenv("OPENROUTER_FREE_MODELS", "").split(",") if m.strip()]
        chosen = override or (FREE_WRITERS if role == "writer" else FREE_HELPERS)
        extra = [] if override else [m for m in listed_free() if m not in chosen][:2]
        _FREE[role] = chosen + extra
        log(f"  OpenRouter free models ({role}): {', '.join(_FREE[role]) or 'none'}")
    return _FREE[role]


def _openrouter(model, prompt, json_mode, role="writer"):
    paid = not model.endswith(":free")
    if paid and spent_today() >= OPENROUTER_DAILY_USD:
        return 402, "", f"daily OpenRouter budget ${OPENROUTER_DAILY_USD:.2f} reached"
    if not paid and not free_allowed(1, role):
        return 409, "", "free daily requests kept for writing"   # 409: skip for this call only
    body = {"model": model, "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.6, "max_tokens": 12000, "usage": {"include": True}}
    if paid:
        body["reasoning"] = {"effort": "low"}
    if json_mode and not any(k in model for k in NO_JSON_MODE):
        body["response_format"] = {"type": "json_object"}
    r = requests.post("https://openrouter.ai/api/v1/chat/completions", json=body, timeout=(20, 400 if paid else 150),
                      headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}",
                               "HTTP-Referer": PAGES_URL, "X-Title": SHOW_TITLE})
    if not paid:
        note_free()
    if r.status_code != 200:
        err = r.text[:300].replace("\n", " ")
        if not paid and free_limit_hit(r.status_code, err):
            return 429, "", err
        return r.status_code, "", err
    data = r.json()
    if "error" in data:
        return 502, "", str(data["error"])[:300]
    cost = float((data.get("usage") or {}).get("cost") or 0)
    if cost:
        SPEND.append([dt.datetime.now(TZ).isoformat(), cost])
    msg = (data.get("choices") or [{}])[0].get("message", {}) or {}
    text = re.sub(r"<think>.*?</think>", "", msg.get("content") or "", flags=re.S).strip()
    return 200, text, ""


_GHM = None


def github_models():
    """GitHub Models: free with the workflow's own token (permission models: read), small daily limits and
    small prompts on the free tier, so it is the last fallback."""
    global _GHM
    if _GHM is not None:
        return _GHM
    override = os.getenv("GITHUB_MODELS", "").strip()
    if override:
        _GHM = [m.strip() for m in override.split(",") if m.strip()]
        return _GHM
    _GHM = []
    try:
        data = requests.get("https://models.github.ai/catalog/models", timeout=30,
                            headers={**UA, "Authorization": f"Bearer {GITHUB_TOKEN}",
                                     "Accept": "application/vnd.github+json"}).json()
        items = data if isinstance(data, list) else (data.get("models") or data.get("data") or [])
        text = [m for m in items if isinstance(m, dict) and m.get("id")
                and "text" in (m.get("supported_output_modalities") or ["text"])
                and "embed" not in m["id"].lower()]
        pref = ["deepseek", "gpt", "llama", "mistral", "phi"]

        def rank(m):
            mid = m["id"].lower()
            return (next((i for i, p in enumerate(pref) if p in mid), len(pref)),
                    -((m.get("limits") or {}).get("max_input_tokens") or 0))
        _GHM = [m["id"] for m in sorted(text, key=rank)][:2]
    except Exception as e:
        log(f"  could not list GitHub Models: {str(e)[:120]}")
    log(f"  GitHub Models: {', '.join(_GHM) or 'none'}")
    return _GHM


def _github(model, prompt, json_mode, role="writer"):
    body = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.6, "max_tokens": 4000}
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    r = requests.post("https://models.github.ai/inference/chat/completions", json=body, timeout=(20, 300),
                      headers={"Authorization": f"Bearer {GITHUB_TOKEN}", "Content-Type": "application/json",
                               "Accept": "application/vnd.github+json"})
    if r.status_code != 200:
        return r.status_code, "", r.text[:300].replace("\n", " ")
    msg = ((r.json().get("choices") or [{}])[0].get("message") or {})
    return 200, re.sub(r"<think>.*?</think>", "", msg.get("content") or "", flags=re.S).strip(), ""


PROVIDERS = {"gemini": _gemini_stream, "openrouter": _openrouter, "github": _github}


def expand_chain(chain, role="writer"):
    out = []
    for item in [c.strip() for c in chain.split(",") if c.strip()]:
        prov, _, model = item.partition(":")
        if prov == "gemini" and GEMINI_API_KEY:
            out.append(("gemini", model))
        elif prov == "openrouter" and OPENROUTER_API_KEY:
            out += [("openrouter", m) for m in free_models(role)] if model == "free" else [("openrouter", model)]
        elif prov == "github" and GITHUB_TOKEN:
            out += [("github", m) for m in github_models()] if model == "auto" else [("github", model)]
    return out


def llm(prompt, json_mode=False, min_words=0, label="", role="writer"):
    """Returns (text, model). role 'writer' = quality chain, 'helper' = free-first chain."""
    chain = expand_chain(WRITER_CHAIN if role == "writer" else HELPER_CHAIN, role)
    if not chain:
        raise RuntimeError("No LLM provider configured")
    if role == "writer" and WRITER_PICK and WRITER_PICK[0] in chain:
        chain.remove(WRITER_PICK[0])
        chain.insert(0, WRITER_PICK[0])
    errors = []
    for prov, model in chain:
        if model in DEAD:
            continue
        for attempt in (1, 2):
            t0 = time.time()
            try:
                status, text, err = PROVIDERS[prov](model, prompt, json_mode, role)
            except (requests.RequestException, ValueError, KeyError) as e:
                log(f"    {model}: connection problem ({str(e)[:120]})")
                errors.append(f"{model}: connection")
                time.sleep(8)
                continue
            if status == 200:
                text = fix_text(text)
                n = words(text)
                log(f"    {label} {prov}:{model}: {n} words in {time.time() - t0:.0f}s")
                if json_mode and text:
                    try:
                        parse_json(text)
                    except (ValueError, TypeError):
                        quick = attempt == 1 and time.time() - t0 < 90
                        log(f"    {model}: reply was not complete JSON" + ("; retrying" if quick else "; next model"))
                        errors.append(f"{model}: bad JSON")
                        if quick:
                            continue
                        break
                if text and n >= min_words:
                    if role == "writer":
                        WRITER_PICK[:] = [(prov, model)]
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


_PACE = threading.Lock()
_LAST_FREE = [0.0]


def free_pace(gap=3.2):
    """Free models allow about 20 requests a minute; keep parallel scoring under that."""
    with _PACE:
        wait = _LAST_FREE[0] + gap - time.time()
        if wait > 0:
            time.sleep(wait)
        _LAST_FREE[0] = time.time()


def decide_chat(model, state_obj, questions):
    """Ask a free chat model the scoring questions for one thread; answers in the Decisions API's shape."""
    asks = "\n".join(f'- "{k}" ({q["type"]}): {q["instructions"]} Criteria: {json.dumps(q["criteria"])}'
                     for k, q in questions.items())
    prompt = f"""Judge this AI discussion thread. Answer each question carefully, on its own merits.

THREAD:
{json.dumps(state_obj, ensure_ascii=False)}

QUESTIONS:
{asks}

Return JSON only, with one key per question:
- type "noul": a probability 0-1 that the answer is true
- type "score": an integer index into the criteria list (0 = first)
- type "choice": one of the criteria keys
Example: {{"relevant": 0.9, "substance": 2, "level": "technical", "needs_check": 0.7, "hype": 0.1}}"""
    for attempt in (1, 2):
        free_pace()
        try:
            status, text, err = _openrouter(model, prompt, True, "helper")
        except (requests.RequestException, ValueError) as e:
            status, text, err = 503, "", str(e)[:120]
        if status == 200 and text:
            try:
                a = parse_json(text)
            except (ValueError, TypeError):
                return None
            out = {}
            for k, q in questions.items():
                v = a.get(k) if isinstance(a, dict) else None
                if v is None:
                    continue
                try:
                    out[k] = ({"noul": max(0.0, min(1.0, float(v)))} if q["type"] == "noul" else
                              {"score": max(0, min(len(q["criteria"]) - 1, int(float(v))))} if q["type"] == "score" else
                              {"choice": str(v) if str(v) in q["criteria"] else next(iter(q["criteria"]))})
                except (TypeError, ValueError):
                    continue
            return out or None
        if status in (429, 503) and FREE_DAY_OUT[0] != utc_day() and attempt == 1:
            time.sleep(20)   # per-minute limit; wait and try once more
            continue
        if status in (400, 401, 402, 403, 404):
            log(f"    decision model {model}: HTTP {status} -> skipping | {err[:160]}")
            DEAD.add(model)
        return None
    return None


def decide(state_obj, questions):
    """Answers for one thread: free chat models via JSON, paid decision models via the Decisions API."""
    if not OPENROUTER_API_KEY:
        return None
    for model in DECISION_MODELS:
        if model in DEAD:
            continue
        free = model.endswith(":free")
        if free:
            if not free_allowed(1, "helper"):
                return None
            ans = decide_chat(model, state_obj, questions)
            if ans:
                return ans
            continue
        if OPENROUTER_DAILY_USD > 0 and spent_today() >= OPENROUTER_DAILY_USD:
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
                if OPENROUTER_DAILY_USD <= 0 and model not in DEAD:
                    log(f"    decision model {model} charges money -> dropped (OPENROUTER_DAILY_USD is 0)")
                    DEAD.add(model)
            return data.get("answers")
        if r.status_code in (400, 401, 402, 403, 404):
            log(f"    decision model {model}: HTTP {r.status_code} -> skipping | {r.text[:160]}")
            DEAD.add(model)
    return None


# =============================== STATE ===============================
def load_state():
    data = json.loads(EPISODES_FILE.read_text(encoding="utf-8")) if EPISODES_FILE.exists() else {}
    for k in ("episodes", "memory", "seen", "feedback", "tts_log", "or_spend", "free_log"):
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


def load_pool():
    """Threads gathered by the hourly collect_reddit workflow (comments re-read once a thread has matured)."""
    if not POOL_FILE.exists():
        return [], "no hourly Reddit pool yet"
    try:
        pool = json.loads(POOL_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return [], f"hourly Reddit pool unreadable ({e})"
    now = dt.datetime.now(dt.timezone.utc)

    def age_h(iso):
        try:
            return (now - dt.datetime.fromisoformat(str(iso).replace("Z", "+00:00"))).total_seconds() / 3600
        except ValueError:
            return 1e9
    age = age_h(pool.get("updated_at", ""))
    if age > 12:
        return [], f"hourly Reddit pool is {age:.0f} h old"
    posts = [p for p in (pool.get("posts") or {}).values()
             if age_h(p.get("date") or p.get("first_seen", "")) <= SNAPSHOT_MAX_AGE_H]
    with_c = sum(1 for p in posts if p.get("comments"))
    log(f"  hourly pool: {len(posts)} threads ({with_c} with comments), updated {age:.1f} h ago")
    return posts, f"Reddit, collected hourly ({len(posts)} threads)"


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


def load_subjects():
    """subreddits.txt third column: ai (default) or science."""
    out = {}
    if SUBREDDITS_FILE.exists():
        for line in SUBREDDITS_FILE.read_text(encoding="utf-8").splitlines():
            parts = [x.strip() for x in line.split("#", 1)[0].split("|")]
            if parts[0]:
                out[parts[0].removeprefix("r/").lower()] = (parts[2].lower() if len(parts) > 2 and parts[2] else "ai")
    return out


SUBJECTS = load_subjects()


def story_subject(s):
    subs = [SUBJECTS.get(x.lower(), "ai") for x in s["subs"]]
    return "ai" if "ai" in subs else (subs[0] if subs else "ai")


BOT_COMMENT = re.compile(r"I am a bot|performed automatically|AutoModerator|contact the moderators|"
                         r"^\s*(this|\^this|same|lol|lmao|\+1)\W*$", re.I)


def build_stories(posts, state):
    seen = {s["link"] for s in state["seen"]}
    stories = []
    for p in posts:
        if p["url"] in seen or (p.get("link") and p["link"] in seen):
            continue
        p = dict(p, title=fix_text(p["title"]), text=fix_text(p.get("text", "")),
                 comments=[fix_text(c) for c in p.get("comments", []) if not BOT_COMMENT.search(c)])
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
        s["subject"] = story_subject(s)
        s["comments"] = s["comments"][:24]
    return stories


# ======================= TRUSTED SOURCES (cross-check) =======================
STOP = set("the and for with from into that this what when your have about will just more than over after their "
           "they them been were does using used new how why can are its not you our".split())
PRIMARY_DOMAINS = ("arxiv.org", "github.com", "huggingface.co", "openreview.net", "openai.com", "anthropic.com",
                   "deepmind.google", "blog.google", "ai.google.dev", "research.google", "ai.meta.com", "mistral.ai",
                   "nvidia.com", "microsoft.com", "qwenlm.github.io", "deepseek.com", "x.ai", "fda.gov", "europa.eu")


def sig_words(t):
    return {w for w in re.findall(r"[a-z0-9][a-z0-9.\-]*[a-z0-9]", (t or "").lower()) if len(w) > 2 and w not in STOP}


def arxiv_ids(text):
    return set(re.findall(r"\b(\d{4}\.\d{4,5})(?:v\d+)?\b", text or ""))


def load_trusted(days=4):
    """Recent items from feeds.txt plus Hugging Face Daily Papers - what careful outlets and primary sources said."""
    feeds = []
    if FEEDS_FILE.exists():
        for line in FEEDS_FILE.read_text(encoding="utf-8").splitlines():
            line = line.split("#", 1)[0].strip()
            name, _, url = line.partition("|")
            if name.strip() and url.strip().startswith("http"):
                feeds.append((name.strip(), url.strip()))
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)

    def one(feed):
        name, url = feed
        if not feedparser:
            return name, [], "feedparser not installed"
        try:
            r = requests.get(url, headers=UA, timeout=20)
            r.raise_for_status()
            out = []
            for e in feedparser.parse(r.content).entries[:40]:
                t = e.get("published_parsed") or e.get("updated_parsed")
                if t and dt.datetime(*t[:6], tzinfo=dt.timezone.utc) < cutoff:
                    continue
                out.append({"source": name, "title": fix_text(e.get("title", "")), "link": e.get("link", ""),
                            "summary": strip_html(e.get("summary", ""), 700)})
            return name, out, ""
        except Exception as e:
            return name, [], str(e)[:80]

    items = []
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for name, got, err in ex.map(one, feeds):
            log(f"  feed {'OK  ' if not err else 'FAIL'} {name}: {len(got) if not err else err}")
            items += got
    try:
        papers = requests.get("https://huggingface.co/api/daily_papers", headers=UA, timeout=20).json()
        for it in (papers if isinstance(papers, list) else [])[:60]:
            paper = it.get("paper") or it
            pid = paper.get("id", "")
            items.append({"source": "Hugging Face Daily Papers", "title": it.get("title") or paper.get("title", ""),
                          "link": f"https://arxiv.org/abs/{pid}" if pid else "",
                          "summary": strip_html(paper.get("summary") or it.get("summary") or "", 700)})
        log(f"  Hugging Face Daily Papers: {len(papers) if isinstance(papers, list) else 0}")
    except Exception as e:
        log(f"  Hugging Face Daily Papers failed: {str(e)[:80]}")
    for t in items:
        t.update(nlink=norm_link(t["link"]), words=sig_words(t["title"]), arxiv=arxiv_ids(t["link"]))
    return items


def trusted_matches(s, trusted):
    nl, tw = s.get("nlink", ""), sig_words(s["title"])
    ax = arxiv_ids(f"{s.get('link', '')} {s.get('text', '')}")
    hits = []
    for t in trusted:
        same = (nl and nl == t["nlink"]) or (t["arxiv"] & ax)
        if not same:
            common = tw & t["words"]
            same = len(common) >= 3 and len(common) / max(1, min(len(tw), len(t["words"]))) >= 0.6
        if same and t["source"] not in {h["source"] for h in hits}:
            hits.append(t)
    return hits[:3]


def primary_link(s):
    """The thread's own link, or the first link to a primary source mentioned in the post."""
    if s.get("link") and not any(d in urlparse(s["link"]).netloc for d in SKIP_DOMAINS):
        return s["link"]
    for u in re.findall(r"https?://[^\s)\]>\"']+", s.get("text", "")):
        if any(d in urlparse(u).netloc for d in PRIMARY_DOMAINS):
            return u.rstrip(".,;")
    return s.get("link", "")


def hn_lookup(s):
    """The same story on Hacker News - a second, independent community."""
    if "HackerNews" in s["subs"]:
        return None
    try:
        q = ({"query": s["link"], "restrictSearchableAttributes": "url", "tags": "story"} if s.get("link")
             else {"query": s["title"][:120], "tags": "story"})
        hits = requests.get("https://hn.algolia.com/api/v1/search", params=q, headers=UA, timeout=20).json().get("hits", [])
        hits = [h for h in hits if (h.get("points") or 0) >= 10]
        if not s.get("link"):
            hits = [h for h in hits if len(sig_words(h.get("title")) & sig_words(s["title"])) >= 3]
        if not hits:
            return None
        h = max(hits, key=lambda h: h.get("points") or 0)
        item = requests.get(f"https://hn.algolia.com/api/v1/items/{h['objectID']}", headers=UA, timeout=20).json()
        comments = [strip_html(c.get("text") or "", 500) for c in (item.get("children") or [])[:8] if c.get("text")]
        return {"points": h.get("points") or 0, "title": h.get("title", ""), "comments": comments[:5]}
    except Exception:
        return None


def check_block(s):
    lines = []
    if s.get("evidence") and not s["evidence"].startswith("(could not"):
        lines.append(f"- Source read directly: {urlparse(s.get('source_url', '')).netloc or 'linked page'}")
    for t in s.get("also_in", []):
        lines.append(f"- {t['source']} has a matching item (use only if it is clearly the same story): "
                     f"\"{t['title']}\" - {t['summary'][:500]}")
    hn = s.get("hn")
    if hn:
        lines.append(f"- Hacker News discussed it ({hn['points']} points). Comments there: "
                     + " | ".join(c[:300] for c in hn["comments"][:4]))
    if not lines:
        lines.append("- Found nowhere outside Reddit and no source could be read: treat its claims as unconfirmed "
                     "and say so on air.")
    return "INDEPENDENT CHECK:\n" + "\n".join(lines)


# =============================== 2. SCORE ===============================
SCORE_QUESTIONS = {
    "relevant": {"type": "noul", "instructions": "Is this thread about AI, science, technology, biology or "
                 "medicine (research, discoveries, products, the industry, or effects on society), with something "
                 "to discuss?",
                 "criteria": {"true": "News, a release, a study or discovery, a technical question with real "
                              "discussion, a debate about impact.", "false": "Memes, art or video showcases, prompt "
                              "sharing, personal tech or medical support, homework help, giveaways, low-effort "
                              "rants."}},
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


def level_bonus(state):
    """Depth votes move the weights, so 'too technical' changes what gets picked, not only how it is told."""
    votes = [f.get("ratings", {}).get("depth") for f in state["feedback"][-6:]]
    lean = votes.count("too technical") - votes.count("too basic")
    if lean >= 2:
        return {"general": 1.0, "technical": 0.5, "specialist": 0.0}
    if lean <= -2:
        return {"general": 0.6, "technical": 1.0, "specialist": 0.6}
    return {"general": 1.0, "technical": 0.75, "specialist": 0.25}


def score_stories(stories, state, limit=70, batch_only=False):
    top = stories[:limit]

    def one(s):
        st = {"title": s["title"], "communities": sorted(set(s["subs"])), "linked_site": urlparse(s["link"]).netloc,
              "post": s["text"][:1500], "top_comments": [c[:300] for c in s["comments"][:8]]}
        return s["id"], decide(st, SCORE_QUESTIONS)

    results = {}
    if DECISION_MODELS and SCORING in ("auto", "decisions") and not batch_only:
        t0 = time.time()
        with cf.ThreadPoolExecutor(max_workers=4) as ex:
            for sid, ans in ex.map(one, top):
                if ans:
                    results[sid] = ans
        log(f"  decision models scored {len(results)}/{len(top)} in {time.time() - t0:.0f}s")
    missing = [s for s in top if s["id"] not in results]
    if missing:
        results.update(llm_score(missing))
    bonus = level_bonus(state)
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
        lvl_bonus = bonus.get(lvl, 0.6)
        s["score"] = 0.35 * min(s["signal"] / 3, 1) + 0.35 * sub + 0.2 * lvl_bonus + 0.1 * min(len(set(s["subs"])) / 3, 1)
        kept.append(s)
    kept.sort(key=lambda s: -s["score"])
    log(f"  scored {len(results)} threads, {len(kept)} relevant")
    return kept


def llm_score(stories, batch=35):
    """All threads in one or two calls instead of one call per thread - gentle on free daily limits."""
    out = {}
    for i in range(0, len(stories), batch):
        out.update(_score_batch(stories[i:i + batch]))
    log(f"  batched scoring: {len(out)} threads in {(len(stories) + batch - 1) // batch} call(s)")
    return out


def _score_batch(stories):
    listing = "\n".join(f"[{s['id']}] ({', '.join(sorted(set(s['subs'])))}; {len(s['comments'])} comments) {s['title']}"
                        f" :: {s['text'][:220]}" + (f" || top comment: {s['comments'][0][:160]}" if s["comments"] else "")
                        for s in stories)
    prompt = f"""Rate these discussion threads (AI, science, technology, biology, medicine). Return JSON only:
{{"items": [{{"id": <n>, "relevant": 0-1, "substance": 0-3, "level": "general|technical|specialist",
             "needs_check": 0-1, "hype": 0-1}}]}}
relevant = about AI, science, technology, biology or medicine with something to discuss (not memes, showcases,
prompt sharing, personal tech/medical support, homework help).
substance = factual/technical content. level = background needed (general = engineer outside CS can follow).
needs_check = makes checkable factual claims. hype = speculative or sensational framing.

{listing}"""
    try:
        text, _ = llm(prompt, json_mode=True, label="scoring", role="helper")
        out = {}
        for it in as_obj(parse_json(text), "items").get("items", []):
            try:
                out[int(it["id"])] = {"relevant": {"noul": float(it.get("relevant", 0))},
                                      "substance": {"score": float(it.get("substance", 1))},
                                      "level": {"choice": it.get("level", "technical")},
                                      "needs_check": {"noul": float(it.get("needs_check", 0))},
                                      "hype": {"noul": float(it.get("hype", 0))}}
            except (KeyError, TypeError, ValueError):
                continue
        return out
    except Exception as e:
        log(f"  fallback scoring failed: {e}")
        return {s["id"]: {"relevant": {"noul": 1}, "substance": {"score": 1.5}} for s in stories}


# =============================== 3. TOPICS ===============================
# Keys a model uses for "no topic". Tracked as a topic they collect every one-off thread and win the deep dive.
NO_TOPIC = {"none", "no-topic", "notopic", "na", "n-a", "null", "misc", "miscellaneous", "other", "others",
            "general", "one-off", "oneoff", "unrelated", "unassigned", "various"}


def update_topics(state, stories, date_key):
    topics = state["topics"]
    for k in [k for k in topics if k in NO_TOPIC]:
        del topics[k]
    recent = sorted(topics.items(), key=lambda kv: kv[1].get("last_seen", ""), reverse=True)[:40]
    known = "\n".join(f"- {k}: {v['name']}" for k, v in recent) or "(none yet)"
    listing = "\n".join(f"[{s['id']}] {s['title']}" for s in stories[:40])
    prompt = f"""Group these discussion threads (AI, science, technology, biology) into ongoing topics (stories or
themes that can build up over days, e.g. "open-weight reasoning models", "AI coding agents in real teams",
"GLP-1 drugs beyond weight loss", "room-temperature superconductor claims").
Reuse an existing topic key when a thread belongs to it; otherwise create a new short kebab-case key.
A one-off thread gets NO entry at all (never a key like "none" or "misc"). A topic must be a specific
question or development that can carry a 30-minute deep dive - not just a company or product family name.

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
        if not s or not key or key in NO_TOPIC:
            continue
        t = topics.setdefault(key, {"name": a.get("name") or key, "first_seen": date_key, "days": [], "subs": [],
                                    "mentions": 0, "refs": [], "deep_dive": ""})
        s["topic"] = key
        t.setdefault("subject", s.get("subject", "ai"))
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


def sunday_minutes(edition):
    """Long, but within the free budget: Fish makes one request per spoken turn (about 32 words)."""
    left = OPENROUTER_FREE_DAILY - free_used() - FREE_WRITER_RESERVE
    if edition == "morning":
        left //= 2   # leave the other half for the evening deep dive (same UTC day)
    fits = left / (WORDS_PER_MIN / 32)
    minutes = max(30.0, min(SUNDAY_MINUTES, fits))
    log(f"  Sunday deep dive: {minutes:.0f} minutes ({left} free requests available for it)")
    return minutes


def sunday_topic(state, date_key, subject, exclude=()):
    """The most built-up topic of the past week in a subject (ai / science), not deep-dived in three weeks."""
    today = dt.date.fromisoformat(date_key)
    best = None
    for key, t in state["topics"].items():
        if key in NO_TOPIC or key in exclude or t.get("subject", "ai") != subject or not t.get("last_seen"):
            continue
        if (today - dt.date.fromisoformat(t["last_seen"])).days > 7:
            continue
        if t.get("deep_dive") and (today - dt.date.fromisoformat(t["deep_dive"])).days < 21:
            continue
        week = [d for d in t["days"] if (today - dt.date.fromisoformat(d)).days <= 7]
        if len(week) < 2 or len(t.get("refs", [])) < 3:
            continue
        rank = (len(week), len(t["subs"]), t["mentions"])
        if not best or rank > best[0]:
            best = (rank, key)
    return best[1] if best else None




def load_interests():
    """interests.txt: 'phrase | weight' per line (weight 1-3). A phrase may list synonyms with '/'."""
    out = []
    if not INTERESTS_FILE.exists():
        return out
    for line in INTERESTS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        phrase, _, w = line.partition("|")
        try:
            weight = float(w.strip() or 1)
        except ValueError:
            weight = 1.0
        terms = [t.strip() for t in phrase.split("/") if t.strip()]
        if terms and weight > 0:
            out.append((terms[0], weight, re.compile(r"\b(" + "|".join(re.escape(t) for t in terms) + r")(s|es)?\b", re.I)))
    return out


INTERESTS = load_interests()


def interests_text():
    if not INTERESTS:
        return ""
    ranked = sorted(INTERESTS, key=lambda i: -i[1])
    return (f"{LISTENER.upper()}'S INTERESTS (strongest first; prefer these when quality is equal, and connect "
            f"stories to them where it is honest): " + "; ".join(name for name, _, _ in ranked))


def personal_rank(stories, state):
    """Boost threads that match the listener's interests; hold back topics covered in the last two episodes."""
    recent = sorted(state["memory"], key=lambda m: m["published"])[-2:]
    covered = {t for m in recent for t in m.get("topics", [])}
    for s in stories:
        blob = f"{s['title']} {s['text'][:800]} {s.get('link', '')}"
        hits = [name for name, w, rx in INTERESTS if rx.search(blob)]
        s["interests"] = hits
        s["score"] = s.get("score", 0) + min(0.25, 0.06 * sum(w for name, w, _ in INTERESTS if name in hits))
        s["covered"] = s.get("topic") in covered
        if s["covered"] and len(set(s["subs"])) < 3:
            s["score"] -= 0.15
        if s.get("also_in"):
            s["score"] += 0.05   # a trusted outlet or primary source also has it
    stories.sort(key=lambda s: -s.get("score", 0))
    log(f"  interest matches: {sum(1 for s in stories if s['interests'])}, "
        f"held back as recently covered: {sum(1 for s in stories if s['covered'])}")


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
- Every thread comes with an INDEPENDENT CHECK. Say plainly on air whether a claim is backed by the primary
  source or a trusted outlet (name it), or is so far only on Reddit.
- Your own training knowledge may be out of date. For anything recent, rely only on the SOURCES given.
  Never invent numbers, names, dates or quotes. If unsure, say it is unclear."""


def candidate_listing(stories, n_ai=22, n_sci=12):
    """Best threads of each subject, so science stories are not crowded out by the busier AI communities."""
    ai = [s for s in stories if s.get("subject", "ai") == "ai"][:n_ai]
    sci = [s for s in stories if s.get("subject", "ai") != "ai"][:n_sci]
    rows = []
    for s in sorted(ai + sci, key=lambda s: -s.get("score", 0)):
        flags = ", ".join(x for x in (s.get("level", ""), "claims to check" if s.get("needs_check") else "",
                                      "hype" if s.get("hype") else "",
                                      "covered recently" if s.get("covered") else "",
                                      ("also in: " + ", ".join(t["source"] for t in s["also_in"]))
                                      if s.get("also_in") else "",
                                      ("interest: " + "/".join(s["interests"][:2])) if s.get("interests") else "") if x)
        rows.append(f"[{s['id']}] {s.get('subject', 'ai').upper()} ({', '.join(sorted(set(s['subs'])))}; "
                    f"{len(s['comments'])} comments; {flags}) "
                    f"{s['title']}" + (f" :: {s['text'][:220]}" if s["text"] else ""))
    return "\n".join(rows)


def episode_shape(minutes):
    """(number of topics, words per topic) for an episode of this length."""
    body = max(SEGMENT_WORDS, int(minutes * WORDS_PER_MIN) - 250)   # 250 words for the opening and close
    n = max(1, body // SEGMENT_WORDS)
    return n, min(1300, body // n)


def plan_regular(stories, edition, state, minutes=EPISODE_MINUTES):
    n, per = episode_shape(minutes)
    has_sci = any(s.get("subject", "ai") != "ai" for s in stories[:40])
    mix = ("Segment 1: the strongest AI discussion. Segment 2: the strongest SCIENCE discussion (science, "
           "technology, biology, medicine); only if none is worthwhile, a second AI one."
           + (" Further segments: the best remaining of either subject." if n > 2 else "")) if has_sci and n > 1 \
        else "The strongest discussions."
    shape = f"""{edition.upper()} EDITION - {n} topic{'s' if n > 1 else ''} of about {per} words each ({minutes:g} minutes;
the listener's commute). Fewer stories, told properly: depth over breadth. No quickfire round.
- {mix}
- Format "thread" (the thread vs the evidence) for a news discussion with claims to check, or "concept" when the
  thread is really about an idea worth teaching from first principles{' - the evening suits one concept' if edition == 'evening' else ''}.
- Prefer threads with real disagreement or checkable claims that an engineer outside the field can follow.
- Titles are spoken: plain words a listener understands at once - no jargon, model numbers or acronyms."""
    prompt = f"""You are the editor of "{SHOW_TITLE}", a two-host podcast built from AI and science discussions
on Reddit.

{EDITORIAL}

{shape}

{interests_text()}

PAST EPISODES (most recent last; follow developing stories, never repeat one without new information):
{memory_text(state)}

{feedback_text(state)}

CANDIDATE THREADS (best first; same story in several communities already merged):
{candidate_listing(stories)}

Return JSON only:
{{"theme": "<one line>",
  "segments": [{{"title": "<short plain spoken title>", "format": "thread|concept", "concept": "<term or empty>",
                 "item_ids": [<ids>], "angle": "<what to explain and why it matters to {LISTENER}>",
                 "claims_to_check": ["<claim from the thread>", "..."], "connects_to": "<past episode or empty>",
                 "words": <number>}}]}}"""
    text, _ = llm(prompt, json_mode=True, label="editor")
    plan = as_obj(parse_json(text), "segments")
    ids = {s["id"] for s in stories}
    segs = []
    for s in plan.get("segments", []):
        s["item_ids"] = [i for i in s.get("item_ids", []) if isinstance(i, int) and i in ids]
        if s.get("format") not in ("thread", "concept"):
            s["format"] = "thread"
        if s["item_ids"] or s.get("format") == "concept":
            s["words"] = per
            segs.append(s)
    segs = segs[:n]
    if not segs:
        raise RuntimeError("Editor returned no usable segments")
    return plan.get("theme", ""), segs


def plan_deep_dive(topic_key, state, stories, minutes=SUNDAY_MINUTES):
    t = state["topics"][topic_key]
    today_ids = [s["id"] for s in stories if s.get("topic") == topic_key][:4]
    base = {"item_ids": today_ids, "format": "deepdive", "topic": topic_key, "connects_to": ""}
    body = int(minutes * WORDS_PER_MIN) - 300
    parts = [
        ("Why everyone is talking about it", "Walk through how the discussion built up over the past days across "
         "communities: what people claimed, what they argued about, what changed.", 0.15),
        ("The foundations, from scratch", "Explain the underlying idea from first principles for an engineer "
         "outside the field: the problem it solves, the mechanism, analogies, and the key terms.", 0.25),
        ("How we got here", "The history: earlier approaches, key papers or releases, what each step fixed and "
         "what it broke. Only use facts you are sure of or that are in the sources.", 0.2),
        ("Where it meets the real world", f"Where this matters in practice - including, where it honestly applies, "
         f"medicine, manufacturing, quality and regulated work like {LISTENER}'s.", 0.2),
        ("Reality check", "What the community gets right and wrong, the actual evidence today, the honest "
         "limitations, and open questions to watch - no predictions.", 0.2),
    ]
    segs = [dict(base, title=f"{t['name']}: {title}", angle=angle, words=int(body * share))
            for title, angle, share in parts]
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
            s["source_url"] = primary_link(s)
            s["evidence"] = fetch_article(s["source_url"])
            if not s["evidence"] or s["evidence"].startswith("(could not"):
                for t in s.get("also_in", []):   # fall back to the trusted outlet's own write-up
                    ev = fetch_article(t["link"])
                    if ev and not ev.startswith("(could not"):
                        s["source_url"], s["evidence"] = t["link"], ev
                        break
            s["hn"] = hn_lookup(s)
        comments = "\n".join(f"  - {c[:500]}" for c in s["comments"][:14])
        blocks.append(f"THREAD: {s['title']}\nCommunities: {', '.join(sorted(set(s['subs'])))}\n"
                      f"Post text: {s['text'][:2000] or '(link post, no text)'}\n"
                      f"Top comments:\n{comments or '  (none collected)'}\n"
                      f"Linked source: {s.get('source_url') or 'none'}\n"
                      f"Linked source text: {s['evidence'][:6000] or '(not available)'}\n"
                      f"{check_block(s)}")
    if topic:
        refs = "\n".join(f"- {r['date']}: {r['title']}" + (f" | comment: {r['comment']}" if r.get("comment") else "")
                         for r in topic["refs"])
        blocks.append(f"HOW THE TOPIC BUILT UP (threads over the past days, {len(topic['days'])} days, "
                      f"communities: {', '.join(topic['subs'])}):\n{refs}")
        for r in topic["refs"][-10:]:
            if r.get("link"):
                ev = fetch_article(r["link"], 4000)
                if ev:
                    blocks.append(f"SOURCE linked from an earlier thread ({r['title']}):\n{ev}")
    return "\n\n-----\n\n".join(blocks)


# =============================== 6. WRITE ===============================
A, B = HOST_A["name"], HOST_B["name"]
LINE_RE = re.compile(rf"^\s*\**\s*({A}|{B})\s*\**\s*:\s*(.+)$", re.I)
FORMATS = {
    "thread": f"""Flow (keep it conversational, not a checklist):
1. The find: the presenter brings the thread as something she came across ("I was going through the
   LocalLLaMA subreddit and there was this post...") - what was posted and why people cared.
2. The argument: the strongest views in the comments, paraphrased, including the confident ones.
3. Myth vs mechanism: what people believed, then how the technology actually works, using the linked
   source; check the claims listed above against it.
4. The verdict: right, half-right, wrong, or too early to tell - and why.
5. Close with a one-line takeaway the listener can remember.""",
    "quickfire": "Cover each thread briefly: one or two exchanges each - what it is, and one honest sentence on "
                 "whether it holds up. Clear verbal transitions between items.",
    "concept": "Teach the concept from first principles: the problem, the mechanism, an engineering analogy, how "
               "the idea developed, and how it appears in the listed threads. Check understanding with a quick "
               "recap at the end.",
    "deepdive": "This is one part of a multi-part deep dive. Go slower and deeper than a news segment, but keep "
                "the listener's level. Use analogies, recap key ideas, and connect to the earlier parts.",
}


def write_segment(seg, items, state, position, total, theme):
    target = int(seg["words"])
    fmt = seg.get("format", "thread")
    topic = state["topics"].get(seg.get("topic", "")) if fmt == "deepdive" else None
    claims = "; ".join(seg.get("claims_to_check") or []) or "(identify them from the thread)"
    presenter, asker = (A, B) if position % 2 == 1 else (B, A)   # the hosts take turns bringing stories
    sources = sources_block(items, topic)
    prompt = f"""You are writing one segment of "{SHOW_TITLE}", a two-host audio podcast built from AI and
science discussions on Reddit, for {LISTENER} to hear while driving.
Hosts (two women with different ages and temperaments):
- {A}: {HOST_A['persona']}.
- {B}: {HOST_B['persona']}.
In THIS segment {presenter} brings the story she found and leads; {asker} reacts and asks the questions a
curious engineer would ask (with mechanical-engineering analogies where they fit), and sometimes pushes back
in a friendly way. They do not always agree. Let their ages show in how they read the thread: {A} is closer to
the community's excitement, {B} has seen similar claims before.
Humour: light and natural, coming from the material or gentle teasing between them - one or two genuine
laughs, never forced jokes or puns in every line. It stays educational first.
Energy: {A} is lively and wide awake - quick reactions ("Wait, really?", "Okay, that's wild"), short punchy
sentences, genuine excitement. {B} is calm but warm and engaged, never dry or slow.

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
{sources}

WRITE about {target} words of natural spoken dialogue (between {int(target * 0.9)} and {int(target * 1.1)}).
FORMAT - every line starts with a speaker name and a colon:
{A}: ...
{B}: ...
RULES: real conversation with short and longer turns; at most ONE number per turn (a listener in a car cannot
hold more), and round it ("about thirty thousand"); explain every technical term in plain words; no greeting or sign-off;
{"start directly with the story" if position == 1 else "open with a one-line transition"}; no markdown,
stage directions, URLs or usernames; write numbers and abbreviations as a presenter would say them."""
    text, model = llm(prompt, min_words=150, label=f"segment {position}")
    USED_BY[nice_model(model)] += 1
    turns = parse_dialogue(text)
    if FACT_CHECK and turns:
        turns = check_and_revise(seg, turns, sources, target, position)
    return turns


def check_and_revise(seg, turns, sources, target, position):
    """A second model checks the draft against the sources; the writer then fixes what it found."""
    script = "\n".join(f"{s}: {t}" for s, t in turns)
    n = turns_words(turns)
    prompt = f"""You are the fact-checker and script editor of a podcast for an engineer listening in the car.
Check this segment ("{seg['title']}") against its SOURCES.

SOURCES:
{sources}

SCRIPT ({n} words; target {target}):
{script}

Find:
1. Numbers, names, dates, quotes or factual claims NOT supported by the sources (well-known background facts
   are fine). Also claims stated as fact that the sources only report as a Reddit opinion.
2. Turns with more than one number.
3. Technical terms used without a plain-words explanation.
4. Anything confusing to hear (lists of figures, long chains of acronyms).
Return JSON only:
{{"problems": [{{"quote": "<a few words from the script>", "issue": "<what is wrong>", "fix": "<how to fix>"}}]}}
Return an empty list if the segment is accurate and clear."""
    try:
        text, _ = llm(prompt, json_mode=True, label=f"check {position}", role="helper")
        problems = [p for p in as_obj(parse_json(text), "problems").get("problems", []) if isinstance(p, dict)]
    except Exception as e:
        log(f"    fact-check skipped: {str(e)[:120]}")
        return turns
    off_length = not 0.8 * target <= n <= 1.25 * target
    log(f"    check {position}: {len(problems)} problem(s), {n} words for a target of {target}")
    if not problems and not off_length:
        return turns
    fixes = "\n".join(f"- \"{p.get('quote', '')}\": {p.get('issue', '')} -> {p.get('fix', '')}" for p in problems[:25])
    prompt = f"""Revise this segment of the podcast "{SHOW_TITLE}". Keep the hosts, their voices and energy, the flow
and the format (every line "{A}: ..." or "{B}: ..."). Fix every problem listed. Remove or soften any claim the
sources do not support (say it is unclear or only claimed on Reddit). At most ONE number per turn. Explain every
technical term in plain words. Length: about {target} words (now {n}).

PROBLEMS:
{fixes or '- none: only bring it to the right length'}

SOURCES:
{sources}

SCRIPT:
{script}

Return only the revised dialogue."""
    try:
        text, model = llm(prompt, min_words=150, label=f"revise {position}")
    except RuntimeError as e:
        log(f"    revision skipped: {str(e)[:120]}")
        return turns
    revised = parse_dialogue(text)
    if turns_words(revised) < max(120, 0.6 * target):
        log(f"    revision too short ({turns_words(revised)} words) - keeping the draft")
        return turns
    return revised


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
The listener is {LISTENER}; {'it is Sunday and this long deep dive is for a long drive' if deep_dive else 'he listens on a short commute'}.
Today is {now:%A, %d %B %Y}, {edition} edition.
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

Greeting: 60-110 words, 3-5 turns, upbeat ({A} especially). Greet {LISTENER} by name naturally. Link to past
episodes only where it truly connects. Preview each segment in ONE plain sentence - no lists of numbers or names.
If there is recent feedback, say in one sentence what changed because of it.
{'For a deep dive, say why this topic is ready now (it built up over the week).' if deep_dive else ''}
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


def ffmpeg_to_pcm(data, tempo=1.0):
    speed = ["-filter:a", f"atempo={tempo:.3f}"] if abs(tempo - 1) > 0.005 else []
    res = subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-i", "pipe:0", *speed,
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
                if secs < expected * 0.8:   # Gemini speaks ~2.5 words/s; less means the audio was cut off
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
            log(f"  Fish Audio voices: {_FISH or 'none listed - cloning the hosts from short reference clips'}")
        except Exception as e:
            log(f"  Fish Audio voice list failed: {e}")
    return _FISH


REF_TEXT = {
    A: "Good evening, and welcome back. Today we are weighing what people are saying against what the evidence shows.",
    B: "Right, and before we get carried away, let's look at the numbers, the method, and what was actually measured.",
}
_REFS = {}


def fish_reference(spk):
    """A short spoken sample for each host (made once with Edge TTS, kept in voices/) that Fish Audio clones."""
    if spk in _REFS:
        return _REFS[spk]
    path = ROOT / "voices" / f"host_{'a' if spk == A else 'b'}.mp3"
    if not path.exists():
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(asyncio.run(_edge_turn(REF_TEXT[spk], (HOST_A if spk == A else HOST_B)["edge"])))
    _REFS[spk] = [{"type": "input_audio",
                   "input_audio": {"data": "data:audio/mpeg;base64," + base64.b64encode(path.read_bytes()).decode()}},
                  {"type": "text", "text": REF_TEXT[spk]}]
    return _REFS[spk]


def fish_tts_chunk(turns):
    voices = fish_voices()
    pcm = bytearray()
    for spk, text in turns:
        body = {"model": FISH_MODEL, "input": text, "response_format": "mp3"}
        if len(voices) == 2:
            body["voice"] = {A: voices[0], B: voices[1]}[spk]
        else:
            body["input_references"] = fish_reference(spk)
        for attempt in range(1, 4):
            r = requests.post("https://openrouter.ai/api/v1/audio/speech", timeout=(20, 180), json=body,
                              headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}",
                                       "HTTP-Referer": PAGES_URL, "X-Title": SHOW_TITLE})
            note_free()
            if r.status_code == 200 and r.content:
                pcm += ffmpeg_to_pcm(r.content, FISH_TEMPO[0 if spk == A else 1]) + silence(0.25)
                break
            if free_limit_hit(r.status_code, r.text[:300]):
                raise RuntimeError("OpenRouter free daily limit reached")
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


def split_chunks(flat, size):
    chunks, cur, n = [], [], 0
    for row in flat:
        w = words(row[2])
        if cur and n + w > size:
            chunks.append(cur)
            cur, n = [], 0
        cur.append(row)
        n += w
    if cur:
        chunks.append(cur)
    return chunks


def voice_episode(sections, state, edition):
    flat = [(label, spk, text) for label, turns in sections for spk, text in turns]
    # Both editions share one Gemini quota day (it resets at midnight Pacific = 12:30/13:30 IST, so every Pacific
    # day holds one evening and the next morning). Grow the chunks until one episode fits in half the quota.
    size, per_episode = TTS_CHUNK_WORDS, max(1, GEMINI_TTS_DAILY // 2)
    chunks = split_chunks(flat, size)
    while len(chunks) > per_episode and size < TTS_MAX_CHUNK_WORDS:
        size = min(TTS_MAX_CHUNK_WORDS, size + 50)
        chunks = split_chunks(flat, size)
    log(f"  {len(chunks)} voice chunks of up to {size} words")

    # Fish is the standard voice; Gemini (free key) and Edge are fallbacks.
    engines = ["fish", "gemini", "edge"] if TTS_ENGINE == "auto" else [TTS_ENGINE]
    for engine in engines:
        if engine == "gemini":
            if not GEMINI_API_KEY:
                continue
            # Google's free daily quota resets at midnight Pacific time.
            pt = ZoneInfo("America/Los_Angeles")
            day_start = dt.datetime.now(pt).replace(hour=0, minute=0, second=0, microsecond=0)
            state["tts_log"] = [t for t in state.get("tts_log", [])
                                if dt.datetime.fromisoformat(t) >= day_start]
            used = len(state["tts_log"])
            if TTS_ENGINE == "auto" and used + len(chunks) > GEMINI_TTS_DAILY:
                log(f"  Gemini TTS: {used} used today (Pacific), need {len(chunks)} (limit {GEMINI_TTS_DAILY}) -> next voice")
                continue
        if engine == "fish":
            if not OPENROUTER_API_KEY:
                continue
            if not free_allowed(len(flat), "voices"):
                log(f"  Fish: needs {len(flat)} free requests, {free_used()} of {OPENROUTER_FREE_DAILY} used today "
                    f"(UTC) with {FREE_WRITER_RESERVE} kept for writing -> next voice")
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


def make_digest(stories, used, n=12):
    """Ten-second read of the day's top threads, so there is no need to open Reddit to know nothing was missed."""
    top = stories[:n]
    if not top:
        return []
    listing = "\n".join(
        f"[{s['id']}] ({', '.join(sorted(set(s['subs'])))}) {s['title']} :: {s['text'][:300]}"
        + (f" || comments: {' | '.join(c[:150] for c in s['comments'][:3])}" if s["comments"] else "")
        + (f" || also reported by: {', '.join(t['source'] for t in s['also_in'])}" if s.get("also_in") else "")
        for s in top)
    prompt = f"""For each discussion thread below, write ONE plain sentence (at most 30 words) saying what it is
about and where the discussion landed, for {LISTENER}, who will not open Reddit. Then a verdict, one of:
"confirmed" (a primary source or trusted outlet backs it), "plausible", "unclear", "hype", "opinion" (a debate,
not a factual claim). Use only what is given; no usernames.
Return JSON only: {{"items": [{{"id": <n>, "line": "...", "verdict": "..."}}]}}

{listing}"""
    got = {}
    try:
        text, _ = llm(prompt, json_mode=True, label="digest", role="helper")
        for it in as_obj(parse_json(text), "items").get("items", []):
            try:
                got[int(it["id"])] = it
            except (KeyError, TypeError, ValueError):
                continue
    except Exception as e:
        log(f"  digest lines skipped: {e}")
    return [{"title": s["title"], "subs": sorted(set(s["subs"])), "thread": s["threads"][0], "link": s.get("link", ""),
             "line": str(got.get(s["id"], {}).get("line") or s["text"][:180]),
             "verdict": str(got.get(s["id"], {}).get("verdict") or ""),
             "also_in": [t["source"] for t in s.get("also_in", [])], "in_episode": s["id"] in used} for s in top]


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


def release_files():
    res = gh("release", "view", RELEASE_TAG, "--json", "assets", check=False)
    if res.returncode != 0:
        return None
    return {a["name"] for a in json.loads(res.stdout).get("assets", [])}


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
    is_sunday = now.weekday() == 6
    force, dry = env_true("FORCE"), env_true("DRY_RUN")
    log(f"{SHOW_TITLE} | {edition} {date_key} | force={force} dry_run={dry} | TTS={TTS_ENGINE}")

    state = load_state()
    SPEND.extend(state.get("or_spend", []))
    FREE_LOG.extend(state.get("free_log", []))
    FREE_DAY_OUT[0] = state.get("free_day_out", "")
    if any(e["date"] == date_key and e["edition"] == edition for e in state["episodes"]) and not force:
        log("This edition already exists - nothing to do (backup run).")
        return

    close_issues = [] if dry else ingest_feedback(state)
    shift = pace_shift(state)
    if shift:
        PACE_NOTE = "; slightly slower, unhurried pace" if shift < 0 else "; slightly brisker pace"
        EDGE_RATE = "-8%" if shift < 0 else "+8%"

    log("1/8 Collecting discussions")
    posts, pool_note = load_pool()
    snap, snap_note = load_snapshot()
    notes = [pool_note] if posts else []
    if snap:   # office-PC snapshot, if that collector is still running
        have = {p["id"] for p in posts}
        posts += [p for p in snap["posts"] if p["id"] not in have]
        notes.append(snap_note)
    source_note = " + ".join(notes) or f"{pool_note}; {snap_note}"
    if len(posts) < 15:
        log(f"  {source_note}")
        direct = direct_reddit()
        if direct and direct["posts"]:
            posts, source_note = direct["posts"], "Reddit (collected directly)"
    if len(posts) < 15:
        source_note = (source_note + " + Hacker News") if posts else f"Hacker News (no Reddit data: {source_note})"
        posts += hn_threads()
        gh_note("warning", "Little Reddit data", "Using Hacker News too. Check the Collect Reddit workflow.")
    stories = build_stories(posts, state)
    if len(stories) < 5:
        raise RuntimeError(f"Only {len(stories)} new discussions found.")
    log(f"  {len(stories)} distinct stories from {len(posts)} threads ({source_note})")

    log("2/8 Scoring threads")
    stories = score_stories(stories, state, batch_only=is_sunday)   # Sunday saves requests for the long voices
    by_id = {s["id"]: s for s in stories}

    log("3/8 Tracking topics and checking trusted sources")
    update_topics(state, stories, date_key)
    trusted = load_trusted()
    for s in stories:
        s["also_in"] = trusted_matches(s, trusted)
    log(f"  {sum(1 for s in stories if s['also_in'])} of {len(stories)} threads also covered by a trusted source")
    personal_rank(stories, state)
    deep, minutes = None, EPISODE_MINUTES
    if is_sunday:   # no commute: two long deep dives, AI in the morning and science in the evening
        for subject in (("ai", "science") if edition == "morning" else ("science", "ai")):
            deep = sunday_topic(state, date_key, subject)
            if deep:
                break
        if deep:
            minutes = sunday_minutes(edition)
        else:
            log("  no topic built up enough this week for a deep dive -> regular episode")
    real_topics = [k for k in state["topics"] if k not in NO_TOPIC]
    if env_true("DEEP_DIVE") and not deep and real_topics:   # manual "deep dive now" from the workflow
        deep = max(real_topics, key=lambda k: (len(state["topics"][k]["days"]), state["topics"][k]["mentions"]))
        minutes = max(EPISODE_MINUTES, 30)

    log("4/8 Planning")
    if deep:
        theme, plan = plan_deep_dive(deep, state, stories, minutes)
        log(f"  DEEP DIVE ({minutes:.0f} min): {state['topics'][deep]['name']} "
            f"({len(state['topics'][deep]['days'])} days, {len(state['topics'][deep]['subs'])} communities)")
        min_words = int(0.7 * sum(s["words"] for s in plan))
    else:
        theme, plan = plan_regular(stories, edition, state, minutes)
        min_words = int(0.8 * sum(s["words"] for s in plan))
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
    while not deep and sum(turns_words(t) for _, t in written) < min_words and extra < 2:
        spare = [s for s in stories if s["id"] not in used][:1]
        if not spare:
            break
        extra += 1
        seg = {"title": spare[0]["title"][:80], "format": "thread", "item_ids": [spare[0]["id"]],
               "angle": "What the thread claims, what the community argues, and whether it holds up.",
               "words": min(plan[0]["words"], max(500, min_words - sum(turns_words(t) for _, t in written)))}
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
    kind = ("Sunday deep dive" if is_sunday else "Deep dive") if deep else edition.title()
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
                   "summary": summary, "threads": threads, "segments": [s["title"] for s, _ in written],
                   "deep_topic": deep or "",
                   "topics": sorted({by_id[j]["topic"] for j in used if j in by_id and by_id[j].get("topic")})})
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
        present = release_files()
        if present is not None:   # the feed must never point at audio that is gone
            for e in [e for e in keep if e["file"] not in present]:
                log(f"  audio missing for {e['file']} -> removed from the feed")
            keep = [e for e in keep if e["file"] in present]
        sync_release_assets({e["file"] for e in keep})

    log("  writing quiz and digest")
    quiz = make_quiz(written, theme)
    digest = make_digest(stories, used)

    fb_cut = (now - dt.timedelta(days=FEEDBACK_DAYS)).isoformat()
    day_ago = (now - dt.timedelta(hours=24)).isoformat()
    utc_yesterday = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    save_state({"episodes": keep, "memory": memory, "seen": seen,
                "feedback": [f for f in state["feedback"] if f.get("received", "") >= fb_cut],
                "tts_log": state.get("tts_log", []), "or_spend": [s for s in SPEND if s[0] >= day_ago],
                "free_log": [t for t in FREE_LOG if t >= utc_yesterday], "free_day_out": FREE_DAY_OUT[0],
                "topics": state["topics"]})

    transcript = "\n\n".join(f"## {lbl}\n" + "\n".join(f"{spk}: {t}" for spk, t in turns) for lbl, turns in sections)
    write_quiz_files(keep, {
        "file": filename, "title": episode["title"], "label": label, "date": date_key, "edition": edition,
        "published": now.isoformat(), "duration": duration, "theme": theme, "writer": writer,
        "voices": ENGINE_NAMES[engine], "deep_dive": bool(deep),
        "segments": [{"title": s["title"], "category": s.get("format", "")} for s, _ in written],
        "quiz": quiz, "digest": digest, "transcript": f"transcripts/{Path(filename).stem}.txt"}, transcript)
    build_rss(keep)
    for n in close_issues:
        gh("issue", "close", str(n), "--comment", "Thanks - recorded. It will shape the next episodes.", check=False)
    log(f"Done. Feed: {PAGES_URL}/podcast.xml | OpenRouter spend last 24 h: ${spent_today():.3f} | "
        f"free requests today (UTC): {free_used()}/{OPENROUTER_FREE_DAILY}")
    gh_note("notice", "Episode ready", f"{filename} - {fmt_ts(duration)} - {len(written)} segments - {total_words} words "
                                        f"- writer: {writer} - voices: {ENGINE_NAMES[engine]} - source: {source_note}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"ERROR: {exc}")
        gh_note("error", "Briefing failed", exc)
        sys.exit(1)
