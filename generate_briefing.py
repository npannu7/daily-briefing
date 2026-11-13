#!/usr/bin/env python3
"""
Weights & Measures - a two-host podcast about what the AI community is discussing,
checked against the evidence. One run = one episode.

Pipeline
  1. COLLECT  - Reddit threads from the subreddits in subreddits.txt, gathered hourly through the day by
                the Collect Reddit workflow (about one request a minute; comments re-read once a thread
                has matured). Also: reddit/latest.json from the office-PC collector if present.
                Fallbacks: direct Reddit RSS, then Hacker News discussions.
  2. SCORE    - free decision models rate every thread (relevant? substantial? level? needs fact-checking?
                hype?); the rest in one batched call. Duplicates across subreddits are merged.
                CHECK: each thread is matched against feeds.txt, Hugging Face Daily Papers, the
                primary source it links to, and Hacker News; a claim found only on Reddit is web-checked.
  3. TRACK    - threads are grouped into topics that build up over days. When a topic has
                come up on several days in several communities, the evening becomes a deep dive.
  4. PLAN     - morning: "the thread vs the evidence"; evening: follow-ups + a concept from
                scratch, or a deep dive.
  5. RESEARCH - the linked article / paper / repo behind each thread is fetched.
  6. WRITE    - two hosts (Sarah, Laura) in dialogue, pitched at an engineer outside CS.
  7. GREET    - personal greeting linked to past episodes; summary saved as memory.
  8. VOICE    - Fish Audio free (OpenRouter) -> Gemini TTS (free key) -> Edge voices.
  9. PUBLISH  - MP3 to the GitHub release, podcast.xml, quiz.json (the episode's game for quiz.html: bets and
                cards chosen by a decision model, written by the writer, repaired by a coding model) and
                transcript.

Which edition a run makes (morning or evening) is decided by due.py: the one that is due and not yet made.

Zero cost by default: Gemini free key, then OpenRouter free models, then GitHub Models. One writer
per episode where possible. OpenRouter's free daily request limit is counted, with a reserve for writing.
"""

import array
import asyncio
import base64
import concurrent.futures as cf
import datetime as dt
import email.utils
import html
import io
import json
import math
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

import due   # which edition is due, and when each should be ready (shared with the workflows)

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
LISTENER_PROFILE = ("Nikhil has an M.Tech in mechanical engineering and is Head of Quality Assurance at a "
                    "medical-device and pharmaceutical manufacturer, so quality systems, validation, GMP, audits, "
                    "regulation and manufacturing are his daily work. He codes and follows AI as a hobby, reads "
                    "research papers comfortably (not in computer science), follows AI discussions on Reddit, and "
                    "is curious about science, physics, space and technology beyond AI.")
# Terms the listener knows better than the hosts: never explained on air (explaining them sounds patronising).
EXPERT_AREAS = ("quality assurance, validation and verification, CAPA, audits, GMP, FDA / EU MDR / ISO 13485 / "
                "21 CFR Part 11 and other regulation, sterilisation, manufacturing, CNC machining, control "
                "systems and mechanical engineering")
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

# Episode length. Measured on the published episodes (Oct-Nov 2026): about 160 spoken words a minute including
# pauses. A weekday episode is a few topics of seven to nine minutes each; Sunday makes two long deep dives
# instead (no commute), sized to the free daily budget.
EPISODE_MINUTES = float(os.getenv("EPISODE_MINUTES") or 20)
SUNDAY_MINUTES = float(os.getenv("SUNDAY_MINUTES") or 45)
WORDS_PER_MIN = 160
SEGMENT_WORDS = 1150          # one topic, about seven minutes (decides how many topics fit)
MAX_SEGMENT_WORDS = 1500      # longest topic a model writes reliably in one go
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
# Checks the puzzles. Flash-Lite passed an unsolvable question and an ambiguous ranking on 4 Nov 2026, so the
# check uses stronger models first (OpenRouter's free helpers here are different models from the main writer).
CHECK_CHAIN = os.getenv("CHECK_MODELS") or (
    "gemini:gemini-3.8-flash,gemini:gemini-3.7-flash,openrouter:free,gemini:gemini-3.5-flash-lite,github:auto")
# Builds the play page's data: repairs cards that failed the checks and writes formulas. Free coding models
# first (they follow a schema strictly), then the usual helpers.
CODER_CHAIN = os.getenv("CODER_MODELS") or "openrouter:free,gemini:gemini-3.5-flash-lite,gemini:gemini-3.1-flash-lite"
# OpenRouter free models (each model's page read on 13 Nov 2026). "openrouter:free" in a chain expands to these,
# then to any other free model OpenRouter lists. OPENROUTER_FREE_MODELS replaces these lists.
# Nemotron 3 Super lost its free version on 2 Nov 2026 (HTTP 404 "unavailable for free"); Nemotron 3.5 Super VL
# (same family, 120B / 12B active) takes its place. ERNIE 5.5 Flash Preview (free since 10 Nov, 1M context, JSON
# schema, about 200 tokens a second) is the newest fallback. Not used: Inkling 2 Small (its free endpoint is for
# agent harnesses only), LiquidAI's small models (LFM2.5 left on 13 Nov), Dots3-Note (leaves 31 Dec 2026).
FREE_WRITERS = ["nvidia/nemotron-3-ultra-550b-a55b:free", "nvidia/nemotron-3.5-super-vl:free",
                "baidu/ernie-5.5-flash-preview:free", "google/gemma-4-31b-it:free"]
FREE_HELPERS = ["nvidia/nemotron-3.5-super-vl:free", "baidu/ernie-5.5-flash-preview:free",
                "google/gemma-4-31b-it:free", "google/gemma-4-26b-a4b-it:free"]
# Checking facts and puzzles: Apodex 1.1 Mini is a reasoning-first model built for answers grounded in evidence.
FREE_CHECKERS = ["apodex/apodex-1.1-mini:free", "nvidia/nemotron-3.5-super-vl:free",
                 "baidu/ernie-5.5-flash-preview:free", "google/gemma-4-31b-it:free"]
# Coding models: Particle-1 enforces a JSON schema; North Mini Code and Flint are trained for exact edits.
FREE_CODERS = ["particle-ai/particle-1:free", "cohere/north-mini-code:free", "recursive/flint-v1.0:free",
               "baidu/ernie-5.5-flash-preview:free"]
# Free models without a JSON mode: asked for JSON in the prompt only.
NO_JSON_MODE = ("nemotron-3-ultra", "nemotron-3.5-lightning", "nano-omni", "inkling", "laguna", "north-mini", "ling-")
# Never picked automatically from OpenRouter's free list: coding-only, guardrail, tiny, audio, decision-only or
# retiring models.
NOT_FOR_WRITING = r"safety|lyria|code|flint|particle|inkling|liquid|lfm|omni|span-|decider|decisions|heygen|fish-audio|dots-3"
# Decisions: typed questions about one item (a yes/no probability, a choice, a level on a scale). Used to score
# each thread and to choose the play page's templates. Perplexity's Decider models (free since 12 Nov 2026) are
# made for exactly this - they return calibrated probabilities instead of text - and come first. The Nemotron
# chat models answer the same questions as JSON when the Deciders are unavailable (Lightning took about 50 s a
# thread and often returned no answers). Threads nobody scored are scored in one batched call.
DECISION_MODELS = [m.strip() for m in (os.getenv("DECISION_MODELS") or
                   "perplexity/pplx-decider-flash-v1.3:free,perplexity/pplx-decider-v1.3:free,"
                   "nvidia/nemotron-3.5-lightning:free,nvidia/nemotron-3.5-super-vl:free").split(",") if m.strip()]
# Models that only answer through OpenRouter's Decisions API (they do not generate text), even when free.
DECISIONS_ONLY = ("respan/", "perplexity/pplx-decider", "parallel/decisions")
# A web-grounded decision model: asked whether a claim found only on Reddit is reported anywhere else, with the
# pages it used. "off" disables it.
WEB_CHECK_MODEL = (os.getenv("WEB_CHECK_MODEL") or "parallel/decisions-fast:free").strip()
SCORING = (os.getenv("SCORING") or "auto").lower()      # auto | batch | decisions
# Per-thread scoring may take this long; threads not reached by then are scored in one batched call. The free
# Nemotron 3.5 Lightning takes about 50 s a thread, so without a limit scoring took 16 minutes (4 Nov 2026).
SCORING_BUDGET_S = float(os.getenv("SCORING_BUDGET_S") or 240)
SCORING_WORKERS = max(1, int(os.getenv("SCORING_WORKERS") or 8))
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
# Pace feedback ("too fast" / "too slow") moves both a few percent from here (see apply_pace).
FISH_TEMPO_BASE = [float(os.getenv("FISH_TEMPO_A") or 1.08), float(os.getenv("FISH_TEMPO_B") or 1.0)]
FISH_TEMPO = list(FISH_TEMPO_BASE)
# Fish requests in flight at once. Starts stay at least 3.2 s apart (free models: 20 requests a minute), so this
# only overlaps the seconds each request spends rendering. 1 = one turn at a time, as before.
FISH_WORKERS = max(1, int(os.getenv("FISH_WORKERS") or 3))
# Sound: podcasts are mastered to about -16 LUFS (Apple's guideline). The raw Fish audio measured -26.5 LUFS,
# ten decibels quieter than other shows. "off" keeps the raw level.
TARGET_LUFS = os.getenv("TARGET_LUFS") or "-16"
SEGMENT_CHIME = os.getenv("SEGMENT_CHIME", "true").strip().lower() not in ("0", "false", "no", "off")
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
DOSSIER_FILE = ROOT / "reddit" / "dossier.json"   # the week's research, kept as a release asset like the pool
DOSSIER_DAYS = 9
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
FAIL_STREAK = Counter()   # failed calls in a row per model; a model that keeps failing is dropped for the run
_FAIL_LOCK = threading.Lock()
# Models that failed in earlier runs: {model: {"until": utc iso, "n": times in a row}}. Until then they go to
# the back of every chain. On 6-12 Nov 2026 Gemini 3.8 Flash and 3.7 Flash timed out in every run, and each run
# waited about four minutes to find that out again.
HEALTH = {}
COOLING = set()
COOL_HOURS = 6


def load_health(state):
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    HEALTH.update({m: h for m, h in (state.get("model_health") or {}).items() if isinstance(h, dict)})
    COOLING.update(m for m, h in HEALTH.items() if h.get("until", "") > now)
    if COOLING:
        log(f"  resting after failing in recent runs (tried last): {', '.join(sorted(COOLING))}")


def save_health():
    """Models that failed in this run and never worked rest for 6 hours, doubling each time in a row (at most
    48); a model that worked is cleared. A resting model is still used when everything ahead of it fails."""
    now = dt.datetime.now(dt.timezone.utc)
    for m in list(HEALTH):
        if m in WORKED:
            del HEALTH[m]
    failed = DEAD | {m for m, n in FAIL_STREAK.items() if n > 0}   # one timeout already costs 100 seconds
    for m in failed - WORKED:
        if m in COOLING:
            continue   # failed again while resting: keep its date
        n = int(HEALTH.get(m, {}).get("n", 0)) + 1
        HEALTH[m] = {"n": n, "until": (now + dt.timedelta(hours=min(48, COOL_HOURS * 2 ** (n - 1)))).isoformat()}
    cut = (now - dt.timedelta(days=4)).isoformat()
    return {m: h for m, h in HEALTH.items() if h.get("until", "") > cut}


WORKED = set()


def model_failed(model, why, limit):
    """Count a failed call. After `limit` in a row the model is skipped for the rest of this run, so an
    overloaded or rate-limited model costs a few minutes once instead of on every later call."""
    with _FAIL_LOCK:
        FAIL_STREAK[model] += 1
        n = FAIL_STREAK[model]
        drop = n >= limit and model not in DEAD
        if drop:
            DEAD.add(model)
    if n == 1:
        log(f"    {model}: failed ({why[:160]})")
    if drop:
        log(f"    {model}: {n} failures in a row -> skipping it for the rest of this run")


def model_ok(model):
    with _FAIL_LOCK:
        FAIL_STREAK[model] = 0
        WORKED.add(model)


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
    with requests.post(url, json=payload, headers=headers, stream=True, timeout=(20, 100)) as r:
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
                and not re.search(NOT_FOR_WRITING, m["id"])
                and "text" in ((m.get("architecture") or {}).get("output_modalities") or ["text"])]
        pref = ["nemotron", "ernie", "gemma", "apodex", "deepseek", "qwen", "kimi", "glm", "llama", "mistral"]

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
        chosen = override or {"writer": FREE_WRITERS, "checker": FREE_CHECKERS, "coder": FREE_CODERS}.get(role, FREE_HELPERS)
        extra = [] if override or role == "coder" else [m for m in listed_free() if m not in chosen][:2]
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
        r = requests.get("https://models.github.ai/catalog/models", timeout=30,
                         headers={**UA, "Authorization": f"Bearer {GITHUB_TOKEN}",
                                  "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})
        if r.status_code != 200 or "json" not in r.headers.get("content-type", ""):
            raise ValueError(f"HTTP {r.status_code}, {r.headers.get('content-type', '?')}: {r.text[:100]!r}")
        data = r.json()
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
    """Returns (text, model). role 'writer' = quality chain, 'helper' = free-first chain, 'checker' = the
    stricter chain that checks puzzles, 'coder' = free coding models that build exact JSON."""
    chain = expand_chain({"writer": WRITER_CHAIN, "checker": CHECK_CHAIN, "coder": CODER_CHAIN}.get(role, HELPER_CHAIN), role)
    if not chain:
        raise RuntimeError("No LLM provider configured")
    chain = [c for c in chain if c[1] not in COOLING] + [c for c in chain if c[1] in COOLING]
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
                if isinstance(e, requests.Timeout) or "timed out" in str(e).lower():
                    # An overloaded model that went silent for minutes (a streamed reply reports this as a
                    # connection error): try the next model instead of waiting another three minutes.
                    log(f"    {model}: no reply in time -> next")
                    errors.append(f"{model}: timeout")
                    model_failed(model, "timeout", 2)
                    break
                log(f"    {model}: connection problem ({str(e)[:120]})")
                errors.append(f"{model}: connection")
                if attempt == 2:
                    model_failed(model, "connection", 2)
                time.sleep(8)
                continue
            if status == 200:
                model_ok(model)
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
            if status != 409:   # 409 = free requests kept for writing, not the model's fault
                model_failed(model, f"HTTP {status}", 2)
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


def decide_chat(model, state_obj, questions, subject="AI discussion thread"):
    """Ask a free chat model typed questions about one item; answers in the Decisions API's shape."""
    asks = "\n".join(f'- "{k}" ({q["type"]}): {q["instructions"]} Criteria: {json.dumps(q["criteria"])}'
                     for k, q in questions.items())
    example = json.dumps({k: 0.8 if q["type"] == "noul" else 1 if q["type"] == "score" else "<one criteria key>"
                          for k, q in questions.items()})
    prompt = f"""Judge this {subject}. Answer each question carefully, on its own merits.

ITEM:
{json.dumps(state_obj, ensure_ascii=False)}

QUESTIONS:
{asks}

Return JSON only, with one key per question:
- type "noul": a probability 0-1 that the answer is true
- type "score": an integer index into the criteria list (0 = first)
- type "choice": one of the criteria keys, exactly as written
Example: {example}"""
    for attempt in (1, 2):
        if model in DEAD:
            return None
        free_pace()
        try:
            status, text, err = _openrouter(model, prompt, True, "helper")
        except (requests.RequestException, ValueError) as e:
            status, text, err = 503, "", str(e)[:120]
        if status == 200 and text:
            try:
                a = parse_json(text)
            except (ValueError, TypeError):
                model_failed(model, "reply was not JSON", 6)
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
            if out:
                model_ok(model)
            else:
                model_failed(model, "reply had none of the answers", 6)
            return out or None
        if status == 409:   # free requests kept for writing: not the model's fault
            return None
        if status in (400, 401, 402, 403, 404):
            if model not in DEAD:
                log(f"    decision model {model}: HTTP {status} -> skipping | {err[:160]}")
            DEAD.add(model)
            return None
        if status in (429, 503) and FREE_DAY_OUT[0] != utc_day() and attempt == 1 and FAIL_STREAK[model] < 3:
            time.sleep(20)   # per-minute limit; wait and try once more
            continue
        # Before this check, a rate-limited model failed silently on every thread: 8 minutes, 0 of 70 scored.
        model_failed(model, f"HTTP {status} {err[:120]}", 6)
        return None
    return None


def norm_answers(answers, questions):
    """The Decisions API is in alpha and its reply may change shape. Keep only answers that read cleanly as
    {"noul": 0-1} / {"score": level} / {"choice": key, "probabilities": {...}}; anything else is left out."""
    out = {}
    for k, q in questions.items():
        a = answers.get(k) if isinstance(answers, dict) else None
        if isinstance(a, (int, float, str)) and not isinstance(a, bool):
            a = {q["type"]: a}
        if not isinstance(a, dict):
            continue
        v = a.get(q["type"], a.get("value", a.get("answer")))
        probs = a.get("probabilities") or a.get("distribution")
        try:
            if q["type"] == "noul":
                v = float(v.get("probability") if isinstance(v, dict) else v)
                if 0 <= v <= 1:
                    out[k] = {"noul": v}
            elif q["type"] == "score":
                n = len(q["criteria"])
                if isinstance(probs, list) and len(probs) == n and all(isinstance(x, (int, float)) for x in probs) \
                        and sum(probs) > 0:
                    v = sum(i * x for i, x in enumerate(probs)) / sum(probs)   # expected level, counted from 0
                out[k] = {"score": max(0.0, min(n - 1.0, float(v)))}
            elif str(v) in q["criteria"]:
                out[k] = {"choice": str(v)}
                if isinstance(probs, dict):
                    out[k]["probabilities"] = {str(c): float(x) for c, x in probs.items()
                                               if isinstance(x, (int, float)) and str(c) in q["criteria"]}
        except (TypeError, ValueError, AttributeError):
            continue
    return out


def decide_api(model, state_obj, questions):
    """OpenRouter's Decisions API: typed answers with calibrated probabilities (no text generation)."""
    free = model.endswith(":free")
    if free:
        free_pace()
    try:
        r = requests.post("https://openrouter.ai/api/alpha/decisions", timeout=(15, 90),
                          json={"model": model, "state": state_obj, "questions": questions},
                          headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}",
                                   "HTTP-Referer": PAGES_URL, "X-Title": SHOW_TITLE})
    except requests.RequestException as e:
        model_failed(model, f"connection: {e}", 6)
        return None
    if free:
        note_free()
    if r.status_code == 200:
        data = r.json()
        cost = float((data.get("usage") or {}).get("cost") or 0)
        if cost:
            SPEND.append([dt.datetime.now(TZ).isoformat(), cost])
            if OPENROUTER_DAILY_USD <= 0 and model not in DEAD:
                log(f"    decision model {model} charges money -> dropped (OPENROUTER_DAILY_USD is 0)")
                DEAD.add(model)
        answers = norm_answers(data.get("answers"), questions)
        if len(answers) * 2 >= len(questions):   # a reply with most answers unreadable is a failed reply
            model_ok(model)
            cites = _urls(data.get("citations") or data.get("sources") or [])
            if cites:
                answers["_citations"] = cites
            return answers
        model_failed(model, "no usable answers in the reply", 6)
        return None
    if free and free_limit_hit(r.status_code, r.text[:300]):
        return None
    if r.status_code in (400, 401, 402, 403, 404):
        if model not in DEAD:
            log(f"    decision model {model}: HTTP {r.status_code} -> skipping | {r.text[:160]}")
        DEAD.add(model)
        return None
    model_failed(model, f"HTTP {r.status_code} {r.text[:120]}", 6)
    return None


def _urls(obj, limit=6):
    """Every web address inside a reply's citations, whatever their shape."""
    found = []

    def walk(x):
        if isinstance(x, str):
            if x.startswith("http") and x not in found:
                found.append(x)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(obj)
    return found[:limit]


def decide(state_obj, questions, subject="AI discussion thread"):
    """Answers for one item: free chat models via JSON, paid decision models via the Decisions API."""
    if not OPENROUTER_API_KEY:
        return None
    for model in sorted(DECISION_MODELS, key=lambda m: m in COOLING):   # models resting after earlier failures last
        if model in DEAD:
            continue
        free = model.endswith(":free")
        if free and not free_allowed(1, "helper"):
            return None
        if free and not model.startswith(DECISIONS_ONLY):
            ans = decide_chat(model, state_obj, questions, subject)
        elif not free and OPENROUTER_DAILY_USD > 0 and spent_today() >= OPENROUTER_DAILY_USD:
            continue
        else:
            ans = decide_api(model, state_obj, questions)
        if ans:
            return ans
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
        return [], []
    done, fresh = [], []
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
            if not isinstance(fb, dict):
                continue
            # The page may send an episode's feedback again when something is added (the same issue was also
            # read twice on 4 Nov 2026). The newest entry replaces the older one, and a pace vote that was
            # already counted is not counted again.
            old = next((f for f in state["feedback"] if fb.get("episode") and f.get("episode") == fb.get("episode")), None)
            if old:
                if {k: v for k, v in old.items() if k not in ("received", "pace_counted")} == fb:
                    continue
                state["feedback"].remove(old)
                if (old.get("ratings") or {}).get("pace") == (fb.get("ratings") or {}).get("pace"):
                    fb["pace_counted"] = True
            fb["received"] = dt.datetime.now(TZ).isoformat()
            state["feedback"].append(fb)
            fresh.append(fb)
        done.append(iss["number"])
        log(f"  feedback issue #{iss['number']}: {iss.get('title', '')}")
    return done, fresh


PACE_STEP, PACE_LIMIT = 0.04, 0.12


def update_pace(state, new_feedback):
    """Each new 'too fast' / 'too slow' vote moves the speaking speed 4% (at most 12% either way); 'right'
    keeps it. The setting is remembered, so one vote changes the pace once instead of for weeks.
    Returns the change made by this run's feedback (0 if none)."""
    before = float(state.get("pace_adjust", 0.0))
    adj = before
    for f in new_feedback:
        vote = None if f.get("pace_counted") else (f.get("ratings") or {}).get("pace")
        adj += -PACE_STEP if vote == "too fast" else PACE_STEP if vote == "too slow" else 0
    adj = round(max(-PACE_LIMIT, min(PACE_LIMIT, adj)), 3)
    state["pace_adjust"] = adj
    return round(adj - before, 3)


def apply_pace(adj):
    """The same pace setting for every voice engine (before, Fish - the voice actually used - ignored it)."""
    global PACE_NOTE, EDGE_RATE
    FISH_TEMPO[:] = [t * (1 + adj) for t in FISH_TEMPO_BASE]
    EDGE_RATE = f"{round(adj * 100):+d}%"
    PACE_NOTE = "; slightly slower, unhurried pace" if adj <= -0.02 else "; slightly brisker pace" if adj >= 0.02 else ""


def new_feedback_text(new_feedback, pace_change):
    """What the listener said since the last episode - the only feedback the greeting may mention."""
    if not new_feedback and not pace_change:
        return ""   # nothing to say: even "no feedback today" was said on air when this was a section
    out = ["NEW FEEDBACK SINCE THE LAST EPISODE (acknowledge it once, in one sentence):"]
    for f in new_feedback:
        r = f.get("ratings") or {}
        bits = [f"{k}: {r[k]}" for k in ("overall", "depth", "pace", "voices") if r.get(k)]
        pz = f.get("puzzles") or {}
        if pz.get("total"):
            bits.append(f"puzzles finished: {pz.get('done', 0)} of {pz['total']}")
        pl = f.get("play") or {}
        if pl.get("cards"):
            bits.append(f"played {pl.get('done', 0)} of {pl['cards']} cards on the page, {pl.get('points', 0)} points")
        more = [s for s, v in (f.get("segments") or {}).items() if v == "more"]
        less = [s for s, v in (f.get("segments") or {}).items() if v == "less"]
        line = f"- {f.get('date', '')} {f.get('edition', '')}: " + ", ".join(bits)
        if more:
            line += f"; wanted more like: {'; '.join(more)}"
        if less:
            line += f"; wanted less like: {'; '.join(less)}"
        if f.get("comment"):
            line += f"; comment: {f['comment'][:300]}"
        out.append(line)
    if pace_change:
        out.append(f"- Because of it, the voices are now about {abs(round(pace_change * 100))}% "
                   f"{'slower' if pace_change < 0 else 'faster'} than in the last episode.")
    else:
        out.append("- The speaking pace was NOT changed: do not claim it was.")
    return "\n".join(out)


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
        missed += (f.get("puzzles") or {}).get("struggled", [])   # puzzle steps that needed hints or the answer
        m = meta.get(f.get("episode"))
        if m and r.get("overall"):
            by_writer.append(f"{m.get('writer', '?')}/{m.get('voices', '?')}: {r['overall']} stars")
    out = [f"LISTENER FEEDBACK from {LISTENER} (most recent last). Follow it quietly - do not talk about it on air:"]
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
            out.append("- Quiz questions and puzzles he found hard (explain these ideas more clearly): "
                       + "; ".join(missed[-6:]))
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


def outside_link(s):
    """True when the thread points at something other than Reddit or social media."""
    u = primary_link(s)
    return bool(u) and not any(d in urlparse(u).netloc for d in SKIP_DOMAINS)


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


def unconfirmed(s):
    """Nothing outside Reddit backs this thread: no readable source, no trusted outlet, no Hacker News."""
    return not ((s.get("evidence") and not s["evidence"].startswith("(could not")) or s.get("also_in") or s.get("hn"))


WEB_CHECK = {"reported": {
    "type": "choice",
    "instructions": "Search the web. Is the central factual claim of this discussion thread reported by a primary "
                    "source (the company, agency, journal or paper itself) or a reputable news outlet?",
    "criteria": {"confirmed": "A primary source or reputable outlet reports the same thing.",
                 "disputed": "Reputable sources contradict it, or report something materially different.",
                 "not_found": "No primary source or reputable outlet reports it; it appears only on forums or "
                              "social media."}}}


def web_check(s):
    """For a thread nothing else backs: ask the web-grounded decision model whether the claim is reported
    anywhere, then READ the pages it cites. Only a page that could actually be read counts as a source - the
    model's word alone never turns a rumour into a confirmed story."""
    if "web" in s or not unconfirmed(s):
        return
    s["web"] = {}
    model = WEB_CHECK_MODEL
    if model.lower() in ("", "off", "none", "false") or not OPENROUTER_API_KEY or model in DEAD:
        return
    if model.endswith(":free") and not free_allowed(1, "helper"):
        return
    ans = decide_api(model, {"title": s["title"], "post": s["text"][:1500], "linked_site": urlparse(s.get("link", "")).netloc},
                     WEB_CHECK)
    if not ans:
        return
    verdict = (ans.get("reported") or {}).get("choice", "")
    probs = (ans.get("reported") or {}).get("probabilities") or {}
    s["web"] = {"verdict": verdict, "p": probs.get(verdict), "pages": []}
    for url in ans.get("_citations", [])[:3]:
        text = fetch_article(url, 5000)
        if text and not text.startswith("(could not"):
            s["web"]["pages"].append({"site": urlparse(url).netloc, "url": url})
            if unconfirmed(s):   # the first readable page becomes the segment's source
                s["source_url"], s["evidence"] = url, text
    log(f"    web check ({nice_model(model)}): {verdict or '?'}"
        + (f", read {', '.join(pg['site'] for pg in s['web']['pages'])}" if s["web"]["pages"] else ", no readable page"))


def check_block(s):
    lines = []
    if s.get("evidence") and not s["evidence"].startswith("(could not"):
        lines.append(f"- Source read directly: {urlparse(s.get('source_url', '')).netloc or 'linked page'}")
    web = s.get("web") or {}
    if web.get("pages"):
        lines.append(f"- A web search found it reported by: {', '.join(pg['site'] for pg in web['pages'])} "
                     f"(text above; search verdict: {web.get('verdict')}).")
    elif web.get("verdict") == "disputed":
        lines.append("- A web search suggests reputable sources contradict this claim, but no page could be read: "
                     "say the claim is disputed and unverified.")
    elif web.get("verdict") == "confirmed":
        lines.append("- A web search suggests it is reported elsewhere, but no such page could be read here: say it "
                     "is not yet verified by the show.")
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

    deadline = [time.time() + SCORING_BUDGET_S]

    def one(s):
        if time.time() > deadline[0]:   # out of time: this thread goes to the batched call
            return s["id"], None
        st = {"title": s["title"], "communities": sorted(set(s["subs"])), "linked_site": urlparse(s["link"]).netloc,
              "post": s["text"][:1500], "top_comments": [c[:300] for c in s["comments"][:8]]}
        return s["id"], decide(st, SCORE_QUESTIONS)

    results = {}
    if DECISION_MODELS and SCORING in ("auto", "decisions") and not batch_only:
        t0 = time.time()
        deadline[0] = t0 + SCORING_BUDGET_S
        with cf.ThreadPoolExecutor(max_workers=SCORING_WORKERS) as ex:
            for sid, ans in ex.map(one, top):
                if ans:
                    results[sid] = ans
        log(f"  decision models scored {len(results)}/{len(top)} in {time.time() - t0:.0f}s "
            f"(limit {SCORING_BUDGET_S:.0f}s; the rest are scored in one batch)")
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


# =============================== WEEKLY DOSSIER ===============================
# Reddit threads leave the pool after 36 hours. Each weekday run keeps what it learned about every tracked topic
# (best comments, the source it read, Hacker News) and its best unused threads, so Sunday has a week of material.
DOSSIER = {}


def load_dossier():
    try:
        data = json.loads(DOSSIER_FILE.read_text(encoding="utf-8")) if DOSSIER_FILE.exists() else {}
    except (OSError, ValueError):
        data = {}
    data.setdefault("topics", {})
    data.setdefault("missed", [])
    return data


def _entry(s, date_key, used):
    e = {"date": date_key, "title": s["title"], "subs": sorted(set(s["subs"])), "thread": s["threads"][0],
         "link": s.get("link", ""), "text": s.get("text", "")[:800], "comments": [c[:400] for c in s["comments"][:5]],
         "score": round(s.get("score", 0), 3), "used": s["id"] in used, "subject": s.get("subject", "ai"),
         "topic": s.get("topic", "")}
    if s.get("evidence") and not s["evidence"].startswith("(could not"):
        e["evidence"], e["source_url"] = s["evidence"][:2500], s.get("source_url", "")
    if s.get("hn"):
        e["hn"] = s["hn"]
    return e


def _merge(entries, e):
    """Same thread seen again later: keep the richer copy (comments mature over the day)."""
    for i, old in enumerate(entries):
        if old["thread"] == e["thread"]:
            keep = e if len(" ".join(e["comments"])) >= len(" ".join(old["comments"])) else dict(old)
            for k in ("evidence", "source_url", "hn"):
                keep.setdefault(k, old.get(k) or e.get(k))
            keep["used"] = old.get("used") or e.get("used")
            keep["date"] = old["date"]
            entries[i] = {k: v for k, v in keep.items() if v is not None}
            return
    entries.append(e)


def update_dossier(stories, used, digest, state, date_key):
    cutoff = (dt.date.fromisoformat(date_key) - dt.timedelta(days=DOSSIER_DAYS)).isoformat()
    topics = DOSSIER.setdefault("topics", {})
    for s in stories:
        key = s.get("topic")
        if not key or key in NO_TOPIC:
            continue
        t = topics.setdefault(key, {"entries": []})
        t["name"] = state["topics"].get(key, {}).get("name", key)
        t["subject"] = state["topics"].get(key, {}).get("subject", s.get("subject", "ai"))
        _merge(t["entries"], _entry(s, date_key, used))
    for key in list(topics):
        es = sorted((e for e in topics[key]["entries"] if e["date"] >= cutoff), key=lambda e: -e.get("score", 0))[:15]
        if es:
            topics[key]["entries"] = sorted(es, key=lambda e: e["date"])
        else:
            del topics[key]
    lines = {d["thread"]: d.get("line", "") for d in digest}
    missed = DOSSIER.setdefault("missed", [])
    for s in [s for s in stories if s["id"] not in used][:4]:   # the best threads that did not make the episode
        e = _entry(s, date_key, used)
        e["line"] = lines.get(e["thread"], "")
        _merge(missed, e)
    DOSSIER["missed"] = sorted((e for e in missed if e["date"] >= cutoff), key=lambda e: e["date"])[-60:]
    DOSSIER["updated"] = dt.datetime.now(dt.timezone.utc).isoformat()
    log(f"  dossier: {len(topics)} topics, {sum(len(t['entries']) for t in topics.values())} threads, "
        f"{len(DOSSIER['missed'])} that got away")


def save_dossier(dry):
    DOSSIER_FILE.parent.mkdir(parents=True, exist_ok=True)
    DOSSIER_FILE.write_text(json.dumps(DOSSIER, ensure_ascii=False), encoding="utf-8")
    if dry:
        return
    if gh("release", "view", "reddit-pool", check=False).returncode != 0:
        gh("release", "create", "reddit-pool", "--title", "Reddit pool", "--latest=false",
           "--notes", "Reddit threads and the weekly dossier for the podcast. Managed automatically.", check=False)
    res = gh("release", "upload", "reddit-pool", str(DOSSIER_FILE), "--clobber", check=False)
    if res.returncode != 0:
        log(f"  could not save the dossier: {res.stderr.strip()[:200]}")


def dossier_block(key, limit=24000):
    t = DOSSIER.get("topics", {}).get(key)
    if not t:
        return ""
    out = []
    for e in t["entries"]:
        row = [f"{e['date']} | {e['title']} ({', '.join(e['subs'])})" + (" [covered in our episode]" if e.get("used") else "")]
        if e.get("text"):
            row.append(f"  Post: {e['text'][:400]}")
        if e.get("comments"):
            row.append("  Comments: " + " | ".join(c[:250] for c in e["comments"][:4]))
        if e.get("evidence"):
            row.append(f"  Source ({urlparse(e.get('source_url', '')).netloc}): {e['evidence'][:1200]}")
        if e.get("hn"):
            row.append(f"  Hacker News ({e['hn'].get('points', 0)} points): " + " | ".join(c[:200] for c in e["hn"].get("comments", [])[:3]))
        out.append("\n".join(row))
    return "\n\n".join(out)[:limit]


def dossier_story(e, sid):
    """A dossier entry in the shape write_segment expects."""
    st = {"id": sid, "title": e["title"], "subs": e.get("subs") or ["Reddit"], "threads": [e["thread"]],
          "link": e.get("link", ""), "nlink": norm_link(e.get("link", "")), "text": e.get("text", ""),
          "comments": e.get("comments", []), "also_in": [], "subject": e.get("subject", "ai"),
          "topic": e.get("topic", ""), "score": e.get("score", 0)}
    if e.get("evidence"):
        st.update(evidence=e["evidence"], source_url=e.get("source_url", ""), hn=e.get("hn"))
    return st


def got_away(state, subject, date_key, n):
    """The best threads of the week that never made an episode, preferring this episode's subject."""
    today = dt.date.fromisoformat(date_key)
    seen = {x["link"] for x in state["seen"]}
    cands = [e for e in DOSSIER.get("missed", [])
             if (today - dt.date.fromisoformat(e["date"])).days <= 7 and e["thread"] not in seen
             and not (e.get("link") and e["link"] in seen)]
    cands.sort(key=lambda e: (e.get("subject", "ai") != subject, -e.get("score", 0)))
    picks, topics = [], set()

    def same_story(a, b):   # the same story posted again on another day
        if a.get("link") and norm_link(a["link"]) == norm_link(b.get("link", "")):
            return True
        wa, wb = title_words(a["title"]), title_words(b["title"])
        return len(wa) >= 3 and len(wa & wb) / max(1, len(wa | wb)) >= 0.6

    for e in cands:
        if (e.get("topic") and e["topic"] in topics) or any(same_story(e, x) for x in picks):
            continue
        picks.append(e)
        topics.add(e.get("topic"))
        if len(picks) == n:
            break
    return picks


def follow_ups(state, date_key, limit=3):
    """Stories from this week's episodes that moved on afterwards: newer dossier threads on the same topic."""
    today = dt.date.fromisoformat(date_key)
    covered = {}
    for m in state["memory"]:
        if m.get("date") and (today - dt.date.fromisoformat(m["date"])).days <= 7 and not m.get("deep_topic"):
            for t in m.get("topics", []):
                covered.setdefault(t, (m["date"], m.get("segments", [])))
    out = []
    for key, (when, segs) in covered.items():
        t = DOSSIER.get("topics", {}).get(key)
        newer = [e for e in (t or {}).get("entries", []) if e["date"] > when and not e.get("used")]
        if newer:
            out.append((key, when, sorted(newer, key=lambda e: -e.get("score", 0))[:2]))
    out.sort(key=lambda x: -max(e.get("score", 0) for e in x[2]))
    return out[:limit]


def plain_titles(entries):
    """Short spoken titles for threads that did not go through the editor."""
    listing = "\n".join(f"[{i}] {e['title']} :: {e.get('line') or e.get('text', '')[:200]}" for i, e in enumerate(entries))
    try:
        text, _ = llm(f"""Give each thread a short spoken podcast title in plain words (no jargon, model numbers or
acronyms; at most 8 words). Return JSON only: {{"titles": ["...", "..."]}}

{listing}""", json_mode=True, label="titles", role="helper")
        titles = as_obj(parse_json(text), "titles").get("titles", [])
    except Exception:
        titles = []
    return [str(titles[i]) if i < len(titles) and titles[i] else e["title"][:70] for i, e in enumerate(entries)]


def plan_sunday(edition, state, stories, date_key, minutes):
    """Weekend magazine: what happened next, the week's deep dive, and the ones that got away."""
    subject = "ai" if edition == "morning" else "science"
    deep = sunday_topic(state, date_key, subject) or sunday_topic(state, date_key,
                                                                    "science" if subject == "ai" else "ai")
    got = got_away(state, subject, date_key, 3 if deep else 6)
    news = follow_ups(state, date_key)
    share = {"news": 0.15 if news else 0, "deep": 0.56 if deep else 0, "got": 0.29 if got else 0}
    if not sum(share.values()):
        return "", [], None
    body = int(minutes * WORDS_PER_MIN) - 300
    words = {k: int(body * v / sum(share.values())) for k, v in share.items()}
    plan, sid = [], 100000
    if news:
        items = []
        for key, when, entries in news:
            for e in entries:
                items.append(dossier_story(e, sid))
                sid += 1
        names = "; ".join(f"{DOSSIER['topics'][k].get('name', k)} (covered {w}: {', '.join(m[:2])})" for k, w, m in
                          [(k, w, next((m.get('segments', []) for m in state['memory'] if m.get('date') == w), []))
                           for k, w, _ in news])
        plan.append({"title": "What happened next", "format": "followup", "item_ids": [], "stories": items,
                     "angle": f"Updates on stories from this week's episodes: {names}", "words": words["news"]})
    if deep:
        _, parts = plan_deep_dive(deep, state, stories, words["deep"] / WORDS_PER_MIN + 300 / WORDS_PER_MIN)
        plan += parts
    if got:
        for e, title in zip(got, plain_titles(got)):
            plan.append({"title": title, "format": "thread", "item_ids": [], "stories": [dossier_story(e, sid)],
                         "angle": "One of the week's most interesting threads that did not make a weekday episode: "
                                  "what it claims, what people argued, and whether it holds up.",
                         "words": words["got"] // len(got)})
            sid += 1
    name = state["topics"][deep]["name"] if deep else "the ones that got away"
    theme = f"Sunday edition: {name}" + (" - plus the week's best threads that got away" if deep and got else "")
    return theme, plan, deep


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
        material = len(DOSSIER.get("topics", {}).get(key, {}).get("entries", []))   # what the week saved
        rank = (len(week), material, len(t["subs"]), t["mentions"])
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
- Pitch: a smart engineer outside computer science and outside biology. Define each specialist term (CS/ML,
  genetics, cell biology, clinical medicine, physics) once, the first time, in plain words, ideally with an
  analogy from mechanical engineering, control systems, optimisation, materials, or manufacturing quality.
  One idea at a time; never stack jargon. Prefer the mechanism over the buzzword. If a term will not be
  needed again, do not use it at all - say the plain thing instead.
- {LISTENER} is the expert in: {EXPERT_AREAS}. Never explain those terms to him (no "validation - that's
  the process of proving..."); use them the way a colleague would, and as the analogies. Explaining his own
  field to him sounds patronising.
- Do not avoid real concepts to keep it easy: he enjoys meeting new ideas even when he does not follow every
  detail at first - that is how new words stick. Name each concept by its proper name ("Dalton's law",
  "partial pressure", "learning rate") so he can remember it and look it up; the page's puzzles let him try it.
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
                                      "no outside source" if s.get("needs_check") and not s.get("also_in")
                                      and not outside_link(s) else "",
                                      ("interest: " + "/".join(s["interests"][:2])) if s.get("interests") else "") if x)
        rows.append(f"[{s['id']}] {s.get('subject', 'ai').upper()} ({', '.join(sorted(set(s['subs'])))}; "
                    f"{len(s['comments'])} comments; {flags}) "
                    f"{s['title']}" + (f" :: {s['text'][:220]}" if s["text"] else ""))
    return "\n".join(rows)


def episode_shape(minutes):
    """(number of topics, words per topic) for an episode of this length."""
    body = max(SEGMENT_WORDS, int(minutes * WORDS_PER_MIN) - 250)   # 250 words for the opening and close
    n = max(1, body // SEGMENT_WORDS)
    # The old 1300-word cap made "20-minute" episodes about 18 minutes (2 x 1300 words + bookends).
    return n, min(MAX_SEGMENT_WORDS, body // n)


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
- A news claim flagged "no outside source" exists only on Reddit so far: choose it only when nothing better is
  on the list, or when the discussion itself (not the claim) is the story.
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
        ("The week's story", "How the discussion built up over the week, day by day, using THE WEEK'S DOSSIER: what "
         "people claimed on which day, what they argued about, and what changed as evidence came in.", 0.25),
        ("How it actually works", "From first principles for an engineer outside the field: the problem it solves, "
         "the mechanism, analogies and key terms - and briefly how we got here. Only use facts you are sure of "
         "or that are in the sources.", 0.4),
        ("Reality check", "What the evidence shows today; where it meets the real world, including (where it "
         f"honestly applies) medicine, manufacturing, quality and regulated work like {LISTENER}'s; the honest "
         "limitations and open questions to watch - no predictions.", 0.35),
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
            web_check(s)
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
    "followup": "Short updates on stories from this week's episodes. For each: one line on what we said then, then "
                "what happened since, only from the sources (newer threads). Skip a story if nothing new is in "
                "the sources. Clear verbal transitions.",
    "deepdive": "This is one part of a multi-part deep dive. Go slower and deeper than a news segment, but keep "
                "the listener's level. Use analogies, recap key ideas, and connect to the earlier parts.",
}


def write_segment(seg, items, state, position, total, theme):
    target = int(seg["words"])
    ask = int(target * 1.15)   # the writers deliver about 85% of the words asked for (1250 for 1475 on 4 Nov 2026)
    fmt = seg.get("format", "thread")
    topic = state["topics"].get(seg.get("topic", "")) if fmt == "deepdive" else None
    claims = "; ".join(seg.get("claims_to_check") or []) or "(identify them from the thread)"
    presenter, asker = (A, B) if position % 2 == 1 else (B, A)   # the hosts take turns bringing stories
    sources = sources_block(items, topic)
    # On 12 Nov 2026 a seven-minute segment on an FDA warning ended with "by the way, we could not confirm any
    # of this". The listener must hear that first, not last.
    if fmt in ("thread", "concept") and items and all(unconfirmed(s) for s in items):
        confirm = ("CONFIRMATION: nothing outside Reddit confirms this story. Say so plainly within the first three "
                   "turns (\"worth saying up front: we could not find this anywhere else\"). Keep the unconfirmed "
                   "details brief and spend most of the segment on what IS established - the mechanism, the "
                   "background, and how such a claim would be checked. Never leave the caveat to the end.")
    elif fmt in ("thread", "concept"):
        confirm = ("CONFIRMATION: within the first few turns, say in half a sentence where the facts come from (the "
                   "paper, the company's own post, a named outlet) - see INDEPENDENT CHECK in the sources.")
    else:
        confirm = ""
    if fmt == "deepdive" and dossier_block(seg.get("topic", "")):
        sources += "\n\n-----\n\nTHE WEEK'S DOSSIER (what each day's threads, comments and sources said):\n" \
                   + dossier_block(seg.get("topic", ""))
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
{confirm}

PAST EPISODES (mention one only if it genuinely connects):
{memory_text(state, 5)}

{feedback_text(state, short=True)}

SOURCES:
{sources}

WRITE about {ask} words of natural spoken dialogue (between {int(ask * 0.9)} and {int(ask * 1.1)}).
FORMAT - every line starts with a speaker name and a colon:
{A}: ...
{B}: ...
RULES: real conversation with short and longer turns; at most ONE number per turn (a listener in a car cannot
hold more), and round it ("about thirty thousand"); explain each specialist term (computer science, AI, biology,
medicine, physics) once, briefly, in plain words, and use at most one new term per turn - and never explain
quality, regulatory, manufacturing or engineering terms ({LISTENER} knows them);
do not chain several definitions in one turn; no greeting or sign-off; do not mention listener feedback;
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
3. Specialist terms from computer science, AI, genetics, cell biology, clinical medicine or physics used
   without a short plain-words explanation the first time ("latent space", "chromatin accessibility"). Do NOT
   flag terms from the listener's own field ({EXPERT_AREAS}); instead flag any place that EXPLAINS such a
   term to him - he is an expert and it sounds patronising.
4. Anything confusing to hear (lists of figures, long chains of acronyms, several definitions in one turn).
Return JSON only:
{{"problems": [{{"quote": "<a few words from the script>", "issue": "<what is wrong>", "fix": "<how to fix>"}}]}}
Return an empty list if the segment is accurate and clear."""
    try:
        text, _ = llm(prompt, json_mode=True, label=f"check {position}", role="helper")
        problems = [p for p in as_obj(parse_json(text), "problems").get("problems", []) if isinstance(p, dict)]
    except Exception as e:
        log(f"    fact-check skipped: {str(e)[:120]}")
        return turns
    off_length = not 0.9 * target <= n <= 1.25 * target   # short segments made 20-minute episodes 17-18 minutes
    log(f"    check {position}: {len(problems)} problem(s), {n} words for a target of {target}")
    if not problems and not off_length:
        return turns
    fixes = "\n".join(f"- \"{p.get('quote', '')}\": {p.get('issue', '')} -> {p.get('fix', '')}" for p in problems[:25])
    prompt = f"""Revise this segment of the podcast "{SHOW_TITLE}". Keep the hosts, their voices and energy, the flow
and the format (every line "{A}: ..." or "{B}: ..."). Fix every problem listed. Remove or soften any claim the
sources do not support (say it is unclear or only claimed on Reddit). At most ONE number per turn. Explain each
specialist term (computer science, AI, biology, medicine, physics) once in plain words, or replace it with
plain words; never explain {LISTENER}'s own field ({EXPERT_AREAS}).
Length: about {target if n >= target else int(target * 1.1)} words (now {n}).

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


def next_episode(edition, now):
    """When the listener hears the show next. There are two episodes every day, weekends included."""
    if edition == "morning":
        return (f"this evening, ready by {due.ready_by('evening')}"
                + (" (Sunday's long science edition)" if now.weekday() == 6 else ""))
    return (f"tomorrow morning, ready by {due.ready_by('morning')}"
            + (" (Sunday's long AI edition)" if now.weekday() == 5 else ""))


def write_bookends(state, segments_written, theme, edition, now, deep_dive, source_note, fresh_feedback="",
                   play=None):
    outline = "\n".join(f"- {s['title']}: {s.get('angle', '')}" for s, _ in segments_written)
    puzzle_note = ""
    if play:
        kinds = sorted({ROUNDS[r["type"]]["name"].lower() if r["type"] in ROUNDS else "a hands-on experiment"
                        for sg in play["segments"] for r in sg["rounds"]})
        bets = [sg["bet"]["claim"] for sg in play["segments"] if sg.get("bet")]
        puzzle_note = (f"The episode's page has a three-minute game called \"{play['title']}\" ({', '.join(kinds)}). "
                       "In the closing, one host invites him to play it in ONE short sentence that says what he "
                       "gets to do - no list, no web address.")
        if bets:
            puzzle_note += ("\nA BET FOR THE DRIVE: at the end of the greeting, one host puts this to him as a quick "
                            "true-or-false bet to keep in mind while listening, WITHOUT giving the answer or a hint: "
                            f"\"{bets[0]}\"")
    prompt = f"""You write the opening and closing for "{SHOW_TITLE}" (hosts {A} and {B}).
The listener is {LISTENER}; {'it is Sunday: a long magazine episode for a long drive' if now.weekday() == 6 else 'he listens on a short commute'}.
Today is {now:%A, %d %B %Y}, {edition} edition.
{'This episode is a DEEP DIVE: ' + theme if deep_dive else 'Theme: ' + theme}
Where today's material came from: {source_note}
The next episode is {next_episode(edition, now)}. The show comes out twice EVERY day, weekends included: never
say "see you Monday", "that's the week" or "have a great weekend".
{puzzle_note}

TODAY'S SEGMENTS:
{outline}

PAST EPISODES (most recent last):
{memory_text(state)}

{fresh_feedback}

Return JSON only:
{{"greeting": [["{A}", "..."], ["{B}", "..."], ...],
  "closing": [["{A}", "..."], ["{B}", "..."]],
  "summary": "<3-4 sentence factual summary of today's episode for future episodes>",
  "threads": ["<story to watch for follow-ups>", "..."]}}

Greeting: 60-110 words, 3-5 turns, upbeat ({A} especially). Greet {LISTENER} by name naturally; the hosts do not
introduce themselves by name. Link to past episodes only where it truly connects. Preview each segment in ONE
plain sentence a stranger would understand - no lists of numbers or names, and no technical term or acronym
the episode has not explained yet. Only if a NEW FEEDBACK section appears above, acknowledge it in
one sentence; otherwise say nothing at all about feedback.
{'For a deep dive, say why this topic is ready now (it built up over the week).' if deep_dive else ''}
Vary the wording every time. Closing: 2-3 short turns, 30-60 words, no hype; say when the next episode is,
exactly as given above (never invent a time)."""
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


def fish_turn(spk, text, voices):
    """One spoken turn from Fish Audio (OpenRouter free) as PCM."""
    body = {"model": FISH_MODEL, "input": text, "response_format": "mp3"}
    if len(voices) == 2:
        body["voice"] = {A: voices[0], B: voices[1]}[spk]
    else:
        body["input_references"] = fish_reference(spk)
    for attempt in range(1, 4):
        if FREE_DAY_OUT[0] == utc_day():
            raise RuntimeError("OpenRouter free daily limit reached")
        free_pace()   # request starts stay 3.2 s apart even with several in flight (20 a minute)
        try:
            r = requests.post("https://openrouter.ai/api/v1/audio/speech", timeout=(20, 180), json=body,
                              headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}",
                                       "HTTP-Referer": PAGES_URL, "X-Title": SHOW_TITLE})
        except requests.RequestException as e:
            if attempt < 3:
                time.sleep(10 * attempt)
                continue
            raise RuntimeError(f"Fish Audio connection failed: {str(e)[:120]}")
        note_free()
        if r.status_code == 200 and r.content:
            return ffmpeg_to_pcm(r.content, FISH_TEMPO[0 if spk == A else 1])
        if free_limit_hit(r.status_code, r.text[:300]):
            raise RuntimeError("OpenRouter free daily limit reached")
        if r.status_code in (429, 500, 502, 503, 504) and attempt < 3:
            time.sleep(30 if r.status_code == 429 else 10 * attempt)
            continue
        raise RuntimeError(f"Fish Audio HTTP {r.status_code}: {r.text[:160]}")
    raise RuntimeError("Fish Audio failed")


def fish_tts_turns(turns):
    """All turns of a chunk, a few requests in flight at once (one at a time took 12-15 minutes an episode)."""
    voices = fish_voices()
    if len(voices) != 2:
        for spk in (A, B):
            fish_reference(spk)   # make the reference clips once, before the parallel requests
    ex = cf.ThreadPoolExecutor(max_workers=FISH_WORKERS)
    try:
        futures = [ex.submit(fish_turn, spk, text, voices) for spk, text in turns]
        return [f.result() for f in futures]
    finally:
        ex.shutdown(wait=True, cancel_futures=True)   # after a failure, do not spend requests on the rest


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


def edge_tts_turns(turns):
    voice = {A: HOST_A["edge"], B: HOST_B["edge"]}
    return [ffmpeg_to_pcm(asyncio.run(_edge_turn(text, voice[spk]))) for spk, text in turns]


ENGINE_NAMES = {"gemini": "Gemini TTS", "fish": "Fish Audio (free)", "edge": "Edge TTS"}
TURN_GAP = 0.25
TURN_RMS = 2000.0   # about -24 dBFS: every turn is brought near this level, so the two hosts sound equally loud


def level_match(pcm):
    """Bring one spoken turn to a common level (at most 6 dB up or down, never clipping). The two Fish voices
    differed by about 3 dB, so the car volume had to be right for one host or the other."""
    a = array.array("h")
    a.frombytes(pcm[:len(pcm) // 2 * 2])
    if sys.byteorder != "little":
        a.byteswap()
    if len(a) < RATE:   # under a second ("Wait, really?"): leave it as spoken
        return pcm
    rms = math.sqrt(sum(x * x for x in a) / len(a))
    if rms < 50:
        return pcm
    peak = max(max(a), -min(a), 1)
    gain = min(max(0.5, min(2.0, TURN_RMS / rms)), 32000 / peak)
    if abs(gain - 1) < 0.06:
        return pcm
    out = array.array("h", [int(x * gain) for x in a])
    if sys.byteorder != "little":
        out.byteswap()
    return out.tobytes()


def chime():
    """A soft two-note chime (E5 then C5) that marks a new segment, so a driver hears where a story starts."""
    n = int(RATE * 1.1)
    buf = [0.0] * n
    for freq, start, amp in ((659.25, 0.0, 0.16), (523.25, 0.28, 0.14)):
        s0 = int(start * RATE)
        for i in range(n - s0):
            t = i / RATE
            env = min(1.0, t / 0.006) * math.exp(-t * 4.0)
            buf[s0 + i] += amp * env * (math.sin(2 * math.pi * freq * t) + 0.25 * math.sin(4 * math.pi * freq * t))
    out = array.array("h", [int(max(-1.0, min(1.0, x)) * 32767) for x in buf])
    if sys.byteorder != "little":
        out.byteswap()
    return out.tobytes()


def transition():
    return silence(0.6) + chime() + silence(0.5) if SEGMENT_CHIME else silence(1.2)


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
            pcm, marks, prev = bytearray(), [], None
            t0 = time.time()
            for i, chunk in enumerate(chunks, 1):
                turns = [(spk, text) for _, spk, text in chunk]
                log(f"  {engine} TTS chunk {i}/{len(chunks)} ({turns_words(turns)} words)")
                if engine == "gemini":   # one audio file per chunk: segment starts are estimated from word counts
                    audio = gemini_tts_chunk(turns)
                    state["tts_log"].append(dt.datetime.now(TZ).isoformat())
                    if prev is not None and chunk[0][0] != prev:
                        pcm += transition()
                    start, total_w, acc = len(pcm) / BYTES_PER_SEC, max(1, turns_words(turns)), 0
                    for label, _, text in chunk:
                        if label != prev:
                            marks.append((label, start + (acc / total_w) * len(audio) / BYTES_PER_SEC))
                            prev = label
                        acc += words(text)
                    pcm += audio + silence(0.4)
                    time.sleep(8)
                    continue
                # Fish and Edge speak one turn at a time: matched levels, a transition between segments,
                # and exact segment start times for the chapters.
                parts = fish_tts_turns(turns) if engine == "fish" else edge_tts_turns(turns)
                for (label, _, _), audio in zip(chunk, parts):
                    if label != prev:
                        if prev is not None:
                            pcm += transition()
                        marks.append((label, len(pcm) / BYTES_PER_SEC))
                        prev = label
                    pcm += level_match(audio) + silence(TURN_GAP)
            log(f"  voices: {len(flat)} turns in {time.time() - t0:.0f}s")
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


def loudness_filter(pcm):
    """ffmpeg loudnorm settings for TARGET_LUFS: measured first (two passes land within about 1 LU of the
    target; one pass about 1.3 LU under). Returns "" when switched off."""
    try:
        target = float(TARGET_LUFS)
    except ValueError:
        return ""
    if target >= 0:
        return ""
    base = f"loudnorm=I={target:g}:TP=-1.5:LRA=11"
    res = subprocess.run([FFMPEG, "-hide_banner", "-nostats", "-f", "s16le", "-ar", str(RATE), "-ac", "1",
                          "-i", "pipe:0", "-af", base + ":print_format=json", "-f", "null", "-"],
                         input=pcm, capture_output=True)
    err = res.stderr.decode("utf-8", "replace")
    try:
        m = json.loads(err[err.rindex("{"):err.rindex("}") + 1])
        log(f"  loudness: {m['input_i']} LUFS -> {target:g} LUFS")
        return (f"{base}:measured_I={m['input_i']}:measured_TP={m['input_tp']}:measured_LRA={m['input_lra']}"
                f":measured_thresh={m['input_thresh']}:offset={m['target_offset']}")
    except (ValueError, KeyError):
        log("  loudness: could not measure; normalising in one pass")
        return base


def ffmeta(s):
    return re.sub(r"([=;#\\\n])", r"\\\1", str(s))


def encode_mp3(pcm, out_path, title="", chapters=()):
    """MP3 at podcast loudness, with the episode title and one chapter per segment (podcast apps show them,
    and the car's next / previous buttons can skip a story)."""
    duration_ms = int(len(pcm) / BYTES_PER_SEC * 1000)
    meta = [";FFMETADATA1", f"title={ffmeta(title)}", f"artist={ffmeta(SHOW_TITLE)}", f"album={ffmeta(SHOW_TITLE)}",
            "genre=Podcast"]
    starts = [(label, int(sec * 1000)) for label, sec in chapters]
    for i, (label, start) in enumerate(starts):
        end = starts[i + 1][1] if i + 1 < len(starts) else duration_ms
        if end > start:
            meta += ["[CHAPTER]", "TIMEBASE=1/1000", f"START={start}", f"END={end}", f"title={ffmeta(label)}"]
    meta_file = Path(out_path).with_suffix(".meta.txt")
    meta_file.write_text("\n".join(meta) + "\n", encoding="utf-8")
    filt = loudness_filter(pcm)
    res = subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-y", "-f", "s16le", "-ar", str(RATE),
                          "-ac", "1", "-i", "pipe:0", "-i", str(meta_file), "-map", "0:a", "-map_metadata", "1",
                          "-map_chapters", "1", *(["-af", filt] if filt else []), "-ar", str(RATE),
                          "-codec:a", "libmp3lame", "-b:a", "64k", "-id3v2_version", "3", str(out_path)],
                         input=pcm, capture_output=True)
    if res.returncode != 0:
        raise RuntimeError(f"ffmpeg encode failed: {res.stderr[:200]}")


# =============================== 8a. PUZZLES ===============================
# Brilliant-style lessons on quiz.html: a model writes them as data (sliders with formulas, predictions, numbers to
# work out, steps to put in order); the page brings them to life. The model never writes code, and every formula
# is run here first, so a lesson that does not compute never reaches the page. The same tiny formula language is
# implemented in quiz.html.
def _ncdf(x):
    """Share of a normal (bell-curve) population below x standard deviations. Abramowitz & Stegun 7.1.26
    (error under 1.5e-7); quiz.html uses the same formula, so both give identical numbers."""
    z = abs(x) / math.sqrt(2)
    t = 1 / (1 + 0.3275911 * z)
    y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * math.exp(-z * z)
    return 0.5 * (1 + (y if x >= 0 else -y))


EXPR_FUNCS = {"min": min, "max": max, "abs": abs, "sqrt": math.sqrt, "exp": math.exp, "ln": math.log,
              "log10": math.log10, "pow": pow, "round": round, "floor": math.floor, "ceil": math.ceil,
              "sin": math.sin, "cos": math.cos, "tan": math.tan, "ncdf": _ncdf}
EXPR_CONST = {"pi": math.pi, "e": math.e}
_EXPR_TOKEN = re.compile(r"\s*(?:(\d+\.?\d*(?:[eE][-+]?\d+)?|\.\d+(?:[eE][-+]?\d+)?)|([A-Za-z_]\w*)|(\S))")
IDENT = re.compile(r"^[A-Za-z_]\w{0,24}$")


def expr_parse(src):
    """Formula -> tree. Numbers, names, + - * / ^, brackets and EXPR_FUNCS; nothing else (no code)."""
    src = str(src)
    if len(src) > 240:
        raise ValueError("formula too long")
    toks, pos = [], 0
    while pos < len(src):
        m = _EXPR_TOKEN.match(src, pos)
        if not m or m.end() == pos:
            break
        pos = m.end()
        if m.group(1):
            toks.append(("num", float(m.group(1))))
        elif m.group(2):
            toks.append(("id", m.group(2)))
        elif m.group(3):
            if m.group(3) not in "+-*/^(),":
                raise ValueError(f"unexpected {m.group(3)!r}")
            toks.append(("op", m.group(3)))
    if src[pos:].strip():
        raise ValueError("unreadable formula")
    i = [0]

    def peek(v=None):
        t = toks[i[0]] if i[0] < len(toks) else ("end", None)
        return t if v is None else t == ("op", v)

    def take(v):
        if not peek(v):
            raise ValueError(f"expected {v!r}")
        i[0] += 1

    def expr(depth=0):
        if depth > 30:
            raise ValueError("formula too deep")
        node = term(depth)
        while peek("+") or peek("-"):
            op = toks[i[0]][1]; i[0] += 1
            node = ("bin", op, node, term(depth))
        return node

    def term(depth):
        node = unary(depth)
        while peek("*") or peek("/"):
            op = toks[i[0]][1]; i[0] += 1
            node = ("bin", op, node, unary(depth))
        return node

    def unary(depth):
        if peek("-"):
            i[0] += 1
            return ("neg", unary(depth + 1))
        if peek("+"):
            i[0] += 1
            return unary(depth + 1)
        node = atom(depth)
        if peek("^"):
            i[0] += 1
            node = ("bin", "^", node, unary(depth + 1))
        return node

    def atom(depth):
        kind, val = peek()
        if kind == "num":
            i[0] += 1
            return ("num", val)
        if kind == "id":
            i[0] += 1
            if peek("("):
                if val not in EXPR_FUNCS:
                    raise ValueError(f"unknown function {val}")
                i[0] += 1
                args = [expr(depth + 1)]
                while peek(","):
                    i[0] += 1
                    args.append(expr(depth + 1))
                take(")")
                return ("call", val, args)
            return ("num", EXPR_CONST[val]) if val in EXPR_CONST else ("var", val)
        if peek("("):
            i[0] += 1
            node = expr(depth + 1)
            take(")")
            return node
        raise ValueError("incomplete formula")

    tree = expr()
    if i[0] != len(toks):
        raise ValueError("unexpected text at the end of the formula")
    return tree


def expr_names(tree):
    if tree[0] == "var":
        return {tree[1]}
    if tree[0] == "neg":
        return expr_names(tree[1])
    if tree[0] == "bin":
        return expr_names(tree[2]) | expr_names(tree[3])
    if tree[0] == "call":
        return set().union(*(expr_names(a) for a in tree[2]))
    return set()


def expr_eval(tree, env):
    k = tree[0]
    if k == "num":
        return tree[1]
    if k == "var":
        return env[tree[1]]
    if k == "neg":
        return -expr_eval(tree[1], env)
    if k == "call":
        return float(EXPR_FUNCS[tree[1]](*[expr_eval(a, env) for a in tree[2]]))
    a, b = expr_eval(tree[2], env), expr_eval(tree[3], env)
    op = tree[1]
    v = a + b if op == "+" else a - b if op == "-" else a * b if op == "*" else a / b if op == "/" else a ** b
    if isinstance(v, complex) or not math.isfinite(v):
        raise ValueError("not a real number")
    return v


def _num(v, default=None):
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _txt(v, limit):
    return re.sub(r"\s+", " ", fix_text(str(v or ""))).strip()[:limit]


def _slider_values(inp, count):
    """Values the slider can actually take (on its step grid), at most `count` of them, ends included."""
    n = int(round((inp["max"] - inp["min"]) / inp["step"]))
    ks = range(n + 1) if n + 1 <= count else sorted({round(j * n / (count - 1)) for j in range(count)})
    return [min(inp["max"], inp["min"] + k * inp["step"]) for k in ks]


def check_explore(step, max_outputs=4):
    """An exploration: 1-3 sliders, 1-4 live readouts computed from formulas (templates: up to 6), an optional
    chart and goal. Every formula is evaluated over the sliders' whole range; a goal must be reachable with the
    sliders and must not already be met where the sliders start."""
    inputs, seen = [], set(EXPR_FUNCS) | set(EXPR_CONST)
    for raw in (step.get("inputs") or [])[:3]:
        lo, hi, st = _num(raw.get("min")), _num(raw.get("max")), _num(raw.get("step"))
        iid = str(raw.get("id", ""))
        if not IDENT.match(iid) or iid in seen or lo is None or hi is None or not lo < hi:
            raise ValueError(f"bad slider {iid!r}")
        st = st if st and 0 < st <= hi - lo else (hi - lo) / 100
        val = _num(raw.get("value"), lo)
        val = min(hi, max(lo, lo + round((val - lo) / st) * st))
        seen.add(iid)
        inputs.append({"id": iid, "label": _txt(raw.get("label") or iid, 56), "unit": _txt(raw.get("unit"), 14),
                       "min": lo, "max": hi, "step": st, "value": val})
    if not inputs:
        raise ValueError("no sliders")
    outputs, known, trees = [], {i["id"] for i in inputs}, {}
    for raw in (step.get("outputs") or [])[:max_outputs]:
        oid = str(raw.get("id", ""))
        if not IDENT.match(oid) or oid in seen:
            raise ValueError(f"bad readout {oid!r}")
        tree = expr_parse(raw.get("expr", ""))
        missing = expr_names(tree) - known
        if missing:
            raise ValueError(f"readout {oid} uses unknown {sorted(missing)}")
        digits = _num(raw.get("digits"))
        trees[oid] = tree
        outputs.append({"id": oid, "label": _txt(raw.get("label") or oid, 56), "unit": _txt(raw.get("unit"), 14),
                        "expr": str(raw["expr"]),
                        **({"digits": int(min(4, max(0, digits)))} if digits is not None else {})})
        seen.add(oid)
        known.add(oid)
    if not outputs:
        raise ValueError("no readouts")
    per = {1: 201, 2: 61, 3: 21}[len(inputs)]
    grids = [_slider_values(i, per) for i in inputs]

    def run(values):
        env = dict(zip([i["id"] for i in inputs], values))
        for o in outputs:
            env[o["id"]] = expr_eval(trees[o["id"]], env)
        return env

    run([i["value"] for i in inputs])   # must work where the sliders start
    combos, good, bad = [[]], [], 0
    for g in grids:
        combos = [c + [v] for c in combos for v in g]
    for c in combos:
        try:
            good.append(run(c))
        except (ValueError, ZeroDivisionError, OverflowError, TypeError):
            bad += 1
    if bad > 0.05 * len(combos):
        raise ValueError("formulas fail over much of the slider range")
    for o in outputs:
        vals = [env[o["id"]] for env in good]
        o["lo"], o["hi"] = min(vals), max(vals)
    out = {"type": "explore", "text": _txt(step.get("text"), 420), "inputs": inputs, "outputs": outputs}
    chart = step.get("chart") or {}
    ys = [y for y in (chart.get("y") if isinstance(chart.get("y"), list) else [chart.get("y")])
          if y in {o["id"] for o in outputs}][:3]
    if chart.get("x") in {i["id"] for i in inputs} and ys:
        out["chart"] = {"x": chart["x"], "y": ys}
    goal = step.get("goal") or {}
    target = goal.get("output") or goal.get("input")
    lo, hi = _num(goal.get("min")), _num(goal.get("max"))
    if target in known and lo is not None and hi is not None and lo <= hi and goal.get("text"):
        if not any(lo <= env[target] <= hi for env in good):
            log(f"    puzzle goal dropped: {target} never reaches {lo}..{hi} with these sliders")
            return out
        # A mission already solved where the sliders start is no mission. (Moving a slider to an end instead,
        # as tried first, produced nonsense such as a graphics card at its 10 tokens/s minimum.)
        if lo <= run([i["value"] for i in inputs])[target] <= hi:
            log("    puzzle goal dropped: already met where the sliders start")
            return out
        out["goal"] = {"target": target, "min": lo, "max": hi, "text": _txt(goal["text"], 260),
                       "success": _txt(goal.get("success") or step.get("success"), 420)}
    return out


# ----- Puzzle templates -----
# Hands-on pictures built once and tested, like Brilliant's interactives. The maths of each is fixed here as
# formulas (so the page and this check compute the same numbers); a model only fills in the story: labels,
# realistic values and a mission. quiz.html draws each one with its own picture ("view").
def _c(x):
    """A number inside a formula (negative numbers in brackets)."""
    return f"({x:.6g})" if x < 0 else f"{x:.6g}"


def nice_step(lo, hi, n=100):
    raw = max((hi - lo) / n, 1e-9)
    mag = 10 ** math.floor(math.log10(raw))
    return next(round(m * mag, 10) for m in (1, 2, 5, 10) if raw <= m * mag + 1e-15)


def _inp(iid, label, unit, lo, hi, value, step=None):
    step = step or nice_step(lo, hi)
    return {"id": iid, "label": label, "unit": unit, "min": lo, "max": hi, "step": step, "value": min(hi, max(lo, value))}


def _out(oid, label, unit, expr, digits=None):
    return {"id": oid, "label": label, "unit": unit, "expr": expr, **({"digits": digits} if digits is not None else {})}


def _span(lo, hi, minimum=1e-6):
    lo, hi = sorted((lo, hi))
    return (lo, hi) if hi - lo > minimum else (lo, lo + max(1.0, abs(lo)))


def _t_saturation(p):
    lo, hi = _span(p["level_min"], p["level_max"])
    ins = [_inp("t", "Time", p["time_unit"], 0, p["time_max"], 0)]
    if p["vary"] == "halftime":
        hmin, hmax = _span(max(1e-3, p["halftime_min"]), p["halftime_max"])
        ins.append(_inp("hl", "Half-time", p["time_unit"], hmin, hmax, p["halftime"]))
        level, hl, vlo, vhi = _c(p["level"]), "hl", min(p["start"], p["level"]), max(p["start"], p["level"])
    else:
        ins.append(_inp("L", p["level_label"], p["unit"], lo, hi, p["level"]))
        level, hl, vlo, vhi = "L", _c(p["halftime"]), min(p["start"], lo), max(p["start"], hi)
    outs = [_out("x", p["quantity"], p["unit"], f"{level}+({_c(p['start'])}-{level})*2^(-t/{hl})"),
            _out("lvl", p["level_label"], p["unit"], level),
            _out("gap", "Still to go", p["unit"], "abs(lvl-x)")]
    return {"inputs": ins, "outputs": outs, "chart": {"x": "t", "y": ["x", "lvl"]},
            "view": {"kind": "tank", "value": "x", "target": "lvl", "lo": vlo, "hi": vhi, "start": p["start"],
                     "label": p["quantity"], "unit": p["unit"]}}


def _t_doubling(p):
    grow = p["mode"] == "grow"
    pmin, pmax = _span(max(1e-6, p["period_min"]), p["period_max"])
    pmin = max(pmin, p["time_max"] / 40)          # at most 40 doublings: the numbers stay readable
    pmax = max(pmax, pmin * 2)
    sign = "" if grow else "-"
    name = "Doubling time" if grow else "Half-life"
    ins = [_inp("t", "Time", p["time_unit"], 0, p["time_max"], 0), _inp("T", name, p["time_unit"], pmin, pmax, p["period"])]
    outs = [_out("fold", "Times the start" if grow else "Share left", "x" if grow else "", f"2^({sign}t/T)", 3 if not grow else None),
            _out("x", p["quantity"], p["unit"], f"{_c(p['start'])}*fold"),
            _out("n", "Doublings so far" if grow else "Half-lives so far", "", "t/T", 1)]
    return {"inputs": ins, "outputs": outs, "chart": {"x": "t", "y": ["x"]},
            "view": {"kind": "blocks", "fold": "fold", "mode": p["mode"]}}


def _t_chain(p):
    pmin, pmax = _span(p["p_min"], p["p_max"], 0.05)
    step, run = p["step_label"], p["run_label"]
    ins = [_inp("p", f"Each {step} works", "%", pmin, min(99.99, pmax), p["p"], 0.1 if pmax - pmin > 2 else 0.01),
           _inp("n", f"{step[:1].upper() + step[1:]}s in a {run}", "", 1, int(p["n_max"]), int(p["n"]), 1)]
    outs = [_out("ok", f"Whole {run} works", "%", "100*(p/100)^n", 1),
            _out("fails", f"{run[:1].upper() + run[1:]}s that fail, of 100", "", "100-ok", 0)]
    return {"inputs": ins, "outputs": outs, "chart": {"x": "n", "y": ["ok"]},
            "view": {"kind": "runs", "p": "p", "n": "n", "step": step, "run": run}}


def _t_base_rate(p):
    lo, hi = _span(max(0.01, p["prev_min"]), p["prev_max"], 0.01)
    fl = p["flagged"]
    ins = [_inp("prev", f"Share who {p['condition']}", "%", lo, hi, p["prev"]),
           _inp("sens", "Catches real cases (sensitivity)", "%", 50, 99.9, p["sens"], 0.1),
           _inp("spec", "Clears the rest (specificity)", "%", 50, 99.9, p["spec"], 0.1)]
    outs = [_out("tp", f"Real cases {fl}, of 1000", "", "1000*prev/100*sens/100", 1),
            _out("fp", f"{fl[:1].upper() + fl[1:]} by mistake, of 1000", "", "1000*(1-prev/100)*(1-spec/100)", 1),
            _out("ppv", f"Of those {fl}, really {p['condition']}", "%", "100*tp/(tp+fp)", 1)]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "dots", "prev": "prev", "tp": "tp", "fp": "fp", "who": p["who"],
                     "condition": p["condition"], "flagged": fl}}


def _t_threshold(p):
    neg, pos, nl, pl = p["neg_mean"], p["pos_mean"], p["neg_label"], p["pos_label"]
    if pos < neg:
        neg, pos, nl, pl = pos, neg, pl, nl
    if pos - neg < 1e-9:
        pos = neg + p["spread"]
    sd, lo, hi = p["spread"], neg - 3.5 * p["spread"], pos + 3.5 * p["spread"]
    ins = [_inp("th", f"Reject above ({p['measure']})", p["unit"], lo, hi, (neg + pos) / 2)]
    outs = [_out("miss", f"{pl[:1].upper() + pl[1:]} let through", "%", f"100*ncdf((th-{_c(pos)})/{_c(sd)})", 1),
            _out("fa", f"{nl[:1].upper() + nl[1:]} rejected by mistake", "%", f"100*(1-ncdf((th-{_c(neg)})/{_c(sd)}))", 1)]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "bells", "th": "th", "neg": neg, "pos": pos, "sd": sd, "lo": lo, "hi": hi,
                     "neg_label": nl, "pos_label": pl}}


def _t_sampling(p):
    lo, hi = _span(max(1, round(p["n_min"])), max(2, round(p["n_max"])))
    step = max(1, round(nice_step(lo, hi, 200)))
    tv, sd = p["true_value"], p["sd"]
    ins = [_inp("n", "Sample size", "", lo, hi, max(lo, min(hi, round(p["n"]))), step)]
    outs = [_out("margin", "Margin of error (95%)", p["unit"], f"1.96*{_c(sd)}/sqrt(n)"),
            _out("lo", "Could be as low as", p["unit"], f"{_c(tv)}-margin"),
            _out("hi", "Or as high as", p["unit"], f"{_c(tv)}+margin")]
    return {"inputs": ins, "outputs": outs, "chart": {"x": "n", "y": ["margin"]},
            "view": {"kind": "spread", "n": "n", "true": tv, "sd": sd, "lo": "lo", "hi": "hi", "measure": p["measure"],
                     "unit": p["unit"]}}


def _t_gradient_descent(p):
    ins = [_inp("lr", "Learning rate", "", 0.01, max(0.2, p["lr_max"]), p["lr"], 0.01),
           _inp("k", "Training steps", "", 0, int(p["steps"]), int(p["steps"]), 1)]   # whole path visible at once
    outs = [_out("w", "Distance from the best setting", "", f"{_c(p['start'])}*(1-2*lr)^k", 2),
            _out("loss", "Error (loss)", "", "w^2", 3)]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "bowl", "lr": "lr", "k": "k", "w": "w", "start": p["start"], "model": p["model"]}}


def _t_softmax(p):
    words = []
    for w in (p["words"] if isinstance(p["words"], list) else [])[:5]:
        if isinstance(w, dict) and _txt(w.get("word"), 24) and _num(w.get("score")) is not None:
            words.append((_txt(w["word"], 24), max(-10.0, min(10.0, _num(w["score"])))))
    if len(words) < 3 or len({w for w, _ in words}) != len(words):
        raise ValueError("softmax needs 3-5 different words with scores")
    total = "+".join(f"exp({_c(s)}/T)" for _, s in words)
    outs = [_out(f"p{i + 1}", w, "%", f"100*exp({_c(s)}/T)/({total})", 1) for i, (w, s) in enumerate(words)]
    return {"inputs": [_inp("T", "Temperature", "", 0.1, 2.5, p["temp"], 0.05)], "outputs": outs,
            "view": {"kind": "bars", "prompt": p["prompt"], "outs": [o["id"] for o in outs], "readouts": False}}


def _t_flow(p):
    lo, hi = _span(p["lo"], p["hi"])
    ins = [_inp("a", p["side_a"][:1].upper() + p["side_a"][1:], p["unit"], lo, hi, p["a"]),
           _inp("b", p["side_b"][:1].upper() + p["side_b"][1:], p["unit"], lo, hi, p["b"])]
    outs = [_out("diff", "Difference (the push)", p["unit"], "a-b"),
            _out("flow", f"Net flow, {p['side_a']} to {p['side_b']}", p["flow_unit"], f"{_c(p['ease'])}*diff")]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "membrane", "a": "a", "b": "b", "flow": "flow", "lo": lo, "hi": hi, "thing": p["thing"],
                     "side_a": p["side_a"], "side_b": p["side_b"]}}


def _t_inverse_square(p):
    lo, hi = _span(max(1e-9, p["d_min"]), p["d_max"])
    d0 = max(1e-9, p["d0"])
    ins = [_inp("d", f"Distance from {p['source']}", p["d_unit"], lo, hi, lo)]
    outs = [_out("rel", f"{p['quantity']}, % of the value at {d0:g} {p['d_unit']}", "%", f"100*({_c(d0)}/d)^2"),
            _out("area", "Area it spreads over", "x", f"(d/{_c(d0)})^2", 1)]
    return {"inputs": ins, "outputs": outs, "chart": {"x": "d", "y": ["rel"]},
            "view": {"kind": "rays", "d": "d", "rel": "rel", "dmin": lo, "dmax": hi, "source": p["source"]}}


def _t_bottleneck(p):
    stages = []
    for st in (p["stages"] if isinstance(p["stages"], list) else [])[:3]:
        if isinstance(st, dict) and _txt(st.get("label"), 40) and (_num(st.get("max")) or 0) > 0:
            top = _num(st["max"])
            stages.append((_txt(st["label"], 40), top, min(top, max(top / 20, _num(st.get("value"), top / 2)))))
    if len(stages) < 2:
        raise ValueError("bottleneck needs 2-3 stages")
    ids = [f"c{i + 1}" for i in range(len(stages))]
    ins = [_inp(i, label, p["unit"], top / 20, top, val) for i, (label, top, val) in zip(ids, stages)]
    outs = [_out("thr", "Line output", p["unit"], f"min({','.join(ids)})")] + \
           [_out(f"u{n + 1}", f"{label} busy", "%", f"100*thr/{i}", 0) for n, (i, (label, _, _)) in enumerate(zip(ids, stages))]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "pipes", "caps": ids, "thr": "thr", "labels": [s[0] for s in stages], "unit": p["unit"]}}


_TEXT = "text"
TEMPLATES = {
    "saturation": {
        "pick": "Something fills up (or cools, charges, loads) towards a level and slows down as it gets close: "
                "gases loading into tissue, a drug building up, a battery charging, a cup of tea cooling.",
        "build": _t_saturation, "goals": "x (the amount), gap (still to go), lvl (the level)",
        "params": {"quantity": (_TEXT, "Amount", 40), "unit": (_TEXT, "", 12), "start": ("num", 0, -1e6, 1e6),
                   "level": ("num", 100, -1e6, 1e6), "level_min": ("num", 0, -1e6, 1e6), "level_max": ("num", 200, -1e6, 1e6),
                   "halftime": ("num", 10, 1e-3, 1e6), "halftime_min": ("num", 2, 1e-3, 1e6),
                   "halftime_max": ("num", 40, 1e-3, 1e6), "time_max": ("num", 60, 1e-3, 1e6), "time_unit": (_TEXT, "min", 12),
                   "level_label": (_TEXT, "Level it heads for", 40), "vary": ("enum", "level", ("level", "halftime"))},
        "example": {"quantity": "Nitrogen in tissue", "unit": "kPa", "start": 79, "level": 316, "level_min": 79,
                    "level_max": 395, "halftime": 20, "time_max": 120, "time_unit": "min",
                    "level_label": "Nitrogen pushing in", "vary": "level"}},
    "doubling": {
        "pick": "Exponential growth or decay with a doubling time or half-life: compute or costs doubling, bacteria, "
                "radioactive decay, a drug clearing from the body, compound interest.",
        "build": _t_doubling, "goals": "x (the amount), fold (times the start / share left), n (doublings so far)",
        "params": {"quantity": (_TEXT, "Amount", 40), "unit": (_TEXT, "", 12), "start": ("num", 1, 1e-9, 1e12),
                   "mode": ("enum", "grow", ("grow", "decay")), "period": ("num", 2, 1e-6, 1e6),
                   "period_min": ("num", 1, 1e-6, 1e6), "period_max": ("num", 10, 1e-6, 1e6),
                   "time_max": ("num", 20, 1e-3, 1e6), "time_unit": (_TEXT, "years", 12)},
        "example": {"quantity": "Training compute", "unit": "x 2020", "start": 1, "mode": "grow", "period": 0.5,
                    "period_min": 0.25, "period_max": 2, "time_max": 6, "time_unit": "years"}},
    "chain": {
        "pick": "Many steps that must all work: an AI agent's multi-step task, a production line's rolled throughput "
                "yield, a supply chain, a long calculation - small error rates multiply.",
        "build": _t_chain, "goals": "ok (% of runs that fully work), fails (runs that fail, of 100)",
        "params": {"step_label": (_TEXT, "step", 24), "run_label": (_TEXT, "run", 24), "p": ("num", 95, 50, 99.99),
                   "p_min": ("num", 80, 50, 99.9), "p_max": ("num", 99.9, 51, 99.99), "n": ("num", 10, 1, 100),
                   "n_max": ("num", 30, 2, 100)},
        "example": {"step_label": "tool call", "run_label": "task", "p": 95, "p_min": 90, "p_max": 99.9, "n": 20, "n_max": 50}},
    "base_rate": {
        "pick": "A test, detector or screen applied to a large group where the thing is rare: medical screening, "
                "AI-text detectors, defect inspection, fraud alerts - why most positives can be false.",
        "build": _t_base_rate, "goals": "ppv (% of flagged that are real), tp, fp (counts out of 1000)",
        "params": {"who": (_TEXT, "people", 24), "condition": (_TEXT, "have it", 40), "flagged": (_TEXT, "flagged", 24),
                   "prev": ("num", 1, 0.01, 60), "prev_min": ("num", 0.1, 0.01, 50), "prev_max": ("num", 20, 0.1, 90),
                   "sens": ("num", 90, 50, 99.9), "spec": ("num", 95, 50, 99.9)},
        "example": {"who": "essays", "condition": "are AI-written", "flagged": "flagged", "prev": 5, "prev_min": 1,
                    "prev_max": 50, "sens": 95, "spec": 95}},
    "threshold": {
        "pick": "Two overlapping groups told apart by one measurement and a cut-off: good vs bad parts, healthy vs "
                "sick, real vs fake, spam vs not - moving the threshold trades misses for false alarms.",
        "build": _t_threshold, "goals": "miss (% of the second group let through), fa (% of the first group rejected)",
        "params": {"neg_label": (_TEXT, "good parts", 30), "pos_label": (_TEXT, "bad parts", 30),
                   "measure": (_TEXT, "Measurement", 30), "unit": (_TEXT, "", 12), "neg_mean": ("num", 10, -1e6, 1e6),
                   "pos_mean": ("num", 14, -1e6, 1e6), "spread": ("num", 1.5, 1e-6, 1e6)},
        "example": {"neg_label": "healthy people", "pos_label": "people with diabetes", "measure": "fasting glucose",
                    "unit": "mmol/L", "neg_mean": 5.0, "pos_mean": 8.0, "spread": 1.0}},
    "sampling": {
        "pick": "Measuring an average from a sample with noise: benchmark scores, clinical trials, polls, sampling "
                "inspection - how sample size shrinks the margin of error (by the square root).",
        "build": _t_sampling, "goals": "margin (the +/- margin), lo, hi",
        "params": {"measure": (_TEXT, "Average", 40), "unit": (_TEXT, "", 12), "true_value": ("num", 50, -1e9, 1e9),
                   "sd": ("num", 10, 1e-9, 1e9), "n": ("num", 10, 1, 1e6), "n_min": ("num", 2, 1, 1e6),
                   "n_max": ("num", 400, 2, 1e6)},
        "example": {"measure": "Benchmark score", "unit": "%", "true_value": 72, "sd": 45, "n": 50, "n_min": 10,
                    "n_max": 5000}},
    "gradient_descent": {
        "pick": "Training a model: an error that is reduced step by step, and the learning rate - too small is slow, "
                "too big overshoots or blows up. Also any feedback loop that over-corrects.",
        "build": _t_gradient_descent, "goals": "loss (the error), w (distance from the best setting)",
        "params": {"model": (_TEXT, "the model", 60), "lr": ("num", 0.05, 0.01, 1.2), "lr_max": ("num", 1.1, 0.2, 1.5),
                   "steps": ("num", 10, 1, 40), "start": ("num", 4, 1, 10)},
        "example": {"model": "a model that predicts weld strength", "lr": 0.05, "lr_max": 1.1, "steps": 8, "start": 4}},
    "softmax": {
        "pick": "How a language model picks the next word from probabilities, and how temperature makes it safer or "
                "more creative (and more likely to say something wrong).",
        "build": _t_softmax, "goals": "p1..p5 (% chance of each word, in the order given)",
        "params": {"prompt": (_TEXT, "The next word is ___", 120), "words": ("list", []), "temp": ("num", 1, 0.1, 2.5)},
        "example": {"prompt": "The patient's blood pressure is ___", "temp": 1,
                    "words": [{"word": "normal", "score": 3}, {"word": "high", "score": 2.5}, {"word": "low", "score": 1},
                              {"word": "purple", "score": -2}]}},
    "flow": {
        "pick": "Something moves from high to low across a barrier, driven by a difference: gases between lungs and "
                "blood (partial pressure), heat through a wall, ions in a battery, a drug through skin.",
        "build": _t_flow, "goals": "flow (net flow, a to b), diff (the difference)",
        "params": {"thing": (_TEXT, "oxygen", 30), "side_a": (_TEXT, "lungs", 30), "side_b": (_TEXT, "blood", 30),
                   "unit": (_TEXT, "kPa", 12), "a": ("num", 13, -1e9, 1e9), "b": ("num", 5, -1e9, 1e9),
                   "lo": ("num", 0, -1e9, 1e9), "hi": ("num", 20, -1e9, 1e9), "ease": ("num", 1, 1e-6, 1e6),
                   "flow_unit": (_TEXT, "units/min", 16)},
        "example": {"thing": "oxygen", "side_a": "air sacs", "side_b": "blood", "unit": "kPa", "a": 13, "b": 5,
                    "lo": 0, "hi": 20, "ease": 1, "flow_unit": "units/min"}},
    "inverse_square": {
        "pick": "Something that spreads out from a source and weakens with distance squared: light, radiation dose, "
                "radio signal, gravity, sound - space, physics and safety topics.",
        "build": _t_inverse_square, "goals": "rel (% of the value at the reference distance), area",
        "params": {"source": (_TEXT, "the source", 40), "quantity": (_TEXT, "Brightness", 30), "d_min": ("num", 1, 1e-9, 1e12),
                   "d_max": ("num", 10, 1e-9, 1e12), "d0": ("num", 1, 1e-9, 1e12), "d_unit": (_TEXT, "m", 12)},
        "example": {"source": "the Sun", "quantity": "Sunlight", "d_min": 0.4, "d_max": 5.2, "d0": 1, "d_unit": "AU"}},
    "bottleneck": {
        "pick": "A pipeline or process with stages of different capacity, where the slowest stage limits the output: "
                "AI inference (memory vs compute), a production line, a hospital, a data pipeline.",
        "build": _t_bottleneck, "goals": "thr (line output), u1..u3 (% busy of each stage)",
        "params": {"stages": ("list", []), "unit": (_TEXT, "items/hour", 16)},
        "example": {"unit": "tokens/s", "stages": [{"label": "Memory reads", "value": 40, "max": 200},
                                                   {"label": "Compute", "value": 120, "max": 200}]}},
}


# Added after reviewing the episodes of 27 Oct - 3 Nov 2026: local models and hardware, agents retrying, cancer
# vaccine headlines, mountains (isostasy), deep-sea animals and lungs, editing model weights and search by meaning.
def _cap(t):
    return t[:1].upper() + t[1:]


def _t_model_memory(p):
    lo, hi = _span(max(0.1, p["params_min"]), p["params_max"])
    share = max(0.01, p["active_share"] / 100)
    ins = [_inp("P", "Parameters", "billion", lo, hi, p["params_b"]),
           _inp("bits", "Bits per weight (quantization)", "bits", 2, 16, p["bits"], 0.5)]
    outs = [_out("size", "Memory needed (incl. ~10% extra)", "GB", "P*bits/8*1.1", 1),
            _out("free", f"Left over on {p['device']}", "GB", f"{_c(p['vram'])}-size", 1),
            _out("speed", "Tokens per second, at best", "", f"{_c(p['bandwidth'])}/(P*{_c(share)}*bits/8)", 0)]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "memory", "size": "size", "speed": "speed", "vram": p["vram"], "device": p["device"],
                     "model": p["model"]}}


def _t_tries(p):
    lo, hi = _span(max(0.01, p["p_min"]), min(99.0, p["p_max"]), 0.01)
    kmax = max(2, int(p["k_max"]))
    t, su = p["try_label"], p["success_label"]
    ins = [_inp("p", f"Chance each {t} works", "%", lo, hi, p["p"]),
           _inp("k", f"{_cap(t)}s allowed", "", 1, kmax, int(p["k"]), max(1, round(nice_step(1, kmax, 200))))]
    outs = [_out("atleast", f"Chance {su} within those {t}s", "%", "100*(1-(1-p/100)^k)", 1),
            _out("expected", f"{_cap(t)}s needed on average", "", "100/p", 1)]
    if p["cost"] > 0:
        outs.append(_out("spend", f"Average cost until {su}", p["cost_unit"], f"{_c(p['cost'])}*expected", 2))
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "attempts", "p": "p", "k": "k", "try": t, "success": su}}


def _t_risk(p):
    lo, hi = _span(max(0.01, p["base_min"]), min(99.0, p["base_max"]), 0.01)
    tr = p["treatment"]
    ins = [_inp("base", f"Risk without {tr}", "%", lo, hi, p["base"]),
           _inp("rrr", "Relative reduction (the headline)", "%", 1, 95, max(1, p["rrr"]), 1)]
    outs = [_out("treated", f"Risk with {tr}", "%", "base*(1-rrr/100)", 2),
            _out("arr", "Absolute drop", "points", "base-treated", 2),
            _out("nnt", f"{_cap(p['who'])} treated to spare one", "", "100/arr", 0)]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "people", "base": "base", "treated": "treated", "outcome": p["outcome"], "treatment": tr,
                     "who": p["who"]}}


def _t_buoyancy(p):
    rf = p["rho_fluid"]
    hi = min(p["rho_max"], 0.995 * rf)
    lo, hi = _span(min(max(1.0, p["rho_min"]), 0.9 * hi), hi)
    hlo, hhi = _span(max(1e-6, p["h_min"]), p["h_max"])
    obj, unit = p["object"], p["unit"]
    ins = [_inp("rho", f"Density of the {obj}", "kg/m³", lo, hi, p["rho"]),
           _inp("h", f"Total thickness of the {obj}", unit, hlo, hhi, p["h"])]
    outs = [_out("sub", "Share hidden below the surface", "%", f"100*rho/{_c(rf)}", 1),
            _out("above", "Sticks out above", unit, f"h*(1-rho/{_c(rf)})"),
            _out("below", "Reaches down below", unit, f"h*rho/{_c(rf)}")]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "float", "rho": "rho", "h": "h", "above": "above", "below": "below", "hmax": hhi,
                     "object": obj, "fluid": p["fluid"], "unit": unit}}


def _t_compression(p):
    dmax, rho, v0, k = p["depth_max"], p["fluid_density"], p["gas_volume"], p["bulk_modulus"]
    ins = [_inp("depth", "Depth", "m", 0, dmax, 0, nice_step(0, dmax, 200))]
    outs = [_out("P", "Pressure", "kPa", f"101.3+{_c(rho)}*9.81*depth/1000", 0),
            _out("gas", f"{_cap(p['gas_label'])}: volume", p["gas_unit"], f"{_c(v0)}*101.3/P", 2),
            _out("liq", f"{_cap(p['liquid_label'])} shrinks by", "%", f"100*(P-101.3)/({_c(k)}*1000000)", 2)]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "squeeze", "P": "P", "gas": "gas", "liq": "liq", "v0": v0, "depth": "depth", "dmax": dmax,
                     "gas_label": p["gas_label"], "liquid_label": p["liquid_label"], "unit": p["gas_unit"]}}


def _t_vectors(p):
    a, b = p["a_label"], p["b_label"]
    ins = [_inp("angle", f"Angle between {a} and {b}", "°", 0, 180, p["angle"], 1),
           _inp("len", f"Length of {a}", "", 0.2, 2, p["length"], 0.05)]
    outs = [_out("sim", "Cosine similarity", "", "cos(angle*pi/180)", 2),
            _out("along", f"Part pointing along {b}", "", "len*cos(angle*pi/180)", 2),
            _out("left", "Left after removing that direction", "", "len*abs(sin(angle*pi/180))", 2)]
    return {"inputs": ins, "outputs": outs,
            "view": {"kind": "arrows", "angle": "angle", "len": "len", "a_label": a, "b_label": b}}


TEMPLATES.update({
    "model_memory": {
        "pick": "Running an AI model on your own hardware or a phone: parameters, quantization (bits per weight), "
                "memory and speed - will it fit, and how fast will it type? Also mixture-of-experts (active share).",
        "build": _t_model_memory, "goals": "size (GB needed), free (GB left; negative = does not fit), speed (tokens/s)",
        "params": {"model": (_TEXT, "the model", 40), "device": (_TEXT, "your GPU", 40), "params_b": ("num", 27, 0.1, 2000),
                   "params_min": ("num", 1, 0.1, 2000), "params_max": ("num", 120, 0.1, 2000), "bits": ("num", 16, 2, 16),
                   "vram": ("num", 24, 1, 2048), "bandwidth": ("num", 1000, 1, 20000),
                   "active_share": ("num", 100, 1, 100)},
        "example": {"model": "a 32B open model", "device": "a 24 GB graphics card", "params_b": 32, "params_min": 1,
                    "params_max": 120, "bits": 16, "vram": 24, "bandwidth": 1000, "active_share": 100}},
    "tries": {
        "pick": "Trying again and again until something works once: an AI agent retrying (and the bill), random or "
                "property-based tests finding a rare bug, a vaccine with many targets, a screening repeated yearly.",
        "build": _t_tries, "goals": "atleast (% chance of at least one success), expected (tries on average), spend",
        "params": {"try_label": (_TEXT, "try", 24), "success_label": (_TEXT, "success", 40),
                   "p": ("num", 10, 0.01, 99), "p_min": ("num", 1, 0.01, 99), "p_max": ("num", 50, 0.02, 99),
                   "k": ("num", 10, 1, 1000), "k_max": ("num", 50, 2, 1000), "cost": ("num", 0, 0, 1e6),
                   "cost_unit": (_TEXT, "$", 8)},
        "example": {"try_label": "random test", "success_label": "the bug is caught", "p": 2, "p_min": 0.5, "p_max": 20,
                    "k": 20, "k_max": 300}},
    "risk": {
        "pick": "A treatment, vaccine or habit that 'cuts the risk by X%': relative vs absolute risk and how many "
                "people must be treated to spare one - drug trials, cancer vaccines, screening, rising disease rates.",
        "build": _t_risk, "goals": "treated (% risk with it), arr (absolute drop, points), nnt (people treated per one spared)",
        "params": {"outcome": (_TEXT, "get the disease", 40), "treatment": (_TEXT, "the treatment", 30),
                   "who": (_TEXT, "people", 20), "base": ("num", 10, 0.01, 99), "base_min": ("num", 0.5, 0.01, 99),
                   "base_max": ("num", 50, 0.02, 99), "rrr": ("num", 30, 1, 95)},
        "example": {"outcome": "have melanoma come back", "treatment": "the vaccine", "who": "patients", "base": 40,
                    "base_min": 1, "base_max": 60, "rrr": 44}},
    "buoyancy": {
        "pick": "Something floating with most of it hidden below: icebergs, ships, and continents floating on the "
                "mantle (isostasy - why eroded mountains rise back up).",
        "build": _t_buoyancy, "goals": "sub (% below the surface), above, below",
        "params": {"object": (_TEXT, "iceberg", 30), "fluid": (_TEXT, "seawater", 30), "unit": (_TEXT, "m", 10),
                   "rho": ("num", 917, 1, 30000), "rho_min": ("num", 500, 1, 30000), "rho_max": ("num", 1000, 1, 30000),
                   "rho_fluid": ("num", 1025, 1, 30000), "h": ("num", 100, 1e-6, 1e6), "h_min": ("num", 10, 1e-6, 1e6),
                   "h_max": ("num", 300, 1e-6, 1e6)},
        "example": {"object": "crust", "fluid": "mantle", "unit": "km", "rho": 2700, "rho_min": 2000, "rho_max": 3200,
                    "rho_fluid": 3300, "h": 30, "h_min": 5, "h_max": 70}},
    "compression": {
        "pick": "Pressure with depth squeezing gas but hardly liquid: deep-sea animals, a diver's or whale's lungs, "
                "submarine hulls, why fluid-filled bodies survive where air spaces collapse.",
        "build": _t_compression, "goals": "P (pressure, kPa), gas (gas volume), liq (% the liquid shrinks)",
        "params": {"depth_max": ("num", 1000, 1, 11000), "fluid_density": ("num", 1025, 500, 1500),
                   "gas_label": (_TEXT, "air in a lung", 30), "liquid_label": (_TEXT, "body water", 30),
                   "gas_volume": ("num", 6, 0.001, 1e6), "gas_unit": (_TEXT, "L", 8), "bulk_modulus": ("num", 2.2, 0.01, 1000)},
        "example": {"depth_max": 11000, "fluid_density": 1025, "gas_label": "air in a lung",
                    "liquid_label": "a fish's body water", "gas_volume": 6, "gas_unit": "L", "bulk_modulus": 2.2}},
    "vectors": {
        "pick": "Meaning as directions (embeddings): search by meaning and cosine similarity, RAG retrieval, and "
                "editing a model by removing one direction from its weights (steering, ablation).",
        "build": _t_vectors, "goals": "sim (cosine similarity), along (part along the direction), left (what remains)",
        "params": {"a_label": (_TEXT, "your question", 30), "b_label": (_TEXT, "a document", 30),
                   "angle": ("num", 60, 0, 180), "length": ("num", 1, 0.2, 2)},
        "example": {"a_label": "your question", "b_label": "a manual page", "angle": 70, "length": 1}},
})


def template_params(spec, given):
    """The model's values for a template, kept inside each parameter's allowed range."""
    given = given if isinstance(given, dict) else {}
    out = {}
    for name, (kind, default, *rest) in spec.items():
        v = given.get(name, default)
        if kind == "num":
            out[name] = min(rest[1], max(rest[0], _num(v, default)))
        elif kind == _TEXT:
            out[name] = _txt(v, rest[0]) or default
        elif kind == "enum":
            out[name] = v if v in rest[0] else default
        else:
            out[name] = v if isinstance(v, list) else default
    return out


def check_template(step):
    key = str(step.get("template", ""))
    tpl = TEMPLATES.get(key)
    if not tpl:
        raise ValueError(f"unknown template {key!r}")
    raw = tpl["build"](template_params(tpl["params"], step.get("params")))
    raw.update(text=step.get("text") or "Try it.", goal=step.get("goal"))
    out = check_explore(raw, max_outputs=6)
    out.update(template=key, view=raw["view"])
    return out


def template_sliders(key):
    """What the listener can actually move, with the labels a template builds from its text params."""
    t = TEMPLATES[key]
    shown = dict(t["example"])
    shown.update({n: f"<{n}>" for n, sp in t["params"].items() if sp[0] == _TEXT and n not in ("unit", "time_unit")})
    if key == "bottleneck":
        shown["stages"] = [{"label": "<stage label>", "value": 1, "max": 2}, {"label": "<stage label>", "value": 1, "max": 2}]
    try:
        built = t["build"](template_params(t["params"], shown))
    except (ValueError, TypeError, KeyError):
        return "?"
    return "; ".join(f"{i['label']}" + (f" ({i['unit']})" if i["unit"] else "") for i in built["inputs"])


def template_catalog(keys=None):
    rows = []
    for key in keys or TEMPLATES:
        t = TEMPLATES[key]
        params = ", ".join(f"{n} ({'text' if s[0] == _TEXT else '/'.join(s[2]) if s[0] == 'enum' else s[0]})"
                           for n, s in t["params"].items())
        rows.append(f"- {key}: {t['pick']}\n  sliders the listener moves: {template_sliders(key)}\n  params: {params}\n"
                    f"  goal outputs: {t['goals']}\n  example params: {json.dumps(t['example'], ensure_ascii=False)}")
    return "\n".join(rows)


# ----- The play page -----
# The page that goes with each episode (quiz.html) is a three-minute game, not a worksheet. It is assembled from
# templates that were designed and tested once (here and in quiz.html), by three kinds of free model:
#   1. the DECISION model chooses: the session's theme, the hands-on picture, and which card formats suit each
#      segment (a probability per format - choosing is what decision models are for);
#   2. the WRITING model fills the chosen formats with the episode's story;
#   3. everything is checked here, and a CODING model repairs the cards that fail (it gets the format's schema and
#      the exact error - strict structured edits are what coding models are good at). A stricter model then
#      checks the facts.
# No model writes HTML or JavaScript: a broken page cannot be published.
THEMES = {
    "audit": {"name": "Audit day",
              "pick": "The episode is mostly about claims being checked: a study, a benchmark, a safety or product "
                      "claim, a regulator's decision - things an auditor would probe.",
              "frame": "He is the auditor today: each card is a claim or a record to probe, and wrong statements are "
                       "'findings'."},
    "exchange": {"name": "The evidence exchange",
                 "pick": "The episode is full of surprising or contested claims where it is hard to know who is "
                         "right: hype versus evidence, forecasts, debates.",
                 "frame": "He is trading on evidence today: he stakes points on what will hold up."},
    "bench": {"name": "On the bench",
              "pick": "The episode is mostly about how something works and has real quantities to play with: a "
                      "mechanism, physics, hardware, a dose, a scale.",
              "frame": "He is at a test bench today: he turns the knobs and takes the measurement."},
    "case": {"name": "Case file",
             "pick": "The episode tells a story with a cause to find: something failed, surprised people or was "
                     "discovered, and the question is why.",
             "frame": "He is the investigator today: clues first, then the cause."},
}

# Card formats. "pick" is the yes/no question the decision model answers for each segment; "schema" and "rules"
# go to the writer; quiz.html has one renderer per format (same names).
ROUNDS = {
    "estimate": {
        "name": "Gut number",
        "pick": "The segment states a number that would surprise most people (a rate, size, cost, share or count) "
                "and that he could try to guess.",
        "schema": '{"type": "estimate", "q": "<asks for the number, max 22 words>", "answer": <the number>, '
                  '"unit": "<short unit>", "lo": <lowest plausible guess>, "hi": <highest plausible guess>, '
                  '"why": "<the answer in context, max 35 words>"}',
        "rules": "He drags a slider from lo to hi and scores by how close he lands. Use a number the segment "
                 "states. lo..hi must be wide and plausible, with the answer well away from the middle and the ends."},
    "flaw": {
        "name": "Spot the flaw",
        "pick": "The segment contains a mistaken belief, an overclaim or a reasoning error that could be hidden "
                "among true statements.",
        "schema": '{"type": "flaw", "q": "<one line setting it up, max 16 words>", "items": ["<statement, max 16 '
                  'words>", "...", "...", "..."], "flaw": <index of the WRONG statement>, "why": "<why it is wrong, '
                  'max 35 words>"}',
        "rules": "Four statements about the segment: three true, one wrong in a way that matters (the myth the "
                 "segment corrected, or a tempting wrong conclusion). All four the same length and tone."},
    "sequence": {
        "name": "Chain of events",
        "pick": "The segment explains a chain of cause and effect, or steps that happen in one fixed order.",
        "schema": '{"type": "sequence", "q": "<what to put in order, max 16 words>", "items": ["<first, max 12 '
                  'words>", "<second>", "<third>", "<fourth>"], "why": "<max 35 words>"}',
        "rules": "Items in the CORRECT order (the page shuffles them). One real order in time or cause and effect - "
                 "never a ranking or an opinion. 3-5 items."},
    "match": {
        "name": "Pair them up",
        "pick": "The segment introduces several terms, parts or players that each have a distinct job or meaning.",
        "schema": '{"type": "match", "q": "<max 12 words>", "pairs": [["<term, max 4 words>", "<plain meaning or '
                  'job, max 10 words>"], ["...", "..."], ["...", "..."]]}',
        "rules": "3-4 pairs from the segment. Each meaning must fit only its own term."},
    "duel": {
        "name": "Which is bigger?",
        "pick": "The segment allows a surprising comparison between two quantities (two risks, costs, sizes, "
                "speeds or counts) where intuition often picks the wrong one.",
        "schema": '{"type": "duel", "q": "<Which is bigger / faster / riskier ...?, max 16 words>", "a": {"label": '
                  '"<max 8 words>", "value": <number>}, "b": {"label": "<max 8 words>", "value": <number>}, '
                  '"unit": "<shared unit>", "why": "<max 35 words>"}',
        "rules": "Two things measured in the same unit, both values from the segment or simple arithmetic on its "
                 "numbers. The bigger one should be the less obvious."},
    "clues": {
        "name": "Name it",
        "pick": "The segment centres on one concept, mechanism or object that could be recognised from clues.",
        "schema": '{"type": "clues", "q": "<What is it? / What caused it?, max 10 words>", "clues": ["<vague clue, '
                  'max 16 words>", "<clearer clue>", "<giveaway clue>"], "options": ["<max 5 words>", "...", "...", '
                  '"..."], "answer": <index>, "why": "<max 35 words>"}',
        "rules": "He may answer after any clue: fewer clues, more points. Clue 1 must be truly hard, clue 3 nearly "
                 "gives it away. Four options that all sound possible after clue 1."},
    "call": {
        "name": "Your call",
        "pick": "The segment's idea could be applied to a practical decision at work (a factory, a lab, an audit, a "
                "clinic) with one clearly best action.",
        "schema": '{"type": "call", "q": "<a concrete situation and the decision to make, max 40 words>", '
                  '"options": ["<max 10 words>", "...", "..."], "answer": <index>, "hint": "<a nudge, max 16 '
                  'words>", "why": "<max 35 words>"}',
        "rules": "A new setting with its own numbers, so he must reason rather than remember. Three options, one "
                 "clearly best for a reason from the segment."},
    "quickfire": {
        "name": "Quickfire",
        "pick": "The segment makes several separate factual points that can each be judged true or false at a "
                "glance.",
        "schema": '{"type": "quickfire", "items": [{"text": "<statement, max 14 words>", "truth": true}, {"text": '
                  '"...", "truth": false}, {"text": "...", "truth": true}, {"text": "...", "truth": false}]}',
        "rules": "4-5 statements against the clock, a mix of true and false. The false ones are the wrong ideas the "
                 "segment corrected - not tricks of wording."},
}
BET_SCHEMA = ('{"claim": "<a statement that is either true or false, max 20 words>", "truth": true or false, '
              '"why": "<what the episode found, max 35 words>"}')
TEXT_SLACK = 1.5   # a text up to this many times its limit is kept; longer ones go to the coding model to shorten


def _fit(v, limit, what, optional=False):
    t = _txt(v, 2000).replace("**", "").replace("`", "")
    if not t:
        if optional:
            return ""
        raise ValueError(f"{what} is empty")
    if len(t) > limit * TEXT_SLACK:
        raise ValueError(f"{what} is too long ({len(t)} characters; keep it under {limit})")
    return t


def _bool(v, what):
    if isinstance(v, bool):
        return v
    if str(v).strip().lower() in ("true", "yes", "1"):
        return True
    if str(v).strip().lower() in ("false", "no", "0"):
        return False
    raise ValueError(f"{what} must be true or false")


def _index(v, n, what):
    i = _num(v)
    if i is None or int(i) != i or not 0 <= int(i) < n:
        raise ValueError(f"{what} must be an index from 0 to {n - 1}")
    return int(i)


def _texts(lst, lo, hi, limit, what):
    items = [_fit(x, limit, f"{what} {i + 1}") for i, x in enumerate(lst if isinstance(lst, list) else [])][:hi]
    if len(items) < lo:
        raise ValueError(f"needs at least {lo} {what}s")
    if len({x.lower() for x in items}) != len(items):
        raise ValueError(f"two {what}s are the same")
    return items


def check_bet(raw):
    raw = raw if isinstance(raw, dict) else {}
    return {"claim": _fit(raw.get("claim"), 150, "claim"), "truth": _bool(raw.get("truth"), "truth"),
            "why": _fit(raw.get("why"), 240, "why")}


def _estimate_range(answer, lo, hi, seed, unit=""):
    """The slider's range: wide, with the answer neither in the middle nor at an end (both give it away).
    Returns (lo, hi, log scale?)."""
    if lo is None or hi is None or not lo < answer < hi:
        pad = 3 * max(1.0, abs(answer))
        lo, hi = (answer / 6, answer * 6) if answer > 0 else (answer - pad, answer + pad)
    floor = 0.0 if lo >= 0 else None
    ceil = 100.0 if "%" in unit and hi <= 100 else None

    def is_log(a, c):
        return a > 0 and c / a >= 50

    def pos(a, c):
        return math.log(answer / a) / math.log(c / a) if is_log(a, c) else (answer - a) / (c - a)

    def ok(a, c):
        return a < answer < c and (0.15 <= pos(a, c) <= 0.4 or 0.6 <= pos(a, c) <= 0.85)

    if not ok(lo, hi):
        h = sum(ord(ch) * (i + 1) for i, ch in enumerate(seed)) % 1000
        wants = [0.22 + 0.14 * h / 1000, 0.64 + 0.14 * h / 1000]
        wants = wants if h % 2 else wants[::-1]
        tries = []
        for want in wants:   # first: the same width, moved
            if is_log(lo, hi):
                a = math.exp(math.log(answer) - want * math.log(hi / lo))
                tries.append((a, a * hi / lo))
            else:
                a = answer - want * (hi - lo)
                c = a + (hi - lo)
                tries.append((floor if floor is not None and a < floor else a, ceil if ceil is not None and c > ceil else c))
        for want in wants:   # then: a new width built around the answer, inside 0..100 where that applies
            if floor is None or answer <= floor:
                tries.append((answer - want * (hi - lo), answer + (1 - want) * (hi - lo)))
                continue
            a, c = floor, floor + (answer - floor) / want
            if ceil is not None and c > ceil:
                a, c = (answer - want * ceil) / (1 - want), ceil
            if a >= floor:
                tries.append((a, c))
        lo, hi = next(((a, c) for a, c in tries if ok(a, c)), (lo, hi))

    def tidy(x):
        if x == 0:
            return 0.0
        mag = 10 ** (math.floor(math.log10(abs(x))) - 1)
        return round(round(x / mag) * mag, 10)
    if ok(tidy(lo), tidy(hi)):
        lo, hi = tidy(lo), tidy(hi)
    return lo, hi, is_log(lo, hi)


def check_round(raw):
    """One card, cleaned and held to its format's limits. Raises ValueError saying what is wrong, in words the
    coding model can act on."""
    raw = raw if isinstance(raw, dict) else {}
    kind = str(raw.get("type", ""))

    def why():
        return _fit(raw.get("why") or raw.get("explain"), 240, "why")
    if kind == "estimate":
        answer = _num(raw.get("answer"))
        if answer is None:
            raise ValueError("answer must be a number")
        q = _fit(raw.get("q"), 170, "q")
        unit = _txt(raw.get("unit"), 16)
        lo, hi, log_scale = _estimate_range(answer, _num(raw.get("lo")), _num(raw.get("hi")), q, unit)
        if not lo < answer < hi:
            raise ValueError("answer must lie between lo and hi")
        return {"type": kind, "q": q, "answer": answer, "unit": unit, "lo": lo, "hi": hi,
                "log": log_scale, "why": why()}
    if kind == "flaw":
        items = _texts(raw.get("items"), 3, 4, 120, "statement")
        return {"type": kind, "q": _fit(raw.get("q"), 120, "q", optional=True) or "One of these is wrong. Which?",
                "items": items, "flaw": _index(raw.get("flaw"), len(items), "flaw"), "why": why()}
    if kind == "sequence":
        return {"type": kind, "q": _fit(raw.get("q"), 120, "q"), "items": _texts(raw.get("items"), 3, 5, 95, "item"),
                "why": why()}
    if kind == "match":
        pairs = [x for x in (raw.get("pairs") or []) if isinstance(x, (list, tuple)) and len(x) == 2][:4]
        left = _texts([x[0] for x in pairs], 3, 4, 34, "term")
        right = _texts([x[1] for x in pairs], 3, 4, 80, "meaning")
        return {"type": kind, "q": _fit(raw.get("q"), 100, "q", optional=True) or "Match each one to what it does.",
                "pairs": [list(x) for x in zip(left, right)]}
    if kind == "duel":
        a, b = (raw.get(k) if isinstance(raw.get(k), dict) else {} for k in ("a", "b"))
        va, vb = _num(a.get("value")), _num(b.get("value"))
        if va is None or vb is None:
            raise ValueError("a.value and b.value must be numbers")
        if abs(va - vb) <= 0.05 * max(abs(va), abs(vb)):
            raise ValueError("the two values are too close to call a winner")
        return {"type": kind, "q": _fit(raw.get("q"), 120, "q"), "unit": _txt(raw.get("unit"), 20),
                "a": {"label": _fit(a.get("label"), 60, "a.label"), "value": va},
                "b": {"label": _fit(b.get("label"), 60, "b.label"), "value": vb}, "why": why()}
    if kind == "clues":
        options = _texts(raw.get("options"), 3, 4, 44, "option")
        return {"type": kind, "q": _fit(raw.get("q"), 80, "q", optional=True) or "What is it?",
                "clues": _texts(raw.get("clues"), 2, 3, 120, "clue"), "options": options,
                "answer": _index(raw.get("answer"), len(options), "answer"), "why": why()}
    if kind == "call":
        options = _texts(raw.get("options"), 2, 4, 80, "option")
        return {"type": kind, "q": _fit(raw.get("q"), 280, "q"), "options": options,
                "answer": _index(raw.get("answer"), len(options), "answer"),
                "hint": _fit(raw.get("hint"), 120, "hint", optional=True), "why": why()}
    if kind == "quickfire":
        items = [{"text": _fit(x.get("text"), 105, f"statement {i + 1}"), "truth": _bool(x.get("truth"), "truth")}
                 for i, x in enumerate(raw.get("items") or []) if isinstance(x, dict)][:5]
        if len(items) < 3 or len({x["truth"] for x in items}) < 2:
            raise ValueError("needs 4-5 statements, some true and some false")
        return {"type": kind, "q": _fit(raw.get("q"), 80, "q", optional=True) or "True or false?", "items": items}
    if kind == "lab":
        out = check_template(raw) if raw.get("template") else check_explore(raw)
        if not out.get("goal"):
            raise ValueError("the lab's mission cannot be reached with its sliders, or is already met where they "
                             "start: change the params or the goal's min/max")
        out["type"] = "lab"
        out["text"] = _fit(out.get("text"), 260, "text")
        return out
    raise ValueError(f"unknown card type {kind!r}")


def _used(recent, key):
    return sum(1 for r in list(recent)[-8:] if key in (r.get("formats") or []))


def pick_formats(segs, recent, per_segment):
    """One request to the decision model per segment: which hands-on picture fits (a choice), and for every card
    format how well it would work here (a probability each). The formats are then ranked here, with a penalty
    for ones used in recent episodes or already used today, so the page changes from day to day.
    Returns {segment title: {"lab": template | "none" | None, "formats": [...], "decided": bool}}."""
    labs = {"none": "No picture fits: the main idea is not about a quantity that changes (a debate, a policy, a "
                    "story), or none of the others matches."} | {k: t["pick"] for k, t in TEMPLATES.items()}
    questions = {"lab": {"type": "choice", "criteria": labs,
                         "instructions": "Which hands-on picture would let a curious engineer DISCOVER this segment's "
                                         "main idea by playing with it? Pick the one whose mechanism really matches. "
                                         "When two fit about equally, prefer one not recently used."}}
    for key, r in ROUNDS.items():
        questions[f"fmt_{key}"] = {"type": "noul", "instructions": f'Would a "{r["name"]}" card work well for this segment?',
                                   "criteria": {"true": r["pick"], "false": "The segment has nothing of that kind."}}
    recent_labs = [r.get("template") for r in recent if r.get("template")]
    plan, today = {}, []
    for s, turns in segs:
        state_obj = {"segment": s["title"], "angle": s.get("angle", ""), "excerpt": " ".join(t for _, t in turns)[:2500],
                     "pictures_used_recently": recent_labs[-6:], "used_already_today": list(today)}
        ans = decide(state_obj, questions, subject="podcast segment, to choose the game cards for it") or {}
        lab = (ans.get("lab") or {}).get("choice")
        lab = lab if lab in TEMPLATES or lab == "none" else None
        probs = {k: (ans.get(f"fmt_{k}") or {}).get("noul") for k in ROUNDS}
        decided = any(v is not None for v in probs.values())
        seed = sum(ord(c) for c in s["title"] + dt.datetime.now(TZ).strftime("%Y%m%d"))
        order = list(ROUNDS)
        order = order[seed % len(order):] + order[:seed % len(order)]   # no decision: a different rotation each day
        rank = sorted(order, key=lambda k: -((probs[k] if probs[k] is not None else 0.5)
                                             - 0.12 * _used(recent, k) - 0.3 * today.count(k)))
        n = per_segment - (1 if lab and lab != "none" else 0)
        chosen = rank[:max(1, n)]
        today += chosen + ([lab] if lab and lab != "none" else [])
        plan[s["title"]] = {"lab": lab, "formats": chosen, "decided": decided or lab is not None}
        log(f"  game for \"{s['title'][:44]}\": picture={lab or 'writer chooses'}, cards={', '.join(chosen)}"
            + ("" if decided else " (no decision model answered: daily rotation)"))
    return plan


def pick_theme(written, theme, recent):
    """The session's look and framing, chosen by the decision model (or rotated when none answers)."""
    last = [r.get("theme") for r in recent if r.get("theme")][-3:]
    ans = decide({"theme": theme, "segments": [s["title"] for s, _ in written], "themes_used_recently": last},
                 {"session": {"type": "choice", "criteria": {k: t["pick"] for k, t in THEMES.items()},
                              "instructions": "Which framing suits this episode's game best? When two fit about "
                                              "equally, prefer one not used recently."}},
                 subject="podcast episode, to choose the framing of its companion game") or {}
    key = (ans.get("session") or {}).get("choice")
    if key not in THEMES:
        fresh = [k for k in THEMES if k not in last] or list(THEMES)
        key = fresh[dt.datetime.now(TZ).toordinal() % len(fresh)]
    return key


PLAY_GUIDE = """Write the cards of a three-minute phone game about ONE segment of today's podcast episode. The
listener plays it after the drive. He stopped using the old page because it read like an exam: long questions,
long options, specialist words. So:
- You are a sharp pub-quiz host, not an examiner. Every card is read in five seconds: keep to the word limits
  in each format - they are hard limits.
- Plain words. No term he would need a glossary for; if the segment's own key term is needed, the card must
  still make sense to someone who has forgotten it.
- Go for the surprise: the fact, number or mechanism where a smart person's gut feeling is wrong. No trivia
  (names, dates, who said what) and nothing that depends on remembering the exact wording.
- Each card stands on its own and gives every value it needs. Use only facts and numbers the segment states,
  or a clearly simple example - never invent technical data.
- At most one card may be set in his own world (a factory, an audit, a lab); do not explain his field to him
  ({expert}).
- Light humour is welcome. Vary the angle from the recent games listed below.

THE BET (always): one statement about this segment that is either true or false, where most people would
hesitate. He bets on it before the cards and it is settled at the end of the segment:
{bet}

THE CARDS for this segment, in this order - one of each format listed, exactly as specified:
{formats}
{lab}
Also return: "takeaway" - the one line worth remembering (max 20 words) - and "words": 2-4 terms from the
segment worth keeping, each with a plain one-line meaning (max 14 words).{session}

Return JSON only:
{"bet": {...}, "rounds": [<one object per card, in order>], "takeaway": "...",
 "words": [{"term": "...", "means": "..."}]{session_json}}"""

LAB_GUIDE = """- "lab" - a hands-on picture with sliders and a mission. The picture and its maths are built in; you
  only fill in the story:
  {"type": "lab", "template": "<name>", "text": "<what to try, max 30 words>", "params": {<the template's params>},
   "goal": {"text": "<the mission: Find... / Make... / Get...>", "output": "<one of its goal outputs>",
            "min": <n>, "max": <n>, "success": "<what he just discovered, max 30 words>"}}
  Realistic values from the segment. The sliders must START where the mission is NOT yet achieved, and the
  mission must be reachable by moving them. Talk only about the sliders the template has.
"""


def write_cards(seg, turns, spec, theme_key, theme, recent, first):
    """The writing model fills in one segment's cards. Returns the raw reply as a dict."""
    formats = "\n".join(f'- "{k}" ({ROUNDS[k]["name"]}): {ROUNDS[k]["rules"]}\n  {ROUNDS[k]["schema"]}' for k in spec["formats"])
    lab = ""
    if spec["lab"] and spec["lab"] != "none":
        lab = (LAB_GUIDE + f'  THE TEMPLATE (chosen by the decision model): "{spec["lab"]}"\n'
               + template_catalog([spec["lab"]]) + "\n  Put the lab second among the cards.\n")
    elif spec["lab"] is None:
        lab = (LAB_GUIDE + "  Optional: add ONE lab only if one of these templates truly fits the segment's main idea "
               "(otherwise no lab):\n" + "\n".join(f"  - {k}: {t['pick']}" for k, t in TEMPLATES.items()) + "\n")
    session = session_json = ""
    if first:
        session = (f'\nAnd for the whole game: "session" - a title (max 6 words, intriguing, plain), a "brief" that '
                   f"sets the scene in one line (max 18 words) in this framing: {THEMES[theme_key]['frame']} - and "
                   '"chips": 4 short comments he might want to send back about THIS episode with one tap (max 5 '
                   'words each, e.g. "More like the first story", "Too many numbers", "Go deeper on this").')
        session_json = ',\n "session": {"title": "...", "brief": "...", "chips": ["...", "...", "...", "..."]}'
    seen = "\n".join(f"- {r.get('date', '')}: {r.get('title', '')} ({', '.join(r.get('formats') or []) or r.get('template') or '-'})"
                     for r in list(recent)[-8:]) or "(none yet)"
    script = "\n".join(f"{spk}: {t}" for spk, t in turns)[:12000]
    prompt = (PLAY_GUIDE.replace("{expert}", EXPERT_AREAS).replace("{bet}", BET_SCHEMA).replace("{formats}", formats)
              .replace("{lab}", lab).replace("{session}", session).replace("{session_json}", session_json)
              + f"\n\nRECENT GAMES (vary the angle):\n{seen}\n\nListener: {LISTENER_PROFILE}\nToday's theme: {theme}"
                f"\n\nSEGMENT: {seg['title']}\n{script}")
    text, _ = llm(prompt, json_mode=True, min_words=60, label=f"cards for \"{seg['title'][:30]}\"", role="writer")
    data = parse_json(text)
    return data if isinstance(data, dict) else {}


def repair_cards(broken):
    """The coding model fixes cards that failed their checks. broken = [{"id", "kind", "card", "error"}]; returns
    {id: repaired card}. One request for the whole episode."""
    if not broken:
        return {}
    jobs = []
    for b in broken:
        kind = b["kind"]
        spec = BET_SCHEMA if kind == "bet" else LAB_GUIDE + template_catalog([b["card"].get("template")]) \
            if kind == "lab" and b["card"].get("template") in TEMPLATES else LAB_GUIDE if kind == "lab" \
            else ROUNDS.get(kind, {}).get("schema", "")
        jobs.append({"id": b["id"], "format": kind, "schema": spec, "error": b["error"], "card": b["card"]})
    prompt = f"""These JSON cards for a quiz page failed validation. Repair each one so that it matches its schema
and no longer has the error. Change as little as possible: keep the facts, the numbers and the right answer; shorten
text by rewording, never by cutting a sentence off; for a "lab", adjust params or the goal's min/max so the mission
is reachable but not already met at the start. If a card cannot be repaired honestly, leave it out.

{json.dumps(jobs, ensure_ascii=False)[:24000]}

Return JSON only: {{"cards": [{{"id": "<id>", "card": {{...}}}}]}}"""
    try:
        text, _ = llm(prompt, json_mode=True, label="card repair", role="coder")
        fixed = as_obj(parse_json(text), "cards").get("cards") or []
    except Exception as e:
        log(f"    card repair skipped: {str(e)[:120]}")
        return {}
    return {str(c.get("id")): c.get("card") for c in fixed if isinstance(c, dict) and isinstance(c.get("card"), dict)}


def _check_card(kind, card):
    return check_bet(card) if kind == "bet" else check_round(card)


def review_cards(segments):
    """A stricter model checks the facts and the answers; cards it finds wrong are removed."""
    shown = []
    for si, sg in enumerate(segments):
        if sg.get("bet"):
            shown.append({"segment": si, "card": "bet", **sg["bet"]})
        for ri, r in enumerate(sg["rounds"]):
            row = {k: v for k, v in r.items() if k not in ("lo", "hi", "view")} if r["type"] == "lab" else dict(r)
            if r["type"] == "lab":
                row["outputs"] = [{k: o[k] for k in ("label", "unit", "expr")} | {"range": [round(o["lo"], 3), round(o["hi"], 3)]}
                                  for o in r["outputs"]]
            shown.append({"segment": si, "card": ri, **row})
    prompt = f"""You check the cards of a science quiz before a learner sees them. For each card, look for: wrong
science or maths; a marked answer that is wrong or ambiguous (bet "truth", "flaw" index, "answer" index, a
quickfire statement's "truth", a "duel" whose bigger value is wrong); an explanation that contradicts the answer;
a "sequence" whose items are not in the correct order or that is a ranking or opinion rather than one real
order; an "estimate" whose answer is not a single well-defined number; a "match" where a meaning fits more than
one term; invented technical data presented as fact; a "lab" whose text promises controls its sliders (inputs) do
not have or whose values are unrealistic for the story; and trivia (a name, date or who-said-what that needs no
reasoning). Ignore style. Be strict about facts, not wording.

CARDS:
{json.dumps(shown, ensure_ascii=False)[:30000]}

Return JSON only: {{"problems": [{{"segment": <n>, "card": <n or "bet">, "issue": "<what is wrong>"}}]}} (empty if all correct)"""
    try:
        text, _ = llm(prompt, json_mode=True, label="card check", role="checker")
        problems = [x for x in as_obj(parse_json(text), "problems").get("problems", []) if isinstance(x, dict)]
    except Exception as e:
        log(f"  card check skipped: {str(e)[:120]}")
        return segments, 0
    bad = set()
    for x in problems:
        si = _num(x.get("segment"))
        ci = "bet" if str(x.get("card")) == "bet" else _num(x.get("card"))
        if si is not None and ci is not None:
            bad.add((int(si), ci if ci == "bet" else int(ci)))
            log(f"    card check: segment {int(si)} card {ci if ci == 'bet' else int(ci)}: {str(x.get('issue', ''))[:140]}")
    out = []
    for si, sg in enumerate(segments):
        rounds = [r for ri, r in enumerate(sg["rounds"]) if (si, ri) not in bad]
        sg = dict(sg, rounds=rounds, bet=None if (si, "bet") in bad else sg.get("bet"))
        if rounds or sg["bet"]:
            out.append(sg)
    return out, len(bad)


def make_play(written, theme, recent=()):
    """The game for the episode's page. Returns (play, log entry) or (None, None) when nothing usable was made."""
    segs = [(s, turns) for s, turns in written if s.get("format") != "followup"][:3]
    if not segs:
        return None, None
    recent = list(recent)
    theme_key = pick_theme(written, theme, recent)
    plan = pick_formats(segs, recent, per_segment=3 if len(segs) == 1 else 2)
    segments, broken, session, words, dropped = [], [], {}, [], 0
    for n, (seg, turns) in enumerate(segs):
        spec = plan[seg["title"]]
        try:
            raw = write_cards(seg, turns, spec, theme_key, theme, recent, first=n == 0)
        except Exception as e:
            log(f"  cards for segment {n + 1} skipped: {str(e)[:160]}")
            continue
        sg = {"title": seg["title"], "bet": None, "rounds": [], "takeaway": _txt(raw.get("takeaway"), 200)}
        most = len(spec["formats"]) + 1   # the chosen formats plus one hands-on picture: a short game gets played
        cards = [("bet", raw.get("bet"))] + [(str(r.get("type", "")), r) for r in (raw.get("rounds") or [])
                                             if isinstance(r, dict)][:most]
        for i, (kind, card) in enumerate(cards):
            if not isinstance(card, dict):
                continue
            try:
                good = _check_card(kind, card)
            except (ValueError, TypeError, KeyError, ZeroDivisionError, OverflowError) as e:
                broken.append({"id": f"{len(segments)}.{i}", "kind": kind, "card": card, "error": str(e)[:200]})
                if kind != "bet":
                    sg["rounds"].append(None)   # keeps its place for the repaired card
                continue
            if kind == "bet":
                sg["bet"] = good
            else:
                sg["rounds"].append(good)
        if isinstance(raw.get("session"), dict) and not session:
            session = raw["session"]
        words += [{"term": _txt(w.get("term"), 50), "means": _txt(w.get("means"), 160)}
                  for w in (raw.get("words") or [])[:4] if isinstance(w, dict) and w.get("term") and w.get("means")]
        segments.append(sg)
    if broken:
        log(f"  {len(broken)} card(s) failed their checks -> coding model: "
            + "; ".join(f"{b['kind']}: {b['error'][:70]}" for b in broken[:4]))
        fixed = repair_cards(broken)
        for b in broken:
            si, i = (int(x) for x in b["id"].split("."))
            card = fixed.get(b["id"])
            try:
                good = _check_card(b["kind"], dict(card, type=b["kind"]) if b["kind"] != "bet" else card) if card else None
            except (ValueError, TypeError, KeyError, ZeroDivisionError, OverflowError) as e:
                log(f"    repaired {b['kind']} still fails ({str(e)[:90]}) -> dropped")
                good = None
            if good is None:
                dropped += 1
            elif b["kind"] == "bet":
                segments[si]["bet"] = good
            else:
                slot = [k for k, r in enumerate(segments[si]["rounds"]) if r is None]
                segments[si]["rounds"][slot[0]] = good
    for sg in segments:
        sg["rounds"] = [r for r in sg["rounds"] if r is not None]
    segments = [sg for sg in segments if sg["rounds"] or sg["bet"]]
    if segments:
        segments, more = review_cards(segments)
        dropped += more
    if not segments or not any(sg["rounds"] for sg in segments):
        log("  no usable game cards for this episode")
        return None, None
    seen, uniq = set(), []
    for w in words:
        if w["term"].lower() not in seen:
            seen.add(w["term"].lower())
            uniq.append(w)
    title = _txt(session.get("title"), 60) or segments[0]["title"][:60]
    chips = [c for c in (_txt(x, 40) for x in (session.get("chips") or [])[:5] if isinstance(x, str)) if c]
    play = {"v": 2, "theme": theme_key, "title": title, "brief": _txt(session.get("brief"), 160),
            "segments": segments, "words": uniq[:8], "chips": chips}
    formats = [r["type"] for sg in segments for r in sg["rounds"]]
    labs = [r.get("template") for sg in segments for r in sg["rounds"] if r.get("template")]
    log(f"  game \"{title}\" ({THEMES[theme_key]['name']}): {len(segments)} segment(s), {len(formats)} cards "
        f"({', '.join(formats)}), {sum(1 for sg in segments if sg['bet'])} bet(s); {dropped} card(s) dropped")
    return play, {"title": title, "template": labs[0] if labs else "", "formats": formats, "theme": theme_key}


def heard_lines(state, fresh, pace_change):
    """'You said -> we did': what the listener's feedback changed, shown at the top of the page. People stop giving
    feedback when nothing visibly comes of it. Every line states something this script really does."""
    out = []
    if pace_change:
        out.append({"said": "The pace was too " + ("fast" if pace_change < 0 else "slow"),
                    "did": f"From this episode the voices are {abs(round(pace_change * 100))}% "
                           f"{'slower' if pace_change < 0 else 'faster'}."})
    votes = [f.get("ratings", {}).get("depth") for f in state["feedback"][-6:]]
    lean = votes.count("too technical") - votes.count("too basic")
    if abs(lean) >= 2:
        out.append({"said": f"\"{'Too technical' if lean > 0 else 'Too basic'}\" in {max(votes.count('too technical'), votes.count('too basic'))} of your last {len(votes)} ratings",
                    "did": "Stories that need specialist background are ranked lower when topics are picked."
                           if lean > 0 else "More technical stories are ranked higher when topics are picked."})
    for f in reversed(fresh):
        more = [s for s, v in (f.get("segments") or {}).items() if v == "more"]
        less = [s for s, v in (f.get("segments") or {}).items() if v == "less"]
        if more or less:
            out.append({"said": (f"More like \"{more[0][:60]}\"" if more else f"Less like \"{less[0][:60]}\""),
                        "did": "The editor sees this when it chooses the next episodes' stories."})
        if f.get("comment"):
            out.append({"said": f"\"{f['comment'][:110]}\"", "did": "Passed to the editor and the writers of every episode from now on."})
    last = max((f.get("received", "") for f in state["feedback"]), default="")
    return {"lines": out[:4], "last": last[:10]}


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


def write_quiz_files(keep, entry, transcript_text, heard=None):
    TRANSCRIPT_DIR.mkdir(exist_ok=True)
    (TRANSCRIPT_DIR / (Path(entry["file"]).stem + ".txt")).write_text(transcript_text, encoding="utf-8")
    old = json.loads(QUIZ_FILE.read_text(encoding="utf-8")).get("episodes", []) if QUIZ_FILE.exists() else []
    files = {e["file"] for e in keep}
    eps = [e for e in old if e["file"] in files and e["file"] != entry["file"]] + [entry]
    eps.sort(key=lambda e: e["published"], reverse=True)
    ready = {name: due.hhmm(w[2]) for name, w in due.WINDOWS.items()}
    QUIZ_FILE.write_text(json.dumps({"show": SHOW_TITLE, "listener": LISTENER, "repo": REPO_FULL, "ready": ready,
                                     "heard": heard or {}, "episodes": eps},
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
    now = dt.datetime.now(TZ)
    edition = os.getenv("EDITION", "auto").strip().lower()
    force, dry = env_true("FORCE"), env_true("DRY_RUN")
    if edition not in ("morning", "evening"):
        # Not the clock alone: GitHub started the "morning" runs after noon for a week (6-12 Nov 2026) and each
        # became a second evening edition. The edition is the one that is due and not yet made (see due.py).
        edition, _, why = due.decide("auto", force or dry, now)
        if edition == "none":
            log(f"{SHOW_TITLE} | {why} - nothing to do.")
            return
    date_key = now.strftime("%Y-%m-%d")
    is_sunday = now.weekday() == 6
    log(f"{SHOW_TITLE} | {edition} {date_key} | force={force} dry_run={dry} | TTS={TTS_ENGINE}")

    state = load_state()
    DOSSIER.update(load_dossier())
    SPEND.extend(state.get("or_spend", []))
    FREE_LOG.extend(state.get("free_log", []))
    FREE_DAY_OUT[0] = state.get("free_day_out", "")
    if any(e["date"] == date_key and e["edition"] == edition for e in state["episodes"]) and not force:
        log("This edition already exists - nothing to do (backup run).")
        return

    load_health(state)
    close_issues, fresh = ([], []) if dry else ingest_feedback(state)
    pace_change = update_pace(state, fresh)
    apply_pace(state["pace_adjust"])
    if pace_change or state["pace_adjust"]:
        log(f"  pace: {state['pace_adjust']:+.0%} from the original speed (change today {pace_change:+.0%})")

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
    deep, minutes, plan = None, EPISODE_MINUTES, []
    real_topics = [k for k in state["topics"] if k not in NO_TOPIC]
    log("4/8 Planning")
    if is_sunday and not env_true("DEEP_DIVE"):   # no commute: a long magazine episode, AI morning, science evening
        minutes = sunday_minutes(edition)
        theme, plan, deep = plan_sunday(edition, state, stories, date_key, minutes)
        if plan:
            min_words = int(0.7 * sum(s["words"] for s in plan))
            log(f"  SUNDAY ({minutes:.0f} min): {theme}")
        else:
            log("  nothing built up this week and no threads got away -> regular episode")
            minutes = EPISODE_MINUTES
    if env_true("DEEP_DIVE") and real_topics and not plan:   # manual "deep dive now" from the workflow
        deep = max(real_topics, key=lambda k: (len(state["topics"][k]["days"]), state["topics"][k]["mentions"]))
        minutes = max(EPISODE_MINUTES, 30)
    if plan:
        pass   # the Sunday magazine is planned
    elif deep:
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
        items = seg.get("stories") or [by_id[j] for j in seg["item_ids"] if j in by_id]
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

    log("  building the game for the episode's page")
    play, play_log = make_play(written, theme, state.get("lesson_log", []))
    if play_log:
        state["lesson_log"] = (state.get("lesson_log", []) + [dict(play_log, date=date_key)])[-30:]

    log("7/8 Greeting and voices")
    greeting, closing, summary, threads = write_bookends(state, written, theme, edition, now, bool(deep), source_note,
                                                         new_feedback_text(fresh, pace_change), play)
    sections = [("Welcome", greeting)] + [(s["title"], t) for s, t in written] + [("Close", closing)]
    pcm, marks, engine = voice_episode(sections, state, edition)
    BUILD_DIR.mkdir(exist_ok=True)
    filename = f"ai_{date_key}_{edition}_{now:%H%M}.mp3"
    mp3 = BUILD_DIR / filename
    kind = "Sunday" if is_sunday else "Deep dive" if deep else edition.title()
    headline = state["topics"][deep]["name"] if deep else ("The ones that got away" if is_sunday else plan[0]["title"])
    ep_title = f"{kind} - {now.day} {now:%b}: {headline}"[:120]
    encode_mp3(pcm, mp3, ep_title, marks)
    duration, size = int(len(pcm) / BYTES_PER_SEC), mp3.stat().st_size
    writer = USED_BY.most_common(1)[0][0] if USED_BY else "?"
    log(f"  {filename}: {fmt_ts(duration)}, {size / 1e6:.1f} MB, writer={dict(USED_BY)}, voices={engine}")

    log("8/8 Publishing")
    pretty = f"{now:%A}, {now.day} {now:%B %Y}"
    notes = [f"{'Sunday edition' if is_sunday else 'Deep dive' if deep else edition.title() + ' edition'} - {pretty}",
             theme, ""]
    for (label, start), (seg, _) in zip(marks[1:], written):   # chapters only: Reddit links are of no use to him
        notes.append(f"{fmt_ts(start)}  {label}")
    notes += ["", (f"Play \"{play['title']}\", a three-minute game on this episode" if play else "Quiz and feedback")
              + f": {PAGES_URL}/quiz.html#play={filename}",   # opens this episode's game directly
              f"Written by: {writer} · Voices: {ENGINE_NAMES[engine]} · Discussions: {source_note}"]
    episode = {"file": filename, "date": date_key, "edition": edition, "title": ep_title,
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
    for st in [by_id[j] for j in used if j in by_id] + [x for sg, _ in written for x in sg.get("stories", [])]:
        seen += [{"date": date_key, "link": u} for u in st["threads"]]
        if st.get("link"):
            seen.append({"date": date_key, "link": st["link"]})
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

    log("  writing quiz (and thread notes for the weekly dossier)")
    quiz = make_quiz(written, theme)
    digest = make_digest(stories, used)
    update_dossier(stories, used, digest, state, date_key)
    save_dossier(dry)

    fb_cut = (now - dt.timedelta(days=FEEDBACK_DAYS)).isoformat()
    day_ago = (now - dt.timedelta(hours=24)).isoformat()
    utc_yesterday = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    save_state({"episodes": keep, "memory": memory, "seen": seen,
                "feedback": [f for f in state["feedback"] if f.get("received", "") >= fb_cut],
                "tts_log": state.get("tts_log", []), "or_spend": [s for s in SPEND if s[0] >= day_ago],
                "free_log": [t for t in FREE_LOG if t >= utc_yesterday], "free_day_out": FREE_DAY_OUT[0],
                "pace_adjust": state.get("pace_adjust", 0.0), "lesson_log": state.get("lesson_log", []),
                "model_health": save_health(), "topics": state["topics"]})

    transcript = "\n\n".join(f"## {lbl}\n" + "\n".join(f"{spk}: {t}" for spk, t in turns) for lbl, turns in sections)
    write_quiz_files(keep, {
        "file": filename, "title": episode["title"], "label": label, "date": date_key, "edition": edition,
        "published": now.isoformat(), "duration": duration, "theme": theme, "writer": writer,
        "voices": ENGINE_NAMES[engine], "deep_dive": bool(deep),
        "segments": [{"title": s["title"], "category": s.get("format", "")} for s, _ in written],
        **({"play": play} if play else {}), "quiz": quiz,
        "transcript": f"transcripts/{Path(filename).stem}.txt"}, transcript, heard_lines(state, fresh, pace_change))
    build_rss(keep)
    for n in close_issues:
        gh("issue", "close", str(n), check=False)  # no comment: it only triggered an empty notification email
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
