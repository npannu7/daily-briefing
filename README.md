# Weights & Measures

A personal two-host podcast built from AI discussions on Reddit, checked against the evidence.
Two episodes a day (08:00 and 16:00 IST), at zero cost by default.

- **Feed:** `https://npannu7.github.io/daily-briefing/podcast.xml`
- **Digest, quiz and feedback:** `https://npannu7.github.io/daily-briefing/quiz.html`

## How it works

| Workflow | When | What it does |
|---|---|---|
| **Collect Reddit** (`collect_reddit.yml`) | every hour | About 10 Reddit requests, one a minute. Threads and their comments build up in a pool (the `reddit-pool` release asset). Comments are read again once a thread is a few hours old. |
| **Weights and Measures** (`daily_briefing.yml`) | 08:00 and 16:00 IST | Scores the pool, checks stories against trusted sources, writes the script, voices it and publishes. |

Each episode's quiz page also shows **At a glance**: the day's top threads, each with one line and a verdict.
It covers threads that didn't make the episode, so you don't need to open Reddit.

## Files you edit

- `subreddits.txt`: which subreddits, with weights.
- `interests.txt`: what you personally want more of. Matching threads rank higher.
- `feeds.txt`: trusted outlets used to confirm stories (Hugging Face Daily Papers and Hacker News are built in).

## Cost: free by default

| Job | Used, in order |
|---|---|
| Writing | Gemini API free key, then OpenRouter free models (Nemotron 3 Ultra, Nemotron 3 Super, Gemma 4 31B), then GitHub Models. Once a model works, it keeps writing the rest of the episode. |
| Scoring | Each thread judged on its own by Nemotron 3 Super (free), Gemma 4 31B as backup, about 70 requests per episode. Threads they miss are scored in one batched call. |
| Topics, quiz, digest | Gemini Flash-Lite free key, then OpenRouter free models (Nemotron 3 Super, Gemma 4). |
| Voices | Fish Audio (OpenRouter free), then Gemini TTS (free key), then Edge. |

OpenRouter's free models (text and Fish) share one daily request limit that resets at 00:00 UTC.
The script counts its own requests and keeps a reserve for writing.

## Settings (Settings → Secrets and variables → Actions)

Secrets: `GEMINI_API_KEY`, `OPENROUTER_API_KEY`.

Optional variables:

| Variable | Default | Meaning |
|---|---|---|
| `OPENROUTER_FREE_DAILY` | `1000` | Your OpenRouter free-model daily limit (1000 after buying $10 of credits; 50 otherwise) |
| `OPENROUTER_DAILY_USD` | `0` | Spend cap for paid models. Leave at 0 to stay free |
| `WRITER_MODELS`, `HELPER_MODELS` | free chains | e.g. `gemini:gemini-3.8-flash,openrouter:free,github:auto` |
| `OPENROUTER_FREE_MODELS` | Nemotron / Gemma | Replace the free model lists, comma-separated |
| `DECISION_MODELS` | Nemotron 3 Super, Gemma 4 31B (free) | Models that score each thread. A paid one that charges money is dropped while the cap is 0 |
| `SCORING` | `auto` | `batch` or `decisions` |
| `FISH_VOICE_A`, `FISH_VOICE_B` | set | Fish Audio voice ids for Sarah and Laura |
