# Weights & Measures

A personal two-host podcast built from AI and science discussions on Reddit, checked against the evidence.
At zero cost by default:

- **Monday–Saturday:** two 20-minute episodes for the commute (08:00 and 16:00 IST), each with one AI story and one
  science / technology / biology / medicine story.
- **Sunday:** two long magazine episodes (about 45 minutes; shorter if the free daily budget is low), AI in the
  morning and science in the evening. Each has:
  - **What happened next:** updates on stories from the week's episodes.
  - **A deep dive** (about 25 minutes) on the week's most built-up topic, told as the week's story, how it works,
    and a reality check.
  - **The ones that got away:** three of the week's best threads that didn't make a weekday episode.

  The material is gathered all week: each weekday run saves a **dossier** (`dossier.json` in the `reddit-pool`
  release) with the best comments, sources and Hacker News discussion for every tracked topic, plus its best
  unused threads. Reddit threads only stay in the pool for 36 hours, so without it Sunday would have only headlines.

Each topic is drafted, fact-checked against its sources by a second model, then rewritten:
at most one number per sentence, and every technical term explained.

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

- `subreddits.txt`: which subreddits, with weights and subject (`ai` or `science`).
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
| `EPISODE_MINUTES` | `20` | Weekday episode length (15 = 1 topic, 20 = 2, 30 = 3). The Run workflow button also has a minutes box |
| `SUNDAY_MINUTES` | `45` | Longest Sunday deep dive. Shorter if the free budget is low |
| `FACT_CHECK` | `true` | Draft, fact-check, rewrite for each topic |
| `FISH_TEMPO_A`, `FISH_TEMPO_B` | `1.08`, `1.0` | Speaking speed for Sarah and Laura |
| `OPENROUTER_FREE_DAILY` | `1000` | Your OpenRouter free-model daily limit (1000 after buying $10 of credits; 50 otherwise) |
| `OPENROUTER_DAILY_USD` | `0` | Spend cap for paid models. Leave at 0 to stay free |
| `WRITER_MODELS`, `HELPER_MODELS` | free chains | e.g. `gemini:gemini-3.8-flash,openrouter:free,github:auto` |
| `OPENROUTER_FREE_MODELS` | Nemotron / Gemma | Replace the free model lists, comma-separated |
| `DECISION_MODELS` | Nemotron 3 Super, Gemma 4 31B (free) | Models that score each thread. A paid one that charges money is dropped while the cap is 0 |
| `SCORING` | `auto` | `batch` or `decisions` |
| `FISH_VOICE_A`, `FISH_VOICE_B` | set | Fish Audio voice ids for Sarah and Laura |
