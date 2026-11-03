# Weights & Measures

A personal two-host podcast built from AI and science discussions on Reddit, checked against the evidence.
At zero cost by default:

- **Monday–Saturday:** two 20-minute episodes for the commute, ready before 08:00 and 16:00 IST, each with one AI
  story and one science / technology / biology / medicine story.
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
at most one number per sentence, and each computer-science term explained once (never the listener's own field:
quality, regulation, manufacturing, engineering).

- **Feed:** `https://npannu7.github.io/daily-briefing/podcast.xml`
- **Puzzles, quiz and feedback:** `https://npannu7.github.io/daily-briefing/quiz.html`

## How it works

| Workflow | When | What it does |
|---|---|---|
| **Collect Reddit** (`collect_reddit.yml`) | every hour | About 10 Reddit requests, one a minute. Threads and their comments build up in a pool (the `reddit-pool` release asset). Comments are read again once a thread is a few hours old. |
| **Weights and Measures** (`daily_briefing.yml`) | starts 06:20 and 14:20 IST | Scores the pool, checks stories against trusted sources, writes the script, voices it and publishes. GitHub starts scheduled runs 30–50 minutes late, so episodes are ready at about 07:15 and 15:15. |

Each episode is mastered to podcast loudness (−16 LUFS), with the two hosts matched in level, a soft chime
between segments, and chapter markers in the MP3, so podcast apps (and the car's skip buttons) can jump between
stories.

Each episode's page has **puzzles**: short, Brilliant-style lessons on the episode's ideas, done on the phone
after the drive. Each lesson is built around one hands-on **template**: a picture with sliders whose maths is
written and tested once, in `generate_briefing.py` (`TEMPLATES`) and drawn by `quiz.html`:

| Template | The picture | Fits stories about |
|---|---|---|
| `saturation` | a tank filling towards a level | gases loading into tissue, drugs building up, charging, cooling |
| `doubling` | a grid of blocks doubling or halving | compute growth, bacteria, radioactive decay, drug clearance |
| `chain` | 12 runs, each breaking at its first failed step | AI agents' multi-step tasks, rolled throughput yield |
| `base_rate` | 1000 dots: real cases, misses and false alarms | screening tests, AI-text detectors, inspection |
| `threshold` | two overlapping bell curves and a cut-off | good vs bad parts, healthy vs sick, spam filters |
| `sampling` | 20 repeat experiments closing in on the truth | benchmarks, clinical trials, polls, sampling plans |
| `gradient_descent` | a ball stepping down an error bowl | training a model, learning rate, over-correcting loops |
| `softmax` | next-word bars and a "Generate" button | how a language model picks words, temperature |
| `flow` | particles crossing a membrane from high to low | partial pressure in the lungs, heat, batteries |
| `inverse_square` | light spreading over a growing area | sunlight, radiation dose, signal strength, gravity |
| `bottleneck` | pipes with the narrowest stage marked | AI inference, production lines, hospitals |

How a lesson is made:

1. The **decision model** chooses the template for each segment: a multiple-choice question over the
   library, or "none" when no template fits. It sees the templates used recently, so they rotate.
2. The **writing model** fills in only the story: realistic values, plain labels and a mission ("Find the
   learning rate that settles in 8 steps"). It never writes code or formulas for a template. Around it, it
   writes the rest of the lesson to fixed rules: a hook (a puzzle, paradox or bet), a prediction, the
   hands-on template with the "aha" as its mission, a short explanation, and a transfer to a new setting such
   as a factory, a hospital or the sky. No trivia, and no repeating recent angles.
3. **Checks**: every value is held to the template's limits; every formula runs before publishing; a mission
   the sliders can't reach is dropped, and one already solved at the start gets a new starting point; a
   second model removes wrong science, unrealistic values and trivia; number answers are computed, not
   taken from the model.

When no template fits, the lesson uses predictions, numbers and ordering (or a slider with its own checked
formulas). The page also keeps **words to keep** (new terms with a one-line meaning), a quick recap quiz and
the feedback form. Puzzles that needed a hint are sent back with the feedback, so later episodes explain
those ideas better. There are no Reddit links on the page or in the show notes.

## Files you edit

- `subreddits.txt`: which subreddits, with weights and subject (`ai` or `science`).
- `interests.txt`: what you personally want more of. Matching threads rank higher.
- `feeds.txt`: trusted outlets used to confirm stories (Hugging Face Daily Papers and Hacker News are built in).

## Cost: free by default

| Job | Used, in order |
|---|---|
| Writing | Gemini API free key, then OpenRouter free models (Nemotron 3 Ultra, Nemotron 3.5 Super VL, Gemma 4 31B), then GitHub Models. Once a model works, it keeps writing the rest of the episode. A model that times out or fails twice is skipped for the rest of the run. |
| Scoring | Each thread judged on its own by Nemotron 3.5 Lightning (free), Nemotron 3.5 Super VL as backup, about 70 requests per episode. A model that fails six times in a row is dropped for the run; threads it misses are scored in one batched call. |
| Puzzles | Template chosen by the decision models (one request per segment), filled in by the writing chain, checked by the helper chain. |
| Topics, quiz, dossier notes | Gemini Flash-Lite free key, then OpenRouter free models (Nemotron 3.5 Super VL, Gemma 4). |
| Voices | Fish Audio (OpenRouter free, three requests in flight), then Gemini TTS (free key), then Edge. |

Nemotron 3 Super lost its free version on 2 November 2026; Nemotron 3.5 Super VL replaced it. NVIDIA's free endpoints
log prompts, and LiquidAI's free model may train on them. Public Reddit threads are fine, but never send company
or confidential documents to a free model.

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
| `FISH_TEMPO_A`, `FISH_TEMPO_B` | `1.08`, `1.0` | Speaking speed for Sarah and Laura. Each "too fast" / "too slow" vote on the quiz page moves both 4% (at most 12%) |
| `FISH_WORKERS` | `3` | Fish requests in flight at once (starts stay 3.2 s apart). `1` = one at a time |
| `TARGET_LUFS` | `-16` | Loudness of the finished episode (the podcast standard). `off` keeps the raw voices, which were about 10 dB quieter |
| `SEGMENT_CHIME` | `true` | A soft chime between segments. `false` leaves a short pause instead |
| `OPENROUTER_FREE_DAILY` | `1000` | Your OpenRouter free-model daily limit (1000 after buying $10 of credits; 50 otherwise) |
| `OPENROUTER_DAILY_USD` | `0` | Spend cap for paid models. Leave at 0 to stay free |
| `WRITER_MODELS`, `HELPER_MODELS` | free chains | e.g. `gemini:gemini-3.8-flash,openrouter:free,github:auto` |
| `OPENROUTER_FREE_MODELS` | Nemotron / Gemma | Replace the free model lists, comma-separated |
| `DECISION_MODELS` | Nemotron 3.5 Lightning, Nemotron 3.5 Super VL (free) | Models that score each thread. `respan/span-01-lite:free` (a free Decisions-API scoring model) also works here. A paid one that charges money is dropped while the cap is 0 |
| `SCORING` | `auto` | `batch` or `decisions` |
| `FISH_VOICE_A`, `FISH_VOICE_B` | set | Fish Audio voice ids for Sarah and Laura |
