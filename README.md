# Weights & Measures

A personal two-host podcast built from AI and science discussions on Reddit, checked against the evidence.
At zero cost by default:

- **Monday–Saturday:** two 20-minute episodes for the commute, ready by **09:00 and 18:00 IST** (normally about
  two hours earlier), each with one AI story and one science / technology / biology / medicine story.
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
at most one number per sentence, and each specialist term (computer science, biology, medicine, physics) explained
once (never the listener's own field: quality, regulation, manufacturing, engineering). A story that nothing
outside Reddit confirms is web-checked, and if it stays unconfirmed the hosts say so in the first three turns,
not at the end.

- **Feed:** `https://npannu7.github.io/daily-briefing/podcast.xml`
- **The game for each episode, and feedback:** `https://npannu7.github.io/daily-briefing/quiz.html`

## How it works

| Workflow | When | What it does |
|---|---|---|
| **Collect Reddit** (`collect_reddit.yml`) | every hour | About 10 Reddit requests, one a minute (up to 30 after a gap). Threads and their comments build up in a pool (the `reddit-pool` release asset). Comments are read again once a thread is a few hours old. Each run also starts the episode workflow when an edition is due. |
| **Weights and Measures** (`daily_briefing.yml`) | checks every 30 minutes | Makes the edition that is due: scores the pool, checks stories against trusted sources, writes the script, voices it and publishes. A check with nothing to do ends in a few seconds. |

### When episodes are made

`due.py` holds the schedule (India time):

| Edition | Started from | Ready by | Not started after |
|---|---|---|---|
| Morning | 06:00 | 09:00 | 13:00 |
| Evening | 15:00 | 18:00 | 23:30 |

An edition is **due** when it has not been published today and the time is inside its window. A weekday
episode takes 25–35 minutes to make, a Sunday one up to an hour.

Why it works this way: GitHub does not start scheduled workflows on time. In October 2026 they started
30–60 minutes late; from 6 November they started **6–10 hours late**. The old rule was "before noon = morning
edition, after = evening", so every late morning run made a second *evening* edition and the real evening run
found it done: one episode a day and no morning show from 6 to 12 November. Now:

- the edition is chosen by what is due and missing, never by the clock alone;
- there is a check every half hour (48 a day instead of 4), and the hourly Reddit collector checks too;
- a check that arrives up to 2.5 hours **before** a start time waits for it, reading Reddit meanwhile, instead
  of trusting the next check to arrive in time;
- when a check finds an earlier run already waiting or building, it stops.

Replaying the times at which GitHub actually started runs on 6–12 November (when there were only about eight
starts a day) through these rules, the morning edition would have been ready before 08:00 on six of the seven
days and the evening edition before 16:00 on six of seven. It is not a guarantee: if GitHub starts nothing at all
between 03:30 and 08:00, the morning edition is late.

**For a guarantee, add an outside alarm clock** (five minutes, free). A service such as cron-job.org can call
GitHub at 06:00 and 15:00 IST:

1. On GitHub: Settings → Developer settings → Fine-grained tokens → new token for this repository only, with
   the permission **Actions: Read and write**.
2. In the cron service, two jobs (00:30 and 09:30 UTC) that send
   `POST https://api.github.com/repos/npannu7/daily-briefing/actions/workflows/daily_briefing.yml/dispatches`
   with the headers `Authorization: Bearer <token>` and `Accept: application/vnd.github+json`, and the body
   `{"ref":"main"}`.

The **Run workflow** button does the same by hand: `auto` makes whichever edition is due; choosing `morning` or
`evening` makes that one now.

Each episode is mastered to podcast loudness (−16 LUFS), with the two hosts matched in level, a soft chime
between segments, and chapter markers in the MP3, so podcast apps (and the car's skip buttons) can jump between
stories.

## The play page

`quiz.html` used to be a worksheet: long questions, a recap quiz and a feedback form. It is now a
**three-minute game** for each episode, built from templates:

1. **Bets.** One true-or-false statement per story, with how sure you are (55 / 70 / 85 / 95%). The points
   follow a proper scoring rule: being 95% sure and wrong costs far more than it can win, so the best strategy
   is to say what you really believe. The page keeps your **calibration** (when you say 85%, how often are you
   right?). Bets can be placed before listening; they are kept and settled afterwards. The hosts put the first
   bet to you at the start of the episode.
2. **Cards**, two or three per story, from ten formats:

   | Format | What you do |
   |---|---|
   | `estimate` Gut number | drag a slider to guess a number from the story; scored by how close |
   | `flaw` Spot the flaw | four statements, one wrong: find it |
   | `sequence` Chain of events | put cause and effect in order |
   | `match` Pair them up | match terms to what they do |
   | `duel` Which is bigger? | pick the bigger of two quantities |
   | `clues` Name it | name the concept from as few clues as possible |
   | `call` Your call | a decision in a factory, lab or audit |
   | `quickfire` Quickfire | true or false against an 8-second clock |
   | `lab` Hands on | one of the 17 pictures with sliders below, with a mission |
   | bonus round | the four-question recap quiz, optional |

3. **Settling each story:** the bet's answer, the one line worth keeping, and one tap for the show
   (more like it / fine / less of this).
4. **The end:** points, streak, words collected, and feedback in ten seconds: stars, depth, pace, voices,
   one-tap comments written for that episode, and a text box.

Cards you got wrong come back two days later as a **rematch**. The page also shows whether today's editions
are out, and **"You said → we did"**: what your last feedback changed.

### Who builds it

| Step | Done by |
|---|---|
| The templates: card formats, pictures, themes, scoring, the page | written and tested once (`ROUNDS`, `TEMPLATES`, `THEMES` in `generate_briefing.py`; one renderer each in `quiz.html`) |
| Choosing the session theme (Audit day, The evidence exchange, On the bench, Case file), the picture, and which card formats suit each story | the **decision model** (Perplexity Decider, free): one choice and a probability per format, then ranked here with a penalty for formats used recently |
| Writing the bet and the cards for each story | the **writing model**, to hard word limits ("a pub-quiz host, not an examiner") |
| Checking every card: limits, answers, slider ranges, reachable missions | `check_round` in `generate_briefing.py` |
| Repairing cards that fail the checks | a **coding model** (Particle-1, North Mini Code, Flint: free), given the format's schema and the exact error |
| Checking the facts | a stricter model (Apodex 1.1 Mini, free; Gemini Flash when it answers) |

No model writes HTML or JavaScript, so a bad reply can lose a card but cannot break the page. When no decision
model answers, the formats rotate by day.

### One-tap feedback

Without setup, **Send** opens GitHub with the feedback filled in and you press "Create". To send from the page
itself (and automatically when you close a game): open **Settings** at the bottom of the page and paste a
fine-grained GitHub token for this repository only, with the permission **Issues: Read and write**. It is
stored only in that browser.

### The hands-on pictures

Each `lab` card is built around one **template**: a picture with sliders whose maths is written and tested
once, in `generate_briefing.py` (`TEMPLATES`, 17 so far) and drawn by `quiz.html`:

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
| `model_memory` | a memory bar that overflows when the model doesn't fit | running models locally or on phones, quantization |
| `tries` | 12 runs retrying until the first success | agents retrying (and the bill), random tests finding rare bugs |
| `risk` | two crowds, without and with treatment, the spared marked | "cuts the risk by X%", absolute risk, number needed to treat |
| `buoyancy` | a block floating with most of it below the waterline | icebergs, ships, mountains floating on the mantle |
| `compression` | a balloon shrinking next to a water drop that barely does | deep-sea animals, divers' and whales' lungs, hulls |
| `vectors` | two arrows, the projection and what is left | embeddings, search by meaning, editing model weights |

The last six were added after reviewing the episodes of 27 October to 3 November 2026: of that week's 33
segments, about 10 fitted the first eleven templates and about 21 fit the seventeen. Most of the rest have no
quantity to play with (policy, lawsuits, a firmware hack) and get the other card formats instead.

For a `lab`, the model fills in only the story: realistic values, plain labels and a mission ("Find the
learning rate that settles in 8 steps"). It never writes code or formulas for a template. Every value is held to
the template's limits; a mission the sliders cannot reach, or one already solved at the start, sends the card to
the coding model for repair.

## Files you edit

- `due.py`: when each edition starts and should be ready (or set `MORNING_START` / `EVENING_START` below).
- `subreddits.txt`: which subreddits, with weights and subject (`ai` or `science`).
- `interests.txt`: what you personally want more of. Matching threads rank higher.
- `feeds.txt`: trusted outlets used to confirm stories (Hugging Face Daily Papers and Hacker News are built in).

## Cost: free by default

| Job | Used, in order |
|---|---|
| Writing | Gemini API free key, then OpenRouter free models (Nemotron 3 Ultra, Nemotron 3.5 Super VL, ERNIE 5.5 Flash Preview, Gemma 4 31B), then GitHub Models. Once a model works, it keeps writing the rest of the episode. |
| Scoring threads, choosing game templates | Perplexity Decider Flash V1.3 and Decider V1.3 (free, through OpenRouter's Decisions API), then Nemotron 3.5 Lightning and Super VL answering the same questions as JSON. Eight at a time for up to 4 minutes (`SCORING_BUDGET_S`); threads not scored by then are scored in one batched call. |
| Checking a claim found only on Reddit | Parallel Decisions Fast (free, searches the web). The pages it cites are fetched and read; only a page that could be read counts as a source. |
| Game cards | Written by the writing chain, repaired by free coding models (Particle-1, North Mini Code, Flint), fact-checked by `CHECK_MODELS` (Gemini Flash, then Apodex 1.1 Mini). |
| Topics, quiz, dossier notes | Gemini Flash-Lite free key, then OpenRouter free models (Nemotron 3.5 Super VL, ERNIE 5.5 Flash Preview, Gemma 4). |
| Voices | Fish Audio S2.1 Pro Free (OpenRouter, three requests in flight), then Gemini TTS (free key), then Edge. |

A model that times out or fails twice is skipped for the rest of the run, and then **rests for 6 hours** (doubling
each time, at most 48): it goes to the back of every chain until then. Before this, Gemini 3.8 Flash and 3.7
Flash timed out in every run from 6 to 12 November and each run spent about four minutes finding that out again.

The free model list was read from each model's OpenRouter page on 13 November 2026. Not used, on purpose:
Inkling 2 Small (its free endpoint is for agent harnesses only), LiquidAI LFM (may train on prompts; LFM2.5 left
on 13 Nov), Dots3-Note Preview (leaves 31 Dec), HeyGen Voice Turbo (leaves 30 Nov). The Decisions API is in
alpha: if its replies change shape, the answers are ignored and the Nemotron models take over.

NVIDIA's free endpoints log prompts. Public Reddit threads are fine, but never send company or confidential
documents to a free model.

OpenRouter's free models (text and Fish) share one daily request limit that resets at 00:00 UTC.
The script counts its own requests and keeps a reserve for writing.

## Settings (Settings → Secrets and variables → Actions)

Secrets: `GEMINI_API_KEY`, `OPENROUTER_API_KEY`.

Optional variables:

| Variable | Default | Meaning |
|---|---|---|
| `MORNING_START`, `EVENING_START` | `06:00`, `15:00` | When each edition may start (India time). Ready about 40 minutes later |
| `WAIT_AHEAD_MIN` | `150` | A check that arrives this many minutes before a start time waits for it. `0` = never wait |
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
| `DECISION_MODELS` | Perplexity Decider Flash V1.3, Decider V1.3, Nemotron 3.5 Lightning, Nemotron 3.5 Super VL (all free) | Models that score each thread and choose the game's templates. A paid one that charges money is dropped while the cap is 0 |
| `CODER_MODELS` | `openrouter:free,…` | Models that repair game cards (the free coding models first) |
| `WEB_CHECK_MODEL` | `parallel/decisions-fast:free` | Web check for claims found only on Reddit. `off` disables it |
| `SCORING` | `auto` | `batch` or `decisions` |
| `FISH_VOICE_A`, `FISH_VOICE_B` | set | Fish Audio voice ids for Sarah and Laura |
