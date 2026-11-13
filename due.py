#!/usr/bin/env python3
"""
Weights & Measures - which edition is due right now?

GitHub starts scheduled workflows late: 30-60 minutes in October 2026, then 6-10 hours from 6 Nov 2026. The
old rule ("before noon = morning, after = evening") turned every late morning run into a second evening
edition, so there was no morning show for days. This file replaces the clock rule with a schedule:

    edition   start making it   ready by   stop making it
    morning   06:00 IST         09:00      13:00
    evening   15:00 IST         18:00      23:30

An edition is DUE when it has not been published today and the time is between "start" and "stop". A run that
arrives up to WAIT_AHEAD_MIN before "start" waits for it (reading Reddit in the meantime) instead of giving up,
because the next run GitHub starts may be hours away. Any run, at any time, started by anything (GitHub's
schedule, the hourly Reddit collector, the Run workflow button, an outside pinger) then does the right thing.

Standard library only: the workflows call it before Python packages are installed.

    python due.py                         -> prints edition=..., wait=..., why=...
    python due.py --github-output         -> also writes them to $GITHUB_OUTPUT
    python due.py --edition morning       -> a manual run: that edition, now (unless it exists; --force overrides)
    python due.py --wait --pool reddit/pool.json
                                          -> waits until the edition's start time, topping up the Reddit pool
"""
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
TZ = ZoneInfo("Asia/Kolkata")
EPISODES_FILE = ROOT / "episodes.json"


def _minutes(name, default):
    """A time of day from a variable such as MORNING_START=06:00, in minutes after midnight (India time)."""
    raw = (os.getenv(name) or default).strip()
    try:
        h, _, m = raw.partition(":")
        v = int(h) * 60 + int(m or 0)
        return v if 0 <= v < 24 * 60 else _minutes("", default)
    except ValueError:
        return _minutes("", default)


# name -> (start, stop, ready by), minutes after midnight in India
WINDOWS = {
    "morning": (_minutes("MORNING_START", "06:00"), _minutes("MORNING_STOP", "13:00"), _minutes("MORNING_READY", "09:00")),
    "evening": (_minutes("EVENING_START", "15:00"), _minutes("EVENING_STOP", "23:30"), _minutes("EVENING_READY", "18:00")),
}
try:
    WAIT_AHEAD_MIN = max(0, min(240, int(os.getenv("WAIT_AHEAD_MIN") or 150)))
except ValueError:
    WAIT_AHEAD_MIN = 150


def hhmm(minutes):
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def ready_by(edition):
    """'9 am' / '6 pm': how the hosts say when the next episode is ready."""
    m = WINDOWS[edition][2]
    h, mm = divmod(m, 60)
    return f"{(h - 1) % 12 + 1}{':%02d' % mm if mm else ''} {'am' if h < 12 else 'pm'}"


def made_today(now, episodes_file=EPISODES_FILE):
    """Editions already published today (India date)."""
    try:
        eps = json.loads(Path(episodes_file).read_text(encoding="utf-8")).get("episodes", [])
    except (OSError, ValueError):
        return set()
    today = now.strftime("%Y-%m-%d")
    return {e.get("edition") for e in eps if e.get("date") == today}


def plan(now, made, wait_ahead=None):
    """(edition, seconds to wait, reason). edition is "none" when nothing is due or about to be."""
    wait_ahead = WAIT_AHEAD_MIN if wait_ahead is None else wait_ahead
    m = now.hour * 60 + now.minute
    for name, (start, stop, ready) in WINDOWS.items():
        if name in made:
            continue
        if start <= m < stop:
            late = f" - late: it should have been ready by {hhmm(ready)}" if m > ready - 25 else ""
            return name, 0, f"{name} edition is due (it is {hhmm(m)} in India, window {hhmm(start)}-{hhmm(stop)}){late}"
        if 0 < start - m <= wait_ahead:
            return name, (start - m) * 60 - now.second, (f"{name} edition starts at {hhmm(start)}; it is {hhmm(m)}, so this "
                                                         f"run waits {start - m} min instead of trusting a later one")
    done = ", ".join(sorted(x for x in made if x)) or "nothing yet"
    return "none", 0, f"nothing is due at {hhmm(m)} India time (made today: {done})"


def decide(edition="auto", force=False, now=None, episodes_file=EPISODES_FILE):
    now = now or dt.datetime.now(TZ)
    made = made_today(now, episodes_file)
    if edition in WINDOWS:   # asked for by hand
        if edition in made and not force:
            return "none", 0, f"the {edition} edition already exists today (tick 'force' to re-make it)"
        return edition, 0, f"{edition} edition requested by hand"
    if force:   # "auto" + force: re-make whichever edition the clock says
        name = "evening" if now.hour * 60 + now.minute >= WINDOWS["evening"][0] else "morning"
        return name, 0, f"{name} edition forced"
    return plan(now, made)


def other_runs_active():
    """True when an older run of the briefing workflow is already waiting or building, so this one can stand
    down. Only OLDER runs count: when two start together, the older goes on and the younger stops."""
    me = os.getenv("GITHUB_RUN_ID", "")
    if not me or not (os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")):
        return False
    try:
        res = subprocess.run(["gh", "run", "list", "--workflow", "daily_briefing.yml", "--limit", "30",
                              "--json", "databaseId,status"], capture_output=True, text=True, timeout=60)
        runs = json.loads(res.stdout or "[]") if res.returncode == 0 else []
    except (OSError, ValueError, subprocess.SubprocessError):
        return False
    busy = {"in_progress", "queued", "waiting", "pending", "requested"}
    return any(r.get("status") in busy and str(r.get("databaseId")) != me and int(r.get("databaseId") or 0) < int(me)
               for r in runs)


def pool_age_hours(pool_file):
    try:
        iso = json.loads(Path(pool_file).read_text(encoding="utf-8")).get("updated_at", "")
        when = dt.datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        return (dt.datetime.now(dt.timezone.utc) - when).total_seconds() / 3600
    except (OSError, ValueError, TypeError):
        return 1e9


def wait_and_top_up(seconds, pool_file=None):
    """Wait for the edition's start time. The Reddit collector normally runs every hour, but GitHub has been
    starting it only three or four times a day, so the wait is used to read Reddit (one request a minute).
    When there is no wait and the pool is stale, a short top-up (at most 8 minutes) still runs."""
    end = time.time() + max(0, seconds)
    requests = 0
    if pool_file:
        age = pool_age_hours(pool_file)
        if seconds >= 120:
            requests = min(60, int(seconds // 62))
        elif age > 2:
            requests = 8
        print(f"Reddit pool last updated {age:.1f} h ago; top-up: {requests} request(s)", flush=True)
    if requests:
        budget = max(60, int(seconds) - 20) if seconds >= 120 else 8 * 62
        try:
            subprocess.run([sys.executable, str(ROOT / "collector" / "reddit_collector.py"), "--pool", str(pool_file),
                            "--repo", os.getenv("GITHUB_REPOSITORY", "npannu7/daily-briefing"),
                            "--requests", str(requests), "--pause", "60", "--budget", str(budget)],
                           timeout=budget + 300, check=False)
        except (OSError, subprocess.SubprocessError) as e:
            print(f"Reddit top-up skipped: {e}", flush=True)
    left = end - time.time()
    if left > 0:
        print(f"Waiting {left / 60:.0f} more min for the edition's start time", flush=True)
        time.sleep(left)


def arg(name, default=""):
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith("--"):
            return sys.argv[i + 1]
        return "true"
    return default


def main():
    edition = (arg("--edition", os.getenv("EDITION", "auto")) or "auto").strip().lower()
    force = (arg("--force", os.getenv("FORCE", "false")) or "false").strip().lower() in ("1", "true", "yes")
    name, wait, why = decide(edition, force)
    if "--wait" in sys.argv:   # inside the build job: wait for the start time, then say what to make
        if name != "none":
            wait_and_top_up(wait, arg("--pool") or None)
            name, wait, why = decide(edition, force)
            if wait > 90:   # should not happen; never loop
                name, why = "none", "still not due after waiting"
            wait = 0
    elif name != "none" and edition not in WINDOWS and "--no-busy-check" not in sys.argv and other_runs_active():
        name, wait, why = "none", 0, "an earlier run is already waiting for or making this edition"
    lines = [f"edition={name}", f"wait={int(wait)}", f"why={why}"]
    print("\n".join(lines), flush=True)
    out = os.getenv("GITHUB_OUTPUT")
    if "--github-output" in sys.argv and out:
        with open(out, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
