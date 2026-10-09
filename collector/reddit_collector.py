#!/usr/bin/env python3
"""
Weights & Measures - Reddit collector.

Reads the day's top posts (and the top comments of the best ones) from the subreddits
listed in subreddits.txt, using Reddit's public RSS feeds, and uploads the result to
the GitHub repository as reddit/latest.json. The podcast generator reads that file.

Why this runs on the office PC: Reddit blocks most requests that come from cloud
servers (like GitHub Actions), but normal office/home connections still work.

Standard library only - no pip installs needed.

Usage
  python reddit_collector.py            collect + upload (uses collector_config.json)
  python reddit_collector.py --test     collect 3 subreddits, print a summary, no upload
"""
import base64
import datetime as dt
import html
import json
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG_FILE = HERE / "collector_config.json"
LOG_FILE = HERE / "collector.log"
LOCAL_COPY = HERE / "latest.json"
DEFAULT_REPO = "npannu7/daily-briefing"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) WeightsAndMeasuresCollector/1.0 (personal podcast)"
ATOM = {"a": "http://www.w3.org/2005/Atom"}

# Reddit's public feeds allow very few requests (often about one per minute), so subreddits are
# fetched together as combined feeds (r/A+B+C), one per weight tier, and comments are read only
# for the most promising threads.
GROUP_SIZE = 15            # subreddits per combined feed
TIER_LIMIT = {3: 30, 2: 25, 1: 15}   # posts per combined feed, by weight
COMMENT_LIMIT = 20         # threads whose comments are read (best first)
MAX_COMMENTS = 12
PAUSE = 2.0                # extra pause between requests; Reddit's own rate headers are obeyed too


class Blocked(Exception):
    pass


def log(msg):
    line = f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def http_get(url, tries=4):
    for attempt in range(1, tries + 1):
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/atom+xml,*/*"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                remaining = r.headers.get("x-ratelimit-remaining")
                reset = r.headers.get("x-ratelimit-reset")
                body = r.read()
                if remaining is not None and reset is not None:
                    try:
                        if float(remaining) < 1:
                            wait = min(float(reset) + 1, 120)
                            log(f"  Reddit rate limit: waiting {wait:.0f}s")
                            time.sleep(wait)
                    except ValueError:
                        pass
                return body
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = e.headers.get("x-ratelimit-reset") or e.headers.get("retry-after") or 60
                try:
                    wait = min(float(wait) + 2, 180)
                except ValueError:
                    wait = 60
                log(f"  rate limited, waiting {wait:.0f}s")
                time.sleep(wait)
                continue
            if e.code in (403, 401):
                raise Blocked(f"HTTP {e.code} from Reddit")
            if e.code in (404, 410):
                return None
            time.sleep(10 * attempt)
        except (urllib.error.URLError, TimeoutError) as e:
            log(f"  network error: {e}")
            time.sleep(10 * attempt)
    return None


def strip_html(s, limit=None):
    s = re.sub(r"(?is)<(script|style).*?</\1>", " ", s or "")
    s = html.unescape(re.sub(r"<[^>]+>", " ", s))
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"submitted by\s+/u/\S+", "", s).replace("[link]", "").replace("[comments]", "").strip()
    return s[:limit] if limit else s


def parse_entries(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    out = []
    for e in root.findall("a:entry", ATOM):
        content = e.findtext("a:content", default="", namespaces=ATOM)
        link_el = e.find("a:link", ATOM)
        cat = e.find("a:category", ATOM)
        out.append({
            "sub": cat.get("term", "") if cat is not None else "",
            "id": e.findtext("a:id", default="", namespaces=ATOM),
            "title": html.unescape(e.findtext("a:title", default="", namespaces=ATOM)),
            "url": link_el.get("href") if link_el is not None else "",
            "date": e.findtext("a:published", default="", namespaces=ATOM)
                    or e.findtext("a:updated", default="", namespaces=ATOM),
            "content": content,
        })
    return out


def external_link(content_html):
    m = re.search(r'<a href="([^"]+)">\[link\]</a>', content_html or "")
    if not m:
        return ""
    link = html.unescape(m.group(1))
    return "" if "reddit.com/r/" in link or "redd.it" in link else link


def read_subreddits(repo):
    url = f"https://raw.githubusercontent.com/{repo}/main/subreddits.txt"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=30) as r:
            text = r.read().decode("utf-8")
    except Exception as e:
        log(f"could not read subreddits.txt from GitHub ({e}); using local copy")
        text = (HERE.parent / "subreddits.txt").read_text(encoding="utf-8")
    subs = []
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        name, _, w = line.partition("|")
        name = name.strip().removeprefix("r/")
        try:
            weight = float(w.strip() or 1)
        except ValueError:
            weight = 1.0
        if name and weight > 0:
            subs.append((name, weight))
    return subs


def collect(subs, pause=PAUSE, log=log, budget_s=None, comment_limit=COMMENT_LIMIT):
    t0, posts, errors = time.time(), [], 0
    weight_of = {name.lower(): w for name, w in subs}

    def out_of_time():
        return budget_s is not None and time.time() - t0 > budget_s

    tiers = {}
    for name, w in subs:
        tiers.setdefault(w, []).append(name)
    for weight in sorted(tiers, reverse=True):
        names = tiers[weight]
        for i in range(0, len(names), GROUP_SIZE):
            if posts and out_of_time():
                break
            group = names[i:i + GROUP_SIZE]
            limit = TIER_LIMIT.get(int(weight), 15)
            body = http_get(f"https://www.reddit.com/r/{'+'.join(group)}/top/.rss?t=day&limit={limit}")
            time.sleep(pause)
            if body is None:
                errors += 1
                log(f"  weight {weight:g} group ({len(group)} subreddits): no data")
                continue
            entries = [e for e in parse_entries(body) if "/comments/" in e["url"]]
            log(f"  weight {weight:g} group ({len(group)} subreddits): {len(entries)} posts")
            for rank, e in enumerate(entries):
                sub = e["sub"] or group[0]
                posts.append({
                    "sub": sub, "weight": weight_of.get(sub.lower(), weight), "rank": rank,
                    "id": e["id"].replace("t3_", ""), "title": e["title"], "url": e["url"], "date": e["date"],
                    "link": external_link(e["content"]), "text": strip_html(e["content"], 2500), "comments": [],
                })
    best = sorted(posts, key=lambda p: -p["weight"] / (1 + p["rank"]))[:comment_limit]
    for n, p in enumerate(best, 1):
        if out_of_time():
            log(f"  time budget reached after {n - 1} comment threads")
            break
        cbody = http_get(f"https://www.reddit.com/r/{p['sub']}/comments/{p['id']}/.rss?sort=top&limit={MAX_COMMENTS + 1}")
        time.sleep(pause)
        if not cbody:
            continue
        for c in parse_entries(cbody):
            if c["id"].startswith("t3_"):
                continue  # the post itself
            text = strip_html(c["content"], 700)
            if text and text.lower() not in ("[deleted]", "[removed]"):
                p["comments"].append(text)
        p["comments"] = p["comments"][:MAX_COMMENTS]
    log(f"  comments read for {sum(1 for p in posts if p['comments'])} threads")
    return {"collected_at": dt.datetime.now(dt.timezone.utc).isoformat(), "source": "reddit-rss",
            "subreddits": len(subs), "errors": errors, "posts": posts}


def upload(snapshot, repo, token):
    api = f"https://api.github.com/repos/{repo}/contents/reddit/latest.json"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
               "User-Agent": UA, "X-GitHub-Api-Version": "2022-11-28"}
    sha = None
    try:
        with urllib.request.urlopen(urllib.request.Request(api, headers=headers), timeout=30) as r:
            sha = json.loads(r.read())["sha"]
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    payload = {"message": f"Reddit snapshot {snapshot['collected_at'][:16]}Z",
               "content": base64.b64encode(json.dumps(snapshot, ensure_ascii=False).encode("utf-8")).decode()}
    if sha:
        payload["sha"] = sha
    req = urllib.request.Request(api, data=json.dumps(payload).encode(), method="PUT",
                                 headers={**headers, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status


def main():
    test = "--test" in sys.argv
    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8-sig")) if CONFIG_FILE.exists() else {}
    repo = cfg.get("repo", DEFAULT_REPO)
    subs = read_subreddits(repo)
    if test:
        subs = subs[:3]
    log(f"Collecting {len(subs)} subreddits{' (test)' if test else ''}")
    try:
        snap = collect(subs, comment_limit=2 if test else COMMENT_LIMIT)
    except Blocked as e:
        log(f"STOPPED: Reddit refused this connection ({e}).")
        sys.exit(2)
    n_c = sum(len(p["comments"]) for p in snap["posts"])
    log(f"Collected {len(snap['posts'])} posts and {n_c} comments ({snap['errors']} subreddits failed)")
    LOCAL_COPY.write_text(json.dumps(snap, indent=1, ensure_ascii=False), encoding="utf-8")
    if test:
        for p in snap["posts"][:5]:
            log(f"  [{p['sub']}] {p['title'][:90]}  ({len(p['comments'])} comments)")
        log("Test finished - nothing uploaded.")
        return
    token = cfg.get("github_token", "").strip()
    if not token:
        log("No github_token in collector_config.json - run setup_collector.bat first.")
        sys.exit(3)
    status = upload(snap, repo, token)
    log(f"Uploaded to {repo}/reddit/latest.json (HTTP {status})")


if __name__ == "__main__":
    main()
