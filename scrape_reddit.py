"""
Scrape all posts + comments from a Reddit user and analyze them.
Usage: python scrape_reddit.py
Set REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET in environment or edit below.
"""

import os, json, sys
from datetime import datetime, timezone
from collections import Counter
import praw

# ── Credentials ──────────────────────────────────────────────────────────────
CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "YOUR_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
USER_AGENT    = "dupr-scraper/1.0 by u/your_reddit_username"
TARGET_USER   = "DUPR-data-scientist"

# ── Scrape ───────────────────────────────────────────────────────────────────
def scrape(reddit):
    redditor = reddit.redditor(TARGET_USER)
    results = {"comments": [], "submissions": []}

    print(f"Fetching comments for u/{TARGET_USER}...")
    for c in redditor.comments.new(limit=None):
        results["comments"].append({
            "id": c.id,
            "subreddit": c.subreddit.display_name,
            "body": c.body,
            "score": c.score,
            "created_utc": datetime.fromtimestamp(c.created_utc, tz=timezone.utc).isoformat(),
            "permalink": f"https://reddit.com{c.permalink}",
            "post_title": c.link_title,
        })

    print(f"Fetching posts for u/{TARGET_USER}...")
    for s in redditor.submissions.new(limit=None):
        results["submissions"].append({
            "id": s.id,
            "subreddit": s.subreddit.display_name,
            "title": s.title,
            "selftext": s.selftext,
            "score": s.score,
            "url": s.url,
            "created_utc": datetime.fromtimestamp(s.created_utc, tz=timezone.utc).isoformat(),
            "permalink": f"https://reddit.com{s.permalink}",
            "num_comments": s.num_comments,
        })

    return results

# ── Analyze ──────────────────────────────────────────────────────────────────
def analyze(data):
    comments     = data["comments"]
    submissions  = data["submissions"]
    all_text     = [c["body"] for c in comments] + [s["selftext"] for s in submissions]

    print("\n" + "═"*60)
    print(f"  REDDIT ANALYSIS: u/{TARGET_USER}")
    print("═"*60)

    # Volume
    print(f"\n{'VOLUME':─<40}")
    print(f"  Total comments:    {len(comments)}")
    print(f"  Total posts:       {len(submissions)}")
    print(f"  Total activity:    {len(comments) + len(submissions)}")

    # Date range
    all_dates = [c["created_utc"] for c in comments] + [s["created_utc"] for s in submissions]
    if all_dates:
        all_dates.sort()
        print(f"\n{'DATE RANGE':─<40}")
        print(f"  Earliest: {all_dates[0][:10]}")
        print(f"  Latest:   {all_dates[-1][:10]}")

    # Subreddits
    sub_counts = Counter(c["subreddit"] for c in comments)
    sub_counts.update(s["subreddit"] for s in submissions)
    print(f"\n{'TOP SUBREDDITS':─<40}")
    for sub, n in sub_counts.most_common(15):
        print(f"  r/{sub:<35} {n}")

    # Top scored comments
    top_comments = sorted(comments, key=lambda x: x["score"], reverse=True)[:10]
    print(f"\n{'TOP 10 COMMENTS BY SCORE':─<40}")
    for c in top_comments:
        snippet = c["body"][:120].replace("\n", " ")
        print(f"  [{c['score']:>4}] {snippet}")
        print(f"        {c['permalink']}")

    # Top scored posts
    top_posts = sorted(submissions, key=lambda x: x["score"], reverse=True)[:5]
    if top_posts:
        print(f"\n{'TOP 5 POSTS BY SCORE':─<40}")
        for s in top_posts:
            print(f"  [{s['score']:>4}] {s['title']}")
            print(f"        {s['permalink']}")

    # Keyword frequency (DUPR-relevant)
    keywords = [
        "rating", "algorithm", "dupr", "score", "doubles", "singles",
        "match", "data", "accuracy", "update", "glicko", "elo",
        "bug", "issue", "fix", "api", "player", "tournament",
        "confidence", "provisional", "reliable", "manipulation",
        "sandbag", "sandbagging", "inflate", "deflate",
    ]
    text_blob = " ".join(all_text).lower()
    print(f"\n{'KEYWORD FREQUENCY':─<40}")
    kw_counts = {kw: text_blob.count(kw) for kw in keywords}
    for kw, n in sorted(kw_counts.items(), key=lambda x: -x[1]):
        if n > 0:
            print(f"  {kw:<25} {n}")

    # Full comment dump (sorted by date)
    print(f"\n{'ALL COMMENTS (oldest → newest)':─<40}")
    for c in sorted(comments, key=lambda x: x["created_utc"]):
        print(f"\n  [{c['created_utc'][:10]}] r/{c['subreddit']} | score:{c['score']}")
        print(f"  Post: {c['post_title'][:80]}")
        print(f"  {c['body'][:500]}")
        if len(c["body"]) > 500:
            print(f"  ... [{len(c['body'])-500} more chars]")
        print(f"  {c['permalink']}")

    print("\n" + "═"*60)


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if CLIENT_ID == "YOUR_CLIENT_ID":
        print("Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET env vars first.")
        print("  export REDDIT_CLIENT_ID=xxxxxxxxxxxx")
        print("  export REDDIT_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxx")
        sys.exit(1)

    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT,
        read_only=True,
    )

    data = scrape(reddit)

    # Save raw data
    out_file = f"dupr_reddit_{TARGET_USER}.json"
    with open(out_file, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nRaw data saved → {out_file}")

    analyze(data)
