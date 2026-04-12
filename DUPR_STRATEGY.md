# DUPR Feed — Strategy & Research Doc

**Goal: Use this app as a portfolio piece to get a job at DUPR.**
Laith built a social activity feed for DUPR that the CEO publicly said they "should already have."
This doc contains everything needed to make smart feature decisions.

---

## The Core Pitch

The DUPR CEO (Tito) said in a Nov 2025 Reddit AMA:
> *"A feed of people you follow is 100 percent something we should already have."*
> *"We're working toward a Strava-style experience so players can track what they actually do on court, not just the score."*
> *"DUPR Replay, subscores, and a full social layer are what I'm most excited about."*

Laith built exactly this. The app is:
- Instagram/Venmo-style match result feed
- Auto-generated from real DUPR API data (no manual posting)
- Match cards with scores, opponents, partners, rating delta, win/loss
- Profile overlays, H2H compare, stories, tournament page, connect tab

---

## DUPR AMA Key Insights (Nov 20–21, 2025)

### CEO (u/dupr_ceo = Tito)
100 comments, all in one AMA thread.

**Product roadmap he revealed:**
- **DUPR 2.0** — full app redesign, cleaner/faster, "early 2026"
- **DUPR Impact** — match-level rating breakdown, replaces "Forecast"/Genie. Shows match weight, reliability, exact rating impact per match. Launched Dec 2025.
- **Social layer** — real feed, follow friends, community features coming back
- **Rating graph** — fixed axis, longer window, "already being fixed"
- **Subscores** — mixed vs gender performance breakdown
- **Achievements/medals** — "on the roadmap"
- **Verified events** — official results carry more weight, clearly marked
- **Block/control followers** — on the list
- **Self-posted matches may be removed** — "on the table"

**Business context:**
- ~50 employees, $0 charged to clubs/partners historically
- Revenue: ads (hated), sponsors, now freemium (DUPR Plus)
- Ads confirmed going away in 2.0
- 1.5M users, 170 countries, 10,000 clubs, 250 API partners, 10M+ matches
- Also owns/runs: Minor League Pickleball, College Pickleball (with scholarships), high school programs, adaptive programs

**Anti-gaming they admitted:**
- Flag device IDs + behavior patterns for duplicate accounts
- Deleting account and re-signing doesn't work — match network reconnects you instantly
- Sandbagging patterns → reliability score drops → match impact reduced
- "DUPR police" — they remove accounts and matches for fake results

**Team:**
- Sarah (u/DUPR-data-scientist) — Lead Data Scientist, PhD pure mathematics, plays pickleball
- Scott — senior quant from Citadel + Goldman Sachs, built equity algos
- Tito — CEO, English is 2nd language, very accessible, responds to everyone

### Data Scientist (u/DUPR-data-scientist = Sarah)
25 comments, same AMA thread.

**Algorithm — how it actually works:**
- **Spread-based since July 2025** — expected score vs. actual score, not win/loss
- Win and go down (or lose and go up) is NORMAL — happens ~20% of wins
- Match weight = recency × format (1 game vs. best-of-3) × uploader (director > player)
- Old matches decay but never disappear — just become statistically irrelevant
- New players: volatile (~0.04 rating change per game early on, 0.25+ jumps possible)
- Established players: move incrementally (lots of historical data = smaller updates)
- Partners move differently based on MATCH COUNT not rating — fewer/less recent matches = more volatile in both directions
- **Reliability = opponent diversity, not just match count** — 7 connected matches beats 200 in a closed loop

**Things they tested and rejected:**
1. Bonus for winning — only improves accuracy at 5.0+ in close matches
2. Downweighting "against the grain" matches — accuracy DECREASING, they kept them
3. "Losers can go up but winners can't go down" — would inflate ratings over time
4. Team rating weighted by partner reliability — accuracy decreasing
5. Level-lock feature — rejected, "mainly protects feelings"

**Known weaknesses they admitted:**
- Geography/gender/age disconnected clusters — actively working on "bridge match" extrapolation
- Partner targeting/icing out in doubles — can't see individual contributions, only shared score
- Early-stage volatility — needed for fast correction but frustrating

**Key quotes:**
> *"DUPR is a measurement tool, not a reward or penalty system."*
> *"The algo is a bit of a whack-a-mole."*
> *"Play with different partners — like a diversified investment portfolio."*
> *"This is one area where I could see AI having a really cool role — helping us understand who contributed what in a match."*
> Her DUPR rating: **4.8**

---

## Competitive Landscape

### Rating System Competitors

| System | Scale | Basis | Weakness |
|---|---|---|---|
| **DUPR** | 2.0–8.0 | Spread-based (expected vs actual score) | Self-posting gameable, sandbagging |
| **UTR-P** | 1.0–10.0 | Points-based, verified track only | Less recreational adoption, recalibrated ratings by -0.5 in Apr 2025 causing confusion |
| **UTPR** | 1.0–6.0+ | Tournament wins/losses only | Legacy, obsolete for recreational players |
| **Self-Rating** | 1.0–5.0+ | Questionnaire, subjective | Completely gameable |

DUPR is the dominant standard. UTR-P is backed by USA Pickleball but confusing to players.

### App Competitors — What They Do & What's Missing

**Pickleheads** — Best court finder (18,700+ courts), best round robin organizer, Court Chat groups. No match history, no ratings, no feed, no player profiles. Raised $2.5M. Official partner of USA Pickleball.

**PicklePlay** — 32,000+ courts (largest DB), club management, player connections. Acquired by UTR Sports late 2024. No social feed, no match stats. Official APP Tour app partner.

**Main Court Social** — Closest to a pickleball social network. Event-based posting, milestone auto-posts, groups. Small user base, no DUPR integration, no pro player tracking.

**Reclub** — Official DUPR partner. Has partner/opponent analytics, Kudos system (sportsmanship reputation), local leaderboards. Best stats view but niche, no feed.

**PickleWave** — Pro-only match tracker. Live scores across PPA/APP/MLP, player stats, leaderboards. No amateur features.

**OpenSports** — Multi-sport drop-in management tool. Best for organizers running open play sessions. No ratings, no stats, no social.

**Playtomic** — Court booking + Open Matches (join skill-matched games with strangers). Strong internationally/Europe. 7M+ downloads. New DUPR partnership (2025). Best UX for finding strangers to play with right now.

**DUPR App itself** — Has Feeds (generic social posts, not match-result-driven), match history per profile, player directory. No DMs, no reactions, no match cards in a timeline, no activity feed of friends' results.

### The Gap Nobody Has Filled
An auto-generated, chronological Venmo/Strava-style timeline of actual match results for players you follow — with scores, opponents, rating deltas, win/loss context, rendered as beautiful match cards. DUPR has a "posts" feed. Nobody has the "activity" feed. That's this app.

---

## Feature Ideas (Priority Order)

### High Priority — Directly Aligned with CEO's Vision

**1. Achievement Cards in the Feed**
Detect milestones from match history, inject as special cards alongside regular match cards:
- 🏆 First win against a 5.0+
- 🔥 N-match win streak (5, 10, 25)
- 📈 New all-time rating high
- ⚡ Biggest upset ever (beat someone X above their rating)
- 💀 Snapped someone's win streak
- 🎯 Beat a head-to-head nemesis (was 0-3+ against them)
- 🔄 Crossed rating threshold (3.5→4.0 etc.)
CEO said achievements are "on the roadmap." Ship it first.

**2. Open Court / Find Game Now**
Real-time court listings: players post location, time, open spots, rating range.
Like Craigslist + Meetup but specifically for "I want to play RIGHT NOW."
CEO said "connecting players more directly is something we want to keep exploring."
Softer version: "Available to Play" toggle on profile — visible to players in your rating range nearby.

**3. Tournament Resume on Profiles**
Every player profile shows their tournament history with placements:
"APP Nationals 2025 — 3rd Place | Bull City League S5 — Champion"
Currently DUPR shows ratings but nothing about what you've actually won. Big gap.

**4. Partner Chemistry**
On player profiles: record broken down by partner.
| Partner | W | L | Avg Margin |
The data scientist literally said "play with different partners" is the key to a well-rounded rating.
Nobody has visualized this yet.

**5. Form Strip on Player Cards**
Last 5 results as colored dots + streak badge directly on every player card:
`● ● ● ○ ●  🔥 4W`
One glance = immediate read on whether someone is hot or cold.

**6. Tournament Bracket Viewer**
Visual bracket tree for tournaments — not just standings/results.
Highlight where followed players are in the draw.
Show "underdog runs" — lower-seeded teams making deep runs.
Nobody in pickleball has built a good bracket viewer.

**7. DUPR Wrapped / Season Snapshot**
Shareable card: matches played, peak rating, biggest win, best streak, favorite partner, most played opponent, rating journey.
Designed to screenshot and post. Pure viral social engagement.

**8. Rivalry Cards**
When you've played someone 3+ times, auto-surface:
"Your Rivalry with Mike Chen | You lead 5-3 | Last played 2 weeks ago | Avg margin: +1.4"
Strava's equivalent is the segment leaderboard.

**9. Reactions/Kudos on Match Cards**
One-tap 🔥 👏 🫶 on any match card in the feed.
Simplest Strava feature, biggest for engagement.

**10. Tournament Discovery + Social Layer**
Browse upcoming DUPR-gated events by location/date/rating/format.
Show which followed players are registered.
Show expected seeding based on current rating.
Deep link to actual registration (Pickleheads, etc.).

### Medium Priority

- **Weekly digest section** — "This week in your network" — biggest movers, upsets, streaks
- **Rating graph** — fixed axis, full history, hoverable (DUPR app graph is famously bad)
- **Pickleball Passport** — map of everywhere you've played (tournaments + clubs)
- **"Playing up vs down" analyzer** — rating change when favored vs underdog (CEO explained this pattern in depth)
- **Form leaderboard** — among followed players, who's on the hottest streak right now
- **Opponent diversity indicator** — Sarah revealed this is how reliability actually works

### Already Built (Differentiation Points)
- Auto-generated match feed (CEO: "should already have")
- Stories (Strava-style moments)
- H2H compare
- Profile overlays
- Tournament page
- Connect tab

---

## How to Talk About This App to DUPR

1. "I built the social feed your CEO said you should already have — in a weekend."
2. "I read your AMA and built toward your exact Strava vision: auto-generated activity timeline, not manual posts."
3. "I pull from your public API and render match results as shareable moments — achievements, streaks, rivalries."
4. Show the feed. Show the tournament page. Show the stories. Show H2H.
5. Pitch the features above as "here's what I'd build next if I were on your team."

Contact: support@mydupr.com (escalates to Tito), or reach Tito directly via LinkedIn.
Sarah (data scientist) is reachable via Reddit (u/DUPR-data-scientist) — she's very responsive.

---

## Raw Data Files
- `dupr_reddit_raw.json` — all 25 comments from u/DUPR-data-scientist
- `dupr_ceo_raw.json` — all 100 comments from u/dupr_ceo
- AMA thread: https://reddit.com/r/Pickleball/comments/1p28vpt/
