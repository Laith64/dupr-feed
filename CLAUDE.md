# DUPR Feed

A DUPR (pickleball rating platform) activity timeline feed. Think Instagram/Venmo but for pickleball matches.

**Strategic goal: Use this app as a portfolio piece to get a job at DUPR.**
See [`DUPR_STRATEGY.md`](./DUPR_STRATEGY.md) for:
- Full research from the DUPR CEO + data scientist Reddit AMA (Nov 2025) — algorithm insights, roadmap, their exact words
- Competitive landscape (Pickleheads, PicklePlay, Main Court, Reclub, PickleWave, UTR-P, etc.)
- Feature ideas ranked by priority and alignment with DUPR's stated vision
- How to pitch this app to the DUPR team

---

## Tech stack

| Layer | Technology |
|---|---|
| **Backend** | Python 3.12, Flask 3.x |
| **Frontend** | Single-page HTML with inline CSS/JS (no build step) |
| **HTTP client** | `requests` — all DUPR API calls |
| **Concurrency** | `ThreadPoolExecutor` — parallel API fetches (feed, connect search) |
| **Server** | Gunicorn (gthread worker, 2 workers × 4 threads) |
| **Hosting** | Render.com (auto-deploy on push to `main`) |
| **Container** | Dockerfile (python:3.12-slim) |
| **Fonts** | Google Fonts — Bebas Neue, Montserrat, Inter |
| **PWA** | Web app manifest (`static/manifest.json`) |
| **Dependencies** | Flask, requests, python-dotenv, gunicorn |

No database — data comes from the DUPR API at runtime. Watch lists persist as JSON files on disk. In-memory dict cache with TTL (5 min feed, 60s search).

---

## Rules Claude must follow (read this first)

**Read existing code before writing new code.** When touching a DUPR API endpoint, grep `server.py` for existing usage of that endpoint first. The pattern is already there — reuse it. Guessing field names causes wasted iterations.

**Self-verify with logs.** When adding a new feature that calls an API or transforms data, add `app.logger.info(...)` to log the raw response shape. Read those logs yourself (via curl or local server) and confirm the data looks right before removing the log and shipping.

**Ask before assuming.** If unclear about the expected behavior, format, or edge case, ask Laith one focused question before proceeding.

**Visual verify with Playwright bash workaround.** After every UI/CSS/UX/design change, take a screenshot and verify visually. Never assume UI is correct without a screenshot. This is mandatory for all frontend work.

For the **feed page** (simple URL load):
```bash
npx playwright screenshot --wait-for-timeout=3000 --viewport-size="390,844" "http://localhost:5001" /tmp/screenshot.png
```

For the **profile page** or any overlay/interactive page (requires JS navigation):
```bash
node -e "
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 390, height: 844 } });
  await page.goto('http://localhost:5001');
  await page.waitForTimeout(3000);
  await page.evaluate(() => openPlayerProfile('5374679100', 'Itziar Rios'));
  await page.waitForTimeout(4000);
  // Optional: scroll down to see more content
  // await page.evaluate(() => document.querySelector('.profile-body').scrollTop = 500);
  await page.screenshot({ path: '/tmp/profile-screenshot.png' });
  await browser.close();
})();
"
```
Then read the screenshot image to confirm the change looks correct before telling the user it's done.

---

## How to run

```bash
source venv/bin/activate
python server.py        # http://localhost:5001
# or: PORT=5002 python server.py
```

Kill a stuck port: `lsof -ti :5001 | xargs kill -9`

---

## Architecture

Single-file Flask backend (`server.py`) + single-page frontend (`templates/index.html`). All CSS and JS are inline in the HTML — no build step.

### Backend routes

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Serve main page (redirects to `/login` if not auth'd) |
| `/api/login` | POST | Authenticate with DUPR API, store JWT in session |
| `/api/feed` | GET | Fetch followed/watched players' recent matches, sorted by date |
| `/api/search` | POST | Search DUPR players by name — used by Compare tab |
| `/api/watch` | POST | Add or remove a player from the local watch list |
| `/api/watches` | GET | Return the current watch list |
| `/api/connect/search` | POST | Find nearby players to play with (connect tab) |
| `/api/h2h` | POST | Head-to-head stats between two players |
| `/api/h2h/teams` | POST | Head-to-head stats between two teams |
| `/health` | GET | Health check — use this to confirm server is running new code |

### Data flow

1. User logs in → DUPR returns Bearer JWT stored in Flask session.
2. Feed fetches last 10 matches per followed/watched player in parallel (`ThreadPoolExecutor`).
3. Results merged, sorted newest-first, cached 5 minutes.
4. Frontend renders match cards with scores, win/loss badges, rating deltas, timestamps.

### Watch list (`watches.json`)

Committed to git so it persists across machines/deploys. Stores `{id, name, rating, doublesRating, singlesRating, imageUrl}`. Auto-seeded on first login with 13 default pros.

---

## DUPR API — known gotchas (read before touching API code)

**Base URL:** `https://api.dupr.gg` — all calls include `Authorization: Bearer {token}`. 401 → clear session, redirect to login.

**Search hits have NO location fields.** `POST /player/v1.0/search` returns hits with keys: `id, fullName, firstName, lastName, ratings, distance, distanceInMiles, ...` — no city, state, country, shortAddress. To get location, fetch `GET /player/v1.0/{pid}` per player in parallel. See `_get_loc` in `api_connect_search` for the established pattern.

**Ratings are nested strings, not top-level floats.** In search hits: `ratings.doubles` and `ratings.singles` are either `"NR"` or a numeric string like `"4.91"`. Never top-level. Always use `_extract_ratings(h)` — it handles all cases including nested dicts and "NR".

**Search sorts by proximity, not name.** DUPR returns nearby users first. Pass `lat/lng/locationText` in the filter to target a city. For compare tab search, prepend watch-list members that match the query client-side so known players always surface.

**Connect search uses geo searches.** It geocodes the city via Nominatim, then fires parallel A-Z letter searches with `{lat, lng, locationText}` filter across all nearby city clusters. Up to 80 parallel requests — this is intentional.

**Following list:** Try in order: `GET /social/v1.0/following/` → `GET /user/v1.0/following/` → `GET /user/v1.0/profile/following`

**Match history:** `POST /player/v1.0/{playerId}/history`

**Player profile:** `GET /player/v1.0/{playerId}` — returns `shortAddress`, `city`, `hometown` for location.

---

## Design system

### Visual identity — the canonical reference

The feed page is the design baseline. All new pages/components must match these exact values.

**Background:** Blue gradient — `linear-gradient(160deg, #0163D0 0%, #0555B8 40%, #0A4AA5 70%, #0163D0 100%)`. Shared across feed, search, and all page backgrounds. Bottom nav uses `rgba(1,99,208,0.85)` with `backdrop-filter:blur(12px)` to blend seamlessly.

**Cards:** White (`#fff`), `border-radius:18px`, `box-shadow:0 2px 8px rgba(0,0,0,0.08)`, no border. Hover: `translateY(-2px)` + stronger shadow.

**Sidebar:** Semi-transparent blue `rgba(1,99,208,0.92)` with `backdrop-filter:blur(16px)`. White text, white-alpha badges. Blends with the page gradient.

**Typography — strict system (do not invent new sizes/weights):**

| Element | Font | Size | Weight | Color |
|---|---|---|---|---|
| Player name (card) | Inter | 17px | 400 | `#1E293B` |
| DUPR rating (card) | Inter | 14.5px | 500 | `#2563EB` |
| Partner/date text | Inter | 13-14px | 400 | `#64748B` / `#94A3B8` |
| Opponent name (vs line) | Inter | 15px | 300 | `#334155` |
| Match scores | Inter | 18px (desktop) / 16px (mobile) | 500 | `#1E293B` |
| Rating delta | Inter | 9px | 500 | `#16A34A` (pos) / `#DC2626` (neg) |
| Type badge (SINGLES etc) | Inter | 10px (desktop) / 9px (mobile) | 500 | `#2563EB` |
| W/L badge | Inter | 12px (desktop) / 10px (mobile) | 600 | `#fff` |
| Sidebar player name | Inter | 14px | 400 | `#fff` |
| Sidebar DUPR badges | Inter | 11.3px | 500 | `#fff` |
| H2H player name | Montserrat | 17px | 700 | `#1E293B` |
| H2H win count | Montserrat | 48px | 800 | `#16A34A` / `#DC2626` |

**Color palette:**

| Role | Value |
|---|---|
| Page background gradient | `#0163D0 → #0555B8 → #0A4AA5 → #0163D0` |
| Card background | `#fff` |
| Primary blue (links, ratings, badges) | `#2563EB` |
| Interactive blue (buttons, active states) | `#3B82F6` |
| Win green | `#16A34A` |
| Loss red | `#DC2626` |
| Primary text (dark) | `#1E293B` |
| Secondary text (dark) | `#334155` |
| Muted text | `#64748B` |
| Subtle text | `#94A3B8` |
| Light border | `#E2E8F0` |
| Light background | `#F1F5F9` / `#F8FAFC` |
| White-on-blue text | `#fff` / `rgba(255,255,255,0.6)` |

**Badges:**
- W/L: 28px square (24px mobile), solid green/red, white text, `border-radius:8px` (6px mobile)
- Type (SINGLES/DOUBLES/MIXED): Same height as W/L, `background:rgba(37,99,235,0.08)`, `color:#2563EB`, `border:1px solid rgba(37,99,235,0.15)`, uppercase, `border-radius:8px` (6px mobile)

**Avatars:** Gradient background, `border-radius:50%`, blue ring `box-shadow:0 0 0 2.5px #fff, 0 0 0 4.5px #2563EB`. Initials: weight 600, uppercase. Sizes: 48px (feed cards), 44px (mobile), 36px (sidebar).

**Scores:** Comma-separated (e.g., `11-3, 11-5`), `font-variant-numeric:tabular-nums`, `letter-spacing:0.01em`.

**Fonts loaded:** Bebas Neue (logo only), Montserrat (headings, H2H), Inter (everything else), DM Sans (available but not primary), Plus Jakarta Sans (available but not primary).

Key UI components: topnav, sidebar (collapses < 768px), match cards (white, no win/loss border), profile overlay, H2H/compare tab (white card design), connect tab, globe view, tournament modal.

### Design ground rules (Apple-level polish — follow these always)

**No text truncation or wrapping mid-word.** If content doesn't fit, fix the layout — don't let CSS clip names or split words across lines. Use `white-space:nowrap` on names/labels, shorten with last names or abbreviations in tight columns, or give the column more space.

**Symmetry and alignment.** Stacked elements must align consistently. If one item has a label below a value (e.g., `+14%` with a team name under it), ALL items in that column must follow the same layout — never mix inline and stacked within the same column.

**Tables: keep rows scannable.** Each cell's content should be atomic — no line-breaking within a name. For team names in table cells, keep both names on one line (`white-space:nowrap`). Use shorter labels (last names, abbreviations) in column headers when full names would cause wrapping.

**Breathing room over density.** Prefer clean spacing over cramming. If removing an element (like Avg DUPR) makes the layout cleaner, remove it. Whitespace is a feature.

**Consistent formatting patterns.** If a format is used once (e.g., `LastName1/LastName2` for team shorthand), use it everywhere in that context. Don't mix `FirstName` in one place and `LastName` in another.

**No placeholder/sentinel values in the UI.** Never show raw API sentinels like `-1` scores. Filter them out before rendering. If data is missing, show nothing — not a broken value.

**Test visual output.** After any UI change, mentally walk through how the data renders with real names (long names, short names, international names). If a 15-character last name would break the layout, the layout is wrong.

---

## Deployment

- **Live:** Render.com, auto-deploys on push to `main` — `git push origin main`
- **GitHub:** `https://github.com/Laith64/dupr-feed.git`
- **Env vars:** `SECRET_KEY` (required), `PORT` (default 5001)

---

## Cache

Simple in-memory `_cache` dict: `key -> (timestamp, data)`. Feed TTL = 5 min. Search results cached 60s per query. Globe region data cached separately.
