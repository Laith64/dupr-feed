# DUPR Feed

A DUPR (pickleball rating platform) activity timeline feed. Think Instagram/Venmo but for pickleball matches.

---

## Rules Claude must follow (read this first)

**Verify before shipping.** After any backend change: kill the old server, restart it, hit `/health`, confirm it's up with new code, THEN say "try it." Never claim a fix works on a server that was started before the change.

**Read existing code before writing new code.** When touching a DUPR API endpoint, grep `server.py` for existing usage of that endpoint first. The pattern is already there — reuse it. Guessing field names causes wasted iterations.

**Self-verify with logs.** When adding a new feature that calls an API or transforms data, add `app.logger.info(...)` to log the raw response shape. Read those logs yourself (via curl or local server) and confirm the data looks right before removing the log and shipping.

**Ask before assuming.** If unclear about the expected behavior, format, or edge case, ask Laith one focused question before proceeding.

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

Glassmorphism dark theme. CSS variables are in `:root` in `index.html` — key colors: `--bg: #05155E`, `--accent: #0163D0`, `--blue: #4B97FE`, `--green: #00C853`, `--red: #FF3D3D`. Fonts: Bebas Neue (logo), Montserrat (headings), Inter (body).

Key UI components: topnav, sidebar (collapses < 768px), match cards (green/red left border for win/loss), profile overlay, H2H/compare tab, connect tab, globe view, tournament modal.

---

## Deployment

- **Live:** Render.com, auto-deploys on push to `main` — `git push origin main`
- **GitHub:** `https://github.com/Laith64/dupr-feed.git`
- **Env vars:** `SECRET_KEY` (required), `PORT` (default 5001)

---

## Cache

Simple in-memory `_cache` dict: `key -> (timestamp, data)`. Feed TTL = 5 min. Search results cached 60s per query. Globe region data cached separately.
