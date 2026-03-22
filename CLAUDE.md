# DUPR Feed

A DUPR (pickleball rating platform) activity timeline feed. Think Instagram/Venmo but for pickleball matches. Log in with your DUPR credentials and see a chronological feed of recent matches from players you follow.

## How to run

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python server.py
# Open http://localhost:5001
```

DUPR credentials are entered via the login UI — never stored on disk.

## Architecture

Single-file Flask backend (`server.py`) + single-page frontend (`templates/index.html`).

### Backend routes

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Serve main page (redirects to `/login` if not auth'd) |
| `/login` | GET | Serve login page |
| `/api/login` | POST | Authenticate with DUPR API, store JWT in session |
| `/api/feed` | GET | Fetch followed/watched players' recent matches, merge and sort by date |
| `/api/search` | POST | Search DUPR players by name |
| `/api/watch` | POST | Add or remove a player from the local watch list |
| `/api/watches` | GET | Return the current watch list |
| `/api/logout` | POST | Clear session |
| `/health` | GET | Health check |

### Data flow

1. User logs in — DUPR returns a Bearer JWT token stored in Flask session.
2. Feed request triggers parallel fetches (via `ThreadPoolExecutor`) of the last 10 matches per followed/watched player.
3. Results are merged, sorted newest-first, and cached for 5 minutes.
4. Frontend renders match cards with scores, win/loss badges, rating deltas, and relative timestamps.

### Watch list (`watches.json`)

Persistent local JSON file storing player objects: `{id, name, rating, doublesRating, singlesRating, imageUrl}`.

- Added/removed via `/api/watch` POST endpoint
- Auto-seeded on first login if file doesn't exist — seeds 13 default pro players (see `DEFAULT_PLAYER_NAMES` in `server.py`)
- Ratings extracted via `_extract_ratings()` which handles both top-level fields and nested `ratings.doubles`/`ratings.singles` from DUPR API, and filters out `"NR"` strings

Default players seeded on first login:
- Ben Johns, Andrei Daescu, Hayden Patriquin, JW Johnson, Gabriel Tardio
- Christian Alshon, Federico Staksrud, Anna Leigh Waters, Anna Bright
- Hurricane Tyra Black, Jorja Johnson, Hunter Johnson, Christopher Haworth

## DUPR API details

- **Base URL:** `https://api.dupr.gg`
- **Auth:** `POST /auth/v1.0/login/` with `{"email": "...", "password": "..."}` returns a Bearer token.
- **Profile:** `GET /user/v1.0/profile/`
- **Match history:** `POST /player/v1.0/{playerId}/history`
- **Player search:** `POST /player/v1.0/search` with `{"filter": {}, "query": "...", "limit": 10}`
  - Returns hits with ratings nested under `p.ratings.doubles` / `p.ratings.singles` (not top-level)
- **Following list:** Tries in order:
  1. `GET /social/v1.0/following/`
  2. `GET /user/v1.0/following/`
  3. `GET /user/v1.0/profile/following`

All API calls include `Authorization: Bearer {token}`. 401 → redirect to login.

## Design system

Glassmorphism dark theme matching DUPR.com brand.

### CSS variables (`:root` in `index.html`)

```css
--bg: #05155E              /* dark navy */
--bg-dark: #0A1628
--accent: #0163D0          /* primary blue */
--blue: #4B97FE            /* bright blue */
--green: #00C853           /* win */
--red: #FF3D3D             /* loss */
--radius: 16px
--glass-bg: rgba(10,22,40,0.65)
--glass-blur: blur(20px)
--glass-border: rgba(255,255,255,0.12)
--glass-highlight: inset 0 1px 0 rgba(255,255,255,0.08)
--glow-blue: 0 0 30px rgba(75,151,254,0.2)
--glow-green: 0 0 20px rgba(0,200,83,0.25)
--glow-red: 0 0 20px rgba(255,61,61,0.25)
--gradient-brand: linear-gradient(135deg, #05155E, #0163D0, #4B97FE)
--gradient-btn: linear-gradient(135deg, #0163D0, #4B97FE)
--gradient-avatar: linear-gradient(135deg, #0163D0, #4B97FE)
```

### Fonts (Google Fonts)

- **Bebas Neue** — logo/display (matches DUPR.com)
- **Barlow Condensed** — sidebar section headers
- **Montserrat** — headings
- **Inter** — body/UI

### Key UI components

- **Topnav** — frosted glass (`backdrop-filter: blur(20px)`)
- **Sidebar** — frosted glass, player list with avatars, search panel
- **Match cards** — glass cards with win (green) / loss (red) left border + glow on hover
- **Filter pills** — gradient when active
- **Profile overlay** — full-screen player profile with stats, match history, follow button
- **H2H view** — head-to-head comparison between any two followed players
- **Tournament modal** — standings, matches, upsets tabs
- **Mini popup** — quick stats hover popup on player name links

Mobile-responsive: sidebar collapses on screens < 768px.

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `SECRET_KEY` | Yes | Flask session secret. Long random string in production. |
| `PORT` | No | Port to listen on. Defaults to `5001`. |

## Deployment

### Render.com (live)

- GitHub repo: `https://github.com/Laith64/dupr-feed.git`
- Auto-deploys on push to `main`
- Config in `render.yaml`

```bash
git push origin main  # triggers deploy
```

### Local

```bash
python server.py  # http://localhost:5001
```

### Docker

```bash
docker build -t dupr-feed .
docker run -p 5001:5001 -e SECRET_KEY=$(openssl rand -hex 32) dupr-feed
```

## Session context (for continuity across machines)

This app was built iteratively across multiple Claude Code sessions. Key decisions:

- **watches.json is committed to git** so your following list persists across machines/deploys
- The frontend is entirely in `templates/index.html` — all CSS and JS are inline (no build step)
- The backend is entirely in `server.py` — no separate modules
- DUPR API endpoints were discovered experimentally; some are undocumented
- Feed cache TTL is 5 minutes (`CACHE_TTL = 300` in `server.py`)
- Player profile overlay, H2H view, tournament modal, and mini popup are all implemented client-side in `index.html`
