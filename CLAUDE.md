# DUPR Feed

A DUPR (pickleball rating platform) activity timeline feed. Think Instagram/Venmo but for pickleball matches. Log in with your DUPR credentials and see a chronological feed of recent matches from players you follow.

## How to run

```bash
# 1. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy env and set a secret key
cp .env.example .env
# Edit .env and set a random SECRET_KEY

# 4. Run the app
python server.py
# Open http://localhost:5001
```

DUPR credentials are entered via the login UI at runtime -- they are never stored in .env or on disk.

## Architecture

Single-file Flask backend (`server.py`) serving a single-page frontend (`templates/index.html`).

### Backend routes

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Serve main page (redirects to `/login` if not authenticated) |
| `/login` | GET | Serve login page |
| `/api/login` | POST | Authenticate with DUPR API, store JWT in session |
| `/api/feed` | GET | Fetch followed/watched players' recent matches, merge and sort by date |
| `/api/search` | POST | Search DUPR players by name |
| `/api/watch` | POST | Add or remove a player from the local watch list |
| `/api/watches` | GET | Return the current watch list |
| `/api/logout` | POST | Clear session |
| `/health` | GET | Health check |

### Data flow

1. User logs in -- DUPR returns a Bearer JWT token stored in Flask session.
2. Feed request triggers parallel fetches (via `ThreadPoolExecutor`) of the last 10 matches per followed/watched player.
3. Results are merged, sorted newest-first, and cached for 5 minutes.
4. Frontend renders match cards with scores, win/loss badges, rating deltas, and relative timestamps.

### Watch list

If the DUPR "following" API endpoints are unavailable, or the user wants to track additional players, a local `watches.json` file stores player IDs, names, and ratings. This file is gitignored.

## DUPR API details

- **Base URL:** `https://api.dupr.gg`
- **Auth:** `POST /auth/v1.0/login/` with `{"email": "...", "password": "..."}` returns a Bearer token.
- **Profile:** `GET /user/v1.0/profile/` with `Authorization: Bearer {token}`.
- **Match history:** `POST /player/v1.0/{playerId}/history` with body specifying limit, offset, and sort order.
- **Player search:** `POST /player/v1.0/search` with query string, limit, offset.
- **Following list:** The app tries these endpoints in order:
  1. `GET /social/v1.0/following/`
  2. `GET /user/v1.0/following/`
  3. `GET /user/v1.0/profile/following`
  Falls back to the local watch list if none work.

All API calls include the Bearer token in the Authorization header. A 401 response triggers a redirect to the login page.

## Design system

Matches the official DUPR brand:

| Token | Value |
|---|---|
| Background | `#05155E` (dark navy) |
| Card background | `#0A1628` |
| Primary accent | `#0163D0` |
| Bright blue | `#4B97FE` |
| Win green | `#00C853` |
| Loss red | `#FF3D3D` |
| Primary text | `#FFFFFF` |
| Secondary text | `rgba(255,255,255,0.7)` |
| Card border | `1px solid rgba(255,255,255,0.1)` |
| Card bg overlay | `rgba(255,255,255,0.05)` |
| Border radius | `16px` |
| Heading font | Montserrat (Google Fonts) |
| Body font | Inter (Google Fonts) |

Mobile-responsive: sidebar collapses on screens narrower than 768px.

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `SECRET_KEY` | Yes | Flask session secret. Use a long random string in production. |
| `PORT` | No | Port to listen on. Defaults to `5001`. |

## Deployment

### Render.com (free tier)

1. Push the repo to GitHub.
2. Connect the repo in Render and it will auto-detect `render.yaml`.
3. The `SECRET_KEY` is auto-generated. No other config needed.

### Docker

```bash
docker build -t dupr-feed .
docker run -p 5001:5001 -e SECRET_KEY=$(openssl rand -hex 32) dupr-feed
```
