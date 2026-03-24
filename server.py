"""DUPR Feed — pickleball activity timeline."""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, session, url_for

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-change-me")

DUPR_BASE = "https://api.dupr.gg"
WATCHES_FILE = Path(__file__).parent / "watches.json"
CONNECT_PROFILE_FILE = Path(__file__).parent / "connect_profile.json"

# Static region -> pro player name mapping for globe view
GLOBE_REGION_PLAYERS = {
    "North America": ["Ben Johns", "Anna Leigh Waters", "JW Johnson", "Tyson McGuffin", "Jay Devilliers"],
    "South America": ["Federico Staksrud", "Andrei Daescu", "Gabriel Tardio", "Jorge Gutierrez", "Pablo Tellez"],
    "Europe": ["Christian Alshon", "Anna Bright", "Lucie Dodd", "Irina Tereschenko", "Giulia Sussarello"],
    "Asia": ["Wei Shen", "Yu Cao", "Jing Huang", "Yuto Yamamoto", "Lee Sung Ho"],
    "Africa": ["Njideka Isichei", "Nandita Bhardwaj", "Fiona Ellis", "Ahmed Khalil", "Sipho Dlamini"],
    "Oceania": ["Ben Sherwood", "Yana Sherwood", "Ned Sherwood", "Tom Sherwood", "Lucy Sherwood"],
    "Middle East": ["Omar Al-Rashid", "Fatima Al-Zahra", "Khalid Hassan", "Nadia Al-Mansouri", "Tariq Shaikh"],
}

# Simple in-memory cache: key -> (timestamp, data)
_cache: dict[str, tuple[float, object]] = {}
CACHE_TTL = 300  # 5 minutes

# Connect: nearby city clusters keyed by lowercase primary city name
# "close" = ~15 min drive (no score penalty), "far" = 15-45 min (heavy penalty + strict DUPR gate)
CITY_CLUSTERS: dict[str, dict[str, list[str]]] = {
    "raleigh":      {"close": ["Cary, NC", "Durham, NC"], "far": ["Chapel Hill, NC", "Morrisville, NC", "Apex, NC", "Wake Forest, NC"]},
    "charlotte":    {"close": ["Concord, NC", "Matthews, NC", "Huntersville, NC"], "far": ["Gastonia, NC", "Mooresville, NC", "Rock Hill, SC"]},
    "austin":       {"close": ["Round Rock, TX", "Cedar Park, TX"], "far": ["Georgetown, TX", "Kyle, TX", "Pflugerville, TX", "Leander, TX"]},
    "dallas":       {"close": ["Plano, TX", "Irving, TX", "Arlington, TX"], "far": ["McKinney, TX", "Frisco, TX", "Garland, TX"]},
    "houston":      {"close": ["Pasadena, TX", "Sugar Land, TX", "Pearland, TX"], "far": ["The Woodlands, TX", "Katy, TX", "Baytown, TX"]},
    "atlanta":      {"close": ["Smyrna, GA", "Decatur, GA", "Sandy Springs, GA"], "far": ["Marietta, GA", "Roswell, GA", "Alpharetta, GA"]},
    "phoenix":      {"close": ["Scottsdale, AZ", "Tempe, AZ", "Mesa, AZ"], "far": ["Chandler, AZ", "Gilbert, AZ", "Glendale, AZ"]},
    "denver":       {"close": ["Aurora, CO", "Lakewood, CO", "Westminster, CO"], "far": ["Centennial, CO", "Arvada, CO", "Thornton, CO"]},
    "seattle":      {"close": ["Bellevue, WA", "Redmond, WA", "Kirkland, WA"], "far": ["Renton, WA", "Kent, WA", "Bothell, WA"]},
    "portland":     {"close": ["Beaverton, OR", "Gresham, OR"], "far": ["Hillsboro, OR", "Vancouver, WA", "Lake Oswego, OR"]},
    "san diego":    {"close": ["Chula Vista, CA", "El Cajon, CA", "Santee, CA"], "far": ["Escondido, CA", "Oceanside, CA", "La Mesa, CA"]},
    "los angeles":  {"close": ["Santa Monica, CA", "Burbank, CA", "Pasadena, CA"], "far": ["Long Beach, CA", "Inglewood, CA", "Glendale, CA"]},
    "miami":        {"close": ["Coral Gables, FL", "Hialeah, FL", "Miami Beach, FL"], "far": ["Fort Lauderdale, FL", "Hollywood, FL", "Doral, FL"]},
    "orlando":      {"close": ["Kissimmee, FL", "Sanford, FL", "Ocoee, FL"], "far": ["Winter Garden, FL", "Altamonte Springs, FL"]},
    "chicago":      {"close": ["Evanston, IL", "Oak Park, IL"], "far": ["Naperville, IL", "Schaumburg, IL", "Aurora, IL"]},
    "new york":     {"close": ["Brooklyn, NY", "Queens, NY", "Newark, NJ"], "far": ["Hoboken, NJ", "Jersey City, NJ", "Yonkers, NY"]},
    "boston":       {"close": ["Cambridge, MA", "Somerville, MA", "Quincy, MA"], "far": ["Newton, MA", "Brookline, MA"]},
    "nashville":    {"close": ["Brentwood, TN", "Franklin, TN"], "far": ["Murfreesboro, TN", "Hendersonville, TN", "Spring Hill, TN"]},
    "tampa":        {"close": ["St. Petersburg, FL", "Clearwater, FL"], "far": ["Brandon, FL", "Lakeland, FL", "Bradenton, FL"]},
    "minneapolis":  {"close": ["St. Paul, MN", "Bloomington, MN"], "far": ["Plymouth, MN", "Brooklyn Park, MN", "Edina, MN"]},
    "san antonio":  {"close": ["Schertz, TX", "New Braunfels, TX"], "far": ["Seguin, TX", "San Marcos, TX"]},
    "las vegas":    {"close": ["Henderson, NV", "North Las Vegas, NV"], "far": ["Boulder City, NV", "Summerlin, NV"]},
    "washington":   {"close": ["Arlington, VA", "Alexandria, VA", "Bethesda, MD"], "far": ["Silver Spring, MD", "Reston, VA", "Rockville, MD"]},
    "philadelphia": {"close": ["Camden, NJ", "Wilmington, DE"], "far": ["Cherry Hill, NJ", "Norristown, PA", "Trenton, NJ"]},
    "san jose":     {"close": ["Santa Clara, CA", "Sunnyvale, CA"], "far": ["Fremont, CA", "Mountain View, CA", "Milpitas, CA"]},
    "san francisco":{"close": ["Oakland, CA", "Berkeley, CA"], "far": ["Daly City, CA", "South San Francisco, CA", "San Mateo, CA"]},
}
FAR_SCORE_MULTIPLIER = 0.5   # far-city players get half score
FAR_MAX_RATING_DIFF  = 0.4   # far-city players only qualify if DUPR diff ≤ this


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_token() -> str:
    """Return auth token: session first, then DUPR_TOKEN env var as fallback."""
    return session.get("token") or os.getenv("DUPR_TOKEN", "")


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _dupr_get(path: str, token: str) -> requests.Response:
    return requests.get(f"{DUPR_BASE}{path}", headers=_headers(token), timeout=15)


def _dupr_post(path: str, token: str, body: dict) -> requests.Response:
    return requests.post(f"{DUPR_BASE}{path}", headers=_headers(token), json=body, timeout=15)


def _load_watches() -> list[dict]:
    if WATCHES_FILE.exists():
        try:
            return json.loads(WATCHES_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_watches(watches: list[dict]):
    WATCHES_FILE.write_text(json.dumps(watches, indent=2))


def _player_name(p: dict) -> str:
    full = p.get("fullName", "")
    if full:
        return full
    first = p.get("firstName", p.get("first", ""))
    last = p.get("lastName", p.get("last", ""))
    if first or last:
        return f"{first} {last}".strip()
    return p.get("name", p.get("displayName", "Unknown"))


# Default players to pre-populate on first run (before any watches.json exists)
DEFAULT_PLAYER_NAMES = [
    "Ben Johns",
    "Andrei Daescu",
    "Hayden Patriquin",
    "JW Johnson",
    "Gabriel Tardio",
    "Christian Alshon",
    "Federico Staksrud",
    "Anna Leigh Waters",
    "Anna Bright",
    "Hurricane Tyra Black",
    "Jorja Johnson",
    "Hunter Johnson",
    "Christopher Haworth",
]


_CC_NAME: dict[str, str] = {  # ISO-2 code → full name

    "AF":"Afghanistan","AL":"Albania","DZ":"Algeria","AR":"Argentina","AU":"Australia",
    "AT":"Austria","BE":"Belgium","BR":"Brazil","CA":"Canada","CL":"Chile","CN":"China",
    "CO":"Colombia","HR":"Croatia","CZ":"Czech Republic","DK":"Denmark","EG":"Egypt",
    "FI":"Finland","FR":"France","DE":"Germany","GR":"Greece","HU":"Hungary","IN":"India",
    "ID":"Indonesia","IE":"Ireland","IL":"Israel","IT":"Italy","JP":"Japan","JO":"Jordan",
    "KW":"Kuwait","MY":"Malaysia","MX":"Mexico","NL":"Netherlands","NZ":"New Zealand",
    "NO":"Norway","PK":"Pakistan","PE":"Peru","PH":"Philippines","PL":"Poland",
    "PT":"Portugal","QA":"Qatar","RO":"Romania","RU":"Russia","SA":"Saudi Arabia",
    "RS":"Serbia","SG":"Singapore","ZA":"South Africa","KR":"South Korea","ES":"Spain",
    "SE":"Sweden","CH":"Switzerland","TW":"Taiwan","TH":"Thailand","TR":"Turkey",
    "UA":"Ukraine","AE":"UAE","GB":"United Kingdom","US":"United States","UY":"Uruguay",
    "VE":"Venezuela","PA":"Panama","EC":"Ecuador","GT":"Guatemala","CR":"Costa Rica",
    "DO":"Dominican Republic","PR":"Puerto Rico","BO":"Bolivia","PY":"Paraguay",
}
# Reverse map: lowercase full name → ISO-2 code
_CC_BY_NAME: dict[str, str] = {v.lower(): k for k, v in _CC_NAME.items()}


_US_NAMES = {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}


def _format_location(h: dict) -> str:
    """Return 'City, ST' for US, 'City, Country' for international, '' if unknown."""
    city = (h.get("city") or "").strip()
    state = (h.get("state") or h.get("stateProvince") or "").strip()
    country = (h.get("country") or h.get("countryCode") or "").strip().upper()

    if city:
        if country in _US_NAMES:
            return f"{city}, {state}" if state else city
        country_name = _CC_NAME.get(country, "")
        return f"{city}, {country_name}" if country_name else city

    # Fallback: parse shortAddress e.g. "Raleigh, NC" / "Austin, TX, United States" /
    # "Cádiz, AN, ES" / "CN, Spain" / "Santa Cruz de la Sierra, Santa Cruz Dept, BO"
    short = (h.get("shortAddress") or h.get("displayLocation") or "").strip()
    if not short:
        return ""
    parts = [p.strip() for p in short.split(",")]

    if len(parts) >= 3:
        last_up = parts[-1].upper()
        # US with full country name: "City, ST, United States" → "City, ST"
        if last_up in _US_NAMES:
            return f"{parts[0]}, {parts[1].strip()}"
        # "City, Region, CountryCode" → "City, Country"
        country_name = _CC_NAME.get(last_up, "")
        return f"{parts[0]}, {country_name}" if country_name else parts[0]

    if len(parts) == 2:
        first, second = parts[0].strip(), parts[1].strip()
        second_up = second.upper()

        # Second part is an ISO-2 country code
        if second_up in _CC_NAME:
            if second_up in ("US", "USA"):
                return short  # "City, ST" — keep as-is
            return f"{first}, {_CC_NAME[second_up]}"

        # Second part is a full country name (e.g. "Spain", "South Africa", "United States")
        if second_up in _US_NAMES:
            return short  # "City, ST" — keep as-is
        cc = _CC_BY_NAME.get(second.lower(), "")
        if cc:
            # First part is a region code (≤3 all-caps letters), not a city → drop it
            if len(first) <= 3 and first.isalpha() and first == first.upper():
                return second
            return f"{first}, {second}"

        # Default: US "City, ST" style or unknown — return as-is
        return short

    return short


def _extract_ratings(p: dict) -> dict:
    """Extract doubles/singles ratings from a DUPR player object.

    The API may nest ratings under 'ratings' or at the top level,
    and may return the string "NR" for unrated players.
    The nested value may itself be a dict like {"rating": 7.112, ...}.
    """
    def _unwrap(v):
        """If v is a dict, pull out the numeric rating field."""
        if isinstance(v, dict):
            return v.get("rating") or v.get("value") or v.get("glicko")
        return v

    def _to_float(v):
        v = _unwrap(v)
        if not v or v == "NR" or v == "N/R":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    doubles = p.get("doublesRating")
    singles = p.get("singlesRating")
    # Some endpoints nest under 'ratings'
    ratings_obj = p.get("ratings") or {}
    if not doubles and ratings_obj:
        doubles = ratings_obj.get("doubles") or ratings_obj.get("doublesRating")
    if not singles and ratings_obj:
        singles = ratings_obj.get("singles") or ratings_obj.get("singlesRating")
    doubles = _to_float(doubles)
    singles = _to_float(singles)
    rating = doubles or singles
    return {"rating": rating, "doublesRating": doubles, "singlesRating": singles}


def _seed_default_watches(token: str):
    """Search DUPR for default players and save them to watches.json.

    Only runs once — when watches.json does not yet exist.
    """
    if WATCHES_FILE.exists():
        return
    watches = []
    for name in DEFAULT_PLAYER_NAMES:
        try:
            resp = _dupr_post("/player/v1.0/search", token, {
                "filter": {}, "query": name, "limit": 10,
            })
            if resp.status_code != 200:
                continue
            hits = resp.json().get("result", {}).get("hits", [])
            if not hits:
                continue
            # Pick the best match: prefer exact name match with highest rating
            best = None
            best_rating = -1
            name_lower = name.lower()
            for h in hits:
                h_name = _player_name(h).lower()
                r = _extract_ratings(h)
                h_rating = r["rating"] or 0
                # Exact or close name match gets priority
                if h_name == name_lower or name_lower in h_name:
                    if h_rating > best_rating:
                        best = h
                        best_rating = h_rating
            if not best:
                best = hits[0]
            r = _extract_ratings(best)
            watches.append({
                "id": str(best.get("id", "")),
                "name": _player_name(best),
                "imageUrl": best.get("imageUrl", ""),
                **r,
            })
        except Exception:
            continue
    if watches:
        _save_watches(watches)


def _get_following(token: str) -> list[dict]:
    """Try DUPR following endpoints; fall back to local watch list."""
    endpoints = [
        "/social/v1.0/following/",
        "/user/v1.0/following/",
        "/user/v1.0/profile/following",
    ]
    for ep in endpoints:
        try:
            resp = _dupr_get(ep, token)
            if resp.status_code == 200:
                data = resp.json()
                # Normalize — the response shape may vary
                players = data if isinstance(data, list) else data.get("result", data.get("data", data.get("following", [])))
                if isinstance(players, list) and players:
                    return players
        except Exception:
            continue
    return []


def _fetch_player_history(player_id: str, token: str, limit: int = 25, offset: int = 0) -> list[dict]:
    """Fetch recent matches for a single player."""
    body = {
        "filters": {},
        "limit": limit,
        "offset": offset,
        "sort": {"order": "DESC", "parameter": "MATCH_DATE"},
    }
    try:
        resp = _dupr_post(f"/player/v1.0/{player_id}/history", token, body)
        if resp.status_code == 200:
            data = resp.json()
            result = data.get("result", {})
            matches = result.get("hits", []) if isinstance(result, dict) else []
            return matches if isinstance(matches, list) else []
        if resp.status_code == 401:
            return ["__401__"]
    except Exception as e:
        print(f"DUPR history ERROR pid={player_id}: {e}", flush=True)
    return []


def _build_feed(token: str, user_id: str | None = None) -> dict:
    """Build the merged, sorted feed for all followed/watched players."""
    cache_key = f"feed:{user_id or 'anon'}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < CACHE_TTL:
        return cached[1]

    # Collect player IDs from DUPR following + local watches
    following = _get_following(token)
    watches = _load_watches()

    player_map: dict[str, dict] = {}  # id -> {id, name, rating, ...}

    for p in following:
        pid = str(p.get("id", p.get("playerId", p.get("userId", ""))))
        if pid:
            player_map[pid] = {
                "id": pid,
                "name": _player_name(p),
                "rating": p.get("rating", p.get("doublesRating", p.get("singlesRating", None))),
                "doublesRating": p.get("doublesRating"),
                "singlesRating": p.get("singlesRating"),
                "imageUrl": p.get("imageUrl", p.get("image", "")),
            }

    for w in watches:
        pid = str(w.get("id", ""))
        if pid and pid not in player_map:
            player_map[pid] = w

    if not player_map:
        result = {"matches": [], "players": []}
        _cache[cache_key] = (time.time(), result)
        return result

    # Parallel fetch of match histories — 2 pages of 25 per player
    all_matches: list[dict] = []
    got_401 = False
    seen_match_ids: set = set()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(_fetch_player_history, pid, token, 25, offset): (pid, offset)
            for pid in player_map
            for offset in (0, 25)
        }
        for future in as_completed(futures):
            pid, _ = futures[future]
            try:
                matches = future.result()
                if matches and matches[0] == "__401__":
                    got_401 = True
                    continue
                for m in matches:
                    mid = m.get("matchId") or m.get("id")
                    dedup_key = f"{pid}:{mid}"
                    if dedup_key in seen_match_ids:
                        continue
                    seen_match_ids.add(dedup_key)
                    m["_playerInfo"] = player_map.get(pid, {})
                    all_matches.append(m)
            except Exception:
                continue

    if got_401 and not all_matches:
        return {"error": "unauthorized"}

    # Sort by match date descending
    def sort_key(m):
        d = m.get("matchDate", m.get("date", m.get("eventDate", "")))
        if not d:
            return ""
        return d

    all_matches.sort(key=sort_key, reverse=True)

    result = {
        "matches": all_matches[:300],
        "players": list(player_map.values()),
    }
    _cache[cache_key] = (time.time(), result)
    return result






# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    if "token" not in session:
        return redirect(url_for("login_page"))
    return render_template("index.html")


@app.route("/login")
def login_page():
    return render_template("index.html", show_login=True)


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    email = data.get("email", "").strip()
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    try:
        resp = requests.post(
            f"{DUPR_BASE}/auth/v1.0/login/",
            json={"email": email, "password": password},
            timeout=15,
        )
    except requests.RequestException as e:
        return jsonify({"error": f"Could not reach DUPR: {e}"}), 502

    if resp.status_code != 200:
        msg = "Invalid credentials"
        try:
            msg = resp.json().get("message", msg)
        except Exception:
            pass
        return jsonify({"error": msg}), resp.status_code

    body = resp.json()
    token = body.get("result", body.get("data", body)).get("accessToken", body.get("result", body.get("data", body)).get("token", ""))
    if not token:
        # Try alternate shapes
        token = body.get("accessToken", body.get("token", ""))

    if not token:
        return jsonify({"error": "Login succeeded but no token was returned"}), 500

    session["token"] = token
    session["email"] = email

    # Fetch user profile
    try:
        profile_resp = _dupr_get("/user/v1.0/profile/", token)
        if profile_resp.status_code == 200:
            profile = profile_resp.json()
            user_data = profile.get("result", profile.get("data", profile))
            session["user"] = {
                "id": str(user_data.get("id", "")),
                "name": _player_name(user_data),
                "email": email,
                "doublesRating": user_data.get("doublesRating"),
                "singlesRating": user_data.get("singlesRating"),
                "imageUrl": user_data.get("imageUrl", ""),
            }
    except Exception:
        session["user"] = {"name": email, "email": email}

    # Seed default watch list on very first login (when watches.json doesn't exist)
    _seed_default_watches(token)

    return jsonify({"ok": True, "user": session.get("user", {})})


@app.route("/api/feed")
def api_feed():
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    user_id = (session.get("user") or {}).get("id")
    result = _build_feed(token, user_id)

    if result.get("error") == "unauthorized":
        session.clear()
        return jsonify({"error": "unauthorized"}), 401

    result["me"] = session.get("user", {})
    return jsonify(result)


@app.route("/api/search", methods=["POST"])
def api_search():
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = data.get("query", "").strip()
    ensure_ids = [str(i) for i in data.get("ensureIds", [])]  # watch-list IDs to always include
    if not query:
        return jsonify({"results": []})

    cache_key = f"search:{query.lower()}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 60:
        cached_results = cached[1]
        # Ensure watch-list players are in cached results (may not have been in original search)
        cached_ids = {p["id"] for p in cached_results}
        missing_ids = [i for i in ensure_ids if i not in cached_ids]
        if not missing_ids:
            return jsonify({"results": cached_results})
        # Fall through to fetch missing profiles

    body = {"filter": {}, "query": query, "limit": 25}
    try:
        resp = _dupr_post("/player/v1.0/search", token, body)
        if resp.status_code == 401:
            session.clear()
            return jsonify({"error": "unauthorized"}), 401
        if resp.status_code == 200:
            rdata = resp.json()
            result = rdata.get("result", {})
            hits = result.get("hits", []) if isinstance(result, dict) else []
            if not isinstance(hits, list):
                hits = []
            # Filter to rated players only
            rated = []
            hit_ids = set()
            for h in hits:
                r = _extract_ratings(h)
                h["_r"] = r
                if r["doublesRating"] is not None or r["singlesRating"] is not None:
                    rated.append(h)
                    hit_ids.add(str(h.get("id", "")))

            # Fetch profiles in parallel for rated hits + any missing ensureIds
            def _get_loc_by_id(pid):
                try:
                    pr = _dupr_get(f"/player/v1.0/{pid}", token)
                    if pr.status_code == 200:
                        det = pr.json().get("result") or {}
                        return pid, det
                except Exception:
                    pass
                return pid, {}

            all_pids_to_fetch = list(hit_ids) + [i for i in ensure_ids if i not in hit_ids]
            with ThreadPoolExecutor(max_workers=min(20, len(all_pids_to_fetch) + 1)) as ex:
                profile_map = dict(ex.map(_get_loc_by_id, all_pids_to_fetch))

            normalized = []
            for h in rated:
                pid = str(h.get("id", ""))
                r = h["_r"]
                det = profile_map.get(pid, {})
                normalized.append({
                    "id": pid,
                    "name": _player_name(h),
                    "doublesRating": r["doublesRating"],
                    "singlesRating": r["singlesRating"],
                    "imageUrl": h.get("imageUrl", ""),
                    "location": _format_location(det),
                })

            # Add ensureIds players that weren't in DUPR search results
            existing_ids = {p["id"] for p in normalized}
            for pid in ensure_ids:
                if pid in existing_ids:
                    continue
                det = profile_map.get(pid, {})
                if not det:
                    continue
                r = _extract_ratings(det)
                if r["doublesRating"] is None and r["singlesRating"] is None:
                    continue
                normalized.insert(0, {
                    "id": pid,
                    "name": _player_name(det),
                    "doublesRating": r["doublesRating"],
                    "singlesRating": r["singlesRating"],
                    "imageUrl": det.get("imageUrl", ""),
                    "location": _format_location(det),
                })

            _cache[cache_key] = (time.time(), normalized)
            return jsonify({"results": normalized})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"results": []})


@app.route("/api/watch", methods=["POST"])
def api_watch():
    data = request.get_json(silent=True) or {}
    player_id = str(data.get("id", "")).strip()
    action = data.get("action", "add")  # add or remove

    if not player_id:
        return jsonify({"error": "Player ID required"}), 400

    watches = _load_watches()

    if action == "remove":
        watches = [w for w in watches if str(w.get("id", "")) != player_id]
        _save_watches(watches)
        # Invalidate cache
        _cache.clear()
        return jsonify({"ok": True, "watches": watches})

    # Add
    if any(str(w.get("id", "")) == player_id for w in watches):
        return jsonify({"ok": True, "watches": watches, "message": "Already watching"})

    new_entry = {
        "id": player_id,
        "name": data.get("name", "Unknown"),
        "rating": data.get("rating"),
        "doublesRating": data.get("doublesRating"),
        "singlesRating": data.get("singlesRating"),
        "imageUrl": data.get("imageUrl", ""),
    }
    watches.append(new_entry)
    _save_watches(watches)
    _cache.clear()
    return jsonify({"ok": True, "watches": watches})


@app.route("/api/watches")
def api_watches():
    return jsonify({"watches": _load_watches()})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Clear cache and re-fetch."""
    _cache.clear()
    return jsonify({"ok": True})


@app.route("/api/h2h", methods=["POST"])
def api_h2h():
    """Head-to-head stats between two players."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    p1_id = str(data.get("p1", "")).strip()
    p2_id = str(data.get("p2", "")).strip()
    p1_name = data.get("p1Name", p1_id)
    p2_name = data.get("p2Name", p2_id)

    if not p1_id or not p2_id or p1_id == p2_id:
        return jsonify({"error": "Two distinct player IDs required"}), 400

    def fetch_all_history(player_id: str, max_matches: int = 1000) -> list[dict]:
        """Fetch full match history by paginating until the player's history is exhausted."""
        all_m: list[dict] = []
        page_size = 25
        with ThreadPoolExecutor(max_workers=10) as ex:
            offset = 0
            while offset < max_matches:
                # Fire a batch of 5 pages in parallel
                batch_offsets = list(range(offset, min(offset + page_size * 5, max_matches), page_size))
                futures = {ex.submit(_fetch_player_history, player_id, token, page_size, off): off
                           for off in batch_offsets}
                got_any = False
                short_page = False
                for f in as_completed(futures):
                    try:
                        r = f.result()
                        if r and r[0] == "__401__":
                            return ["__401__"]
                        if r:
                            all_m.extend(r)
                            got_any = True
                            if len(r) < page_size:
                                short_page = True  # last page — history exhausted
                    except Exception:
                        pass
                if not got_any or short_page:
                    break
                offset += page_size * 5
        return all_m

    p1_matches = fetch_all_history(p1_id, max_matches=1000)
    p2_matches = fetch_all_history(p2_id, max_matches=1000)
    if p1_matches and p1_matches[0] == "__401__":
        return jsonify({"error": "unauthorized"}), 401
    if p2_matches and p2_matches[0] == "__401__":
        return jsonify({"error": "unauthorized"}), 401

    def get_team_players(team):
        return [p for p in [team.get("player1"), team.get("player2")] if p]

    def player_in_team(pid, team):
        return any(str(p["id"]) == pid for p in get_team_players(team))

    def score_str(my_team, opp_team):
        games = []
        for g in range(1, 6):
            s1 = my_team.get(f"game{g}")
            s2 = opp_team.get(f"game{g}")
            if s1 is not None and s1 >= 0 and s2 is not None and s2 >= 0:
                games.append(f"{s1}-{s2}")
        return ", ".join(games)

    def is_doubles(m):
        return "DOUBLE" in (m.get("eventFormat") or "").upper()

    # ----- H2H direct matchups -----
    h2h_matches = []
    seen_h2h = set()
    for m in p1_matches:
        mid = m.get("matchId") or m.get("id")
        if mid in seen_h2h:
            continue
        teams = m.get("teams", [])
        if len(teams) < 2:
            continue
        p1_team_idx = next((i for i, t in enumerate(teams) if player_in_team(p1_id, t)), -1)
        if p1_team_idx < 0:
            continue
        opp_team_idx = 1 - p1_team_idx
        if not player_in_team(p2_id, teams[opp_team_idx]):
            continue
        seen_h2h.add(mid)
        p1_team = teams[p1_team_idx]
        p2_team = teams[opp_team_idx]
        p1_won = p1_team.get("winner") is True
        doubles = is_doubles(m)
        rating_key = "Double" if doubles else "Single"
        # rating delta for p1
        def player_num_in_team(pid, team):
            players = get_team_players(team)
            for i, p in enumerate(players):
                if str(p["id"]) == pid:
                    return i + 1
            return None
        pn1 = player_num_in_team(p1_id, p1_team)
        pn2 = player_num_in_team(p2_id, p2_team)
        rim1 = p1_team.get("preMatchRatingAndImpact") or {}
        rim2 = p2_team.get("preMatchRatingAndImpact") or {}
        p1_delta = rim1.get(f"match{rating_key}RatingImpactPlayer{pn1}") if pn1 else None
        p2_delta = rim2.get(f"match{rating_key}RatingImpactPlayer{pn2}") if pn2 else None
        p1_partners = [p.get("fullName") for p in get_team_players(p1_team) if str(p["id"]) != p1_id]
        p2_partners = [p.get("fullName") for p in get_team_players(p2_team) if str(p["id"]) != p2_id]
        h2h_matches.append({
            "matchId": mid,
            "date": m.get("eventDate", ""),
            "eventName": m.get("eventName") or m.get("league") or "",
            "format": m.get("eventFormat", ""),
            "score": score_str(p1_team, p2_team),
            "p1Won": p1_won,
            "p1Delta": round(p1_delta, 3) if p1_delta is not None else None,
            "p2Delta": round(p2_delta, 3) if p2_delta is not None else None,
            "p1Partners": p1_partners,
            "p2Partners": p2_partners,
        })

    h2h_matches.sort(key=lambda m: m["date"], reverse=True)

    # ----- As partners (same team, doubles) -----
    partner_matches = []
    seen_partner = set()
    for m in p1_matches:
        mid = m.get("matchId") or m.get("id")
        if mid in seen_partner:
            continue
        teams = m.get("teams", [])
        if len(teams) < 2:
            continue
        # Find a team that contains BOTH p1 and p2
        partner_team_idx = next(
            (i for i, t in enumerate(teams)
             if player_in_team(p1_id, t) and player_in_team(p2_id, t)),
            -1
        )
        if partner_team_idx < 0:
            continue
        seen_partner.add(mid)
        my_team = teams[partner_team_idx]
        opp_team = teams[1 - partner_team_idx]
        won = my_team.get("winner") is True
        opp_names = [p.get("fullName", "?") for p in get_team_players(opp_team)]
        def player_num_in_team(pid, team):
            for i, p in enumerate(get_team_players(team)):
                if str(p["id"]) == pid:
                    return i + 1
            return None
        pn1 = player_num_in_team(p1_id, my_team)
        pn2 = player_num_in_team(p2_id, my_team)
        rim = my_team.get("preMatchRatingAndImpact") or {}
        p1_delta = rim.get(f"matchDoubleRatingImpactPlayer{pn1}") if pn1 else None
        p2_delta = rim.get(f"matchDoubleRatingImpactPlayer{pn2}") if pn2 else None
        partner_matches.append({
            "matchId": mid,
            "date": m.get("eventDate", ""),
            "eventName": m.get("eventName") or m.get("league") or "",
            "format": m.get("eventFormat", ""),
            "score": score_str(my_team, opp_team),
            "won": won,
            "opponents": opp_names,
            "p1Delta": round(p1_delta, 3) if p1_delta is not None else None,
            "p2Delta": round(p2_delta, 3) if p2_delta is not None else None,
        })
    partner_matches.sort(key=lambda m: m["date"], reverse=True)
    partner_wins = sum(1 for m in partner_matches if m["won"])

    p1_wins = sum(1 for m in h2h_matches if m["p1Won"])
    p2_wins = len(h2h_matches) - p1_wins

    def _h2h_fmt(m):
        en = (m.get("eventName") or "").upper()
        fmt = (m.get("format") or "").upper()
        if "MIXED" in en or "MIXED" in fmt:
            return "mixed"
        if "SINGLE" in fmt or "SINGLE" in en:
            return "singles"
        if "DOUBLE" in fmt or "DOUBLE" in en:
            return "doubles"
        return "unknown"

    singles_matches = [m for m in h2h_matches if _h2h_fmt(m) == "singles"]
    doubles_matches = [m for m in h2h_matches if _h2h_fmt(m) == "doubles"]
    mixed_matches = [m for m in h2h_matches if _h2h_fmt(m) == "mixed"]
    p1_singles_wins = sum(1 for m in singles_matches if m["p1Won"])
    p1_doubles_wins = sum(1 for m in doubles_matches if m["p1Won"])
    p1_mixed_wins = sum(1 for m in mixed_matches if m["p1Won"])

    # ----- Common opponents -----
    def build_opponent_record(matches, my_id):
        """For each opponent faced, build W/L record split by singles/doubles/mixed."""
        record = {}  # opp_id -> {name, sWins, sLosses, dWins, dLosses, mWins, mLosses}
        for m in matches:
            teams = m.get("teams", [])
            if len(teams) < 2:
                continue
            my_idx = next((i for i, t in enumerate(teams) if player_in_team(my_id, t)), -1)
            if my_idx < 0:
                continue
            opp_team = teams[1 - my_idx]
            my_team = teams[my_idx]
            i_won = my_team.get("winner") is True
            fmt = _match_format(m)
            for p in get_team_players(opp_team):
                oid = str(p["id"])
                oname = p.get("fullName", oid)
                if oid not in record:
                    record[oid] = {"name": oname,
                                   "sWins": 0, "sLosses": 0,
                                   "dWins": 0, "dLosses": 0,
                                   "mWins": 0, "mLosses": 0}
                if fmt == "mixed":
                    if i_won: record[oid]["mWins"] += 1
                    else:     record[oid]["mLosses"] += 1
                elif fmt == "doubles":
                    if i_won: record[oid]["dWins"] += 1
                    else:     record[oid]["dLosses"] += 1
                else:
                    if i_won: record[oid]["sWins"] += 1
                    else:     record[oid]["sLosses"] += 1
        return record

    p1_record = build_opponent_record(p1_matches, p1_id)
    p2_record = build_opponent_record(p2_matches, p2_id)

    common_opp_ids = set(p1_record.keys()) & set(p2_record.keys())
    common_opp_ids.discard(p1_id)
    common_opp_ids.discard(p2_id)

    common_opponents = []
    for oid in common_opp_ids:
        r1 = p1_record[oid]
        r2 = p2_record[oid]
        # Only include per-format stats where BOTH players faced this opponent in that format
        has_singles = (r1["sWins"] + r1["sLosses"] > 0) and (r2["sWins"] + r2["sLosses"] > 0)
        has_doubles = (r1["dWins"] + r1["dLosses"] > 0) and (r2["dWins"] + r2["dLosses"] > 0)
        has_mixed   = (r1["mWins"] + r1["mLosses"] > 0) and (r2["mWins"] + r2["mLosses"] > 0)
        if not (has_singles or has_doubles or has_mixed):
            continue  # no format in common — skip entirely
        common_opponents.append({
            "oppId": oid,
            "oppName": r1["name"] or r2["name"],
            "p1sWins":  r1["sWins"]  if has_singles else 0,
            "p1sLosses":r1["sLosses"]if has_singles else 0,
            "p1dWins":  r1["dWins"]  if has_doubles else 0,
            "p1dLosses":r1["dLosses"]if has_doubles else 0,
            "p1mWins":  r1["mWins"]  if has_mixed   else 0,
            "p1mLosses":r1["mLosses"]if has_mixed   else 0,
            "p2sWins":  r2["sWins"]  if has_singles else 0,
            "p2sLosses":r2["sLosses"]if has_singles else 0,
            "p2dWins":  r2["dWins"]  if has_doubles else 0,
            "p2dLosses":r2["dLosses"]if has_doubles else 0,
            "p2mWins":  r2["mWins"]  if has_mixed   else 0,
            "p2mLosses":r2["mLosses"]if has_mixed   else 0,
            "hasSingles": has_singles, "hasDoubles": has_doubles, "hasMixed": has_mixed,
        })
    # Sort by total shared games desc
    common_opponents.sort(key=lambda x: (
        x["p1sWins"]+x["p1sLosses"]+x["p1dWins"]+x["p1dLosses"]+x["p1mWins"]+x["p1mLosses"] +
        x["p2sWins"]+x["p2sLosses"]+x["p2dWins"]+x["p2dLosses"]+x["p2mWins"]+x["p2mLosses"]
    ), reverse=True)

    return jsonify({
        "p1Id": p1_id, "p1Name": p1_name,
        "p2Id": p2_id, "p2Name": p2_name,
        "p1Wins": p1_wins, "p2Wins": p2_wins,
        "p1SinglesWins": p1_singles_wins, "p2SinglesWins": len(singles_matches) - p1_singles_wins,
        "p1DoublesWins": p1_doubles_wins, "p2DoublesWins": len(doubles_matches) - p1_doubles_wins,
        "p1MixedWins": p1_mixed_wins, "p2MixedWins": len(mixed_matches) - p1_mixed_wins,
        "totalMatches": len(h2h_matches),
        "singlesMatches": len(singles_matches),
        "doublesMatches": len(doubles_matches),
        "mixedMatches": len(mixed_matches),
        "matches": h2h_matches,
        "partnerMatches": partner_matches,
        "partnerWins": partner_wins,
        "partnerLosses": len(partner_matches) - partner_wins,
        "commonOpponents": common_opponents[:40],
    })


@app.route("/api/h2h/teams", methods=["POST"])
def api_h2h_teams():
    """Compare two teams of two players each."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    t1p1_id = str(data.get("t1p1", "")).strip()
    t1p2_id = str(data.get("t1p2", "")).strip()
    t2p1_id = str(data.get("t2p1", "")).strip()
    t2p2_id = str(data.get("t2p2", "")).strip()
    t1_name = data.get("t1Name", "Team 1")
    t2_name = data.get("t2Name", "Team 2")

    ids = [i for i in [t1p1_id, t1p2_id, t2p1_id, t2p2_id] if i]
    if len(set(ids)) < 4:
        return jsonify({"error": "Need 4 different players"}), 400

    # Use the existing fetch_all_history helper — define it inline here
    def fetch_history(pid):
        results = []
        page = 0
        while len(results) < 500:
            try:
                resp = _dupr_post(f"/player/v1.0/{pid}/history",
                                  token, {"limit": 25, "offset": page * 25})
                if resp.status_code != 200:
                    break
                page_matches = resp.json().get("result", {}).get("matches", [])
                if not page_matches:
                    break
                results.extend(page_matches)
                if len(page_matches) < 25:
                    break
                page += 1
            except Exception:
                break
        return results

    # Fetch all 4 histories in parallel
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {pid: ex.submit(fetch_history, pid) for pid in [t1p1_id, t1p2_id, t2p1_id, t2p2_id]}
    histories = {pid: f.result() for pid, f in futs.items()}

    t1_ids = {t1p1_id, t1p2_id}
    t2_ids = {t2p1_id, t2p2_id}

    team_matches = []  # matches where t1 played as a team against t2
    seen_match_ids = set()

    for pid in [t1p1_id, t1p2_id]:
        for m in histories[pid]:
            mid = str(m.get("id", "")) or str(m.get("matchId", ""))
            if mid in seen_match_ids:
                continue
            teams = m.get("teams", [])
            if len(teams) < 2:
                continue
            for ti, team in enumerate(teams):
                p1obj = team.get("player1") or {}
                p2obj = team.get("player2") or {}
                team_player_ids = {str(p1obj.get("id", "")), str(p2obj.get("id", ""))}
                # Check if this team is t1 and the other team is t2
                other_team = teams[1 - ti]
                op1 = other_team.get("player1") or {}
                op2 = other_team.get("player2") or {}
                other_ids = {str(op1.get("id", "")), str(op2.get("id", ""))}
                if t1_ids <= (team_player_ids | {""}) and t2_ids <= (other_ids | {""}):
                    # t1 vs t2 match found
                    seen_match_ids.add(mid)
                    t1_won = team.get("winner", False)
                    scores = [team.get(f"game{i}") for i in range(1, 4) if team.get(f"game{i}") is not None]
                    opp_scores = [other_team.get(f"game{i}") for i in range(1, 4) if other_team.get(f"game{i}") is not None]
                    score_str = ", ".join(f"{a}-{b}" for a, b in zip(scores, opp_scores)) if scores else ""
                    team_matches.append({
                        "matchId": mid,
                        "date": m.get("matchDate") or m.get("eventDate", ""),
                        "eventName": m.get("eventName", ""),
                        "t1Won": t1_won,
                        "score": score_str,
                    })
                    break

    team_matches.sort(key=lambda x: x.get("date", ""), reverse=True)
    t1_wins = sum(1 for m in team_matches if m["t1Won"])
    t2_wins = len(team_matches) - t1_wins

    return jsonify({
        "t1Name": t1_name,
        "t2Name": t2_name,
        "t1p1Id": t1p1_id, "t1p2Id": t1p2_id,
        "t2p1Id": t2p1_id, "t2p2Id": t2p2_id,
        "t1Wins": t1_wins,
        "t2Wins": t2_wins,
        "totalMatches": len(team_matches),
        "matches": team_matches,
    })


@app.route("/api/tournament", methods=["POST"])
def api_tournament():
    """Discover all matches for a tournament via graph traversal."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    event_name = data.get("eventName", "").strip()
    initial_ids = [str(pid) for pid in data.get("playerIds", []) if pid]

    app.logger.info(f"TOURNAMENT: eventName={event_name!r} initial_ids={initial_ids}")
    if not event_name or not initial_ids:
        return jsonify({"error": "eventName and playerIds are required"}), 400

    MAX_ROUNDS = 4
    MAX_PLAYERS = 60
    MAX_PAGES_PER_PLAYER = 8  # hard ceiling; early-stop logic cuts this in practice
    fetched_ids: set[str] = set()
    all_matches: dict[int, dict] = {}  # matchId -> match

    def _fetch_player_for_tournament(pid: str) -> list[dict]:
        """Fetch one player's history, stopping once we've found AND passed the event."""
        found_event = False
        result: list[dict] = []
        for page in range(MAX_PAGES_PER_PLAYER):
            page_matches = _fetch_player_history(pid, token, 25, page * 25)
            if not page_matches or (page_matches and page_matches[0] == "__401__"):
                break
            page_has_event = any(
                (m.get("eventName") or m.get("league") or "") == event_name
                for m in page_matches
            )
            result.extend(page_matches)
            if page_has_event:
                found_event = True
            # If the page wasn't full, we're at the end of their history
            if len(page_matches) < 25:
                break
            # If we already found the event and this page has none, we've scrolled past it
            if found_event and not page_has_event:
                break
        return result

    ids_to_fetch = set(initial_ids)

    for _round in range(MAX_ROUNDS):
        if not ids_to_fetch:
            break
        # Cap total players
        if len(fetched_ids) + len(ids_to_fetch) > MAX_PLAYERS:
            ids_to_fetch = set(list(ids_to_fetch)[:MAX_PLAYERS - len(fetched_ids)])
        if not ids_to_fetch:
            break

        batch = list(ids_to_fetch)
        fetched_ids.update(batch)

        round_matches: list[dict] = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(_fetch_player_for_tournament, pid): pid for pid in batch}
            for future in as_completed(futures):
                try:
                    round_matches.extend(future.result())
                except Exception:
                    continue

        # Filter to matching event and collect new player IDs
        new_ids: set[str] = set()
        seen_round = set()
        for m in round_matches:
            m_event = m.get("eventName") or m.get("league") or ""
            if m_event != event_name:
                continue
            mid = m.get("matchId") or m.get("id")
            if mid in seen_round:
                continue
            seen_round.add(mid)
            if mid and mid not in all_matches:
                all_matches[mid] = m
            for team in m.get("teams", []):
                for pkey in ("player1", "player2"):
                    player = team.get(pkey)
                    if player and player.get("id"):
                        pid = str(player["id"])
                        if pid not in fetched_ids:
                            new_ids.add(pid)

        # Early exit: no new players discovered this round — graph is fully explored
        if not new_ids:
            break

        ids_to_fetch = new_ids

    if not all_matches:
        return jsonify({"error": "No matches found for this tournament"}), 404

    matches_list = list(all_matches.values())

    # Derive event metadata from first match
    sample = matches_list[0]
    event_date = sample.get("eventDate", "")
    venue = sample.get("venue", "")
    event_format = sample.get("eventFormat", "")
    is_doubles = "DOUBLE" in event_format.upper() if event_format else False

    # Build team stats
    # Key: tuple of sorted player ids on a team
    team_stats: dict[tuple, dict] = {}

    for m in matches_list:
        teams = m.get("teams", [])
        if len(teams) < 2:
            continue
        for ti, team in enumerate(teams):
            p1 = team.get("player1")
            p2 = team.get("player2")
            players = [p for p in [p1, p2] if p]
            pids = tuple(sorted(str(p["id"]) for p in players))
            pnames = [p.get("fullName", "Unknown") for p in players]
            if pids not in team_stats:
                team_stats[pids] = {
                    "players": pnames,
                    "playerIds": [int(pid) for pid in pids],
                    "wins": 0,
                    "losses": 0,
                    "gamesWon": 0,
                    "gamesLost": 0,
                    "duprDeltas": [],
                }
            ts = team_stats[pids]

            if team.get("winner") is True:
                ts["wins"] += 1
            elif team.get("winner") is False:
                ts["losses"] += 1
            other_team = teams[1 - ti]

            for g in range(1, 6):
                s_my = team.get(f"game{g}")
                s_opp = other_team.get(f"game{g}")
                if s_my is not None and s_my >= 0 and s_opp is not None and s_opp >= 0:
                    if s_my > s_opp:
                        ts["gamesWon"] += 1
                    else:
                        ts["gamesLost"] += 1

            # DUPR deltas
            rim = team.get("preMatchRatingAndImpact") or {}
            rating_key = "Double" if is_doubles else "Single"
            for pn in (1, 2):
                impact = rim.get(f"match{rating_key}RatingImpactPlayer{pn}")
                if impact is not None:
                    ts["duprDeltas"].append(impact)

    # Format team output
    teams_output = []
    for pids, ts in team_stats.items():
        total = ts["wins"] + ts["losses"]
        win_pct = round(ts["wins"] / total, 3) if total > 0 else 0
        avg_delta = round(sum(ts["duprDeltas"]) / len(ts["duprDeltas"]), 4) if ts["duprDeltas"] else 0
        teams_output.append({
            "players": ts["players"],
            "playerIds": ts["playerIds"],
            "wins": ts["wins"],
            "losses": ts["losses"],
            "winPct": win_pct,
            "duprDelta": avg_delta,
            "avgMargin": 0,  # calculated below
            "gamesWon": ts["gamesWon"],
            "gamesLost": ts["gamesLost"],
        })

    # Calculate average score margin per team
    for tout in teams_output:
        pids_set = set(str(p) for p in tout["playerIds"])
        margins = []
        for m in matches_list:
            teams = m.get("teams", [])
            if len(teams) < 2:
                continue
            for ti, team in enumerate(teams):
                tp = [p for p in [team.get("player1"), team.get("player2")] if p]
                tp_ids = set(str(p["id"]) for p in tp)
                if tp_ids == pids_set:
                    other = teams[1 - ti]
                    for g in range(1, 6):
                        s_my = team.get(f"game{g}")
                        s_opp = other.get(f"game{g}")
                        if s_my is not None and s_my >= 0 and s_opp is not None and s_opp >= 0:
                            margins.append(s_my - s_opp)
                    break
        if margins:
            tout["avgMargin"] = round(sum(margins) / len(margins), 1)

    # Sort by wins desc, then winPct desc
    teams_output.sort(key=lambda t: (t["wins"], t["winPct"]), reverse=True)

    # Sort matches by date
    matches_list.sort(key=lambda m: m.get("eventDate", ""), reverse=True)

    # Find upsets: lower-rated team won
    upsets = []
    for m in matches_list:
        teams = m.get("teams", [])
        if len(teams) < 2:
            continue
        # Compute avg pre-match rating per team
        def team_avg_rating(team):
            rim = team.get("preMatchRatingAndImpact") or {}
            rating_key = "Double" if is_doubles else "Single"
            ratings = []
            for pn in (1, 2):
                r = rim.get(f"preMatch{rating_key}RatingPlayer{pn}")
                if r is not None:
                    ratings.append(r)
            return sum(ratings) / len(ratings) if ratings else 0

        r0 = team_avg_rating(teams[0])
        r1 = team_avg_rating(teams[1])
        winner_idx = 0 if teams[0].get("winner") else 1
        loser_idx = 1 - winner_idx
        winner_rating = r0 if winner_idx == 0 else r1
        loser_rating = r0 if loser_idx == 0 else r1
        if winner_rating > 0 and loser_rating > 0 and winner_rating < loser_rating:
            upset_match = dict(m)
            upset_match["_ratingDiff"] = round(loser_rating - winner_rating, 3)
            upsets.append(upset_match)

    # Top DUPR gain/loss
    top_gain = {"players": [], "delta": 0}
    top_loss = {"players": [], "delta": 0}
    for tout in teams_output:
        if tout["duprDelta"] > top_gain["delta"]:
            top_gain = {"players": tout["players"], "delta": tout["duprDelta"]}
        if tout["duprDelta"] < top_loss["delta"]:
            top_loss = {"players": tout["players"], "delta": tout["duprDelta"]}

    return jsonify({
        "eventName": event_name,
        "eventDate": event_date,
        "venue": venue,
        "format": event_format,
        "totalMatches": len(matches_list),
        "teams": teams_output,
        "matches": matches_list,
        "upsets": upsets,
        "topDuprGain": top_gain,
        "topDuprLoss": top_loss,
    })


def _match_format(m: dict) -> str:
    """Return 'singles' | 'doubles' | 'mixed' | 'unknown'.
    eventFormat is authoritative; event name is fallback but singles takes priority
    over 'double' appearing in bracket-style names like 'Double Elimination'.
    """
    event_name = (m.get("eventName") or m.get("league") or "").upper()
    event_format = (m.get("eventFormat") or "").upper()
    # Mixed check (name only — no mixed eventFormat value exists)
    if "MIXED" in event_name:
        return "mixed"
    # eventFormat is the reliable field — trust it first
    if "SINGLE" in event_format:
        return "singles"
    if "DOUBLE" in event_format:
        return "doubles"
    # Fallback to event name — check singles before doubles so
    # "Men's Singles ... Double Elimination" is not mis-tagged
    if "SINGLE" in event_name:
        return "singles"
    if "DOUBLE" in event_name:
        return "doubles"
    return "unknown"


@app.route("/api/player/<player_id>")
def api_player(player_id):
    """Player profile: stats + match history (100 matches, cached 10 min)."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    cache_key = f"player:{player_id}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 600:
        return jsonify(cached[1])

    # Fetch 300 matches (12 pages × 25) in parallel
    all_matches: list[dict] = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(_fetch_player_history, player_id, token, 25, off)
                   for off in range(0, 300, 25)]
        for f in futures:
            try:
                r = f.result()
                if r and r[0] == "__401__":
                    return jsonify({"error": "unauthorized"}), 401
                all_matches.extend(r)
            except Exception:
                pass

    all_matches.sort(key=lambda m: m.get("eventDate", ""), reverse=True)

    # Fetch detailed player profile (gender, age, location, follower counts)
    def _fetch_player_profile(pid: str) -> dict:
        """Try DUPR endpoints to get full player profile."""
        for path in [
            f"/player/v1.0/{pid}",
            f"/user/v1.0/{pid}/profile",
            f"/player/v1.0/{pid}/profile",
        ]:
            try:
                r = _dupr_get(path, token)
                if r.status_code == 200:
                    d = r.json()
                    # Response might be wrapped in result/data
                    result = d.get("result") or d.get("data") or d
                    return result
            except Exception:
                pass
        return {}

    profile_detail = _fetch_player_profile(player_id)

    # Extract player info from matches
    player_info: dict = {"id": player_id, "name": "", "imageUrl": "", "ratings": {}}
    for m in all_matches:
        for team in m.get("teams", []):
            for pkey in ("player1", "player2"):
                p = team.get(pkey)
                if p and str(p.get("id", "")) == str(player_id):
                    player_info["name"] = p.get("fullName", "")
                    player_info["imageUrl"] = p.get("imageUrl", "") or ""
                    pmr = p.get("postMatchRating") or {}
                    player_info["ratings"] = {
                        "singles": pmr.get("singles"),
                        "doubles": pmr.get("doubles"),
                    }
                    break
            if player_info["name"]:
                break
        if player_info["name"]:
            break

    # Compute stats
    wins = losses = 0
    fmt_stats: dict[str, dict] = {
        "singles": {"wins": 0, "losses": 0},
        "doubles": {"wins": 0, "losses": 0},
        "mixed":   {"wins": 0, "losses": 0},
    }
    points_won = total_points = 0
    partners: dict[str, int] = {}
    opponents: dict[str, dict] = {}
    streak_data: list[bool] = []

    for m in all_matches:
        teams = m.get("teams", [])
        if len(teams) < 2:
            continue
        my_idx = next(
            (i for i, t in enumerate(teams)
             if any(str((p or {}).get("id","")) == str(player_id)
                    for p in [t.get("player1"), t.get("player2")])),
            -1
        )
        if my_idx < 0:
            continue
        my_team = teams[my_idx]
        opp_team = teams[1 - my_idx]
        won = my_team.get("winner") is True
        fmt = _match_format(m)

        if won:
            wins += 1
            if fmt in fmt_stats: fmt_stats[fmt]["wins"] += 1
        else:
            losses += 1
            if fmt in fmt_stats: fmt_stats[fmt]["losses"] += 1
        streak_data.append(won)

        # Points
        for g in range(1, 6):
            s_my = my_team.get(f"game{g}")
            s_opp = opp_team.get(f"game{g}")
            if s_my is not None and s_my >= 0 and s_opp is not None and s_opp >= 0:
                points_won += s_my
                total_points += s_my + s_opp

        # Partners (non-self teammates)
        for pkey in ("player1", "player2"):
            p = my_team.get(pkey)
            if p and str(p.get("id","")) != str(player_id):
                pname = p.get("fullName", "Unknown")
                partners[pname] = partners.get(pname, 0) + 1

        # Opponents
        for pkey in ("player1", "player2"):
            p = opp_team.get(pkey)
            if p and p.get("id"):
                oid = str(p["id"])
                oname = p.get("fullName", "Unknown")
                if oid not in opponents:
                    opponents[oid] = {"name": oname, "count": 0}
                opponents[oid]["count"] += 1

    # Longest win streak
    longest_streak = cur = 0
    for won in streak_data:
        cur = cur + 1 if won else 0
        longest_streak = max(longest_streak, cur)

    most_common_partner = max(partners, key=partners.get) if partners else ""
    most_common_opp = max(opponents.values(), key=lambda x: x["count"])["name"] if opponents else ""

    def wpct(w, l): return round(w / (w + l) * 100, 1) if (w + l) > 0 else None

    # Merge profile_detail into player_info
    def _extract_age(detail: dict) -> int | None:
        bd = detail.get("birthDate") or detail.get("dateOfBirth") or detail.get("dob")
        if bd:
            try:
                birth = datetime.fromisoformat(str(bd)[:10])
                today = datetime.now()
                return today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
            except Exception:
                pass
        return detail.get("age") or None

    def _extract_location(detail: dict) -> str:
        return detail.get("shortAddress") or detail.get("city") or detail.get("hometown") or ""

    gender = (profile_detail.get("gender") or profile_detail.get("sex") or "").upper()
    if gender in ("MALE", "M"): gender = "M"
    elif gender in ("FEMALE", "F"): gender = "F"
    else: gender = ""

    age = _extract_age(profile_detail)
    location = _extract_location(profile_detail)
    followers = profile_detail.get("followerCount") or profile_detail.get("followers") or 0
    following = profile_detail.get("followingCount") or profile_detail.get("following") or 0

    player_info["gender"] = gender
    player_info["age"] = age
    player_info["location"] = location
    player_info["followers"] = followers
    player_info["following"] = following
    # Use current ratings from profile detail if available (more accurate than postMatchRating)
    api_ratings = profile_detail.get("ratings") or {}
    def _parse_rating(v):
        try: return float(v)
        except (TypeError, ValueError): return None
    api_d = _parse_rating(api_ratings.get("doubles"))
    api_s = _parse_rating(api_ratings.get("singles"))
    if api_d: player_info["ratings"]["doubles"] = api_d
    if api_s: player_info["ratings"]["singles"] = api_s

    result = {
        "player": player_info,
        "stats": {
            "wins": wins, "losses": losses,
            "winPct": wpct(wins, losses),
            "singlesWins": fmt_stats["singles"]["wins"],
            "singlesLosses": fmt_stats["singles"]["losses"],
            "singlesWinPct": wpct(fmt_stats["singles"]["wins"], fmt_stats["singles"]["losses"]),
            "doublesWins": fmt_stats["doubles"]["wins"],
            "doublesLosses": fmt_stats["doubles"]["losses"],
            "doublesWinPct": wpct(fmt_stats["doubles"]["wins"], fmt_stats["doubles"]["losses"]),
            "mixedWins": fmt_stats["mixed"]["wins"],
            "mixedLosses": fmt_stats["mixed"]["losses"],
            "mixedWinPct": wpct(fmt_stats["mixed"]["wins"], fmt_stats["mixed"]["losses"]),
            "avgPointsPct": round(points_won / total_points * 100, 1) if total_points > 0 else None,
            "longestStreak": longest_streak,
            "mostCommonPartner": most_common_partner,
            "mostCommonOpponent": most_common_opp,
        },
        "matches": all_matches,
    }

    _cache[cache_key] = (time.time(), result)
    return jsonify(result)


@app.route("/api/connect/profile", methods=["GET"])
def api_connect_profile_get():
    try:
        if CONNECT_PROFILE_FILE.exists():
            return jsonify(json.loads(CONNECT_PROFILE_FILE.read_text()))
        return jsonify({})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/connect/profile", methods=["POST"])
def api_connect_profile_post():
    data = request.get_json(silent=True) or {}
    # Merge with session user ratings if available and not provided
    user = session.get("user") or {}
    profile = {
        "age": data.get("age"),
        "city": data.get("city", ""),
        "gender": data.get("gender", ""),
        "singlesRating": data.get("singlesRating") or user.get("singlesRating"),
        "doublesRating": data.get("doublesRating") or user.get("doublesRating"),
    }
    try:
        CONNECT_PROFILE_FILE.write_text(json.dumps(profile, indent=2))
        return jsonify({"ok": True, "profile": profile})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Each country has multiple search points (major pickleball cities) so we
# don't miss top players who live far from the geographic center.
REGION_COUNTRIES: dict[str, list[dict]] = {
    "North America": [
        {"name": "United States", "code": "us", "pts": [
            (38.9, -77.0,   "Washington DC"),   # Ben Johns / East Coast hub
            (30.3, -97.7,   "Austin TX"),         # major pickleball hub
            (33.7, -84.4,   "Atlanta GA"),
            (34.1, -118.2,  "Los Angeles CA"),
            (47.6, -122.3,  "Seattle WA"),
            (41.9, -87.6,   "Chicago IL"),
            (25.8, -80.2,   "Miami FL"),
            (33.4, -112.1,  "Phoenix AZ"),
            (40.7, -74.0,   "New York NY"),
            (29.8, -95.4,   "Houston TX"),
        ]},
        {"name": "Canada", "code": "ca", "pts": [
            (43.7, -79.4,  "Toronto ON"),
            (49.3, -123.1, "Vancouver BC"),
            (45.5, -73.6,  "Montreal QC"),
        ]},
        {"name": "Mexico", "code": "mx", "pts": [
            (19.4, -99.1,  "Mexico City"),
            (20.7, -103.4, "Guadalajara"),
        ]},
    ],
    "South America": [
        {"name": "Brazil",     "code": "br", "pts": [(-23.5, -46.6, "Sao Paulo"), (-22.9, -43.2, "Rio de Janeiro")]},
        {"name": "Argentina",  "code": "ar", "pts": [(-34.6, -58.4, "Buenos Aires"), (-31.4, -64.2, "Cordoba")]},
        {"name": "Colombia",   "code": "co", "pts": [(4.7, -74.1, "Bogota"), (6.2, -75.6, "Medellin")]},
        {"name": "Venezuela",  "code": "ve", "pts": [(10.5, -66.9, "Caracas")]},
        {"name": "Peru",       "code": "pe", "pts": [(-12.0, -77.0, "Lima")]},
    ],
    "Europe": [
        {"name": "United Kingdom", "code": "gb", "pts": [(51.5, -0.1, "London"), (53.5, -2.2, "Manchester")]},
        {"name": "Spain",          "code": "es", "pts": [(40.4, -3.7, "Madrid"), (41.4, 2.2, "Barcelona")]},
        {"name": "Italy",          "code": "it", "pts": [(41.9, 12.5, "Rome"), (45.5, 9.2, "Milan")]},
        {"name": "France",         "code": "fr", "pts": [(48.9, 2.3, "Paris"), (43.3, 5.4, "Marseille")]},
        {"name": "Germany",        "code": "de", "pts": [(52.5, 13.4, "Berlin"), (48.1, 11.6, "Munich")]},
    ],
    "Asia": [
        {"name": "Malaysia",    "code": "my", "pts": [(3.1, 101.7, "Kuala Lumpur"), (1.5, 103.8, "Johor Bahru")]},
        {"name": "India",       "code": "in", "pts": [(28.6, 77.2, "New Delhi"), (12.9, 77.6, "Bangalore"), (19.1, 72.9, "Mumbai")]},
        {"name": "Vietnam",     "code": "vn", "pts": [(21.0, 105.8, "Hanoi"), (10.8, 106.7, "Ho Chi Minh City")]},
        {"name": "Philippines", "code": "ph", "pts": [(14.6, 121.0, "Manila"), (10.3, 123.9, "Cebu")]},
        {"name": "South Korea", "code": "kr", "pts": [(37.6, 127.0, "Seoul"), (35.2, 129.1, "Busan")]},
    ],
    "Oceania": [
        {"name": "Australia",    "code": "au", "pts": [(-33.9, 151.2, "Sydney"), (-37.8, 145.0, "Melbourne"), (-27.5, 153.0, "Brisbane")]},
        {"name": "New Zealand",  "code": "nz", "pts": [(-36.9, 174.8, "Auckland"), (-41.3, 174.8, "Wellington")]},
    ],
    "Middle East": [
        {"name": "UAE",          "code": "ae", "pts": [(25.2, 55.3, "Dubai"), (24.5, 54.4, "Abu Dhabi")]},
        {"name": "Saudi Arabia", "code": "sa", "pts": [(24.7, 46.7, "Riyadh"), (21.5, 39.2, "Jeddah")]},
        {"name": "Qatar",        "code": "qa", "pts": [(25.3, 51.5, "Doha")]},
        {"name": "Turkey",       "code": "tr", "pts": [(41.0, 28.9, "Istanbul"), (39.9, 32.9, "Ankara")]},
        {"name": "Israel",       "code": "il", "pts": [(32.1, 34.8, "Tel Aviv"), (31.8, 35.2, "Jerusalem")]},
    ],
    "Africa": [
        {"name": "Kenya",        "code": "ke", "pts": [(-1.3, 36.8, "Nairobi")]},
        {"name": "Egypt",        "code": "eg", "pts": [(30.1, 31.2, "Cairo"), (31.2, 29.9, "Alexandria")]},
        {"name": "South Africa", "code": "za", "pts": [(-26.2, 28.0, "Johannesburg"), (-33.9, 18.4, "Cape Town")]},
        {"name": "Nigeria",      "code": "ng", "pts": [(6.5, 3.4, "Lagos"), (9.1, 7.4, "Abuja")]},
        {"name": "Morocco",      "code": "ma", "pts": [(33.6, -7.6, "Casablanca"), (34.0, -5.0, "Fes")]},
    ],
}

# Known pro players searched by name for accurate globe region data.
# At least 7 per major country so we always have a solid top-5.
# Tuple: (full name, country code).
CONTINENT_PROS: dict[str, list[tuple[str, str]]] = {
    "North America": [
        # United States — PPA / MLP pros with verified DUPRs
        ("Ben Johns", "us"),
        ("JW Johnson", "us"),
        ("Anna Leigh Waters", "us"),
        ("Tyson McGuffin", "us"),
        ("Anna Bright", "us"),
        ("Riley Newman", "us"),
        ("Zane Navratil", "us"),
        ("AJ Koller", "us"),
        ("Jessie Irvine", "us"),
        ("Hunter Johnson", "us"),
        ("Christopher Haworth", "us"),
        ("Jack Sock", "us"),
        ("Callie Smith", "us"),
        ("Lea Jansen", "us"),
        ("Matt Wright", "us"),
        ("Jay Devilliers", "us"),
        ("Jorja Johnson", "us"),
        ("Dekel Bar", "us"),
        ("DJ Young", "us"),
        ("Salome Devidze", "us"),
        # Canada
        ("Hayden Patriquin", "ca"),
        ("Catherine Parenteau", "ca"),
        ("Andreea Achim", "ca"),
        ("Zachary Schultz", "ca"),
        # Mexico
        ("Juan Navarro", "mx"),
    ],
    "South America": [
        # Argentina
        ("Federico Staksrud", "ar"),
        ("Gabriel Tardio", "ar"),
        ("Andrei Daescu", "ar"),
        ("Pablo Tellez", "ar"),
        ("Gustavo Gomez Orellana", "ar"),
        # Brazil
        ("Vinicius Font", "br"),
        ("Guilherme Melo", "br"),
        # Colombia
        ("Carlos Mogollon", "co"),
        ("Ivan Mogollon", "co"),
    ],
    "Europe": [
        # United Kingdom
        ("Christian Alshon", "gb"),
        ("Lucie Dodd", "gb"),
        ("Irina Tereschenko", "gb"),
        ("James Ignatowich", "gb"),
        ("Ben Newell", "gb"),
        # Spain
        ("Martin Sanchez Lafuente", "es"),
        ("Alejandro Ruiz", "es"),
        # France
        ("Lea Granier", "fr"),
        ("Bastian Migout", "fr"),
        # Germany
        ("Kai Schulte", "de"),
        # Italy
        ("Simone Cremona", "it"),
    ],
    "Asia": [
        # Malaysia — strongest Asian pickleball nation
        ("Amirul Hamizan", "my"),
        ("Nur Amira Izyani", "my"),
        ("Mohd Shahril Hanafiah", "my"),
        ("Lee Zii Jia", "my"),
        # India
        ("Sriram Raju", "in"),
        ("Arjun Kolte", "in"),
        # Philippines
        ("Raymund Millena", "ph"),
        # South Korea
        ("Kim Hyun Woo", "kr"),
    ],
    "Oceania": [
        # Australia
        ("Alicia Bettles", "au"),
        ("Paul Hoang", "au"),
        ("Nathan Pickard", "au"),
        ("Sashi Tripathi", "au"),
        ("Ben Foster", "au"),
        # New Zealand
        ("Andrew Dodd", "nz"),
    ],
    "Middle East": [
        # UAE
        ("Ahmed Al Mansouri", "ae"),
        ("Omar Al Hashmi", "ae"),
        # Israel
        ("Daniel Litt", "il"),
        ("Yael Greenfeld", "il"),
        # Turkey
        ("Bora Tekeli", "tr"),
        ("Ayse Kaya", "tr"),
    ],
    "Africa": [
        # South Africa — strongest African pickleball market
        ("Kyle McKenzie", "za"),
        ("Taryn Klatzow", "za"),
        ("Graeme Morrison", "za"),
        # Kenya
        ("Brian Omondi", "ke"),
        # Egypt
        ("Youssef Salem", "eg"),
        # Morocco
        ("Karim Benzara", "ma"),
    ],
}


@app.route("/api/globe/region-data")
def api_globe_region_data():
    """Name-based pro search (accurate) + geo count per country."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    region = request.args.get("region", "").strip()
    if not region or region not in REGION_COUNTRIES:
        return jsonify({"error": f"Unknown region: {region}"}), 400

    cache_key = f"region_data5:{region}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 1800:
        return jsonify(cached[1])

    countries = REGION_COUNTRIES[region]
    known_pros = CONTINENT_PROS.get(region, [])

    hits_by_code: dict[str, list] = {c["code"]: [] for c in countries}
    seen_ids:     dict[str, set]  = {c["code"]: set() for c in countries}
    count_by_code: dict[str, int] = {c["code"]: 0    for c in countries}

    # ── Named search: finds the exact pro regardless of geography ──
    def _search_pro(name: str, code: str):
        try:
            resp = _dupr_post("/player/v1.0/search", token, {"filter": {}, "query": name, "limit": 10})
            if resp.status_code != 200:
                return code, []
            hits = resp.json().get("result", {}).get("hits", [])
            name_lower = name.lower()
            best, best_r = None, -1.0
            for h in hits:
                hn = _player_name(h).lower()
                r  = _extract_ratings(h)
                hr = (r["doublesRating"] or r["singlesRating"] or 0)
                name_match = (hn == name_lower) or (name_lower in hn) or (hn in name_lower)
                if name_match and hr > best_r:
                    best, best_r = h, hr
            return code, ([best] if best else [])
        except Exception:
            pass
        return code, []

    # ── Geo search: fills player list + gives real player count ──
    # Run 8 letter queries per country from primary city; results go into
    # hits_by_code (for top players) and count_by_code (unique player count).
    GEO_LETTERS = ['a', 'e', 'i', 'j', 'm', 'r', 's', 't']

    def _search_geo_fill(code: str, lat: float, lng: float, loc: str, q: str):
        try:
            body = {"filter": {"lat": lat, "lng": lng, "locationText": loc, "rating": {}},
                    "query": q, "limit": 100, "offset": 0, "includeUnclaimedPlayers": True}
            resp = _dupr_post("/player/v1.0/search", token, body)
            if resp.status_code == 200:
                result = resp.json().get("result", {})
                hits = result.get("hits", []) if isinstance(result, dict) else []
                return code, hits
        except Exception:
            pass
        return code, []

    pro_tasks = list(known_pros)
    geo_tasks = [(c["code"], c["pts"][0][0], c["pts"][0][1], c["pts"][0][2], q)
                 for c in countries if c.get("pts")
                 for q in GEO_LETTERS]

    all_tasks = len(pro_tasks) + len(geo_tasks)
    with ThreadPoolExecutor(max_workers=min(120, all_tasks + 1)) as ex:
        pro_futs = {ex.submit(_search_pro, name, code): "pro" for name, code in pro_tasks}
        geo_futs = {ex.submit(_search_geo_fill, *t): "geo" for t in geo_tasks}

        for f in as_completed(list(pro_futs) + list(geo_futs)):
            code, hits = f.result()
            if code not in seen_ids:
                continue
            for h in (hits or []):
                pid = str(h.get("id", ""))
                if pid and pid not in seen_ids[code]:
                    seen_ids[code].add(pid)
                    hits_by_code[code].append(h)
                    count_by_code[code] += 1

    today = datetime.now()
    country_results: list[dict] = []
    all_rated: list[dict] = []

    for c in countries:
        code = c["code"]
        players: list[dict] = []
        for h in hits_by_code[code]:
            r  = _extract_ratings(h)
            dr, sr = r["doublesRating"], r["singlesRating"]
            # Sort key: doubles first, singles fallback (matches what's displayed)
            sort_rating = dr or sr
            if not sort_rating:
                continue
            age = h.get("age")
            if age is None:
                bd = h.get("birthDate") or h.get("dateOfBirth")
                if bd:
                    try:
                        b   = datetime.fromisoformat(str(bd)[:10])
                        age = today.year - b.year - ((today.month, today.day) < (b.month, b.day))
                    except Exception:
                        pass
            players.append({
                "id": str(h.get("id", "")),
                "name": _player_name(h),
                "doublesRating": dr,
                "singlesRating": sr,
                "bestRating": sort_rating,
                "age": age,
                "imageUrl": h.get("imageUrl", ""),
                "country": c["name"],
                "countryCode": code,
            })

        players.sort(key=lambda x: x["bestRating"], reverse=True)
        all_rated.extend(players)
        country_results.append({
            "name": c["name"],
            "code": code,
            "playerCount": count_by_code.get(code, 0),
            "topPlayers": players[:5],
        })

    country_results.sort(key=lambda x: x["playerCount"], reverse=True)
    all_rated.sort(key=lambda x: x["bestRating"], reverse=True)

    result = {
        "region": region,
        "topPlayer": all_rated[0] if all_rated else None,
        "countries": country_results,
    }
    _cache[cache_key] = (time.time(), result)
    return jsonify(result)


@app.route("/api/globe/players", methods=["GET"])
def api_globe_players():
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    region = request.args.get("region", "").strip()
    player_names = GLOBE_REGION_PLAYERS.get(region)
    if not player_names:
        return jsonify({"error": f"Unknown region: {region}"}), 400

    results = []
    for name in player_names:
        try:
            resp = _dupr_post("/player/v1.0/search", token, {
                "filter": {}, "query": name, "limit": 5,
            })
            if resp.status_code != 200:
                continue
            hits = resp.json().get("result", {}).get("hits", [])
            if not hits:
                continue
            # Pick best match by name
            best = None
            best_rating = -1
            name_lower = name.lower()
            for h in hits:
                h_name = _player_name(h).lower()
                r = _extract_ratings(h)
                h_rating = r["rating"] or 0
                if h_name == name_lower or name_lower in h_name:
                    if h_rating > best_rating:
                        best = h
                        best_rating = h_rating
            if not best:
                best = hits[0]
            r = _extract_ratings(best)
            results.append({
                "id": str(best.get("id", "")),
                "name": _player_name(best),
                "doublesRating": r["doublesRating"],
                "singlesRating": r["singlesRating"],
                "imageUrl": best.get("imageUrl", ""),
            })
        except Exception:
            continue

    return jsonify({"region": region, "players": results})


@app.route("/api/connect/search", methods=["POST"])
def api_connect_search():
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    city = data.get("city", "").strip()
    user_age = data.get("age")
    genders = data.get("genders", [])  # list of "M", "F", or both
    rating_type = data.get("rating_type", "doubles")
    user_rating = data.get("user_rating")
    user_age_val = None
    try:
        user_age_val = float(user_age) if user_age is not None else None
    except (TypeError, ValueError):
        pass
    user_rating_val = None
    try:
        user_rating_val = float(user_rating) if user_rating is not None else None
    except (TypeError, ValueError):
        pass

    if not city:
        return jsonify({"error": "City is required"}), 400

    city_key = city.split(",")[0].strip().lower()
    cluster = CITY_CLUSTERS.get(city_key, {})
    close_cities = cluster.get("close", [])
    far_cities = cluster.get("far", [])

    # --- Geocode all cities in parallel ---
    def _geocode(c):
        try:
            r = requests.get("https://nominatim.openstreetmap.org/search",
                params={"q": c, "format": "json", "limit": 1},
                headers={"User-Agent": "dupr-feed/1.0"}, timeout=5)
            if r.status_code == 200:
                d = r.json()
                if d:
                    return float(d[0]["lat"]), float(d[0]["lon"]), d[0].get("display_name", c)
        except Exception:
            pass
        return None

    all_cities = [(city, "main")] + [(c, "close") for c in close_cities] + [(c, "far") for c in far_cities]
    geocoded: dict[str, tuple] = {}  # city_str -> (lat, lng, loc_text, tier)

    with ThreadPoolExecutor(max_workers=len(all_cities)) as ex:
        geo_futures = {ex.submit(_geocode, c): (c, tier) for c, tier in all_cities}
        for f in as_completed(geo_futures):
            c, tier = geo_futures[f]
            result = f.result()
            if result:
                geocoded[c] = (*result, tier)

    if city not in geocoded:
        return jsonify({"error": "Could not find that city. Try a different format (e.g. 'Raleigh, NC')."}), 400

    app.logger.warning(f"Connect search: {city!r} + {len(geocoded) - 1} nearby cities")

    # --- Parallel alphabet searches across all geocoded cities ---
    import string
    letters = list(string.ascii_lowercase)

    def _search_letter_city(q, lat, lng, loc_text):
        try:
            body = {"filter": {"lat": lat, "lng": lng, "locationText": loc_text, "rating": {}},
                    "query": q, "limit": 25, "offset": 0, "includeUnclaimedPlayers": True}
            resp = _dupr_post("/player/v1.0/search", token, body)
            if resp.status_code == 200:
                result = resp.json().get("result", {})
                return result.get("hits", []) if isinstance(result, dict) else []
        except Exception:
            pass
        return []

    tasks = []
    for city_str, (lat, lng, loc_text, tier) in geocoded.items():
        city_label = city_str.split(",")[0].strip()
        for q in letters:
            tasks.append((q, lat, lng, loc_text, tier, city_label))

    seen_ids: set = set()
    hits_with_tier: list = []  # (hit, tier, city_label)

    with ThreadPoolExecutor(max_workers=min(80, len(tasks))) as ex:
        fut_map = {ex.submit(_search_letter_city, q, lat, lng, loc): (tier, lbl)
                   for q, lat, lng, loc, tier, lbl in tasks}
        for f in as_completed(fut_map):
            tier, city_label = fut_map[f]
            for h in (f.result() or []):
                pid = str(h.get("id", ""))
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    hits_with_tier.append((h, tier, city_label))

    app.logger.warning(f"Connect search: {len(hits_with_tier)} total players across all cities")

    if not hits_with_tier:
        return jsonify({"results": [], "message": "No DUPR players found in that area."})

    scored = []
    now = datetime.now(timezone.utc)

    for h, tier, city_label in hits_with_tier:
        h_id = str(h.get("id", ""))
        h_name = _player_name(h)
        r = _extract_ratings(h)

        player_rating = r["singlesRating"] if rating_type == "singles" else r["doublesRating"]
        has_rating = player_rating is not None

        # Skip players with no rating in the requested format
        if not has_rating:
            continue

        rating_diff = abs(user_rating_val - player_rating) if user_rating_val is not None else None

        # Far-city gate: only include if DUPR diff is tight enough
        if tier == "far" and (rating_diff is None or rating_diff > FAR_MAX_RATING_DIFF):
            continue

        if user_rating_val is not None:
            # Blend closeness (wider 3.0 window) + absolute rating level so that
            # when no one is near the target, highest-rated players still rank first
            closeness = max(0.0, 1.0 - rating_diff / 3.0)
            normalized = min(player_rating / 8.0, 1.0)
            rating_score = 0.70 * closeness + 0.30 * normalized
        else:
            rating_score = min(player_rating / 8.0, 1.0)

        player_age = h.get("age")
        if player_age is None:
            bd = h.get("birthDate") or h.get("dateOfBirth")
            if bd:
                try:
                    birth = datetime.fromisoformat(str(bd)[:10])
                    today = datetime.now()
                    player_age = today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
                except Exception:
                    pass
        try:
            player_age_val = float(player_age) if player_age is not None else None
        except (TypeError, ValueError):
            player_age_val = None

        age_score = max(0.0, 1.0 - abs(user_age_val - player_age_val) / 15.0) if (user_age_val and player_age_val) else 0.5

        recent_matches = h.get("recentMatches") or h.get("matchCount30Days") or 0
        activity_score = min(float(recent_matches), 10.0) / 10.0

        first_match = h.get("firstMatchDate") or h.get("memberSince") or ""
        experience_score = 0.5
        if first_match:
            try:
                fm_date = datetime.fromisoformat(str(first_match)[:10]).replace(tzinfo=timezone.utc)
                months = (now - fm_date).days / 30.0
                experience_score = min(months, 60.0) / 60.0
            except Exception:
                pass

        total_score = 0.90 * rating_score + 0.06 * age_score + 0.02 * activity_score + 0.02 * experience_score

        # Far-city score penalty
        if tier == "far":
            total_score *= FAR_SCORE_MULTIPLIER

        if genders and len(genders) < 2:
            player_gender = (h.get("gender") or h.get("sex") or "").upper()
            if player_gender in ("MALE", "M"):
                player_gender = "M"
            elif player_gender in ("FEMALE", "F"):
                player_gender = "F"
            if player_gender and player_gender not in [g.upper() for g in genders]:
                continue

        scored.append({
            "id": h_id,
            "name": h_name,
            "doublesRating": r["doublesRating"],
            "singlesRating": r["singlesRating"],
            "imageUrl": h.get("imageUrl", ""),
            "age": player_age,
            "gender": h.get("gender", ""),
            "city": city_label,
            "score": round(total_score * 100),
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return jsonify({"results": scored[:50]})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# For Joe — Azalea Classic bracket lookup
# ---------------------------------------------------------------------------

FOR_JOE_TEAMS = {
    "pool1": [
        ("Ryan Favorito", "Michael Favorito"),
        ("Josh Massey", "Bruik Tucker"),
        ("Christopher Sells", "Stephen Goff"),
        ("Zachary Herrmann", "Clayton Walsh"),
        ("Reese Lopez", "Justin Wardell"),
        ("Logan Kaboski", "Benjamin Powell"),
    ],
    "pool2": [
        ("Jake McSwain", "Stephen Katulak"),
        ("Stephen Prior", "Chad Turner"),
        ("Cody Wilson", "Jason Beasley"),
        ("Charles Vassallo", "Jason Goodwin"),
        ("Jensen Smith", "Matt Vogel"),
        ("Owen Mason", "Tyler Mason"),
    ],
}
# Flat list for parallel search
FOR_JOE_PLAYERS = [name for pool in FOR_JOE_TEAMS.values() for pair in pool for name in pair]


def _find_joe_player(name: str, token: str) -> dict:
    """Search DUPR for one player, pick best NC + 3.0–4.5 match."""
    try:
        resp = _dupr_post("/player/v1.0/search", token, {"filter": {}, "query": name, "limit": 10})
        if resp.status_code != 200:
            return {"search_name": name, "found": False}
        hits = (resp.json().get("result") or {}).get("hits") or []
    except Exception:
        return {"search_name": name, "found": False}

    def _rating_in_range(r):
        for v in [r.get("doublesRating"), r.get("singlesRating")]:
            if isinstance(v, (int, float)) and 3.0 <= v <= 4.5:
                return True
        return False

    # Pre-filter by rating range using search result data
    candidates = []
    for h in hits:
        r = _extract_ratings(h)
        h["_r"] = r
        if _rating_in_range(r):
            candidates.append(h)

    # If nothing in range, include all hits (will be shown as "not confirmed")
    pool = candidates if candidates else hits

    # Fetch profiles in parallel to get city
    def _get_loc(h):
        pid = str(h.get("id", ""))
        try:
            pr = _dupr_get(f"/player/v1.0/{pid}", token)
            if pr.status_code == 200:
                det = pr.json().get("result") or {}
                loc = (det.get("shortAddress") or det.get("city") or
                       det.get("hometown") or det.get("location") or "")
                return pid, loc
        except Exception:
            pass
        return pid, ""

    with ThreadPoolExecutor(max_workers=10) as ex:
        loc_map = dict(ex.map(_get_loc, pool))

    # Score each candidate: NC + in range = best
    scored = []
    for h in pool:
        pid = str(h.get("id", ""))
        loc = loc_map.get(pid, "")
        r = h.get("_r") or _extract_ratings(h)
        is_nc = "nc" in loc.lower() or "north carolina" in loc.lower()
        in_range = _rating_in_range(r)
        priority = (2 if (is_nc and in_range) else 1 if is_nc else 0 if in_range else -1)
        scored.append({
            "id": pid,
            "name": _player_name(h),
            "doublesRating": r["doublesRating"],
            "singlesRating": r["singlesRating"],
            "city": loc,
            "imageUrl": h.get("imageUrl", ""),
            "confirmed": is_nc and in_range,
            "priority": priority,
        })

    scored.sort(key=lambda x: -x["priority"])

    # Return top match + any equally-good alternatives
    if not scored:
        return {"search_name": name, "found": False}

    best_priority = scored[0]["priority"]
    matches = [s for s in scored if s["priority"] == best_priority]

    result = scored[0].copy()
    result["search_name"] = name
    result["found"] = True
    result["ambiguous"] = len(matches) > 1
    result["alternatives"] = matches[1:3] if len(matches) > 1 else []
    del result["priority"]
    return result


@app.route("/api/joe-players")
def api_joe_players():
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    # Search all players in parallel
    with ThreadPoolExecutor(max_workers=24) as ex:
        all_results = {r["search_name"]: r for r in ex.map(lambda name: _find_joe_player(name, token), FOR_JOE_PLAYERS)}

    # Restructure into teams by pool
    output = {}
    for pool_key, teams in FOR_JOE_TEAMS.items():
        output[pool_key] = []
        for p1_name, p2_name in teams:
            output[pool_key].append({
                "p1": all_results.get(p1_name, {"search_name": p1_name, "found": False}),
                "p2": all_results.get(p2_name, {"search_name": p2_name, "found": False}),
            })

    return jsonify(output)


@app.route("/api/debug/rating-filter")
def debug_rating_filter():
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    results = {}
    for key in ["minRating", "min_rating", "ratingMin", "doublesRatingMin", "rating_min", "minDoubles"]:
        max_key = key.replace("Min", "Max").replace("min", "max")
        try:
            r = _dupr_post("/player/v1.0/search", token, {
                "filter": {key: 4.1, max_key: 4.7},
                "query": "a", "limit": 5
            })
            d = r.json() if r.status_code == 200 else {}
            hits = (d.get("result") or {}).get("hits", [])
            ratings = [_extract_ratings(h)["doublesRating"] for h in hits]
            results[key] = {"status": r.status_code, "hits": len(hits), "ratings": ratings}
        except Exception as e:
            results[key] = {"error": str(e)}
    return jsonify(results)


@app.route("/api/debug/location-search")
def debug_location_search():
    """Test various DUPR filter/endpoint combos for location-based search."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    results = {}

    # Test 1-7: different filter param names on /player/v1.0/search
    for key in ["location", "city", "state", "hometown", "address", "region", "zip"]:
        try:
            r = _dupr_post("/player/v1.0/search", token, {"filter": {key: "Raleigh"}, "query": "a", "limit": 5})
            d = r.json() if r.status_code == 200 else {}
            hits = (d.get("result") or {}).get("hits", [])
            locs = []
            for h in hits[:3]:
                pid = str(h.get("id", ""))
                pr = _dupr_get(f"/player/v1.0/{pid}", token)
                if pr.status_code == 200:
                    det = pr.json().get("result") or {}
                    locs.append(det.get("shortAddress") or det.get("city") or "?")
            results[f"filter_{key}"] = {"status": r.status_code, "hits": len(hits), "sample_locs": locs}
        except Exception as e:
            results[f"filter_{key}"] = {"error": str(e)}

    # Test 8: leaderboard endpoint
    for path in ["/player/v1.0/leaderboard", "/player/v1.0/rankings"]:
        try:
            r = _dupr_get(f"{path}?city=Raleigh&limit=5", token)
            results[path] = {"status": r.status_code, "body": r.text[:200]}
        except Exception as e:
            results[path] = {"error": str(e)}

    # Test 9: club search
    try:
        r = _dupr_post("/club/v1.0/search", token, {"query": "Raleigh", "limit": 5})
        results["club_search"] = {"status": r.status_code, "body": r.text[:200]}
    except Exception as e:
        results["club_search"] = {"error": str(e)}

    return jsonify(results)


@app.route("/api/debug/history/<player_id>")
def debug_history(player_id):
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401
    body = {"filters": {}, "limit": 2, "offset": 0, "sort": {"order": "DESC", "parameter": "MATCH_DATE"}}
    resp = _dupr_post(f"/player/v1.0/{player_id}/history", token, body)
    return resp.text, resp.status_code, {"Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5001))
    app.run(debug=True, host="0.0.0.0", port=port)
