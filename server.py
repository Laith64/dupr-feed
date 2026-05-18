"""DUPR Feed — pickleball activity timeline."""

import json
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request, session

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-change-me")

DUPR_BASE = "https://api.dupr.gg"
WATCHES_DIR = Path(__file__).parent / "watchlists"
WATCHES_DIR.mkdir(exist_ok=True)
GROUPS_DIR = Path(__file__).parent / "groups"
GROUPS_DIR.mkdir(exist_ok=True)
CONNECT_PROFILE_FILE = Path(__file__).parent / "connect_profile.json"
EVENTS_LOG = Path(__file__).parent / "events.jsonl"
_events_lock = threading.Lock()


def _log_event(event_type: str, **fields) -> None:
    """Append a single event to events.jsonl — never raises."""
    try:
        try:
            sid = session.get("sid") or "?"
        except Exception:
            sid = "?"
        try:
            ip = (request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
                  or request.remote_addr or "?")
            ua = (request.headers.get("User-Agent", "") or "")[:80]
        except Exception:
            ip, ua = "?", ""
        rec = {"ts": int(time.time()), "sid": sid, "ip": ip, "ua": ua, "type": event_type}
        for k, v in fields.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                rec[k] = v if not isinstance(v, str) else v[:200]
        with _events_lock:
            with EVENTS_LOG.open("a") as f:
                f.write(json.dumps(rec) + "\n")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Global DUPR token (shared by all visitors)
# ---------------------------------------------------------------------------
_global_token: str = ""
_global_token_ts: float = 0.0
_token_lock = threading.Lock()
TOKEN_MAX_AGE = 1800  # refresh after 30 min

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

# Persistent club details cache. Value: {"shortAddress": str|None, "mediaUrl": str|None} or None on failure.
_club_info_cache: dict[str, dict | None] = {}

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

def _authenticate() -> str:
    """Authenticate with DUPR using env-var credentials, return JWT."""
    email = os.getenv("DUPR_EMAIL", "")
    password = os.getenv("DUPR_PASSWORD", "")
    if not email or not password:
        return os.getenv("DUPR_TOKEN", "")  # bare-token fallback
    try:
        resp = requests.post(
            f"{DUPR_BASE}/auth/v1.0/login/",
            json={"email": email, "password": password},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"[AUTH] DUPR login failed: {resp.status_code}", flush=True)
            return ""
        body = resp.json()
        token = (body.get("result") or body.get("data") or body).get(
            "accessToken",
            (body.get("result") or body.get("data") or body).get("token", ""),
        )
        if not token:
            token = body.get("accessToken", body.get("token", ""))
        return token or ""
    except Exception as e:
        print(f"[AUTH] error: {e}", flush=True)
        return ""


def _ensure_token(force: bool = False) -> str:
    """Return a valid global DUPR token, refreshing if needed."""
    global _global_token, _global_token_ts
    with _token_lock:
        if not force and _global_token and (time.time() - _global_token_ts < TOKEN_MAX_AGE):
            return _global_token
        _global_token = _authenticate()
        _global_token_ts = time.time()
        return _global_token


def _get_token() -> str:
    """Return the shared DUPR token."""
    return _ensure_token()


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _dupr_get(path: str, token: str) -> requests.Response:
    return requests.get(f"{DUPR_BASE}{path}", headers=_headers(token), timeout=15)


def _dupr_post(path: str, token: str, body: dict) -> requests.Response:
    return requests.post(f"{DUPR_BASE}{path}", headers=_headers(token), json=body, timeout=15)


def _get_sid() -> str:
    """Return per-visitor session ID, creating one if needed."""
    if "sid" not in session:
        session["sid"] = uuid.uuid4().hex
    return session["sid"]


def _watches_path(sid: str) -> Path:
    return WATCHES_DIR / f"{sid}.json"


def _load_watches(sid: str | None = None) -> list[dict]:
    sid = sid or _get_sid()
    path = _watches_path(sid)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return []
    # First visit — seed with defaults
    _seed_default_watches(sid)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_watches(watches: list[dict], sid: str | None = None):
    sid = sid or _get_sid()
    _watches_path(sid).write_text(json.dumps(watches, indent=2))


def _groups_path(sid: str) -> Path:
    return GROUPS_DIR / f"{sid}.json"


def _load_user_groups(sid: str | None = None) -> list[dict]:
    sid = sid or _get_sid()
    path = _groups_path(sid)
    if path.exists():
        try:
            data = json.loads(path.read_text())
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_user_groups(groups: list[dict], sid: str | None = None):
    sid = sid or _get_sid()
    _groups_path(sid).write_text(json.dumps(groups, indent=2))


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
    "Itziar Rios",
    "Drew Sandri",
    "Laith Alkaissi",
    "Joseph Rojas",
    "Alex Liu",
    "Matthew Smith",
    "Kenai Rios",
    "Zander Gillentine",
    "Kenneth Suarez",
    "Tyler Raybin",
    "Vidusha",
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


# Pre-built default watch entries — hardcoded so every visitor sees the same sidebar
_HARDCODED_WATCHES = [
    {"id":"5374679100","name":"Itziar Rios","rating":5.094,"doublesRating":5.094,"singlesRating":4.779,"imageUrl":"/static/itziar_selkirk.png"},
    {"id":"5041179815","name":"Drew Sandri","rating":4.908,"doublesRating":4.908,"singlesRating":4.224,"imageUrl":"/static/drew_sandri.jpg"},
    {"id":"7213071415","name":"Laith Alkaissi","rating":4.027,"doublesRating":None,"singlesRating":4.027,"imageUrl":""},
    {"id":"7000134365","name":"Joseph Rojas","rating":4.554,"doublesRating":4.554,"singlesRating":4.91,"imageUrl":"/static/joseph.jpg"},
    {"id":"4817656960","name":"Alex Liu","rating":4.799,"doublesRating":4.799,"singlesRating":4.652,"imageUrl":""},
    {"id":"4922492947","name":"Matthew Smith","rating":5.187,"doublesRating":5.187,"singlesRating":4.988,"imageUrl":""},
    {"id":"8508956296","name":"Kenai Rios","rating":4.972,"doublesRating":4.972,"singlesRating":4.106,"imageUrl":"https://dupr.s3.us-east-1.amazonaws.com/images/f1b73fab-11c6-4ea6-8f8c-83bb9e22d980.jpg"},
    {"id":"6772003357","name":"Zander Gillentine","rating":4.354,"doublesRating":4.354,"singlesRating":4.428,"imageUrl":"/static/zander.jpg"},
    {"id":"5323340009","name":"Kenneth Suarez","rating":5.058,"doublesRating":5.058,"singlesRating":4.617,"imageUrl":"/static/kenneth.jpg"},
    {"id":"4743016718","name":"Tyler Raybin","rating":4.958,"doublesRating":4.958,"singlesRating":None,"imageUrl":""},
    {"id":"7140133603","name":"Vidusha","rating":4.347,"doublesRating":4.347,"singlesRating":4.488,"imageUrl":""},
]


def _resolve_default_watches() -> list[dict]:
    """Return the hardcoded default watch list."""
    return list(_HARDCODED_WATCHES)


def _seed_default_watches(sid: str):
    """Write the default watch list for a new visitor session."""
    path = _watches_path(sid)
    if path.exists():
        return
    defaults = _resolve_default_watches()
    if defaults:
        path.write_text(json.dumps(defaults, indent=2))


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


def _build_feed(token: str, sid: str | None = None) -> dict:
    """Build the merged, sorted feed for all followed/watched players."""
    sid = sid or "anon"
    cache_key = f"feed:{sid}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < CACHE_TTL:
        return cached[1]

    # Collect player IDs from DUPR following + local watches
    following = _get_following(token)
    watches = _load_watches(sid)

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
        elif pid and pid in player_map and w.get("imageUrl"):
            player_map[pid]["imageUrl"] = w["imageUrl"]

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

    # Interleave: within each date, spread matches so the same player
    # doesn't appear in consecutive cards (round-robin by player per date).
    from itertools import groupby
    interleaved = []
    for _date, group in groupby(all_matches, key=sort_key):
        by_player: dict[str, list] = {}
        for m in group:
            pid = m.get("_playerInfo", {}).get("id", "")
            by_player.setdefault(pid, []).append(m)
        # Round-robin across players
        queues = list(by_player.values())
        idx = 0
        while queues:
            if idx >= len(queues):
                idx = 0
            if queues[idx]:
                interleaved.append(queues[idx].pop(0))
                idx += 1
            else:
                queues.pop(idx)

    # Build clubMeta — top N unique clubIds in the feed → {name, image, short, members}.
    # The Clubs Near Me overlay reads this to put real pickleball photos on cards and
    # show "city, ST" + member count without re-fetching per render.
    feed_club_ids: list[str] = []
    seen_cids: set[str] = set()
    for m in interleaved[:300]:
        cid = m.get("clubId")
        if cid is None: continue
        cid_s = str(cid)
        if cid_s in seen_cids: continue
        seen_cids.add(cid_s)
        feed_club_ids.append(cid_s)
        # Resolve every unique clubId in the feed (capped just to bound parallelism).
        # /club/v1.0/{id} responses are cached forever, so this is cheap on warm runs.
        if len(feed_club_ids) >= 120: break

    def _fetch_club_meta(cid: str) -> dict | None:
        if cid in _club_info_cache:
            return _club_info_cache[cid]
        try:
            r = _dupr_get(f"/club/v1.0/{cid}", token)
            if r.status_code == 200:
                res = (r.json().get("result") or {})
                info = {
                    "shortAddress": res.get("shortAddress") or None,
                    "mediaUrl": res.get("mediaUrl") or res.get("logoUrl") or res.get("imageUrl") or None,
                    "name": res.get("clubName") or res.get("name") or None,
                    "memberCount": res.get("clubMemberCount"),
                }
                _club_info_cache[cid] = info
                return info
        except Exception as exc:
            app.logger.warning("club lookup failed cid=%s err=%s", cid, exc)
        _club_info_cache[cid] = None
        return None

    to_fetch_feed = [cid for cid in feed_club_ids if cid not in _club_info_cache]
    if to_fetch_feed:
        with ThreadPoolExecutor(max_workers=min(12, len(to_fetch_feed))) as executor:
            list(executor.map(_fetch_club_meta, to_fetch_feed))

    club_meta: dict[str, dict] = {}
    for cid in feed_club_ids:
        info = _club_info_cache.get(cid) or {}
        if not info: continue
        club_meta[cid] = {
            "name": info.get("name") or "",
            "image": info.get("mediaUrl") or "",
            "short": info.get("shortAddress") or "",
            "members": info.get("memberCount") or 0,
        }

    result = {
        "matches": interleaved[:300],
        "players": list(player_map.values()),
        "clubMeta": club_meta,
    }
    _cache[cache_key] = (time.time(), result)
    return result






# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/preview/events")
def preview_events():
    """Design preview — standalone events page mock. Not part of main app."""
    try:
        path = Path(__file__).parent / "design-samples" / "events-page.html"
        return Response(path.read_text(encoding="utf-8"), mimetype="text/html")
    except FileNotFoundError:
        return Response("events-page.html not found in design-samples/", status=404)


@app.route("/")
def index():
    _get_sid()  # ensure session ID exists
    _ref_param = request.args.get("ref", "")
    _ref_hdr = request.headers.get("Referer", "-")
    _log_event("visit", ref_param=_ref_param, referer=_ref_hdr[:200])
    if _ref_param:
        _ip = (request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote_addr or "?")
        _ua = (request.headers.get("User-Agent", "")[:80])
        print(f"[VISIT ip={_ip} ua={_ua!r} ref_param={_ref_param!r} referer={_ref_hdr!r}]", flush=True)
    return render_template("index.html", show_onboarding=not bool(session.get("onboarded", False)))


# @app.route("/login")
# def login_page():
#     return render_template("index.html", show_login=True)


# --- Login route commented out (credentials now in env vars) ---
# @app.route("/api/login", methods=["POST"])
# def api_login():
#     data = request.get_json(silent=True) or {}
#     email = data.get("email", "").strip()
#     password = data.get("password", "")
#     if not email or not password:
#         return jsonify({"error": "Email and password are required"}), 400
#     try:
#         resp = requests.post(f"{DUPR_BASE}/auth/v1.0/login/",
#                              json={"email": email, "password": password}, timeout=15)
#     except requests.RequestException as e:
#         return jsonify({"error": f"Could not reach DUPR: {e}"}), 502
#     if resp.status_code != 200:
#         msg = "Invalid credentials"
#         try: msg = resp.json().get("message", msg)
#         except Exception: pass
#         return jsonify({"error": msg}), resp.status_code
#     body = resp.json()
#     token = body.get("result", body.get("data", body)).get("accessToken",
#         body.get("result", body.get("data", body)).get("token", ""))
#     if not token: token = body.get("accessToken", body.get("token", ""))
#     if not token:
#         return jsonify({"error": "Login succeeded but no token was returned"}), 500
#     session["token"] = token
#     session["email"] = email
#     try:
#         profile_resp = _dupr_get("/user/v1.0/profile/", token)
#         if profile_resp.status_code == 200:
#             profile = profile_resp.json()
#             user_data = profile.get("result", profile.get("data", profile))
#             session["user"] = {
#                 "id": str(user_data.get("id", "")),
#                 "name": _player_name(user_data),
#                 "email": email,
#                 "doublesRating": user_data.get("doublesRating"),
#                 "singlesRating": user_data.get("singlesRating"),
#                 "imageUrl": user_data.get("imageUrl", ""),
#                 "age": user_data.get("age"),
#                 "location": _format_location(user_data),
#             }
#     except Exception:
#         session["user"] = {"name": email, "email": email}
#     _seed_default_watches(token)
#     return jsonify({"ok": True, "user": session.get("user", {})})


@app.route("/api/me")
def api_me():
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401
    email = os.getenv("DUPR_EMAIL", "").strip()
    if not email:
        return jsonify({"error": "no email configured"}), 400
    # Search DUPR for the logged-in user's own profile
    cached = _cache.get("me_profile")
    if cached and time.time() - cached[0] < 3600:
        return jsonify(cached[1])
    try:
        resp = _dupr_post("/player/v1.0/search", token, {
            "filter": {}, "query": email, "limit": 5, "offset": 0, "includeUnclaimedPlayers": True
        })
        if resp.status_code == 200:
            hits = resp.json().get("result", {}).get("hits", [])
            if hits:
                h = hits[0]
                r = _extract_ratings(h)
                result = {
                    "id": str(h.get("id", "")),
                    "name": _player_name(h),
                    "doublesRating": r["doublesRating"],
                    "singlesRating": r["singlesRating"],
                }
                _cache["me_profile"] = (time.time(), result)
                return jsonify(result)
    except Exception:
        pass
    return jsonify({"error": "not found"}), 404


@app.route("/api/feed")
def api_feed():
    token = _get_token()
    if not token:
        # Try to re-authenticate
        token = _ensure_token(force=True)
    if not token:
        return jsonify({"error": "Server could not authenticate with DUPR"}), 503

    sid = _get_sid()
    _log_event("feed")
    result = _build_feed(token, sid)

    if result.get("error") == "unauthorized":
        # Token expired — force refresh and retry once
        token = _ensure_token(force=True)
        if token:
            result = _build_feed(token, sid)

    if result.get("error") == "unauthorized":
        return jsonify({"error": "DUPR authentication failed"}), 503

    return jsonify(result)


@app.route("/api/search", methods=["POST"])
def api_search():
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = data.get("query", "").strip()
    ensure_ids = [str(i) for i in data.get("ensureIds", [])]  # watch-list IDs to always include
    location_filter = data.get("location", "").strip()
    gender_filter = data.get("gender", "").strip()       # "MALE" or "FEMALE"
    result_limit = data.get("resultLimit", 100)            # max profiles to fetch
    age_min = data.get("ageMin")                          # int or None
    age_max = data.get("ageMax")                          # int or None
    rating_min = data.get("ratingMin")                    # float or None
    rating_max = data.get("ratingMax")                    # float or None
    if not query and not location_filter:
        return jsonify({"results": []})

    cache_key = f"search:{query.lower()}:{location_filter.lower()}:{gender_filter}:{age_min}:{age_max}:{rating_min}:{rating_max}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 60:
        cached_results = cached[1]
        # Ensure watch-list players are in cached results (may not have been in original search)
        cached_ids = {p["id"] for p in cached_results}
        missing_ids = [i for i in ensure_ids if i not in cached_ids]
        if not missing_ids:
            return jsonify({"results": cached_results})
        # Fall through to fetch missing profiles

    # Geocode location filter if provided
    search_filter = {}
    if location_filter:
        try:
            geo_resp = requests.get("https://nominatim.openstreetmap.org/search",
                params={"q": location_filter, "format": "json", "limit": 1},
                headers={"User-Agent": "dupr-feed/1.0"}, timeout=5)
            if geo_resp.status_code == 200:
                geo_data = geo_resp.json()
                if geo_data:
                    search_filter["lat"] = float(geo_data[0]["lat"])
                    search_filter["lng"] = float(geo_data[0]["lon"])
                    search_filter["locationText"] = geo_data[0].get("display_name", location_filter)
        except Exception:
            pass

    # DUPR API only supports lat/lng/locationText in filter; age/gender/rating are client-side
    # Ensure rating key exists in filter (DUPR API requires it when geo-filtering)
    if "lat" in search_filter and "rating" not in search_filter:
        search_filter["rating"] = {}
    _ip = (request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote_addr or "?")
    _ua = (request.headers.get("User-Agent", "")[:80])
    _ref = request.headers.get("Referer", "-")
    print(f"[SEARCH ip={_ip} ua={_ua!r} ref={_ref!r}] filter={search_filter}, query={query!r}, location={location_filter!r}", flush=True)
    _log_event("search", query=query, location=location_filter, has_geo=bool(search_filter.get("lat")))

    def _search_dupr(q, limit=25, offset=0):
        b = {"filter": search_filter, "query": q, "limit": limit, "offset": offset, "includeUnclaimedPlayers": True}
        return _dupr_post("/player/v1.0/search", token, b)

    try:
        # When location is active: fire A-Z parallel searches to get comprehensive results
        # When query is provided with location: also fire A-Z but with that query across pages
        if location_filter:
            import string
            if not query:
                # Blank search: A-Z with 3 pages each = 78 requests for comprehensive coverage
                queries = list(string.ascii_lowercase)
                tasks = []
                for q in queries:
                    for pg in range(3):
                        tasks.append((q, pg * 25))
            else:
                # Specific query with location: 4 pages
                tasks = [(query, pg * 25) for pg in range(4)]

            with ThreadPoolExecutor(max_workers=min(60, len(tasks))) as ex:
                futures = {ex.submit(_search_dupr, q, 25, off): (q, off) for q, off in tasks}
                hits = []
                for fut in as_completed(futures):
                    try:
                        resp = fut.result()
                        if resp.status_code == 401:
                            _ensure_token(force=True)
                            return jsonify({"error": "DUPR token expired, please retry"}), 503
                        if resp.status_code == 200:
                            result = resp.json().get("result", {})
                            page_hits = result.get("hits", []) if isinstance(result, dict) else []
                            hits.extend(page_hits if isinstance(page_hits, list) else [])
                    except Exception:
                        pass
        else:
            # Fetch 4 pages in parallel (100 results) for better rating-sorted coverage
            hits = []
            with ThreadPoolExecutor(max_workers=4) as ex:
                futs = [ex.submit(_search_dupr, query, 25, off) for off in [0, 25, 50, 75]]
                for fut in as_completed(futs):
                    try:
                        resp = fut.result()
                        if resp.status_code == 401:
                            _ensure_token(force=True)
                            return jsonify({"error": "DUPR token expired, please retry"}), 503
                        if resp.status_code == 200:
                            result = resp.json().get("result", {})
                            page_hits = result.get("hits", []) if isinstance(result, dict) else []
                            if isinstance(page_hits, list):
                                hits.extend(page_hits)
                    except Exception:
                        pass

        print(f"[SEARCH] total hits={len(hits)}", flush=True)

        # Deduplicate and collect all hits (including NR players)
        rated = []
        hit_ids = set()
        for h in hits:
            pid = str(h.get("id", ""))
            if pid in hit_ids:
                continue
            r = _extract_ratings(h)
            h["_r"] = r
            rated.append(h)
            hit_ids.add(pid)

        # Sort by highest rating first, then cap at 50 for profile fetches
        rated.sort(key=lambda h: max(h["_r"]["doublesRating"] or 0, h["_r"]["singlesRating"] or 0), reverse=True)

        top_rated = rated[:result_limit]
        top_ids = {str(h.get("id", "")) for h in top_rated}

        print(f"[SEARCH] unique={len(rated)}, fetching top {len(top_rated)}", flush=True)

        # Fetch profiles in parallel for top hits + any missing ensureIds
        def _get_loc_by_id(pid):
            try:
                pr = _dupr_get(f"/player/v1.0/{pid}", token)
                if pr.status_code == 200:
                    det = pr.json().get("result") or {}
                    return pid, det
            except Exception:
                pass
            return pid, {}

        all_pids_to_fetch = list(top_ids) + [i for i in ensure_ids if i not in top_ids]
        if all_pids_to_fetch:
            with ThreadPoolExecutor(max_workers=min(40, len(all_pids_to_fetch))) as ex:
                profile_map = dict(ex.map(_get_loc_by_id, all_pids_to_fetch))
        else:
            profile_map = {}

        rated = top_rated

        normalized = []
        for h in rated:
            pid = str(h.get("id", ""))
            r_hit = h["_r"]
            det = profile_map.get(pid, {})
            # Prefer profile ratings over search hit ratings (more accurate)
            r_prof = _extract_ratings(det) if det else {"doublesRating": None, "singlesRating": None}
            dr = r_prof["doublesRating"] if r_prof["doublesRating"] is not None else r_hit["doublesRating"]
            sr = r_prof["singlesRating"] if r_prof["singlesRating"] is not None else r_hit["singlesRating"]
            normalized.append({
                "id": pid,
                "name": _player_name(h),
                "doublesRating": dr,
                "singlesRating": sr,
                "imageUrl": h.get("imageUrl") or det.get("imageUrl") or "",
                "location": _format_location(det),
                "age": det.get("age"),
                "gender": det.get("gender"),
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
            normalized.insert(0, {
                "id": pid,
                "name": _player_name(det),
                "doublesRating": r["doublesRating"],
                "singlesRating": r["singlesRating"],
                "imageUrl": det.get("imageUrl", ""),
                "location": _format_location(det),
                "age": det.get("age"),
                "gender": det.get("gender"),
            })

        print(f"[SEARCH] normalized count={len(normalized)}, first 5={[(p.get('name'), p.get('doublesRating'), p.get('location')) for p in normalized[:5]]}", flush=True)
        _cache[cache_key] = (time.time(), normalized)
        return jsonify({"results": normalized})
    except Exception as e:
        import traceback; traceback.print_exc(); print(f"[SEARCH] exception: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/watch", methods=["POST"])
def api_watch():
    data = request.get_json(silent=True) or {}
    player_id = str(data.get("id", "")).strip()
    action = data.get("action", "add")  # add or remove

    if not player_id:
        return jsonify({"error": "Player ID required"}), 400

    sid = _get_sid()
    watches = _load_watches(sid)

    if action == "remove":
        _removed = next((w for w in watches if str(w.get("id", "")) == player_id), {})
        watches = [w for w in watches if str(w.get("id", "")) != player_id]
        _save_watches(watches, sid)
        # Invalidate this user's feed cache
        _cache.pop(f"feed:{sid}", None)
        _log_event("watch_remove", pid=player_id, name=_removed.get("name", ""))
        return jsonify({"ok": True, "watches": watches})

    # Add
    if any(str(w.get("id", "")) == player_id for w in watches):
        return jsonify({"ok": True, "watches": watches, "message": "Already watching"})

    dr = data.get("doublesRating")
    sr = data.get("singlesRating")
    img = data.get("imageUrl", "")
    name = data.get("name", "Unknown")

    # If ratings are missing, fetch from DUPR profile
    if dr is None and sr is None:
        token = _get_token()
        if token:
            try:
                resp = requests.get(
                    f"{DUPR_BASE}/player/v1.0/{player_id}",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=5,
                )
                if resp.ok:
                    profile = resp.json().get("result", {})
                    r = _extract_ratings(profile)
                    dr = r["doublesRating"]
                    sr = r["singlesRating"]
                    if not img:
                        img = profile.get("imageUrl", "")
                    if name == "Unknown":
                        name = _player_name(profile)
            except Exception:
                pass

    new_entry = {
        "id": player_id,
        "name": name,
        "rating": data.get("rating") or dr or sr,
        "doublesRating": dr,
        "singlesRating": sr,
        "imageUrl": img,
    }
    watches.append(new_entry)
    _save_watches(watches, sid)
    _cache.pop(f"feed:{sid}", None)
    _log_event("watch_add", pid=player_id, name=name)
    return jsonify({"ok": True, "watches": watches})


@app.route("/api/watches")
def api_watches():
    sid = _get_sid()
    return jsonify({"watches": _load_watches(sid)})


@app.route("/api/player_loc/<player_id>")
def api_player_loc(player_id):
    """Lightweight endpoint: fetch only the player's shortAddress for use in compare cards."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401
    cache_key = f"ploc:{player_id}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 86400:
        return jsonify(cached[1])
    loc = ""
    def _extract_loc(result: dict) -> str:
        out = (result.get("shortAddress") or result.get("city")
               or result.get("hometown") or result.get("location") or "").strip()
        if out:
            return out
        addr = result.get("addresses") or result.get("address") or {}
        if isinstance(addr, list) and addr:
            addr = addr[0] or {}
        if isinstance(addr, dict):
            city = (addr.get("city") or addr.get("locality") or "").strip()
            state = (addr.get("state") or addr.get("region") or "").strip()
            country = (addr.get("country") or addr.get("countryCode") or "").strip()
            parts = [p for p in [city, state or country] if p]
            return ", ".join(parts)
        return ""
    for path in [f"/player/v1.0/{player_id}", f"/user/v1.0/{player_id}/profile",
                 f"/player/v1.0/{player_id}/profile"]:
        try:
            r = _dupr_get(path, token)
            if r.status_code == 401:
                return jsonify({"error": "unauthorized"}), 401
            if r.status_code != 200:
                continue
            d = r.json()
            result = d.get("result") or d.get("data") or d or {}
            loc = _extract_loc(result)
            if loc:
                break
        except Exception:
            pass
    out = {"loc": loc}
    _cache[cache_key] = (time.time(), out)
    return jsonify(out)


# @app.route("/api/logout", methods=["POST"])
# def api_logout():
#     session.clear()
#     return jsonify({"ok": True})


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

    _log_event("h2h", p1=p1_id, p1_name=p1_name, p2=p2_id, p2_name=p2_name)

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

    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(fetch_all_history, p1_id, 1000)
        f2 = ex.submit(fetch_all_history, p2_id, 1000)
        p1_matches = f1.result()
        p2_matches = f2.result()
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
        # Count individual game wins/losses
        game_wins = 0
        game_losses = 0
        for g in range(1, 6):
            s1 = my_team.get(f"game{g}")
            s2 = opp_team.get(f"game{g}")
            if s1 is not None and s1 >= 0 and s2 is not None and s2 >= 0:
                if s1 > s2:
                    game_wins += 1
                else:
                    game_losses += 1
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
            "gameWins": game_wins,
            "gameLosses": game_losses,
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

    _log_event("h2h_teams", t1=f"{t1p1_id},{t1p2_id}", t2=f"{t2p1_id},{t2p2_id}",
               t1_name=t1_name, t2_name=t2_name)

    def fetch_all_history(player_id: str, max_matches: int = 500) -> list[dict]:
        """Fetch full match history by paginating with parallel batches."""
        all_m: list[dict] = []
        page_size = 25
        with ThreadPoolExecutor(max_workers=10) as ex:
            offset = 0
            while offset < max_matches:
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
                                short_page = True
                    except Exception:
                        pass
                if not got_any or short_page:
                    break
                offset += page_size * 5
        return all_m

    # Fetch t1p1 and t2p1 histories in parallel (one per team — saves 50% API calls)
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_t1 = ex.submit(fetch_all_history, t1p1_id)
        fut_t2 = ex.submit(fetch_all_history, t2p1_id)
    t1p1_matches = fut_t1.result()
    t2p1_matches = fut_t2.result()
    if t1p1_matches and t1p1_matches[0] == "__401__":
        return jsonify({"error": "unauthorized"}), 401
    if t2p1_matches and t2p1_matches[0] == "__401__":
        return jsonify({"error": "unauthorized"}), 401

    matches = t1p1_matches

    t1_ids = {t1p1_id, t1p2_id}
    t2_ids = {t2p1_id, t2p2_id}

    team_matches = []  # matches where t1 played as a team against t2
    seen_match_ids = set()

    for m in matches:
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
            other_team = teams[1 - ti]
            op1 = other_team.get("player1") or {}
            op2 = other_team.get("player2") or {}
            other_ids = {str(op1.get("id", "")), str(op2.get("id", ""))}
            if t1_ids <= team_player_ids and t2_ids <= other_ids:
                seen_match_ids.add(mid)
                t1_won = team.get("winner", False)
                games = []
                for gi in range(1, 6):
                    s1 = team.get(f"game{gi}")
                    s2 = other_team.get(f"game{gi}")
                    if s1 is not None and s1 >= 0 and s2 is not None and s2 >= 0:
                        games.append(f"{s1}-{s2}")
                score_str = ", ".join(games)
                team_matches.append({
                    "matchId": mid,
                    "date": m.get("matchDate") or m.get("eventDate", ""),
                    "eventName": m.get("eventName", ""),
                    "t1Won": t1_won,
                    "score": score_str,
                })
                break

    app.logger.info(f"H2H Teams found {len(team_matches)} team matches")
    team_matches.sort(key=lambda x: x.get("date", ""), reverse=True)
    t1_wins = sum(1 for m in team_matches if m["t1Won"])
    t2_wins = len(team_matches) - t1_wins

    # ----- Common opponent teams -----
    all_four = {t1p1_id, t1p2_id, t2p1_id, t2p2_id}

    def build_team_opp_record(raw_matches, my_ids):
        """Find doubles matches where both my_ids played together; record W/L vs each opponent team."""
        record = {}  # (opp_id_a, opp_id_b) sorted tuple -> {names, wins, losses}
        seen = set()
        for m in raw_matches:
            mid = m.get("matchId") or m.get("id")
            if mid in seen:
                continue
            teams = m.get("teams", [])
            if len(teams) < 2:
                continue
            # Find the team containing both my players
            my_idx = -1
            for i, t in enumerate(teams):
                p1o = t.get("player1") or {}
                p2o = t.get("player2") or {}
                t_ids = {str(p1o.get("id", "")), str(p2o.get("id", ""))}
                if my_ids <= t_ids:
                    my_idx = i
                    break
            if my_idx < 0:
                continue
            seen.add(mid)
            my_team = teams[my_idx]
            opp_team = teams[1 - my_idx]
            op1 = opp_team.get("player1") or {}
            op2 = opp_team.get("player2") or {}
            oid1 = str(op1.get("id", ""))
            oid2 = str(op2.get("id", ""))
            if not oid1 or not oid2:
                continue
            # Skip if opponent team includes any of the 4 main players
            if oid1 in all_four or oid2 in all_four:
                continue
            key = tuple(sorted([oid1, oid2]))
            if key not in record:
                record[key] = {
                    "name1": op1.get("fullName", oid1),
                    "name2": op2.get("fullName", oid2),
                    "wins": 0, "losses": 0,
                }
            if my_team.get("winner") is True:
                record[key]["wins"] += 1
            else:
                record[key]["losses"] += 1
        return record

    t1_opp_record = build_team_opp_record(t1p1_matches, t1_ids)
    t2_opp_record = build_team_opp_record(t2p1_matches, t2_ids)

    common_opp_keys = set(t1_opp_record.keys()) & set(t2_opp_record.keys())
    common_opponents = []
    for key in common_opp_keys:
        r1 = t1_opp_record[key]
        r2 = t2_opp_record[key]
        opp_name = f"{r1['name1']} / {r1['name2']}"
        common_opponents.append({
            "oppTeam": opp_name,
            "oppIds": list(key),
            "t1Wins": r1["wins"], "t1Losses": r1["losses"],
            "t2Wins": r2["wins"], "t2Losses": r2["losses"],
        })
    common_opponents.sort(
        key=lambda x: x["t1Wins"] + x["t1Losses"] + x["t2Wins"] + x["t2Losses"],
        reverse=True,
    )

    return jsonify({
        "t1Name": t1_name,
        "t2Name": t2_name,
        "t1p1Id": t1p1_id, "t1p2Id": t1p2_id,
        "t2p1Id": t2p1_id, "t2p2Id": t2p2_id,
        "t1Wins": t1_wins,
        "t2Wins": t2_wins,
        "totalMatches": len(team_matches),
        "matches": team_matches,
        "commonOpponents": common_opponents[:40],
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

    # If no player IDs provided, seed with followed/watched players
    if not initial_ids:
        sid = _get_sid()
        following = _get_following(token)
        watches = _load_watches(sid)
        initial_ids = list({str(p.get("id", p.get("playerId", p.get("userId", ""))))
                           for p in following if p.get("id") or p.get("playerId") or p.get("userId")})
        initial_ids += [str(w.get("id", "")) for w in watches if str(w.get("id", "")) not in initial_ids]
        initial_ids = [i for i in initial_ids if i][:20]  # cap at 20 seeds

    app.logger.info(f"TOURNAMENT: eventName={event_name!r} initial_ids={initial_ids}")
    if not event_name:
        return jsonify({"error": "eventName is required"}), 400
    if not initial_ids:
        return jsonify({"error": "No players to search. Follow some players first."}), 400

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
                _event_matches_target(m.get("eventName") or m.get("league") or "")
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

    # Sibling-week discovery — if the event name contains "Week N", we
    # opportunistically collect other "Week X" event names sharing the same
    # base (e.g. "Bull City Pickleball League - Season 5") from the histories
    # we fetch anyway. This lets the frontend offer accurate week navigation.
    _week_re = re.compile(r"\bweek\s+(\d+)\b", re.I)
    def _strip_week(s: str) -> str:
        s = re.sub(r"[\(\[\s\-]*week\s+\d+[\)\]\s\-]*", " ", s or "", flags=re.I)
        return re.sub(r"\s+", " ", s).strip(" -").lower()
    target_week_match = _week_re.search(event_name)
    is_week_event = target_week_match is not None
    target_week_num = int(target_week_match.group(1)) if is_week_event else None
    target_base = _strip_week(event_name) if is_week_event else ""
    sibling_weeks: dict[int, str] = {}
    if is_week_event:
        sibling_weeks[target_week_num] = event_name

    def _event_matches_target(m_event: str) -> bool:
        """True if an event name matches the requested target.
        For Week-N events we tolerate minor formatting differences (dashes,
        whitespace, parens) as long as the base name and week number agree."""
        if m_event == event_name:
            return True
        if not is_week_event or not m_event:
            return False
        wm = _week_re.search(m_event)
        if not wm or int(wm.group(1)) != target_week_num:
            return False
        return _strip_week(m_event) == target_base

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
            # Opportunistically record sibling-week events as we iterate.
            if is_week_event and m_event:
                wm = _week_re.search(m_event)
                if wm and _strip_week(m_event) == target_base:
                    wn = int(wm.group(1))
                    sibling_weeks.setdefault(wn, m_event)
            if not _event_matches_target(m_event):
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
        err_payload = {"error": "No matches found for this tournament"}
        if is_week_event and sibling_weeks:
            err_payload["relatedWeeks"] = [
                {"week": w, "eventName": sibling_weeks[w]} for w in sorted(sibling_weeks)
            ]
        return jsonify(err_payload), 404

    matches_list = list(all_matches.values())

    # Derive event metadata from first match
    sample = matches_list[0]
    event_date = sample.get("eventDate", "")
    venue = sample.get("venue", "")
    event_format = _match_format(sample)  # 'singles' | 'doubles' | 'mixed' | 'unknown'
    is_doubles = event_format in ("doubles", "mixed")

    # Detect rotating-partner leagues (standings by individual player, not team)
    is_individual_league = "bull city pickleball league" in event_name.lower()

    if is_individual_league:
        # Build per-player stats
        player_stats: dict[str, dict] = {}
        for m in matches_list:
            teams = m.get("teams", [])
            if len(teams) < 2:
                continue
            for ti, team in enumerate(teams):
                p1 = team.get("player1")
                p2 = team.get("player2")
                players_in_team = [p for p in [p1, p2] if p]
                other_team = teams[1 - ti]
                rim = team.get("preMatchRatingAndImpact") or {}
                rating_key = "Double" if is_doubles else "Single"

                for pi, p in enumerate(players_in_team):
                    pid = str(p["id"])
                    pname = p.get("fullName", "Unknown")
                    if pid not in player_stats:
                        player_stats[pid] = {
                            "name": pname,
                            "wins": 0,
                            "losses": 0,
                            "gamesWon": 0,
                            "gamesLost": 0,
                            "duprDeltas": [],
                            "margins": [],
                        }
                    ps = player_stats[pid]
                    if team.get("winner") is True:
                        ps["wins"] += 1
                    elif team.get("winner") is False:
                        ps["losses"] += 1

                    for g in range(1, 6):
                        s_my = team.get(f"game{g}")
                        s_opp = other_team.get(f"game{g}")
                        if s_my is not None and s_my >= 0 and s_opp is not None and s_opp >= 0:
                            if s_my > s_opp:
                                ps["gamesWon"] += 1
                            else:
                                ps["gamesLost"] += 1
                            ps["margins"].append(s_my - s_opp)

                    pn = pi + 1  # player1 -> 1, player2 -> 2
                    impact = rim.get(f"match{rating_key}RatingImpactPlayer{pn}")
                    if impact is not None:
                        ps["duprDeltas"].append(impact)

        teams_output = []
        for pid, ps in player_stats.items():
            total = ps["wins"] + ps["losses"]
            win_pct = round(ps["wins"] / total, 3) if total > 0 else 0
            avg_delta = round(sum(ps["duprDeltas"]) / len(ps["duprDeltas"]), 4) if ps["duprDeltas"] else 0
            avg_margin = round(sum(ps["margins"]) / len(ps["margins"]), 1) if ps["margins"] else 0
            teams_output.append({
                "players": [ps["name"]],
                "playerIds": [int(pid)],
                "wins": ps["wins"],
                "losses": ps["losses"],
                "winPct": win_pct,
                "duprDelta": avg_delta,
                "avgMargin": avg_margin,
                "gamesWon": ps["gamesWon"],
                "gamesLost": ps["gamesLost"],
            })
        teams_output.sort(key=lambda t: (t["wins"], t["winPct"], t["avgMargin"]), reverse=True)
    else:
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
        teams_output.sort(key=lambda t: (t["wins"], t["winPct"], t["avgMargin"]), reverse=True)

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

    related_weeks_out = [
        {"week": w, "eventName": sibling_weeks[w]} for w in sorted(sibling_weeks)
    ] if is_week_event else []

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
        "relatedWeeks": related_weeks_out,
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

    # Fetch 400 matches (16 pages × 25) in parallel
    all_matches: list[dict] = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(_fetch_player_history, player_id, token, 25, off)
                   for off in range(0, 400, 25)]
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
        "singles": {"wins": 0, "losses": 0, "gamesWon": 0, "gamesTotal": 0, "ptsScored": 0, "ptsAllowed": 0},
        "doubles": {"wins": 0, "losses": 0, "gamesWon": 0, "gamesTotal": 0, "ptsScored": 0, "ptsAllowed": 0},
        "mixed":   {"wins": 0, "losses": 0, "gamesWon": 0, "gamesTotal": 0, "ptsScored": 0, "ptsAllowed": 0},
    }
    points_won = total_points = 0
    points_allowed = 0
    games_won = total_games = 0
    win_margin_sum = 0.0
    win_margin_games = 0
    deciding_wins = 0
    deciding_total = 0
    partners: dict[str, int] = {}
    opponents: dict[str, dict] = {}
    streak_data: list[bool] = []

    venues = {}
    events_set = set()   # unique events
    clubs: dict[str, dict] = {}  # clubId -> {"name": str, "count": int}
    clubs_by_name: dict[str, int] = {}  # fallback when clubId missing
    club_ids_played: set[str] = set()  # for resolving to cities later
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

        # Track venues — use eventName (the label shown on feed cards), fall back to venue/eventLocation
        venue = (m.get("eventName") or m.get("league") or m.get("venue") or m.get("eventLocation") or "").strip()
        if venue:
            venues[venue] = venues.get(venue, 0) + 1
            events_set.add(venue)

        # Collect club IDs so we can resolve to cities via /club/v1.0/{id}.shortAddress below.
        cid = m.get("clubId")
        club_name = (m.get("clubName") or m.get("clientName") or "").strip()
        if cid:
            cid_s = str(cid)
            club_ids_played.add(cid_s)
            entry = clubs.setdefault(cid_s, {"name": club_name, "count": 0})
            entry["count"] += 1
            if club_name and not entry.get("name"):
                entry["name"] = club_name
        elif club_name:
            clubs_by_name[club_name] = clubs_by_name.get(club_name, 0) + 1

        if won:
            wins += 1
            if fmt in fmt_stats: fmt_stats[fmt]["wins"] += 1
        else:
            losses += 1
            if fmt in fmt_stats: fmt_stats[fmt]["losses"] += 1
        streak_data.append(won)

        # Points + games
        match_game_scores = []  # list of (s_my, s_opp) for this match, in order
        _fmt_bucket = fmt_stats.get(fmt)
        for g in range(1, 6):
            s_my = my_team.get(f"game{g}")
            s_opp = opp_team.get(f"game{g}")
            if s_my is not None and s_my >= 0 and s_opp is not None and s_opp >= 0:
                points_won += s_my
                points_allowed += s_opp
                total_points += s_my + s_opp
                total_games += 1
                if s_my > s_opp:
                    games_won += 1
                if _fmt_bucket is not None:
                    _fmt_bucket["gamesTotal"] += 1
                    _fmt_bucket["ptsScored"] += s_my
                    _fmt_bucket["ptsAllowed"] += s_opp
                    if s_my > s_opp:
                        _fmt_bucket["gamesWon"] += 1
                match_game_scores.append((s_my, s_opp))
        # Win margin (per-game margin across games in WON matches)
        if won and match_game_scores:
            for s_my, s_opp in match_game_scores:
                win_margin_sum += (s_my - s_opp)
                win_margin_games += 1
        # Deciding game: last game of a match that went to 3 or 5 games total
        if len(match_game_scores) in (3, 5):
            s_my, s_opp = match_game_scores[-1]
            deciding_total += 1
            if s_my > s_opp:
                deciding_wins += 1

        # Partners (non-self teammates)
        for pkey in ("player1", "player2"):
            p = my_team.get(pkey)
            if p and str(p.get("id","")) != str(player_id):
                pid = str(p.get("id", ""))
                pname = p.get("fullName", "Unknown")
                pimg = p.get("imageUrl") or ""
                if pid not in partners:
                    partners[pid] = {"name": pname, "imageUrl": pimg, "count": 0, "wins": 0, "losses": 0, "ptsWon": 0, "ptsTotal": 0}
                elif pimg and not partners[pid].get("imageUrl"):
                    partners[pid]["imageUrl"] = pimg
                partners[pid]["count"] += 1
                if won is True:
                    partners[pid]["wins"] += 1
                elif won is False:
                    partners[pid]["losses"] += 1
                # Accumulate points for this partner
                for g in range(1, 6):
                    s_my = my_team.get(f"game{g}")
                    s_opp = opp_team.get(f"game{g}")
                    if s_my is not None and s_my >= 0 and s_opp is not None and s_opp >= 0:
                        partners[pid]["ptsWon"] += s_my
                        partners[pid]["ptsTotal"] += s_my + s_opp

        # Opponents
        for pkey in ("player1", "player2"):
            p = opp_team.get(pkey)
            if p and p.get("id"):
                oid = str(p["id"])
                oname = p.get("fullName", "Unknown")
                oimg = p.get("imageUrl") or ""
                if oid not in opponents:
                    opponents[oid] = {"name": oname, "imageUrl": oimg, "count": 0}
                elif oimg and not opponents[oid].get("imageUrl"):
                    opponents[oid]["imageUrl"] = oimg
                opponents[oid]["count"] += 1

    # Longest win streak
    longest_streak = cur = 0
    for won in streak_data:
        cur = cur + 1 if won else 0
        longest_streak = max(longest_streak, cur)

    # Clutch stats — matches decided by 2 points in any game
    clutch_wins = clutch_total = 0
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
        # Check if any game was decided by exactly 2 points (e.g. 11-9, 15-13)
        is_clutch = False
        for g in range(1, 6):
            s1 = my_team.get(f"game{g}")
            s2 = teams[1 - my_idx].get(f"game{g}")
            if s1 is not None and s1 >= 0 and s2 is not None and s2 >= 0:
                if abs(s1 - s2) == 2 and max(s1, s2) >= 11:
                    is_clutch = True
                    break
        if is_clutch:
            clutch_total += 1
            if my_team.get("winner") is True:
                clutch_wins += 1

    # Comeback wins — won the match after losing game 1
    comeback_wins = 0
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
        if my_team.get("winner") is not True:
            continue
        g1_my = my_team.get("game1")
        g1_opp = opp_team.get("game1")
        if g1_my is not None and g1_opp is not None and g1_my >= 0 and g1_opp >= 0:
            if g1_my < g1_opp:
                comeback_wins += 1

    # Upsets — wins against opponents with avg DUPR >= 0.20 higher
    upsets = 0
    my_doubles = player_info["ratings"].get("doubles")
    my_singles = player_info["ratings"].get("singles")
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
        if my_team.get("winner") is not True:
            continue
        opp_team = teams[1 - my_idx]
        fmt = _match_format(m)
        my_rating = my_doubles if fmt in ("doubles", "mixed") else my_singles
        if my_rating is None:
            continue
        # Average opponent DUPR from postMatchRating
        opp_ratings = []
        for pkey in ("player1", "player2"):
            p = opp_team.get(pkey)
            if p and p.get("id"):
                pmr = p.get("postMatchRating") or {}
                r = pmr.get("doubles") if fmt in ("doubles", "mixed") else pmr.get("singles")
                if r is not None:
                    try:
                        opp_ratings.append(float(r))
                    except (TypeError, ValueError):
                        pass
        if opp_ratings:
            avg_opp = sum(opp_ratings) / len(opp_ratings)
            if avg_opp >= my_rating + 0.20:
                upsets += 1

    # Unique partners count
    unique_partners = len(partners)

    # Formats played
    formats_played = sum(1 for f in fmt_stats.values() if f["wins"] + f["losses"] > 0)

    # Max matches in a single day (Ironman)
    from collections import Counter as _Counter
    day_counts: dict[str, int] = {}
    for m in all_matches:
        md = (m.get("matchDate") or m.get("eventDate") or "")[:10]
        if md and md != "0001-01-01":
            day_counts[md] = day_counts.get(md, 0) + 1
    max_matches_in_day = max(day_counts.values()) if day_counts else 0
    ironman_date = max(day_counts, key=day_counts.get) if day_counts else ""

    # Giant Kills — wins against opponents with avg DUPR >= 0.50 higher
    giant_kills = 0
    biggest_upset_gap = 0.0
    for m in all_matches:
        teams = m.get("teams", [])
        if len(teams) < 2:
            continue
        my_idx2 = next(
            (i for i, t in enumerate(teams)
             if any(str((p or {}).get("id","")) == str(player_id)
                    for p in [t.get("player1"), t.get("player2")])),
            -1
        )
        if my_idx2 < 0:
            continue
        my_t = teams[my_idx2]
        if my_t.get("winner") is not True:
            continue
        opp_t = teams[1 - my_idx2]
        fmt2 = _match_format(m)
        my_r = my_doubles if fmt2 in ("doubles", "mixed") else my_singles
        if my_r is None:
            continue
        opp_rs = []
        for pkey in ("player1", "player2"):
            p = opp_t.get(pkey)
            if p and p.get("id"):
                pmr = p.get("postMatchRating") or {}
                r = pmr.get("doubles") if fmt2 in ("doubles", "mixed") else pmr.get("singles")
                if r is not None:
                    try: opp_rs.append(float(r))
                    except: pass
        if opp_rs:
            avg_opp = sum(opp_rs) / len(opp_rs)
            gap = avg_opp - my_r
            if gap >= 0.50:
                giant_kills += 1
                biggest_upset_gap = max(biggest_upset_gap, gap)

    # Pickle count — any game where this player's team won 11-0 (shutout).
    pickles = 0
    for m in all_matches:
        teams = m.get("teams", [])
        if len(teams) < 2:
            continue
        my_idx_p = next(
            (i for i, t in enumerate(teams)
             if any(str((p or {}).get("id","")) == str(player_id)
                    for p in [t.get("player1"), t.get("player2")])),
            -1
        )
        if my_idx_p < 0:
            continue
        my_team = teams[my_idx_p]
        opp_team = teams[1 - my_idx_p]
        for g in range(1, 6):
            s_my = my_team.get(f"game{g}")
            s_opp = opp_team.get(f"game{g}")
            if s_my is not None and s_opp is not None and s_my >= 11 and s_opp == 0:
                pickles += 1

    # Recent form (last 10 matches W/L)
    recent_form = []
    for w in streak_data[:10]:
        recent_form.append("W" if w else "L")

    most_common_partner_data = max(partners.values(), key=lambda x: x["count"]) if partners else None
    most_common_partner = most_common_partner_data["name"] if most_common_partner_data else ""
    most_common_partner_id = max(partners, key=lambda k: partners[k]["count"]) if partners else ""
    most_common_opp_data = max(opponents.values(), key=lambda x: x["count"]) if opponents else None
    most_common_opp = most_common_opp_data["name"] if most_common_opp_data else ""
    most_common_opp_id = max(opponents, key=lambda k: opponents[k]["count"]) if opponents else ""

    fav_venue = max(venues, key=venues.get) if venues else ""
    fav_venue_count = venues.get(fav_venue, 0) if fav_venue else 0

    # Resolve club IDs → {shortAddress, mediaUrl} via /club/v1.0/{id}. Cached forever.
    def _fetch_club_info(cid: str) -> dict | None:
        if cid in _club_info_cache:
            return _club_info_cache[cid]
        try:
            r = _dupr_get(f"/club/v1.0/{cid}", token)
            if r.status_code == 200:
                res = (r.json().get("result") or {})
                info = {
                    "shortAddress": res.get("shortAddress") or None,
                    "mediaUrl": res.get("mediaUrl") or res.get("logoUrl") or res.get("imageUrl") or None,
                    "name": res.get("clubName") or res.get("name") or None,
                }
                _club_info_cache[cid] = info
                return info
        except Exception as exc:
            app.logger.warning("club lookup failed cid=%s err=%s", cid, exc)
        _club_info_cache[cid] = None
        return None

    to_fetch = [cid for cid in club_ids_played if cid not in _club_info_cache]
    if to_fetch:
        with ThreadPoolExecutor(max_workers=min(12, len(to_fetch))) as executor:
            list(executor.map(_fetch_club_info, to_fetch))

    cities_set: set[str] = set()
    # clubId -> {"short": "Naples, FL", "key": "naples"} for per-match lookup on the client.
    club_city_map: dict[str, dict] = {}
    for cid in club_ids_played:
        info = _club_info_cache.get(cid) or {}
        short = info.get("shortAddress")
        media = info.get("mediaUrl") or ""
        city_key = ""
        if short:
            # shortAddress format: "Naples, FL" — take the city segment (before comma).
            city_key = short.split(",")[0].strip().lower()
            if city_key:
                cities_set.add(city_key)
        if short or media:
            club_city_map[str(cid)] = {"short": short or "", "key": city_key, "image": media}

    # Pick favorite club by clubId (fall back to name-only entries if none had an ID).
    fav_club_id = ""
    fav_club = ""
    fav_club_count = 0
    fav_club_image = ""
    if clubs:
        fav_club_id = max(clubs, key=lambda k: clubs[k]["count"])
        fav_entry = clubs[fav_club_id]
        fav_club_count = fav_entry["count"]
        fav_club = fav_entry.get("name") or ""
        info = _club_info_cache.get(fav_club_id) or {}
        if info.get("name") and not fav_club:
            fav_club = info["name"]
        fav_club_image = info.get("mediaUrl") or ""
    elif clubs_by_name:
        fav_club = max(clubs_by_name, key=clubs_by_name.get)
        fav_club_count = clubs_by_name[fav_club]

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
    if gender in ("MALE", "M"): gender = "Male"
    elif gender in ("FEMALE", "F"): gender = "Female"
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
    # Reliability scores (0-100)
    player_info["doublesReliability"] = api_ratings.get("doublesReliabilityScore")
    player_info["singlesReliability"] = api_ratings.get("singlesReliabilityScore")

    result = {
        "player": player_info,
        "stats": {
            "wins": wins, "losses": losses,
            "winPct": wpct(wins, losses),
            "singlesWins": fmt_stats["singles"]["wins"],
            "singlesLosses": fmt_stats["singles"]["losses"],
            "singlesWinPct": wpct(fmt_stats["singles"]["wins"], fmt_stats["singles"]["losses"]),
            "singlesGamesWon": fmt_stats["singles"]["gamesWon"],
            "singlesGamesLost": fmt_stats["singles"]["gamesTotal"] - fmt_stats["singles"]["gamesWon"],
            "singlesPtsScored": fmt_stats["singles"]["ptsScored"],
            "singlesPtsAllowed": fmt_stats["singles"]["ptsAllowed"],
            "doublesWins": fmt_stats["doubles"]["wins"],
            "doublesLosses": fmt_stats["doubles"]["losses"],
            "doublesWinPct": wpct(fmt_stats["doubles"]["wins"], fmt_stats["doubles"]["losses"]),
            "doublesGamesWon": fmt_stats["doubles"]["gamesWon"],
            "doublesGamesLost": fmt_stats["doubles"]["gamesTotal"] - fmt_stats["doubles"]["gamesWon"],
            "doublesPtsScored": fmt_stats["doubles"]["ptsScored"],
            "doublesPtsAllowed": fmt_stats["doubles"]["ptsAllowed"],
            "mixedWins": fmt_stats["mixed"]["wins"],
            "mixedLosses": fmt_stats["mixed"]["losses"],
            "mixedWinPct": wpct(fmt_stats["mixed"]["wins"], fmt_stats["mixed"]["losses"]),
            "mixedGamesWon": fmt_stats["mixed"]["gamesWon"],
            "mixedGamesLost": fmt_stats["mixed"]["gamesTotal"] - fmt_stats["mixed"]["gamesWon"],
            "mixedPtsScored": fmt_stats["mixed"]["ptsScored"],
            "mixedPtsAllowed": fmt_stats["mixed"]["ptsAllowed"],
            "avgPointsPct": round(points_won / total_points * 100, 1) if total_points > 0 else None,
            "gameWinPct": round(games_won / total_games * 100, 1) if total_games > 0 else None,
            "gamesWon": games_won,
            "gamesTotal": total_games,
            "totalPointsScored": points_won,
            "totalPointsAllowed": points_allowed,
            "avgWinMargin": round(win_margin_sum / win_margin_games, 1) if win_margin_games > 0 else None,
            "decidingWins": deciding_wins,
            "decidingTotal": deciding_total,
            "decidingWinPct": round(deciding_wins / deciding_total * 100, 1) if deciding_total > 0 else None,
            "longestStreak": longest_streak,
            "mostCommonPartner": most_common_partner,
            "mostCommonPartnerId": most_common_partner_id,
            "mostCommonPartnerImageUrl": (partners.get(most_common_partner_id) or {}).get("imageUrl", "") if most_common_partner_id else "",
            "mostCommonOpponent": most_common_opp,
            "mostCommonOpponentId": most_common_opp_id,
            "mostCommonOpponentImageUrl": (opponents.get(most_common_opp_id) or {}).get("imageUrl", "") if most_common_opp_id else "",
            "mostCommonOpponentCount": most_common_opp_data["count"] if most_common_opp_data else 0,
            "favoriteVenue": fav_venue,
            "favoriteVenueCount": fav_venue_count,
            "favoriteClub": fav_club,
            "favoriteClubCount": fav_club_count,
            "favoriteClubId": fav_club_id,
            "favoriteClubImageUrl": fav_club_image,
            "totalEvents": len(events_set),
            "uniqueCities": len(cities_set),
            "clubCities": club_city_map,
            "clutchWins": clutch_wins,
            "clutchTotal": clutch_total,
            "clutchWinPct": round(clutch_wins / clutch_total * 100, 1) if clutch_total > 0 else None,
            "upsets": upsets,
            "uniquePartners": unique_partners,
            "partnerStats": sorted([{"id": k, "name": v["name"], "imageUrl": v.get("imageUrl",""), "count": v["count"], "wins": v["wins"], "losses": v["losses"], "ptsWon": v["ptsWon"], "ptsTotal": v["ptsTotal"]} for k, v in partners.items()], key=lambda x: x["count"], reverse=True),
            "formatsPlayed": formats_played,
            "formatsList": [f for f, v in fmt_stats.items() if v["wins"] + v["losses"] > 0],
            "formatCounts": {f: (v["wins"] + v["losses"]) for f, v in fmt_stats.items()},
            "maxMatchesInDay": max_matches_in_day,
            "ironmanDate": ironman_date,
            "giantKills": giant_kills,
            "biggestUpsetGap": round(biggest_upset_gap, 2),
            "comebackWins": comeback_wins,
            "pickles": pickles,
            "recentForm": recent_form,
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
    # No per-user profile anymore; ratings come from the form only
    user = {}
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
    """SSE streaming connect search — streams batches of scored results as they arrive."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    city = data.get("city", "").strip()
    user_age = data.get("age")
    genders = data.get("genders", [])
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

    import string
    letters = list(string.ascii_lowercase)

    city_key = city.split(",")[0].strip().lower()
    cluster = CITY_CLUSTERS.get(city_key, {})
    close_cities = cluster.get("close", [])
    far_cities = cluster.get("far", [])

    def _geocode(c):
        # Try Nominatim first
        try:
            r = requests.get("https://nominatim.openstreetmap.org/search",
                params={"q": c, "format": "json", "limit": 1},
                headers={"User-Agent": "dupr-feed/1.0 (contact: devinkennedy246@gmail.com)"}, timeout=5)
            if r.status_code == 200:
                d = r.json()
                if d:
                    return float(d[0]["lat"]), float(d[0]["lon"]), d[0].get("display_name", c)
                else:
                    print(f"[GEOCODE] nominatim empty for {c!r}", flush=True)
            else:
                print(f"[GEOCODE] nominatim status={r.status_code} for {c!r} body={r.text[:160]!r}", flush=True)
        except Exception as e:
            print(f"[GEOCODE] nominatim exception for {c!r}: {e}", flush=True)

        # Fallback to Photon (Komoot) — same OSM data, more reliable from cloud IPs
        try:
            r = requests.get("https://photon.komoot.io/api/",
                params={"q": c, "limit": 1},
                headers={"User-Agent": "dupr-feed/1.0"}, timeout=5)
            if r.status_code == 200:
                feats = (r.json() or {}).get("features") or []
                if feats:
                    coords = feats[0].get("geometry", {}).get("coordinates") or []
                    props = feats[0].get("properties") or {}
                    if len(coords) == 2:
                        lon, lat = coords
                        label = ", ".join(x for x in [props.get("name"), props.get("state"), props.get("country")] if x) or c
                        return float(lat), float(lon), label
                print(f"[GEOCODE] photon empty for {c!r}", flush=True)
            else:
                print(f"[GEOCODE] photon status={r.status_code} for {c!r} body={r.text[:160]!r}", flush=True)
        except Exception as e:
            print(f"[GEOCODE] photon exception for {c!r}: {e}", flush=True)

        return None

    def _search_letter(q, lat, lng, loc_text, offset=0):
        try:
            body = {"filter": {"lat": lat, "lng": lng, "locationText": loc_text, "rating": {}},
                    "query": q, "limit": 25, "offset": offset, "includeUnclaimedPlayers": True}
            resp = _dupr_post("/player/v1.0/search", token, body)
            if resp.status_code == 200:
                result = resp.json().get("result", {})
                return result.get("hits", []) if isinstance(result, dict) else []
        except Exception:
            pass
        return []

    def _score_hit(h, tier, city_label):
        """Score a single hit. Returns dict or None if filtered out."""
        h_id = str(h.get("id", ""))
        h_name = _player_name(h)
        r = _extract_ratings(h)
        player_rating = r["singlesRating"] if rating_type == "singles" else r["doublesRating"]
        if player_rating is None:
            return None
        rating_diff = abs(user_rating_val - player_rating) if user_rating_val is not None else None
        if tier == "far" and (rating_diff is None or rating_diff > FAR_MAX_RATING_DIFF):
            return None
        if user_rating_val is not None:
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
                months = (datetime.now(timezone.utc) - fm_date).days / 30.0
                experience_score = min(months, 60.0) / 60.0
            except Exception:
                pass
        total_score = 0.90 * rating_score + 0.06 * age_score + 0.02 * activity_score + 0.02 * experience_score
        if tier == "far":
            total_score *= FAR_SCORE_MULTIPLIER
        if genders and len(genders) < 2:
            pg = (h.get("gender") or h.get("sex") or "").upper()
            if pg in ("MALE", "M"): pg = "M"
            elif pg in ("FEMALE", "F"): pg = "F"
            if pg and pg not in [g.upper() for g in genders]:
                return None
        return {
            "id": h_id, "name": h_name,
            "doublesRating": r["doublesRating"], "singlesRating": r["singlesRating"],
            "imageUrl": h.get("imageUrl", ""), "age": player_age,
            "gender": h.get("gender", ""), "city": city_label,
            "score": round(total_score * 100),
        }

    def generate():
        import queue, threading as _thr

        # Step 1: Geocode main city FIRST (blocking), then start its search immediately
        main_geo = _geocode(city)
        if not main_geo:
            yield f"data: {json.dumps({'error': 'Could not find that city. Try a different format.'})}\n\n"
            return
        main_lat, main_lng, main_loc = main_geo

        # Send geo info immediately
        yield f"data: {json.dumps({'geo': {'lat': main_lat, 'lng': main_lng}})}\n\n"

        seen_ids = set()
        result_q = queue.Queue()  # thread-safe queue for scored results
        pending = _thr.Semaphore(0)  # counts completed search tasks

        # Shared search worker
        def _do_search(q, lat, lng, loc_text, tier, city_label, offset):
            hits = _search_letter(q, lat, lng, loc_text, offset)
            batch = []
            for h in (hits or []):
                pid = str(h.get("id", ""))
                if not pid:
                    continue
                # Thread-safe dedup via try-add pattern
                batch.append((h, pid, tier, city_label))
            result_q.put(batch)
            pending.release()

        total_tasks = 0
        pool = ThreadPoolExecutor(max_workers=40)

        # Fire main city searches immediately (full A-Z, 2 pages)
        main_label = city.split(",")[0].strip()
        for q in letters:
            for pg in range(2):
                pool.submit(_do_search, q, main_lat, main_lng, main_loc, "main", main_label, pg * 25)
                total_tasks += 1

        # Fire close/far city geocodes + searches in parallel (#6 overlap)
        def _geocode_and_search(c, tier):
            nonlocal total_tasks
            geo = _geocode(c)
            if not geo:
                return
            lat, lng, loc_text = geo
            lbl = c.split(",")[0].strip()
            for q in letters:
                pool.submit(_do_search, q, lat, lng, loc_text, tier, lbl, 0)
                total_tasks += 1

        for c in close_cities:
            pool.submit(_geocode_and_search, c, "close")
        for c in far_cities:
            pool.submit(_geocode_and_search, c, "far")

        # Stream results as they arrive — collect batches and emit every ~8 completed searches
        all_scored = []
        completed = 0
        batch_interval = 8  # emit after every N completed search requests
        last_emit_count = 0

        # Wait a moment for first results, then start streaming
        import time as _time
        _time.sleep(0.3)

        # Drain loop: keep draining until we've processed at least all main-city tasks
        # We can't know exact total_tasks for close/far since geocoding is async,
        # so use a timeout approach
        deadline = _time.monotonic() + 25  # max 25 seconds total
        idle_deadline = _time.monotonic() + 4  # stop if no new results for 4s

        while _time.monotonic() < deadline:
            try:
                batch = result_q.get(timeout=0.5)
                idle_deadline = _time.monotonic() + 4  # reset idle timer
                for h, pid, tier, city_label in batch:
                    if pid not in seen_ids:
                        seen_ids.add(pid)
                        scored = _score_hit(h, tier, city_label)
                        if scored:
                            all_scored.append(scored)
                completed += 1

                # Emit a batch of top results periodically
                if len(all_scored) - last_emit_count >= 5 or completed % batch_interval == 0:
                    all_scored.sort(key=lambda x: x["score"], reverse=True)
                    top50 = all_scored[:50]
                    yield f"data: {json.dumps({'batch': top50, 'count': len(all_scored), 'done': False})}\n\n"
                    last_emit_count = len(all_scored)

            except queue.Empty:
                # No new results — check if we should stop
                if _time.monotonic() > idle_deadline:
                    break
                # Emit what we have if there's anything new
                if len(all_scored) > last_emit_count:
                    all_scored.sort(key=lambda x: x["score"], reverse=True)
                    top50 = all_scored[:50]
                    yield f"data: {json.dumps({'batch': top50, 'count': len(all_scored), 'done': False})}\n\n"
                    last_emit_count = len(all_scored)

        # Final emit
        all_scored.sort(key=lambda x: x["score"], reverse=True)
        yield f"data: {json.dumps({'batch': all_scored[:50], 'count': len(all_scored), 'done': True})}\n\n"
        pool.shutdown(wait=False)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/clubs/by-city", methods=["GET"])
def api_clubs_by_city():
    """Find clubs in a city by sampling nearby players' match histories.

    DUPR has no public club-search API, so we approximate: geocode the city →
    player search with lat/lng filter → fetch a slice of each player's matches
    → aggregate by clubId → resolve names/photos/addresses via /club/v1.0/{id}.
    Cached 10 min per city.
    """
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    city = (request.args.get("city") or "").strip()
    if not city or len(city) < 2:
        return jsonify({"clubs": []})

    cache_key = f"clubs_by_city::{city.lower()}"
    cached = _cache.get(cache_key)
    if cached and (time.time() - cached[0] < 600):
        return jsonify(cached[1])

    # 1) Geocode — Nominatim first, Photon fallback (mirrors connect search).
    lat = lng = None
    loc_label = city
    try:
        r = requests.get("https://nominatim.openstreetmap.org/search",
            params={"q": city, "format": "json", "limit": 1},
            headers={"User-Agent": "dupr-feed/1.0 (contact: devinkennedy246@gmail.com)"},
            timeout=6)
        if r.status_code == 200:
            d = r.json()
            if d:
                lat = float(d[0]["lat"])
                lng = float(d[0]["lon"])
                loc_label = d[0].get("display_name", city)
    except Exception:
        pass
    if lat is None:
        try:
            r = requests.get("https://photon.komoot.io/api/",
                params={"q": city, "limit": 1},
                headers={"User-Agent": "dupr-feed/1.0"}, timeout=6)
            if r.status_code == 200:
                feats = (r.json() or {}).get("features") or []
                if feats:
                    coords = feats[0].get("geometry", {}).get("coordinates") or []
                    props = feats[0].get("properties") or {}
                    if len(coords) == 2:
                        lng, lat = coords[0], coords[1]
                        loc_label = ", ".join(x for x in [props.get("name"), props.get("state"), props.get("country")] if x) or city
        except Exception:
            pass
    if lat is None:
        return jsonify({"clubs": [], "error": "geocode_failed"})

    # 2) Pull players near the city. We do a few letter searches in parallel
    #    so we sample players whose names start with different letters, not just
    #    the densest local cluster. 5 letters × 25 hits = up to 125 candidates.
    def _search_letter(q: str):
        try:
            body = {
                "filter": {"lat": lat, "lng": lng, "locationText": loc_label, "rating": {}},
                "query": q, "limit": 25, "offset": 0, "includeUnclaimedPlayers": True,
            }
            resp = _dupr_post("/player/v1.0/search", token, body)
            if resp.status_code == 200:
                result = resp.json().get("result") or {}
                return result.get("hits") or []
        except Exception:
            pass
        return []

    seed_letters = ["a", "e", "i", "o", "s"]
    player_ids: list[str] = []
    seen_pids: set[str] = set()
    with ThreadPoolExecutor(max_workers=5) as ex:
        for hits in ex.map(_search_letter, seed_letters):
            for h in hits or []:
                pid = str(h.get("id") or "")
                # Only keep players whose distance suggests they're really near the city.
                # DUPR sorts by proximity; first ~30 per letter are local.
                dist = h.get("distanceInMiles")
                if dist is not None and dist > 60:
                    continue
                if pid and pid not in seen_pids:
                    seen_pids.add(pid)
                    player_ids.append(pid)

    if not player_ids:
        result = {"clubs": [], "city": loc_label}
        _cache[cache_key] = (time.time(), result)
        return jsonify(result)

    # 3) Pull 25 matches per player (parallel), aggregate by clubId.
    player_ids = player_ids[:60]  # cap work
    club_counts: dict[str, dict] = {}  # cid -> {"count":N, "name":str}

    def _hist(pid: str):
        try:
            return _fetch_player_history(pid, token, 25, 0) or []
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=15) as ex:
        for matches in ex.map(_hist, player_ids):
            if matches and matches[0] == "__401__":
                continue
            for m in matches or []:
                cid = m.get("clubId")
                if cid is None:
                    continue
                cid_s = str(cid)
                entry = club_counts.setdefault(cid_s, {"count": 0, "name": ""})
                entry["count"] += 1
                nm = (m.get("clubName") or m.get("clientName") or "").strip()
                if nm and not entry["name"]:
                    entry["name"] = nm

    if not club_counts:
        result = {"clubs": [], "city": loc_label}
        _cache[cache_key] = (time.time(), result)
        return jsonify(result)

    # 4) Resolve top ~24 clubIds to full club details. Reuses _club_info_cache.
    top_cids = sorted(club_counts.keys(), key=lambda k: club_counts[k]["count"], reverse=True)[:24]

    def _fetch_club_full(cid: str) -> dict | None:
        if cid in _club_info_cache:
            return _club_info_cache[cid]
        try:
            r = _dupr_get(f"/club/v1.0/{cid}", token)
            if r.status_code == 200:
                res = (r.json().get("result") or {})
                info = {
                    "shortAddress": res.get("shortAddress") or None,
                    "mediaUrl": res.get("mediaUrl") or res.get("logoUrl") or res.get("imageUrl") or None,
                    "name": res.get("clubName") or res.get("name") or None,
                    "memberCount": res.get("clubMemberCount"),
                }
                _club_info_cache[cid] = info
                return info
        except Exception as exc:
            app.logger.warning("club lookup failed cid=%s err=%s", cid, exc)
        _club_info_cache[cid] = None
        return None

    to_fetch = [cid for cid in top_cids if cid not in _club_info_cache]
    if to_fetch:
        with ThreadPoolExecutor(max_workers=min(12, len(to_fetch))) as ex:
            list(ex.map(_fetch_club_full, to_fetch))

    # 5) Build result. Prefer clubs whose shortAddress contains the city token
    #    (so "Naples" doesn't return clubs from Naples-adjacent counties).
    city_token = city.split(",")[0].strip().lower()
    clubs_out: list[dict] = []
    for cid in top_cids:
        info = _club_info_cache.get(cid) or {}
        name = info.get("name") or club_counts[cid]["name"]
        if not name:
            continue
        short = info.get("shortAddress") or ""
        # Soft city-match score: matched clubs ranked above non-matched.
        is_match = bool(city_token and city_token in short.lower())
        clubs_out.append({
            "id": cid,
            "name": name,
            "short": short,
            "image": info.get("mediaUrl") or "",
            "members": info.get("memberCount") or 0,
            "playCount": club_counts[cid]["count"],
            "cityMatch": is_match,
        })

    clubs_out.sort(key=lambda c: (not c["cityMatch"], -c["playCount"]))
    result = {"clubs": clubs_out[:20], "city": loc_label}
    _cache[cache_key] = (time.time(), result)
    return jsonify(result)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Analytics: /api/track (client events) + /admin/stats (dashboard)
# ---------------------------------------------------------------------------
_TRACK_ALLOWED = {
    "visit_client", "tab_switch", "profile_open", "profile_close",
    "feed_scroll", "search2_submit",
}


@app.route("/api/track", methods=["POST"])
def api_track():
    _get_sid()
    body = request.get_json(silent=True) or {}
    ev = (body.get("type") or "")[:40]
    if ev not in _TRACK_ALLOWED:
        return jsonify({"ok": False}), 400
    data = body.get("data") or {}
    clean = {}
    if isinstance(data, dict):
        for i, (k, v) in enumerate(data.items()):
            if i >= 12:
                break
            if isinstance(v, (str, int, float, bool)) or v is None:
                clean[str(k)[:30]] = v[:200] if isinstance(v, str) else v
    _log_event(ev, **clean)
    return jsonify({"ok": True})


def _read_events(limit_lines: int = 200_000) -> list[dict]:
    if not EVENTS_LOG.exists():
        return []
    try:
        with EVENTS_LOG.open("r") as f:
            lines = f.readlines()[-limit_lines:]
    except Exception:
        return []
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln))
        except Exception:
            continue
    return out


def _sessionize(events: list[dict], gap_s: int = 1800) -> list[dict]:
    """Group events by sid, split into sessions on >gap_s inactivity."""
    by_sid: dict[str, list[dict]] = {}
    for e in events:
        by_sid.setdefault(e.get("sid") or "?", []).append(e)
    sessions = []
    for sid, evs in by_sid.items():
        evs.sort(key=lambda e: e.get("ts") or 0)
        cur: list[dict] = []
        for e in evs:
            if cur and (e.get("ts", 0) - cur[-1].get("ts", 0)) > gap_s:
                sessions.append(_finalize_session(sid, cur))
                cur = []
            cur.append(e)
        if cur:
            sessions.append(_finalize_session(sid, cur))
    sessions.sort(key=lambda s: s["last"], reverse=True)
    return sessions


def _finalize_session(sid: str, evs: list[dict]) -> dict:
    first = evs[0].get("ts", 0)
    last = evs[-1].get("ts", 0)
    types: dict[str, int] = {}
    tabs: dict[str, int] = {}
    profiles: list[str] = []
    search_qs: list[str] = []
    total_profile_dwell = 0
    max_scroll_pct = 0
    ref_param = ""
    referer = ""
    ip = evs[-1].get("ip", "")
    ua = evs[-1].get("ua", "")
    for e in evs:
        t = e.get("type", "")
        types[t] = types.get(t, 0) + 1
        if t == "tab_switch":
            tab = e.get("tab", "")
            if tab:
                tabs[tab] = tabs.get(tab, 0) + 1
        elif t == "profile_open":
            nm = e.get("name") or e.get("pid") or ""
            if nm:
                profiles.append(str(nm))
        elif t == "profile_close":
            dwell = e.get("dwell_ms") or 0
            if isinstance(dwell, (int, float)):
                total_profile_dwell += int(dwell)
        elif t == "search":
            q = e.get("query") or ""
            if q:
                search_qs.append(q)
        elif t == "feed_scroll":
            pct = e.get("max_pct") or 0
            if isinstance(pct, (int, float)) and pct > max_scroll_pct:
                max_scroll_pct = int(pct)
        elif t == "visit":
            if not ref_param:
                ref_param = str(e.get("ref_param") or "")
            if not referer:
                referer = str(e.get("referer") or "")
    return {
        "sid": sid, "first": first, "last": last,
        "duration_s": max(0, last - first), "events": len(evs),
        "ip": ip, "ua": ua, "ref_param": ref_param, "referer": referer,
        "types": types, "tabs": tabs, "profiles": profiles,
        "searches": search_qs,
        "profile_dwell_s": total_profile_dwell // 1000,
        "max_scroll_pct": max_scroll_pct,
    }


def _fmt_dur(s: int) -> str:
    if s < 60:
        return f"{s}s"
    m, sec = divmod(s, 60)
    if m < 60:
        return f"{m}m {sec}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


def _h(s: str) -> str:
    """Tiny HTML escape."""
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


@app.route("/admin/stats")
def admin_stats():
    key = request.args.get("key", "")
    expected = os.environ.get("ADMIN_KEY", "")
    if not expected or key != expected:
        return "forbidden — set ADMIN_KEY env var and pass ?key=<value>", 403

    events = _read_events()
    now = int(time.time())
    day_s = 86400
    sessions = _sessionize(events)

    # Exclude very short/noisy sessions for some summaries
    real_sessions = [s for s in sessions if s["events"] >= 2 or s["duration_s"] >= 5]

    # Aggregates
    from collections import Counter
    tab_counts: Counter = Counter()
    profile_counts: Counter = Counter()
    search_counts: Counter = Counter()
    ref_counts: Counter = Counter()
    for s in sessions:
        for t, n in s["tabs"].items():
            tab_counts[t] += n
        for p in s["profiles"]:
            profile_counts[p] += 1
        for q in s["searches"]:
            search_counts[q.lower()] += 1
        if s["ref_param"]:
            ref_counts[s["ref_param"]] += 1

    # Daily unique sids + return visits
    sid_days: dict[str, set[str]] = {}
    for e in events:
        ts = e.get("ts", 0)
        day = time.strftime("%Y-%m-%d", time.gmtime(ts))
        sid_days.setdefault(e.get("sid", "?"), set()).add(day)
    day_unique: dict[str, set[str]] = {}
    for sid, days in sid_days.items():
        for d in days:
            day_unique.setdefault(d, set()).add(sid)
    returning_sids = [sid for sid, days in sid_days.items() if len(days) > 1]

    # Watch / H2H totals
    watch_adds = sum(1 for e in events if e.get("type") == "watch_add")
    watch_removes = sum(1 for e in events if e.get("type") == "watch_remove")
    h2h_count = sum(1 for e in events if e.get("type") in ("h2h", "h2h_teams"))

    # Dwell stats
    dwells = [s["profile_dwell_s"] for s in real_sessions if s["profile_dwell_s"] > 0]
    avg_dwell = sum(dwells) / len(dwells) if dwells else 0

    durations = [s["duration_s"] for s in real_sessions]
    avg_session = sum(durations) / len(durations) if durations else 0

    # HTML
    out = [
        '<!doctype html><html><head><meta charset="utf-8"><title>DUPR Feed — stats</title>',
        '<style>',
        'body{font-family:-apple-system,BlinkMacSystemFont,system-ui,sans-serif;margin:20px;background:#0b1220;color:#e2e8f0;}',
        'h1{margin:0 0 4px;font-size:22px;color:#fff}h2{margin:30px 0 8px;font-size:15px;text-transform:uppercase;letter-spacing:.08em;color:#94a3b8;border-bottom:1px solid #1e293b;padding-bottom:6px}',
        '.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin:8px 0}',
        '.card{background:#111a2e;border:1px solid #1e293b;border-radius:10px;padding:14px}',
        '.card .v{font-size:24px;font-weight:700;color:#fff}.card .k{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:#64748b;margin-bottom:4px}',
        'table{width:100%;border-collapse:collapse;font-size:12px}',
        'th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #1e293b;vertical-align:top}',
        'th{color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;font-size:10px;font-weight:600}',
        'tr:hover td{background:#0f172a}',
        '.muted{color:#64748b}.num{font-variant-numeric:tabular-nums;text-align:right}',
        'code{background:#0f172a;padding:1px 5px;border-radius:4px;font-size:11px}',
        'details>summary{cursor:pointer;padding:4px 0;color:#60a5fa;font-size:12px}',
        '.cols{display:grid;grid-template-columns:1fr 1fr;gap:20px}@media(max-width:800px){.cols{grid-template-columns:1fr}}',
        '</style></head><body>',
        '<h1>DUPR Feed — Stats</h1>',
        f'<div class="muted" style="font-size:11px">Generated {time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(now))} · {len(events)} events · {len(sessions)} sessions · {len(sid_days)} unique visitors</div>',
    ]

    # Top-level metrics
    out.append('<h2>Overview</h2><div class="grid">')
    out.append(f'<div class="card"><div class="k">Unique visitors</div><div class="v">{len(sid_days)}</div></div>')
    out.append(f'<div class="card"><div class="k">Sessions</div><div class="v">{len(real_sessions)}</div></div>')
    out.append(f'<div class="card"><div class="k">Avg session</div><div class="v">{_fmt_dur(int(avg_session))}</div></div>')
    out.append(f'<div class="card"><div class="k">Avg profile dwell</div><div class="v">{_fmt_dur(int(avg_dwell))}</div></div>')
    out.append(f'<div class="card"><div class="k">Returning visitors</div><div class="v">{len(returning_sids)}</div></div>')
    out.append(f'<div class="card"><div class="k">Watch adds / removes</div><div class="v">{watch_adds} / {watch_removes}</div></div>')
    out.append(f'<div class="card"><div class="k">H2H lookups</div><div class="v">{h2h_count}</div></div>')
    out.append('</div>')

    # Daily actives
    out.append('<div class="cols">')
    out.append('<div><h2>Daily unique visitors</h2><table><tr><th>Day</th><th class="num">Unique sids</th></tr>')
    for d in sorted(day_unique.keys(), reverse=True)[:30]:
        out.append(f'<tr><td>{d}</td><td class="num">{len(day_unique[d])}</td></tr>')
    out.append('</table></div>')

    # Tab popularity
    out.append('<div><h2>Tab popularity</h2><table><tr><th>Tab</th><th class="num">Switches</th></tr>')
    for tab, c in tab_counts.most_common(20):
        out.append(f'<tr><td><code>{_h(tab)}</code></td><td class="num">{c}</td></tr>')
    if not tab_counts:
        out.append('<tr><td colspan=2 class="muted">No tab switches yet</td></tr>')
    out.append('</table></div>')
    out.append('</div>')

    # Profiles, searches, refs
    out.append('<div class="cols">')
    out.append('<div><h2>Top profiles viewed</h2><table><tr><th>Player</th><th class="num">Opens</th></tr>')
    for name, c in profile_counts.most_common(30):
        out.append(f'<tr><td>{_h(name)}</td><td class="num">{c}</td></tr>')
    if not profile_counts:
        out.append('<tr><td colspan=2 class="muted">No profile opens yet</td></tr>')
    out.append('</table></div>')

    out.append('<div><h2>Top search queries</h2><table><tr><th>Query</th><th class="num">Count</th></tr>')
    for q, c in search_counts.most_common(30):
        out.append(f'<tr><td><code>{_h(q)}</code></td><td class="num">{c}</td></tr>')
    if not search_counts:
        out.append('<tr><td colspan=2 class="muted">No searches yet</td></tr>')
    out.append('</table></div>')
    out.append('</div>')

    out.append('<div class="cols">')
    out.append('<div><h2>Referral sources (<code>?ref=</code>)</h2><table><tr><th>Tag</th><th class="num">Sessions</th></tr>')
    for r, c in ref_counts.most_common(30):
        out.append(f'<tr><td><code>{_h(r)}</code></td><td class="num">{c}</td></tr>')
    if not ref_counts:
        out.append('<tr><td colspan=2 class="muted">Share links with <code>?ref=xxx</code> to attribute traffic</td></tr>')
    out.append('</table></div>')
    out.append('<div></div></div>')

    # Session list
    out.append('<h2>Recent sessions</h2>')
    out.append('<table><tr><th>Start (UTC)</th><th>Dur</th><th class="num">Evts</th><th>IP</th><th>Device</th><th>Ref</th><th>Tabs</th><th>Profiles</th><th>Searches</th></tr>')
    for s in sessions[:100]:
        start = time.strftime("%m-%d %H:%M", time.gmtime(s["first"]))
        ua_short = ""
        if "iPhone" in s["ua"]:
            ua_short = "iPhone"
        elif "Android" in s["ua"]:
            ua_short = "Android"
        elif "Macintosh" in s["ua"]:
            ua_short = "Mac"
        elif "Windows" in s["ua"]:
            ua_short = "Windows"
        else:
            ua_short = s["ua"][:20]
        tabs_str = ", ".join(f"{t}×{n}" for t, n in sorted(s["tabs"].items(), key=lambda x: -x[1])[:4])
        profiles_str = ", ".join(s["profiles"][:3]) + (f" +{len(s['profiles'])-3}" if len(s["profiles"]) > 3 else "")
        searches_str = ", ".join(s["searches"][:3]) + (f" +{len(s['searches'])-3}" if len(s["searches"]) > 3 else "")
        ref_disp = s["ref_param"] or (s["referer"][:30] if s["referer"] and s["referer"] != "-" else "")
        out.append(
            f'<tr><td>{start}</td><td>{_fmt_dur(s["duration_s"])}</td>'
            f'<td class="num">{s["events"]}</td><td>{_h(s["ip"])}</td>'
            f'<td>{_h(ua_short)}</td><td><code>{_h(ref_disp)}</code></td>'
            f'<td>{_h(tabs_str)}</td><td>{_h(profiles_str)}</td><td>{_h(searches_str)}</td></tr>'
        )
    out.append('</table>')

    out.append('</body></html>')
    return "".join(out)


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
                loc = _format_location(det)
                if not loc:
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


# Rankings — scrape dupr.com/rankings (Webflow CMS, no public API).
# 12 collections in DOM order: division-major (Open, Senior, Junior),
# format-minor (Men's Doubles, Women's Doubles, Men's Singles, Women's Singles).
_RANKING_CATS = [
    ("open", "mens_doubles"), ("open", "womens_doubles"),
    ("open", "mens_singles"), ("open", "womens_singles"),
    ("senior", "mens_doubles"), ("senior", "womens_doubles"),
    ("senior", "mens_singles"), ("senior", "womens_singles"),
    ("junior", "mens_doubles"), ("junior", "womens_doubles"),
    ("junior", "mens_singles"), ("junior", "womens_singles"),
]
_RANKINGS_ROW_RE = re.compile(
    r'<div role="listitem" class="post_item w-dyn-item">.*?'
    r'<div class="heading-table name">([^<]+)</div>.*?'
    r'<div fs-cmsfilter-field="age" class="heading-table center">([^<]*)</div>.*?'
    r'<div class="heading-table right">([^<]+)</div>',
    re.S,
)
_RANKINGS_UPDATED_RE = re.compile(
    r'Updated:\s*</div>\s*<div[^>]*class="text-size-medium"[^>]*>([^<]+)</div>',
    re.S,
)


_COUNTRY_TAIL_RE = re.compile(r",\s*([A-Za-z]{2})\s*$")
_COUNTRY_CACHE_FILE = Path(__file__).parent / "rankings_countries.json"
_country_cache_lock = threading.Lock()
_country_cache: dict[str, str] = {}
_country_fill_running = False


def _load_country_cache() -> dict[str, str]:
    global _country_cache
    if _country_cache:
        return _country_cache
    try:
        if _COUNTRY_CACHE_FILE.exists():
            _country_cache = json.loads(_COUNTRY_CACHE_FILE.read_text()) or {}
    except Exception:
        _country_cache = {}
    return _country_cache


def _save_country_cache() -> None:
    try:
        with _country_cache_lock:
            tmp = _COUNTRY_CACHE_FILE.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(_country_cache, sort_keys=True, indent=2))
            tmp.replace(_COUNTRY_CACHE_FILE)
    except Exception as e:
        app.logger.warning(f"country cache save failed: {e}")


def _resolve_player_country(name: str, token: str) -> str:
    """Search DUPR for `name`, fetch the top hit's profile, return ISO-2 country (or '').
    Retries each network call once on transient failure."""
    def _retry(fn, attempts: int = 2):
        for i in range(attempts):
            try:
                resp = fn()
                if resp.status_code == 200:
                    return resp
            except Exception:
                pass
            time.sleep(0.15)
        return None

    try:
        r = _retry(lambda: _dupr_post("/player/v1.0/search", token, {"filter": {}, "query": name, "limit": 20}))
        if not r:
            return ""
        hits = (r.json().get("result") or {}).get("hits", []) or []
        norm = lambda s: " ".join((s or "").split()).lower()
        name_l = norm(name)
        best = next((h for h in hits if norm(h.get("fullName")) == name_l), None) or (hits[0] if hits else None)
        if not best:
            return ""
        pid = str(best.get("id", ""))
        pr = _retry(lambda: _dupr_get(f"/player/v1.0/{pid}", token))
        if not pr:
            return ""
        addr = ((pr.json().get("result") or {}).get("shortAddress") or "").strip()
        m = _COUNTRY_TAIL_RE.search(addr)
        return m.group(1).upper() if m else ""
    except Exception:
        return ""


def _scrape_rankings() -> dict:
    """Fetch dupr.com/rankings, parse rows, resolve each unique player's country."""
    r = requests.get(
        "https://www.dupr.com/rankings",
        headers={"User-Agent": "Mozilla/5.0 (compatible; dupr-feed/1.0)"},
        timeout=15,
    )
    r.raise_for_status()
    html = r.text

    updated_m = _RANKINGS_UPDATED_RE.search(html)
    updated = updated_m.group(1).strip() if updated_m else ""

    block_starts = [m.start() for m in re.finditer(r'class="[^"]*ranking-collection[^"]*"', html)]
    if len(block_starts) < 12:
        app.logger.warning(f"rankings: expected 12 blocks, got {len(block_starts)}")

    divisions: dict = {"open": {}, "senior": {}, "junior": {}}
    unique_names: set[str] = set()
    for i, (division, fmt) in enumerate(_RANKING_CATS):
        if i >= len(block_starts):
            divisions[division][fmt] = []
            continue
        end = block_starts[i + 1] if i + 1 < len(block_starts) else len(html)
        chunk = html[block_starts[i]:end]
        rows = []
        for rank, (name, age, rating) in enumerate(_RANKINGS_ROW_RE.findall(chunk), start=1):
            nm = name.strip()
            try:
                age_int = int(age.strip()) if age.strip().isdigit() else None
            except Exception:
                age_int = None
            try:
                rating_f = float(rating.strip())
            except Exception:
                rating_f = None
            rows.append({
                "rank": rank,
                "name": nm,
                "age": age_int,
                "rating": rating_f,
                "country": "",
            })
            unique_names.add(nm)
        divisions[division][fmt] = rows

    # Stamp known countries from persistent cache; fill the rest in the background.
    cache = _load_country_cache()
    for division in divisions.values():
        for rows in division.values():
            for row in rows:
                cc = cache.get(row["name"])
                if cc:
                    row["country"] = cc

    missing = sorted(n for n in unique_names if not cache.get(n))
    if missing:
        _start_country_fill(missing, divisions)

    return {"updated": updated, "divisions": divisions}


def _start_country_fill(missing: list[str], divisions: dict) -> None:
    """Resolve missing player countries in the background, mutating `divisions`
    rows and persisting to disk as each name resolves. Holds a module-level guard
    so concurrent /api/rankings calls don't spawn parallel fillers."""
    global _country_fill_running
    with _country_cache_lock:
        if _country_fill_running:
            return
        _country_fill_running = True

    def _worker():
        global _country_fill_running
        try:
            token = _get_token()
            if not token:
                return
            def _one(name: str):
                cc = _resolve_player_country(name, token)
                if not cc:
                    return
                with _country_cache_lock:
                    _country_cache[name] = cc
                # Stamp onto every row referencing this name (mutates the cached payload).
                for division in divisions.values():
                    for rows in division.values():
                        for row in rows:
                            if row["name"] == name:
                                row["country"] = cc
            with ThreadPoolExecutor(max_workers=4) as ex:
                list(ex.map(_one, missing))
            _save_country_cache()
            app.logger.info(f"rankings: country fill done ({len(missing)} requested)")
        finally:
            with _country_cache_lock:
                _country_fill_running = False

    threading.Thread(target=_worker, name="rankings-country-fill", daemon=True).start()


@app.route("/api/rankings")
def api_rankings():
    """Top 50 DUPR-rated players per division/format, scraped from dupr.com/rankings."""
    cache_key = "rankings:all"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 86400:  # 24h
        return jsonify(cached[1])
    try:
        data = _scrape_rankings()
        _cache[cache_key] = (time.time(), data)
        return jsonify(data)
    except Exception as e:
        app.logger.error(f"rankings scrape failed: {e}")
        if cached:
            return jsonify(cached[1])  # serve stale on failure
        return jsonify({"error": "rankings_unavailable"}), 502


@app.route("/api/rankings/resolve")
def api_rankings_resolve():
    """Resolve a ranking-row name to a DUPR player id so the profile overlay can open."""
    name = (request.args.get("name") or "").strip()
    if not name:
        return jsonify({"error": "missing_name"}), 400

    cache_key = f"rankings:resolve:{name.lower()}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 3600:
        return jsonify(cached[1])

    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    try:
        r = _dupr_post("/player/v1.0/search", token, {"filter": {}, "query": name, "limit": 20})
        d = r.json() if r.status_code == 200 else {}
        hits = (d.get("result") or {}).get("hits", []) or []
        # Prefer exact (case-insensitive, whitespace-collapsed) full-name match
        norm = lambda s: " ".join((s or "").split()).lower()
        name_l = norm(name)
        best = next((h for h in hits if norm(h.get("fullName")) == name_l), None) or (hits[0] if hits else None)
        if not best:
            result = {"found": False}
        else:
            result = {
                "found": True,
                "id": str(best.get("id", "")),
                "name": best.get("fullName") or name,
                "imageUrl": best.get("imageUrl") or "",
            }
        _cache[cache_key] = (time.time(), result)
        return jsonify(result)
    except Exception as e:
        app.logger.error(f"rankings resolve failed for {name!r}: {e}")
        return jsonify({"found": False, "error": "lookup_failed"}), 500


# ============================================================================
# Continental rankings — one page per continent on dupr.com/continental-rankings.
# Each page: 4 ranking-collection blocks (md / wd / ms / ws), 50 rows each.
# Row fields: rank (.c1), name (.heading-table.name), country (.country),
# rating (.heading-table.right). Country is a full English name, not ISO-2.
# ============================================================================
_CONTINENTS = [
    {"slug": "north-america",                 "key": "na",  "name": "North America"},
    {"slug": "south-america",                 "key": "sa",  "name": "South America"},
    {"slug": "central-america-and-caribbean", "key": "cac", "name": "Central America & Caribbean"},
    {"slug": "europe",                        "key": "eu",  "name": "Europe"},
    {"slug": "africa",                        "key": "af",  "name": "Africa"},
    {"slug": "asia",                          "key": "as",  "name": "Asia"},
    {"slug": "australia-oceania",             "key": "oc",  "name": "Oceania"},
]
_CONT_FORMATS = ["mens_doubles", "womens_doubles", "mens_singles", "womens_singles"]

_CONT_ROW_RE = re.compile(
    r'<div role="listitem" class="post_item w-dyn-item">.*?'
    r'class="c\d[^"]*">\s*(\d+)\s*</div>.*?'            # rank (c1, c6, etc.)
    r'class="heading-table name">([^<]+)</div>.*?'      # name
    r'class="country">([^<]+)</div>.*?'                 # country (full name)
    r'class="heading-table right">([\d.]+)</div>',      # rating
    re.S,
)

def _fetch_dupr_html(url: str, timeout: int = 15) -> str:
    r = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; dupr-feed/1.0)"},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.text


def _scrape_continent(slug: str) -> dict:
    """Scrape a single continent page → {formats: {md/wd/ms/ws: [rows]}, updated}."""
    html = _fetch_dupr_html(f"https://www.dupr.com/continental-rankings/{slug}")

    updated_m = _RANKINGS_UPDATED_RE.search(html)
    updated = updated_m.group(1).strip() if updated_m else ""

    block_starts = [m.start() for m in re.finditer(r'class="[^"]*ranking-collection[^"]*"', html)]
    if len(block_starts) < 4:
        app.logger.warning(f"continental({slug}): expected 4 blocks, got {len(block_starts)}")

    formats: dict[str, list] = {f: [] for f in _CONT_FORMATS}
    for i, fmt in enumerate(_CONT_FORMATS):
        if i >= len(block_starts):
            continue
        end = block_starts[i + 1] if i + 1 < len(block_starts) else len(html)
        chunk = html[block_starts[i]:end]
        rows = []
        for rank, name, country, rating in _CONT_ROW_RE.findall(chunk):
            try:
                rows.append({
                    "rank": int(rank),
                    "name": name.strip(),
                    "country": country.strip(),
                    "rating": float(rating),
                })
            except Exception:
                continue
        formats[fmt] = rows

    return {"slug": slug, "updated": updated, "formats": formats}


@app.route("/api/rankings/continental")
def api_rankings_continental():
    """List of available continents (cheap, no scrape)."""
    return jsonify({"continents": _CONTINENTS})


@app.route("/api/rankings/continental/<slug>")
def api_rankings_continental_one(slug: str):
    """One continent's full rankings (4 formats × 50 rows). 24h cache."""
    if not any(c["slug"] == slug for c in _CONTINENTS):
        return jsonify({"error": "unknown_continent"}), 404
    cache_key = f"rankings:continental:{slug}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 86400:
        return jsonify(cached[1])
    try:
        data = _scrape_continent(slug)
        _cache[cache_key] = (time.time(), data)
        return jsonify(data)
    except Exception as e:
        app.logger.error(f"continental scrape failed ({slug}): {e}")
        if cached:
            return jsonify(cached[1])
        return jsonify({"error": "scrape_failed"}), 502


# ============================================================================
# Collegiate power rankings — dupr.com/collegiate/power-rankings.
# 3 year tabs (2025, 2024, 2023). Rows: rank, school name, score, school color.
# ============================================================================
_COLLEGE_ROW_RE = re.compile(
    r'<div role="listitem"[^>]*class="college-ranking-item[^"]*"[^>]*>.*?'
    r'class="rank-text">(\d+)</div>.*?'
    r'style="background-color:(#[0-9A-Fa-f]{3,6})"[^>]*class="school_color[^"]*".*?'
    r'<h4 class="college">([^<]+)</h4>.*?'
    r'<h4 class="score[^"]*">([\d.]+)</h4>',
    re.S,
)
_COLLEGE_YEAR_RE = re.compile(r'data-w-tab="(\d{4})"[^>]*class="[^"]*w-tab-pane[^"]*"')

def _scrape_collegiate() -> dict:
    """DUPR only server-renders the active year's rows; past-year tabs are empty
    placeholders. We return the current year's full list."""
    html = _fetch_dupr_html("https://www.dupr.com/collegiate/power-rankings")
    rows = []
    for rank, color, name, score in _COLLEGE_ROW_RE.findall(html):
        try:
            rows.append({
                "rank": int(rank),
                "name": name.strip(),
                "score": float(score),
                "color": color,
            })
        except Exception:
            continue
    rows.sort(key=lambda r: r["rank"])
    pane_years = [m.group(1) for m in _COLLEGE_YEAR_RE.finditer(html)]
    updated_m = _RANKINGS_UPDATED_RE.search(html)
    return {
        "updated": updated_m.group(1).strip() if updated_m else "",
        "year": pane_years[0] if pane_years else "",
        "rows": rows,
    }


@app.route("/api/rankings/collegiate")
def api_rankings_collegiate():
    cache_key = "rankings:collegiate"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 86400:
        return jsonify(cached[1])
    try:
        data = _scrape_collegiate()
        _cache[cache_key] = (time.time(), data)
        return jsonify(data)
    except Exception as e:
        app.logger.error(f"collegiate scrape failed: {e}")
        if cached:
            return jsonify(cached[1])
        return jsonify({"error": "scrape_failed"}), 502


# ============================================================================
# Club rankings — dupr.com/club-rankings.
# 2 tabs (Facility, Organization), 100 rows each. Rows: rank, name, type.
# No rating / member count exposed in static HTML.
# ============================================================================
_CLUB_ROW_RE = re.compile(
    r'<div role="listitem" class="post_item w-dyn-item">.*?'
    r'class="c\d[^"]*">\s*(\d+)\s*</div>.*?'
    r'class="heading-table name">([^<]+)</div>.*?'
    r'class="heading-table center">([^<]+)</div>',
    re.S,
)
_CLUB_PANE_RE = re.compile(
    r'data-w-tab="(Facility|Organization)"[^>]*class="[^"]*w-tab-pane[^"]*"'
)

def _scrape_clubs() -> dict:
    html = _fetch_dupr_html("https://www.dupr.com/club-rankings")
    pane_starts = [(m.group(1), m.start()) for m in _CLUB_PANE_RE.finditer(html)]
    tabs: dict[str, list] = {}
    for i, (label, start) in enumerate(pane_starts):
        end = pane_starts[i + 1][1] if i + 1 < len(pane_starts) else len(html)
        chunk = html[start:end]
        rows = []
        for rank, name, kind in _CLUB_ROW_RE.findall(chunk):
            try:
                rows.append({
                    "rank": int(rank),
                    "name": name.strip(),
                    "kind": kind.strip(),
                })
            except Exception:
                continue
        tabs[label.lower()] = rows
    updated_m = _RANKINGS_UPDATED_RE.search(html)
    return {
        "updated": updated_m.group(1).strip() if updated_m else "",
        "tabs": tabs,
    }


@app.route("/api/rankings/clubs")
def api_rankings_clubs():
    cache_key = "rankings:clubs"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 86400:
        return jsonify(cached[1])
    try:
        data = _scrape_clubs()
        _cache[cache_key] = (time.time(), data)
        return jsonify(data)
    except Exception as e:
        app.logger.error(f"clubs scrape failed: {e}")
        if cached:
            return jsonify(cached[1])
        return jsonify({"error": "scrape_failed"}), 502


@app.route("/api/tournaments", methods=["POST"])
def api_tournaments():
    """Search DUPR tournaments."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = data.get("query", "").strip()
    status_filter = data.get("status", "")  # "UPCOMING", "PAST", or "" for all
    offset = data.get("offset", 0)
    limit = data.get("limit", 20)

    cache_key = f"tournaments:{query.lower()}:{status_filter}:{offset}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 60:
        return jsonify(cached[1])

    body: dict = {
        "limit": limit,
        "offset": offset,
        "sort": {"order": "DESC", "parameter": "START_DATE"},
    }
    if query:
        body["query"] = query
    # Build filter based on status
    filt: dict = {}
    if status_filter == "UPCOMING":
        filt["status"] = ["OPEN", "NOT_STARTED"]
    elif status_filter == "PAST":
        filt["status"] = ["COMPLETED"]
    body["filter"] = filt

    try:
        resp = _dupr_post("/club-tournament/v1.0/search", token, body)
        if resp.status_code == 401:
            return jsonify({"error": "unauthorized"}), 401
        rj = resp.json()
        hits = rj.get("result", {}).get("hits", [])
        total = rj.get("result", {}).get("total", 0)
        tournaments = []
        for h in hits:
            t = {
                "id": h.get("id"),
                "name": h.get("name", ""),
                "startDate": h.get("startDate", ""),
                "endDate": h.get("endDate", ""),
                "registrationStartDate": h.get("registrationStartDate", ""),
                "registrationEndDate": h.get("registrationEndDate", ""),
                "status": h.get("status", ""),
                "visibility": h.get("visibility", ""),
                "location": h.get("location", ""),
                "city": h.get("city", ""),
                "state": h.get("state", ""),
                "venue": h.get("venue", ""),
                "registeredTeams": h.get("registeredTeams", 0),
                "maxTeams": h.get("maxTeams", 0),
                "brackets": h.get("brackets", []),
                "imageUrl": h.get("imageUrl", ""),
                "clubName": h.get("clubName", h.get("club", {}).get("name", "") if isinstance(h.get("club"), dict) else ""),
                "url": h.get("url", ""),
                "description": h.get("description", ""),
            }
            tournaments.append(t)
        result = {"tournaments": tournaments, "total": total}
        _cache[cache_key] = (time.time(), result)
        return jsonify(result)
    except Exception as exc:
        app.logger.error("Tournament search error: %s", exc)
        return jsonify({"error": "Failed to search tournaments"}), 500


@app.route("/api/events/past", methods=["POST"])
def api_events_past():
    """Return past tournaments for the Events tab: friends, local, top-rated."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    category = data.get("category", "friends")  # friends | local | top-rated

    cache_key = f"events_past:{category}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 120:
        return jsonify(cached[1])

    if category == "friends":
        # Extract unique tournaments from followed players' recent matches
        sid = _get_sid()
        feed = _build_feed(token, sid)
        matches = feed.get("matches", [])
        players_map = {str(p.get("id", "")): p.get("name", "") for p in feed.get("players", [])}

        # Group by eventName, collect metadata
        events = {}
        for m in matches:
            en = m.get("eventName") or m.get("league") or ""
            if not en or en.upper() in ("OPEN PLAY", "REC PLAY", "RECREATIONAL", "LEAGUE PLAY", ""):
                continue
            if en not in events:
                events[en] = {
                    "name": en,
                    "date": m.get("eventDate") or m.get("matchDate") or m.get("date") or "",
                    "location": "",
                    "players": set(),
                    "playerIds": set(),
                    "matchCount": 0,
                    "format": "",
                }
            events[en]["matchCount"] += 1
            if not events[en]["format"]:
                events[en]["format"] = _match_format(m)
            pi = m.get("_playerInfo", {})
            pid = str(pi.get("id", ""))
            pname = pi.get("name", "")
            if pid and pname:
                events[en]["players"].add(pname)
                events[en]["playerIds"].add(pid)
            # Try to get location from venue field
            venue = m.get("venue") or m.get("eventLocation") or ""
            if venue and not events[en]["location"]:
                events[en]["location"] = venue

        # Convert sets to lists and sort by date
        result_list = []
        for ev in events.values():
            ev["players"] = sorted(ev["players"])[:5]
            ev["playerIds"] = list(ev["playerIds"])[:5]
            result_list.append(ev)
        result_list.sort(key=lambda e: e["date"], reverse=True)
        result = {"events": result_list[:50]}
        _cache[cache_key] = (time.time(), result)
        return jsonify(result)

    elif category in ("local", "top-rated"):
        # Derive from feed data — group all matches by event, sort differently
        sid = _get_sid()
        feed = _build_feed(token, sid)
        matches = feed.get("matches", [])

        events = {}
        for m in matches:
            en = m.get("eventName") or m.get("league") or ""
            if not en or en.upper() in ("OPEN PLAY", "REC PLAY", "RECREATIONAL", "LEAGUE PLAY", ""):
                continue
            if en not in events:
                events[en] = {
                    "name": en,
                    "date": m.get("eventDate") or m.get("matchDate") or m.get("date") or "",
                    "location": m.get("venue") or m.get("eventLocation") or "",
                    "matchCount": 0,
                    "playerIds": set(),
                    "players": set(),
                    "format": "",
                }
            events[en]["matchCount"] += 1
            if not events[en]["format"]:
                events[en]["format"] = _match_format(m)
            pi = m.get("_playerInfo", {})
            pid = str(pi.get("id", ""))
            pname = pi.get("name", "")
            if pid and pname:
                events[en]["playerIds"].add(pid)
                events[en]["players"].add(pname)
            venue = m.get("venue") or m.get("eventLocation") or ""
            if venue and not events[en]["location"]:
                events[en]["location"] = venue

        result_list = []
        for ev in events.values():
            ev["players"] = sorted(ev["players"])[:5]
            ev["playerIds"] = list(ev["playerIds"])[:5]
            result_list.append(ev)

        if category == "local":
            # Sort by most recent date
            result_list.sort(key=lambda e: e["date"], reverse=True)
        else:
            # Sort by most matches (proxy for most competitive/popular)
            result_list.sort(key=lambda e: e["matchCount"], reverse=True)

        result = {"events": result_list[:50]}
        _cache[cache_key] = (time.time(), result)
        return jsonify(result)

    return jsonify({"events": []})


@app.route("/api/events/local", methods=["POST"])
def api_events_local():
    """Search DUPR events by city name, return categorized results."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    city = (data.get("city") or "").strip()
    if not city:
        return jsonify({"error": "city is required"}), 400

    cache_key = f"events_local:{city.lower()}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 300:  # 5 min cache
        return jsonify(cached[1])

    # Search DUPR events API — try multiple query variations for coverage
    # "Raleigh, NC" → search "Raleigh" (city only) + "North Carolina" (state name)
    all_hits = {}
    # US state abbreviation → full name for broader search
    _state_abbrevs = {
        "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
        "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
        "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas",
        "KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts",
        "MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana",
        "NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico",
        "NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
        "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
        "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont",
        "VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
    }
    queries = set()
    # Split "Raleigh, NC" → city_part="Raleigh", state_part="NC"
    parts = [p.strip() for p in city.split(",")]
    city_name = parts[0]
    queries.add(city_name)
    if len(parts) > 1:
        state_code = parts[1].upper().strip()
        state_full = _state_abbrevs.get(state_code, "")
        if state_full:
            queries.add(state_full)
    queries = list(queries)
    try:
        for q in queries:
            resp = _dupr_post("/event/v1.0/search", token, {
                "query": q,
                "limit": 25,
                "offset": 0,
            })
            if resp.status_code == 200:
                hits = resp.json().get("result", {}).get("hits", [])
                for h in hits:
                    lid = h.get("leagueId")
                    if lid and lid not in all_hits:
                        all_hits[lid] = h
    except Exception as e:
        app.logger.warning(f"Events local search error: {e}")

    # Classify each event
    def _classify_event(hit):
        name = (hit.get("leagueName") or "").lower()
        brackets = hit.get("brackets", [])
        elims = {b.get("elimination", "").upper() for b in brackets}

        # League signals
        league_kw = ["league", "ladder", "flex", "weekly", "season", "series",
                      "drop-in", "dropin", "club play", "open play", "rec play",
                      "social play", "mixer"]
        if any(kw in name for kw in league_kw):
            return "league"

        # Tournament signals
        tourn_kw = ["tournament", "tourney", "open", "championship", "championships",
                     "classic", "cup", "slam", "shootout", "showdown", "invitational",
                     "nationals", "regionals", "qualifier", "grand prix", "masters",
                     "challenge", "battle", "brawl", "bash", "fest", "ppa", "mlp",
                     "app tour", "ussp", "amateur"]
        if any(kw in name for kw in tourn_kw):
            return "tournament"

        # Elimination type hints
        if "SINGLE_ELIMINATION" in elims or "DOUBLE_ELIMINATION" in elims:
            return "tournament"
        if "ROUND_ROBIN" in elims and ("round robin" in name or "rr" in name):
            return "tournament"

        # Default: event
        return "event"

    events = []
    for h in all_hits.values():
        brackets = h.get("brackets", [])
        formats = sorted({b.get("format", "").upper() for b in brackets if b.get("format")})
        statuses = {b.get("durationStatus", "").upper() for b in brackets}
        elims = sorted({b.get("elimination", "").replace("_", " ").title() for b in brackets if b.get("elimination")})
        addr = h.get("address", {})
        dur = h.get("duration", [])
        reg = h.get("registrationDate", [])

        # Determine overall status
        if "UPCOMING" in statuses:
            status = "upcoming"
        elif "IN_PROGRESS" in statuses or "LIVE" in statuses:
            status = "live"
        elif "COMPLETE" in statuses:
            status = "completed"
        else:
            status = "unknown"

        logo_url = ""
        attrs = h.get("attributes", {})
        if attrs.get("logoUrl"):
            logo_url = attrs["logoUrl"].get("value", "")

        events.append({
            "id": h.get("leagueId"),
            "name": h.get("leagueName", ""),
            "category": _classify_event(h),
            "status": status,
            "address": addr.get("formattedAddress", ""),
            "startDate": dur[0] if dur else "",
            "endDate": dur[1] if len(dur) > 1 else "",
            "regStart": reg[0] if reg else "",
            "regEnd": reg[1] if len(reg) > 1 else "",
            "formats": formats,
            "eliminations": elims,
            "logoUrl": logo_url,
            "price": h.get("leaguePrice", ""),
            "skill": h.get("skillLevel", ""),
            "registrationUrl": h.get("registrationUrl", ""),
            "registeredMembers": h.get("registeredMembers", 0),
            "clubName": h.get("clubName", ""),
        })

    # Sort: upcoming first, then by date
    status_order = {"live": 0, "upcoming": 1, "completed": 2, "unknown": 3}
    events.sort(key=lambda e: (status_order.get(e["status"], 3), e.get("startDate") or "9999"))

    result = {"events": events, "city": city}
    _cache[cache_key] = (time.time(), result)
    return jsonify(result)


@app.route("/api/debug/history/<player_id>")
def debug_history(player_id):
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401
    body = {"filters": {}, "limit": 2, "offset": 0, "sort": {"order": "DESC", "parameter": "MATCH_DATE"}}
    resp = _dupr_post(f"/player/v1.0/{player_id}/history", token, body)
    return resp.text, resp.status_code, {"Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Groups — group-level leaderboard + tournaments + highlights + feed
# ---------------------------------------------------------------------------

def _group_opp_names(opp_team: dict) -> list[str]:
    out = []
    for pkey in ("player1", "player2"):
        p = opp_team.get(pkey)
        if p and p.get("fullName"):
            out.append(p["fullName"])
    return out


def _group_opp_ids(opp_team: dict) -> list[str]:
    out = []
    for pkey in ("player1", "player2"):
        p = opp_team.get(pkey)
        if p and p.get("id") is not None:
            out.append(str(p["id"]))
    return out


def _group_partner_name(my_team: dict, my_id: str) -> str | None:
    for pkey in ("player1", "player2"):
        p = my_team.get(pkey)
        if p and str(p.get("id", "")) != str(my_id) and p.get("fullName"):
            return p["fullName"]
    return None


def _group_partner_id(my_team: dict, my_id: str) -> str | None:
    for pkey in ("player1", "player2"):
        p = my_team.get(pkey)
        if p and str(p.get("id", "")) != str(my_id) and p.get("id") is not None:
            return str(p["id"])
    return None


def _group_scores(my_team: dict, opp_team: dict) -> list[list[int]]:
    out = []
    for g in range(1, 6):
        s_my = my_team.get(f"game{g}")
        s_opp = opp_team.get(f"game{g}")
        if s_my is not None and s_my >= 0 and s_opp is not None and s_opp >= 0:
            out.append([int(s_my), int(s_opp)])
    return out


def _group_parse_date(s: str | None):
    if not s:
        return None
    from datetime import datetime as _dt
    try:
        if "T" in s:
            return _dt.fromisoformat(s.replace("Z", "+00:00"))
        return _dt.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:
        return None


@app.route("/api/group/<group_id>")
def api_group(group_id):
    """Group page: leaderboard, shared tournaments, highlights, feed.

    For now a single hardcoded group id ``method-park`` maps to the current
    session's watchlist (seeded on first login with the 11 default pros).
    """
    from datetime import timedelta  # local — timedelta not in module-level imports

    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    sid = _get_sid()

    if group_id == "your-circle":
        source_members = _your_circle_members(token, sid)
        group_meta = {
            "id": group_id,
            "name": "Your Circle",
            "home": "Everyone you follow",
        }
    elif group_id == "method-park":
        watches = _load_watches(sid)
        if not watches:
            _seed_default_watches(sid)
            watches = _load_watches(sid)
        source_members = watches
        group_meta = {
            "id": group_id,
            "name": "Method Park",
            "home": "Raleigh, NC",
        }
    else:
        ug = next((g for g in _load_user_groups(sid) if g.get("id") == group_id), None)
        if not ug:
            return jsonify({"error": "group not found"}), 404
        source_members = ug.get("members", [])
        group_meta = {
            "id": ug.get("id"),
            "name": ug.get("name", "Group"),
            "home": ug.get("home", ""),
        }

    members = []
    for w in source_members:
        pid = str(w.get("id", ""))
        if not pid:
            continue
        members.append({
            "id": pid,
            "name": w.get("name", ""),
            "imageUrl": w.get("imageUrl", "") or "",
            "duprDoubles": w.get("doublesRating") or w.get("rating"),
            "duprSingles": w.get("singlesRating"),
        })

    cache_key = f"group:{group_id}:{sid}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[0] < 300:
        return jsonify(cached[1])

    # Fetch 300 recent matches (12 pages × 25) per member in parallel.
    # Keep concurrency low (4 inner × 8 outer = 32 simultaneous) — DUPR
    # rate-limits aggressively above ~50 in-flight, returning empty hits.
    def _fetch_member_history(pid: str) -> list:
        collected: list = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = [ex.submit(_fetch_player_history, pid, token, 25, off) for off in range(0, 300, 25)]
            for f in futs:
                try:
                    r = f.result()
                    if r and r[0] == "__401__":
                        return ["__401__"]
                    collected.extend(r)
                except Exception:
                    pass
        return collected

    member_matches: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        fut_to_pid = {ex.submit(_fetch_member_history, m["id"]): m["id"] for m in members}
        for fut in as_completed(fut_to_pid):
            pid = fut_to_pid[fut]
            try:
                r = fut.result()
                if r and r[0] == "__401__":
                    return jsonify({"error": "unauthorized"}), 401
                member_matches[pid] = r
            except Exception:
                member_matches[pid] = []

    now = datetime.now(timezone.utc)
    month_ago = now - timedelta(days=30)
    year_ago = now - timedelta(days=365)
    # Group "story" window — matches the feed Stories row (last 24 days),
    # so the gd-crest story content stays in sync with the per-group circle
    # rendered in the main feed. (kept the variable name for legacy callers).
    two_days_ago = now - timedelta(days=24)

    leaderboard: list[dict] = []
    events_agg: dict[str, dict] = {}
    highlights: list[dict] = []
    feed: list[dict] = []
    # matchId → {match, perspectivePid, perspectiveName, perspectiveImage, date}
    # First member to encounter a match owns the perspective; this avoids
    # duplicate cards when two group members played the same match.
    recent_story_by_mid: dict[str, dict] = {}
    member_by_id = {m["id"]: m for m in members}

    def _new_bucket():
        return {
            "wins": 0, "losses": 0,
            "games_won": 0, "games_lost": 0,
            "pts_won": 0, "pts_lost": 0,
            "delta_month": 0.0, "delta_year": 0.0, "delta_total": 0.0,
            "opp_sum": 0.0, "opp_n": 0,
            "streak": [],
            "tournament_matches": 0,
            # ── Time-windowed counters (month / year), for segment-aware stats ──
            "wins_m": 0, "losses_m": 0,
            "games_won_m": 0, "games_lost_m": 0,
            "pts_won_m": 0, "pts_lost_m": 0,
            "wins_y": 0, "losses_y": 0,
            "games_won_y": 0, "games_lost_y": 0,
            "pts_won_y": 0, "pts_lost_y": 0,
        }

    for m in members:
        pid = m["id"]
        raw = member_matches.get(pid, []) or []
        matches = [x for x in raw if isinstance(x, dict)]

        # Per-type buckets: 'all' is the union; 'singles', 'doubles' and 'mixed'
        # are partitions. Mixed matches still count toward 'doubles' (since mixed
        # is a doubles format) AND get their own dedicated 'mixed' bucket.
        buckets = {
            "all": _new_bucket(),
            "singles": _new_bucket(),
            "doubles": _new_bucket(),
            "mixed": _new_bucket(),
        }

        for mt in matches:
            teams = mt.get("teams", [])
            if len(teams) < 2:
                continue
            my_idx = -1
            for i, t in enumerate(teams):
                for pkey in ("player1", "player2"):
                    p = t.get(pkey) or {}
                    if str(p.get("id", "")) == str(pid):
                        my_idx = i
                        break
                if my_idx >= 0:
                    break
            if my_idx < 0:
                continue
            my_team = teams[my_idx]
            opp_team = teams[1 - my_idx]

            p1 = my_team.get("player1") or {}
            pn = 1 if str(p1.get("id", "")) == str(pid) else 2

            fmt = _match_format(mt)
            is_doubles = fmt in ("doubles", "mixed")
            # Mixed matches feed both the 'doubles' partition AND the dedicated
            # 'mixed' bucket so users can drill into mixed-only stats.
            type_keys = []
            if fmt == "singles":
                type_keys = ["singles"]
            elif fmt == "doubles":
                type_keys = ["doubles"]
            elif fmt == "mixed":
                type_keys = ["doubles", "mixed"]
            target_buckets = [buckets["all"]] + [buckets[k] for k in type_keys]

            winner_flag = my_team.get("winner")
            won = winner_flag is True
            # Resolve match date once for this iteration so we can drive both
            # delta windows AND per-time-window match/game/point counters.
            _md = _group_parse_date(mt.get("eventDate") or mt.get("matchDate"))
            in_month = bool(_md and _md >= month_ago)
            in_year = bool(_md and _md >= year_ago)
            for b in target_buckets:
                if winner_flag is True:
                    b["wins"] += 1
                    if in_month: b["wins_m"] += 1
                    if in_year:  b["wins_y"] += 1
                elif winner_flag is False:
                    b["losses"] += 1
                    if in_month: b["losses_m"] += 1
                    if in_year:  b["losses_y"] += 1
                if winner_flag is True or winner_flag is False:
                    b["streak"].append(won)

            match_games = []
            for g in range(1, 6):
                s_my = my_team.get(f"game{g}")
                s_opp = opp_team.get(f"game{g}")
                if s_my is not None and s_my >= 0 and s_opp is not None and s_opp >= 0:
                    for b in target_buckets:
                        b["pts_won"] += s_my
                        b["pts_lost"] += s_opp
                        if s_my > s_opp:
                            b["games_won"] += 1
                        elif s_my < s_opp:
                            b["games_lost"] += 1
                        if in_month:
                            b["pts_won_m"] += s_my
                            b["pts_lost_m"] += s_opp
                            if s_my > s_opp: b["games_won_m"] += 1
                            elif s_my < s_opp: b["games_lost_m"] += 1
                        if in_year:
                            b["pts_won_y"] += s_my
                            b["pts_lost_y"] += s_opp
                            if s_my > s_opp: b["games_won_y"] += 1
                            elif s_my < s_opp: b["games_lost_y"] += 1
                    match_games.append((s_my, s_opp))

            rim = my_team.get("preMatchRatingAndImpact") or {}
            delta = rim.get(f"matchDoubleRatingImpactPlayer{pn}" if is_doubles else f"matchSingleRatingImpactPlayer{pn}")
            if not isinstance(delta, (int, float)):
                delta = rim.get(f"matchDoubleRatingImpactPlayer{pn}") or rim.get(f"matchSingleRatingImpactPlayer{pn}")
            if isinstance(delta, (int, float)):
                d = _group_parse_date(mt.get("eventDate") or mt.get("matchDate"))
                for b in target_buckets:
                    b["delta_total"] += delta
                    if d:
                        if d >= month_ago:
                            b["delta_month"] += delta
                        if d >= year_ago:
                            b["delta_year"] += delta

            for pkey in ("player1", "player2"):
                op = opp_team.get(pkey) or {}
                pmr = op.get("postMatchRating") or {}
                r_val = pmr.get("doubles") if is_doubles else pmr.get("singles")
                if isinstance(r_val, (int, float)):
                    for b in target_buckets:
                        b["opp_sum"] += r_val
                        b["opp_n"] += 1

            my_rating_val = (member_by_id[pid].get("duprDoubles") if is_doubles
                             else member_by_id[pid].get("duprSingles"))

            opp_ratings: list[float] = []
            for pkey in ("player1", "player2"):
                op = opp_team.get(pkey) or {}
                pmr = op.get("postMatchRating") or {}
                r_val = pmr.get("doubles") if is_doubles else pmr.get("singles")
                if isinstance(r_val, (int, float)):
                    opp_ratings.append(r_val)
            opp_avg = sum(opp_ratings) / len(opp_ratings) if opp_ratings else None

            event_name = (mt.get("eventName") or mt.get("league") or "").strip()
            is_tournament = event_name and event_name.upper() not in (
                "OPEN PLAY", "REC PLAY", "RECREATIONAL", "LEAGUE PLAY", "",
            )

            mid = str(mt.get("matchId") or mt.get("id") or "")
            the_date = mt.get("eventDate") or mt.get("matchDate") or ""
            scores_list = _group_scores(my_team, opp_team)
            partner = _group_partner_name(my_team, pid)
            partner_id = _group_partner_id(my_team, pid)
            opp_names = _group_opp_names(opp_team)
            opp_ids = _group_opp_ids(opp_team)

            # Recent group story: every match (rec + tournament) in last 2 days.
            # First member to encounter the match owns the perspective.
            if mid and mid not in recent_story_by_mid:
                d_obj = _group_parse_date(the_date)
                if d_obj and d_obj >= two_days_ago:
                    recent_story_by_mid[mid] = {
                        "match": mt,
                        "perspectivePid": pid,
                        "perspectiveName": m["name"],
                        "perspectiveImage": m["imageUrl"],
                        "date": the_date,
                    }

            # Big delta highlight (|Δ| ≥ 0.10)
            if isinstance(delta, (int, float)) and abs(delta) >= 0.10:
                highlights.append({
                    "kind": "delta",
                    "memberId": pid,
                    "memberName": m["name"],
                    "memberImage": m["imageUrl"],
                    "delta": round(delta, 3),
                    "won": won,
                    "eventName": event_name,
                    "date": the_date,
                    "scores": scores_list,
                    "partner": partner,
                    "opponents": opp_names,
                    "discipline": fmt,
                    "matchId": mid,
                })

            # Upset highlight: WON with opp avg DUPR ≥ 0.20 higher than mine
            if won and isinstance(my_rating_val, (int, float)) and opp_avg is not None:
                diff = opp_avg - my_rating_val
                if diff >= 0.20:
                    highlights.append({
                        "kind": "upset",
                        "memberId": pid,
                        "memberName": m["name"],
                        "memberImage": m["imageUrl"],
                        "myRating": round(my_rating_val, 3),
                        "oppAvgRating": round(opp_avg, 3),
                        "diff": round(diff, 3),
                        "eventName": event_name,
                        "date": the_date,
                        "scores": scores_list,
                        "partner": partner,
                        "opponents": opp_names,
                        "discipline": fmt,
                        "matchId": mid,
                    })

            if is_tournament:
                for b in target_buckets:
                    b["tournament_matches"] += 1
                agg = events_agg.setdefault(event_name, {
                    "name": event_name,
                    "memberIds": set(),
                    "memberNames": {},
                    "matchIds": set(),
                    "wins": 0,
                    "losses": 0,
                    "delta": 0.0,
                    "date": the_date,
                    "location": mt.get("eventLocation") or mt.get("venue") or mt.get("clubName") or "",
                    "format": fmt,
                })
                agg["memberIds"].add(pid)
                agg["memberNames"][pid] = m["name"]
                if mid:
                    agg["matchIds"].add(mid)
                if winner_flag is True:
                    agg["wins"] += 1
                elif winner_flag is False:
                    agg["losses"] += 1
                if isinstance(delta, (int, float)):
                    agg["delta"] += delta
                if the_date > (agg["date"] or ""):
                    agg["date"] = the_date

                feed.append({
                    "memberId": pid,
                    "memberName": m["name"],
                    "memberImage": m["imageUrl"],
                    "eventName": event_name,
                    "date": the_date,
                    "won": won if (winner_flag is True or winner_flag is False) else None,
                    "delta": round(delta, 3) if isinstance(delta, (int, float)) else None,
                    "scores": scores_list,
                    "partner": partner,
                    "partnerId": partner_id,
                    "opponents": opp_names,
                    "opponentIds": opp_ids,
                    "discipline": fmt,
                    "matchId": mid,
                })

        def _stats_from_bucket(b: dict) -> dict:
            wins = b["wins"]; losses = b["losses"]
            games_won = b["games_won"]; games_lost = b["games_lost"]
            pts_won = b["pts_won"]; pts_lost = b["pts_lost"]
            total = wins + losses
            total_games = games_won + games_lost
            total_pts = pts_won + pts_lost
            match_pct = round((wins / total * 100), 1) if total else 0
            games_pct = round((games_won / total_games * 100), 1) if total_games else 0
            pts_pct = round((pts_won / total_pts * 100), 1) if total_pts else 0
            opp_avg_all = round((b["opp_sum"] / b["opp_n"]), 3) if b["opp_n"] else 0.0
            longest = cur = 0
            for w in b["streak"]:
                cur = cur + 1 if w else 0
                longest = max(longest, cur)
            # Time-windowed stats (month / year) — same shape, narrower window.
            wins_m = b["wins_m"]; losses_m = b["losses_m"]
            wins_y = b["wins_y"]; losses_y = b["losses_y"]
            total_m = wins_m + losses_m
            total_y = wins_y + losses_y
            gw_m = b["games_won_m"]; gl_m = b["games_lost_m"]
            gw_y = b["games_won_y"]; gl_y = b["games_lost_y"]
            pw_m = b["pts_won_m"]; pl_m = b["pts_lost_m"]
            pw_y = b["pts_won_y"]; pl_y = b["pts_lost_y"]
            mw_pct_m = round(wins_m / total_m * 100, 1) if total_m else 0
            mw_pct_y = round(wins_y / total_y * 100, 1) if total_y else 0
            gw_pct_m = round(gw_m / (gw_m + gl_m) * 100, 1) if (gw_m + gl_m) else 0
            gw_pct_y = round(gw_y / (gw_y + gl_y) * 100, 1) if (gw_y + gl_y) else 0
            pw_pct_m = round(pw_m / (pw_m + pl_m) * 100, 1) if (pw_m + pl_m) else 0
            pw_pct_y = round(pw_y / (pw_y + pl_y) * 100, 1) if (pw_y + pl_y) else 0
            return {
                "matches": total,
                "wins": wins,
                "losses": losses,
                "matchWinPct": match_pct,
                "gamesWon": games_won,
                "gamesLost": games_lost,
                "gameWinPct": games_pct,
                "ptsWon": pts_won,
                "ptsLost": pts_lost,
                "ptWinPct": pts_pct,
                "deltaMonth": round(b["delta_month"], 3),
                "deltaYear": round(b["delta_year"], 3),
                "deltaTotal": round(b["delta_total"], 3),
                "avgOppDupr": opp_avg_all,
                "streak": longest,
                "tournamentMatches": b["tournament_matches"],
                # Per-time-window stats — surfaced for segment-aware leaderboard
                "matchesMonth": total_m, "winsMonth": wins_m, "lossesMonth": losses_m,
                "gamesWonMonth": gw_m, "gamesLostMonth": gl_m,
                "ptsWonMonth": pw_m, "ptsLostMonth": pl_m,
                "matchWinPctMonth": mw_pct_m,
                "gameWinPctMonth": gw_pct_m,
                "ptWinPctMonth": pw_pct_m,
                "matchesYear": total_y, "winsYear": wins_y, "lossesYear": losses_y,
                "gamesWonYear": gw_y, "gamesLostYear": gl_y,
                "ptsWonYear": pw_y, "ptsLostYear": pl_y,
                "matchWinPctYear": mw_pct_y,
                "gameWinPctYear": gw_pct_y,
                "ptWinPctYear": pw_pct_y,
            }

        all_stats = _stats_from_bucket(buckets["all"])
        singles_stats = _stats_from_bucket(buckets["singles"])
        doubles_stats = _stats_from_bucket(buckets["doubles"])
        mixed_stats = _stats_from_bucket(buckets["mixed"])

        leaderboard.append({
            "id": pid,
            "name": m["name"],
            "imageUrl": m["imageUrl"],
            "duprDoubles": m.get("duprDoubles"),
            "duprSingles": m.get("duprSingles"),
            "byType": {
                "all": all_stats,
                "singles": singles_stats,
                "doubles": doubles_stats,
                "mixed": mixed_stats,
            },
            **all_stats,
        })

    # Tournaments: include every event any member played. The frontend has a
    # "Shared / All" toggle and filters to ≥2-member events client-side when
    # the user picks "Shared" — which is the default.
    tournaments_out = []
    for key, agg in events_agg.items():
        member_list = []
        for pid in sorted(agg["memberIds"]):
            mb = member_by_id.get(pid, {})
            member_list.append({
                "id": pid,
                "name": agg["memberNames"].get(pid, mb.get("name", "")),
                "imageUrl": mb.get("imageUrl", ""),
            })
        tournaments_out.append({
            "name": agg["name"],
            "members": member_list,
            "memberCount": len(agg["memberIds"]),
            "matchCount": len(agg["matchIds"]),
            "wins": agg["wins"],
            "losses": agg["losses"],
            "delta": round(agg["delta"], 3),
            "date": agg["date"],
            "location": agg["location"],
            "format": agg["format"],
        })
    tournaments_out.sort(key=lambda t: (t.get("date") or ""), reverse=True)

    # Feed — dedup by matchId (same match can appear for two members), keep newest.
    feed.sort(key=lambda f: (f.get("date") or ""), reverse=True)
    seen_m: set[str] = set()
    feed_dedup = []
    for it in feed:
        mid = it.get("matchId") or ""
        if mid and mid in seen_m:
            continue
        if mid:
            seen_m.add(mid)
        feed_dedup.append(it)
    feed_dedup = feed_dedup[:50]

    # Highlights — dedup by (matchId, kind, memberId), newest first, cap.
    highlights.sort(key=lambda h: (h.get("date") or ""), reverse=True)
    seen_h: set = set()
    hl_dedup = []
    for h in highlights:
        hk = (h.get("matchId"), h.get("kind"), h.get("memberId"))
        if hk in seen_h:
            continue
        seen_h.add(hk)
        hl_dedup.append(h)
    hl_dedup = hl_dedup[:30]

    # Group story — chronological (oldest → newest), capped to keep payload small.
    recent_story_list = sorted(
        recent_story_by_mid.values(),
        key=lambda r: (r.get("date") or ""),
    )[:40]

    result = {
        "group": {
            **group_meta,
            "memberCount": len(members),
            "totalMatches": sum(e["matches"] for e in leaderboard),
            "combinedDelta30d": round(sum(e["deltaMonth"] for e in leaderboard), 3),
            "combinedWins": sum(e["wins"] for e in leaderboard),
            "combinedLosses": sum(e["losses"] for e in leaderboard),
            "tournamentCount": len(tournaments_out),
        },
        "members": [{"id": m["id"], "name": m["name"], "imageUrl": m["imageUrl"]} for m in members],
        "leaderboard": leaderboard,
        "tournaments": tournaments_out,
        "highlights": hl_dedup,
        "feed": feed_dedup,
        "recentStory": recent_story_list,
    }
    _cache[cache_key] = (time.time(), result)
    return jsonify(result)


def _your_circle_members(token: str, sid: str | None = None) -> list[dict]:
    """Members of the user's 'Your Circle' auto-group.

    Mirrors exactly what the sidebar / feed shows: union of the DUPR follow
    graph and the local watch list, deduped by id, following-first ordering.
    """
    by_id: dict[str, dict] = {}

    for p in _get_following(token):
        pid = str(p.get("id") or p.get("playerId") or p.get("userId") or "").strip()
        if not pid or pid in by_id:
            continue
        by_id[pid] = {
            "id": pid,
            "name": _player_name(p),
            "imageUrl": p.get("imageUrl") or p.get("image") or "",
            "doublesRating": p.get("doublesRating"),
            "singlesRating": p.get("singlesRating"),
        }

    for w in _load_watches(sid):
        pid = str(w.get("id") or "").strip()
        if not pid:
            continue
        if pid not in by_id:
            by_id[pid] = {
                "id": pid,
                "name": w.get("name", ""),
                "imageUrl": w.get("imageUrl") or "",
                "doublesRating": w.get("doublesRating") or w.get("rating"),
                "singlesRating": w.get("singlesRating"),
            }
        elif w.get("imageUrl") and not by_id[pid].get("imageUrl"):
            by_id[pid]["imageUrl"] = w["imageUrl"]

    return list(by_id.values())


@app.route("/api/groups")
def api_groups_list():
    """List of groups the user has — Your Circle (auto, follow-graph-backed),
    Method Park (auto, watchlist-backed) plus user-created groups."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401
    sid = _get_sid()

    # Your Circle — live mirror of the user's follow graph (union of DUPR
    # following + local watch list, same set the sidebar shows).
    yc_members = _your_circle_members(token, sid)
    yc_preview = [{
        "id": m["id"], "name": m["name"], "imageUrl": m["imageUrl"],
    } for m in yc_members]

    watches = _load_watches(sid)
    if not watches:
        _seed_default_watches(sid)
        watches = _load_watches(sid)
    mp_members = [{
        "id": str(w.get("id", "")),
        "name": w.get("name", ""),
        "imageUrl": w.get("imageUrl", "") or "",
    } for w in watches if w.get("id")]
    groups_out = [{
        "id": "your-circle",
        "name": "Your Circle",
        "home": "Everyone you follow",
        "auto": True,
        "kind": "circle",
        "memberCount": len(yc_preview),
        "members": yc_preview[:8],
        "memberIds": [m["id"] for m in yc_preview],
    }, {
        "id": "method-park",
        "name": "Method Park",
        "home": "Raleigh, NC",
        "memberCount": len(mp_members),
        "members": mp_members[:8],
        # Full id list — used client-side to map feed matches → group story circles.
        "memberIds": [m["id"] for m in mp_members],
    }]
    for g in _load_user_groups(sid):
        gm = g.get("members", []) or []
        preview = [{
            "id": str(m.get("id", "")),
            "name": m.get("name", ""),
            "imageUrl": m.get("imageUrl", "") or "",
        } for m in gm if m.get("id")]
        groups_out.append({
            "id": g.get("id"),
            "name": g.get("name", "Group"),
            "home": g.get("home", ""),
            "memberCount": len(preview),
            "members": preview[:8],
            "memberIds": [m["id"] for m in preview],
        })
    return jsonify({"groups": groups_out})


@app.route("/api/groups/create", methods=["POST"])
def api_groups_create():
    """Create a new user group. Body: { name, members: [{id, name, imageUrl, doublesRating, singlesRating}, ...] }"""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401
    sid = _get_sid()
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    members_in = data.get("members") or []
    if not name:
        return jsonify({"error": "name required"}), 400
    if not isinstance(members_in, list) or not members_in:
        return jsonify({"error": "members required"}), 400

    seen_ids: set[str] = set()
    members: list[dict] = []
    for m in members_in:
        if not isinstance(m, dict):
            continue
        pid = str(m.get("id") or "").strip()
        if not pid or pid in seen_ids:
            continue
        seen_ids.add(pid)
        members.append({
            "id": pid,
            "name": (m.get("name") or "").strip(),
            "imageUrl": m.get("imageUrl") or "",
            "doublesRating": m.get("doublesRating"),
            "singlesRating": m.get("singlesRating"),
        })
    if not members:
        return jsonify({"error": "members required"}), 400

    groups = _load_user_groups(sid)
    new_id = f"g_{int(time.time() * 1000)}"
    while any(g.get("id") == new_id for g in groups):
        new_id = f"g_{int(time.time() * 1000)}_{len(groups)}"
    new_group = {
        "id": new_id,
        "name": name,
        "createdAt": time.time(),
        "members": members,
    }
    groups.append(new_group)
    _save_user_groups(groups, sid)
    return jsonify({"group": {
        "id": new_group["id"],
        "name": new_group["name"],
        "memberCount": len(members),
    }})


@app.route("/api/groups/<group_id>", methods=["DELETE"])
def api_groups_delete(group_id: str):
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401
    sid = _get_sid()
    if group_id in ("method-park", "your-circle"):
        return jsonify({"error": "cannot delete built-in group"}), 400
    groups = _load_user_groups(sid)
    new_groups = [g for g in groups if g.get("id") != group_id]
    if len(new_groups) == len(groups):
        return jsonify({"error": "group not found"}), 404
    _save_user_groups(new_groups, sid)
    # Drop any cached detail
    _cache.pop(f"group:{group_id}:{sid}", None)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Onboarding (first-visit welcome flow)
# ---------------------------------------------------------------------------

@app.route("/api/onboarding/status")
def api_onboarding_status():
    """Return whether this visitor has completed the welcome flow."""
    _get_sid()
    return jsonify({"onboarded": bool(session.get("onboarded", False))})


@app.route("/api/onboarding/complete", methods=["POST"])
def api_onboarding_complete():
    """Mark onboarding done. If `players` is non-empty, replace the watch list."""
    data = request.get_json(silent=True) or {}
    players = data.get("players") or []
    skipped = bool(data.get("skipped"))

    sid = _get_sid()

    if isinstance(players, list) and players:
        new_watches = []
        seen = set()
        for p in players:
            pid = str(p.get("id", "")).strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            dr = p.get("doublesRating")
            sr = p.get("singlesRating")
            new_watches.append({
                "id": pid,
                "name": p.get("name", "Unknown"),
                "rating": p.get("rating") or dr or sr,
                "doublesRating": dr,
                "singlesRating": sr,
                "imageUrl": p.get("imageUrl", ""),
            })
        if new_watches:
            _save_watches(new_watches, sid)
            _cache.pop(f"feed:{sid}", None)

    session["onboarded"] = True
    _log_event("onboarding_complete",
               count=len(players) if isinstance(players, list) else 0,
               skipped=skipped)
    return jsonify({"ok": True})


@app.route("/api/onboarding/network", methods=["POST"])
def api_onboarding_network():
    """Given a playerId, return the player's profile, frequent partners,
    top opponents, and top-rated pros in their city — for follow suggestions."""
    token = _get_token()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    player_id = str(data.get("playerId", "")).strip()
    if not player_id:
        return jsonify({"error": "playerId required"}), 400

    _log_event("onboarding_network", pid=player_id)

    def _fetch_profile():
        try:
            r = _dupr_get(f"/player/v1.0/{player_id}", token)
            if r.status_code == 200:
                return r.json().get("result") or {}
        except Exception:
            pass
        return {}

    def _fetch_history():
        # Pull up to 200 matches for a robust partner/opponent signal
        all_m: list[dict] = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(_fetch_player_history, player_id, token, 25, off)
                       for off in range(0, 200, 25)]
            for f in as_completed(futures):
                try:
                    r = f.result()
                    if r and r[0] != "__401__":
                        all_m.extend(r)
                except Exception:
                    pass
        return all_m

    with ThreadPoolExecutor(max_workers=2) as ex:
        prof_fut = ex.submit(_fetch_profile)
        hist_fut = ex.submit(_fetch_history)
        profile = prof_fut.result()
        matches = hist_fut.result()

    # Tally partners + opponents from history
    partners: dict[str, dict] = {}
    opponents: dict[str, dict] = {}
    seen_match_ids = set()
    for m in matches:
        mid = m.get("matchId") or m.get("id")
        if mid in seen_match_ids:
            continue
        seen_match_ids.add(mid)
        teams = m.get("teams", [])
        if len(teams) < 2:
            continue
        my_idx = -1
        for i, t in enumerate(teams):
            for p in (t.get("player1"), t.get("player2")):
                if p and str(p.get("id", "")) == player_id:
                    my_idx = i
                    break
            if my_idx >= 0:
                break
        if my_idx < 0:
            continue
        opp_idx = 1 - my_idx
        for p in (teams[my_idx].get("player1"), teams[my_idx].get("player2")):
            if p and str(p.get("id", "")) != player_id:
                pid = str(p.get("id"))
                entry = partners.setdefault(pid, {
                    "id": pid,
                    "name": _player_name(p),
                    "imageUrl": p.get("imageUrl", ""),
                    "count": 0,
                })
                entry["count"] += 1
        for p in (teams[opp_idx].get("player1"), teams[opp_idx].get("player2")):
            if p and str(p.get("id", "")) != player_id:
                pid = str(p.get("id"))
                entry = opponents.setdefault(pid, {
                    "id": pid,
                    "name": _player_name(p),
                    "imageUrl": p.get("imageUrl", ""),
                    "count": 0,
                })
                entry["count"] += 1

    partner_list = sorted(partners.values(), key=lambda x: -x["count"])
    opponent_list = sorted(opponents.values(), key=lambda x: -x["count"])[:5]

    # Hydrate ratings for partners + top opponents in parallel
    suggest_ids = list({p["id"] for p in partner_list} | {o["id"] for o in opponent_list})

    def _hydrate(pid: str):
        try:
            r = _dupr_get(f"/player/v1.0/{pid}", token)
            if r.status_code == 200:
                det = r.json().get("result") or {}
                ratings = _extract_ratings(det)
                return pid, {
                    "doublesRating": ratings["doublesRating"],
                    "singlesRating": ratings["singlesRating"],
                    "imageUrl": det.get("imageUrl", ""),
                    "location": _format_location(det),
                }
        except Exception:
            pass
        return pid, {}

    hydrated: dict[str, dict] = {}
    if suggest_ids:
        with ThreadPoolExecutor(max_workers=min(20, len(suggest_ids))) as ex:
            for pid, det in ex.map(_hydrate, suggest_ids):
                hydrated[pid] = det

    for lst in (partner_list, opponent_list):
        for p in lst:
            det = hydrated.get(p["id"], {})
            if det.get("doublesRating") is not None:
                p["doublesRating"] = det["doublesRating"]
            if det.get("singlesRating") is not None:
                p["singlesRating"] = det["singlesRating"]
            if det.get("imageUrl"):
                p["imageUrl"] = det["imageUrl"]
            if det.get("location"):
                p["location"] = det["location"]

    # Top-rated pros in their city — reuse _format_location for display + geocode
    city_pros: list[dict] = []
    city_text = _format_location(profile)

    exclude_ids = {player_id} | set(partners.keys()) | set(opponents.keys())

    if city_text:
        try:
            geo_resp = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": city_text, "format": "json", "limit": 1},
                headers={"User-Agent": "dupr-feed/1.0"}, timeout=5,
            )
            if geo_resp.status_code == 200 and geo_resp.json():
                geo = geo_resp.json()[0]
                search_filter = {
                    "lat": float(geo["lat"]),
                    "lng": float(geo["lon"]),
                    "locationText": geo.get("display_name", city_text),
                    "rating": {},
                }

                def _one_search(letter: str):
                    try:
                        r = _dupr_post("/player/v1.0/search", token, {
                            "filter": search_filter, "query": letter,
                            "limit": 25, "offset": 0, "includeUnclaimedPlayers": True,
                        })
                        if r.status_code == 200:
                            return r.json().get("result", {}).get("hits", []) or []
                    except Exception:
                        pass
                    return []

                # Small fan-out across common letters — fast, covers most names
                letters = list("abcdefghijklmnopqrstuvwxyz")
                hits_all: list[dict] = []
                with ThreadPoolExecutor(max_workers=12) as ex:
                    for batch in ex.map(_one_search, letters):
                        hits_all.extend(batch)

                rated = []
                seen_pids: set[str] = set()
                for h in hits_all:
                    pid = str(h.get("id", ""))
                    if not pid or pid in seen_pids or pid in exclude_ids:
                        continue
                    seen_pids.add(pid)
                    r = _extract_ratings(h)
                    peak = max(r["doublesRating"] or 0, r["singlesRating"] or 0)
                    if peak <= 0:
                        continue
                    rated.append({
                        "id": pid,
                        "name": _player_name(h),
                        "imageUrl": h.get("imageUrl", ""),
                        "doublesRating": r["doublesRating"],
                        "singlesRating": r["singlesRating"],
                        "_peak": peak,
                    })
                rated.sort(key=lambda x: -x["_peak"])
                city_pros = [{k: v for k, v in p.items() if k != "_peak"} for p in rated[:8]]
        except Exception as e:
            print(f"[ONBOARD] city pros error: {e}", flush=True)

    r_self = _extract_ratings(profile)
    me = {
        "id": player_id,
        "name": _player_name(profile),
        "imageUrl": profile.get("imageUrl", ""),
        "doublesRating": r_self["doublesRating"],
        "singlesRating": r_self["singlesRating"],
        "location": _format_location(profile),
    }

    return jsonify({
        "me": me,
        "partners": partner_list,
        "opponents": opponent_list,
        "cityPros": city_pros,
        "cityText": city_text,
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5001))
    app.run(debug=True, host="0.0.0.0", port=port)
