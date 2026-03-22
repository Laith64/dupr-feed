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

# Simple in-memory cache: key -> (timestamp, data)
_cache: dict[str, tuple[float, object]] = {}
CACHE_TTL = 300  # 5 minutes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _player_name(p: dict) -> str:
    full = p.get("fullName", "")
    if full:
        return full
    first = p.get("firstName", p.get("first", ""))
    last = p.get("lastName", p.get("last", ""))
    if first or last:
        return f"{first} {last}".strip()
    return p.get("name", p.get("displayName", "Unknown"))


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

    return jsonify({"ok": True, "user": session.get("user", {})})


@app.route("/api/feed")
def api_feed():
    token = session.get("token")
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
    token = session.get("token")
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"results": []})

    body = {"filter": {}, "query": query, "limit": 10}
    try:
        resp = _dupr_post("/player/v1.0/search", token, body)
        app.logger.info(f"DUPR search status={resp.status_code} body={resp.text[:300]}")
        if resp.status_code == 401:
            session.clear()
            return jsonify({"error": "unauthorized"}), 401
        if resp.status_code == 200:
            rdata = resp.json()
            result = rdata.get("result", {})
            hits = result.get("hits", []) if isinstance(result, dict) else []
            return jsonify({"results": hits if isinstance(hits, list) else []})
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
    token = session.get("token")
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


@app.route("/api/tournament", methods=["POST"])
def api_tournament():
    """Discover all matches for a tournament via graph traversal."""
    token = session.get("token")
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
    token = session.get("token")
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


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/debug/history/<player_id>")
def debug_history(player_id):
    token = session.get("token")
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
