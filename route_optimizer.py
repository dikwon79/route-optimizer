#!/usr/bin/env python3
"""
CJ Logistics -> McLane Warehouse Route Optimizer

Optimizes delivery routes from CJ Logistics distribution centers to McLane
warehouses across the US, respecting capacity, scheduling, and grouping
constraints.

Usage:
    python3 route_optimizer.py --po-file pos.json
    python3 route_optimizer.py --po-file pos.json --ms-origin "Atlanta, Georgia" --l1-origin "Dallas, Texas"
    python3 route_optimizer.py --generate-sample
"""

import argparse
import itertools
import json
import time
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_COST = 500          # USD per truck
COST_PER_KM = 10         # USD per km
MAX_STOPS = 3
MAX_CAPACITY = 60        # units
AVG_SPEED_KMH = 80
LOADING_TIME_H = 1.0     # hours at origin
UNLOAD_TIME_H = 0.75     # hours per stop (45 min)
MAX_DRIVE_H = 11         # HOS: max 11h driving before mandatory rest
MANDATORY_REST_H = 10    # HOS: 10h off-duty rest

OSRM_BASE = "https://router.project-osrm.org"
OSRM_DELAY = 0.1         # seconds between OSRM requests

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".osrm_cache.json")
PREF_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "learned_preferences.json")

# Default origins (Innofoods warehouses in Canada)
DEFAULT_MS_ORIGIN = "Mississauga, Ontario"
DEFAULT_L1_ORIGIN = "Langley, British Columbia"

# ---------------------------------------------------------------------------
# Warehouse Master Data
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Load warehouse data from JSON file
# ---------------------------------------------------------------------------
DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "warehouse_data.json")


def _load_warehouse_data():
    """Load warehouse, origin, and schedule data from JSON file."""
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def _save_warehouse_data(data):
    """Save warehouse data back to JSON file."""
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def reload_data():
    """Reload all data from JSON file into module-level variables."""
    global WAREHOUSES, WH_BY_NAME, COORDS, ORIGIN_COORDS, ORIGIN_TZ, WH_TZ, _raw_schedule, RECEIVING_HOURS
    data = _load_warehouse_data()

    WAREHOUSES = data.get("warehouses", [])
    WH_BY_NAME = {w["name"]: w for w in WAREHOUSES}
    COORDS = {}
    WH_TZ = {}
    for w in WAREHOUSES:
        if "lat" in w and "lon" in w:
            COORDS[w["name"]] = (w["lat"], w["lon"])
        WH_TZ[w["name"]] = ZoneInfo(w.get("timezone", "America/New_York"))

    ORIGIN_COORDS = {}
    ORIGIN_TZ = {}
    for name, info in data.get("origins", {}).items():
        ORIGIN_COORDS[name] = (info["lat"], info["lon"])
        ORIGIN_TZ[name] = ZoneInfo(info.get("timezone", "America/New_York"))

    _raw_schedule = {}
    for code, windows in data.get("receiving_schedules", {}).items():
        _raw_schedule[code] = [(w[0], w[1], w[2]) for w in windows]

    # Parse receiving hours
    RECEIVING_HOURS = {}
    for code, windows in _raw_schedule.items():
        parsed = []
        for day_spec, start_str, end_str in windows:
            days = _day_range(day_spec)
            s = _parse_time(start_str)
            e = _parse_time(end_str)
            parsed.append((days, s, e))
        RECEIVING_HOURS[code] = parsed


# Module-level variables (initialized below after helper functions)
WAREHOUSES: List[dict] = []
WH_BY_NAME: Dict[str, dict] = {}
COORDS: Dict[str, Tuple[float, float]] = {}
ORIGIN_COORDS: Dict[str, Tuple[float, float]] = {}
ORIGIN_TZ: Dict[str, ZoneInfo] = {}
WH_TZ: Dict[str, ZoneInfo] = {}
_raw_schedule: dict = {}
RECEIVING_HOURS: Dict[str, List[Tuple[List[int], float, float]]] = {}

# ---------------------------------------------------------------------------
# Receiving Hours Schedule
# ---------------------------------------------------------------------------
# Format: code -> list of (days, start_hour, end_hour)
# Hours use 24-hour format. end < start means it crosses midnight.
# Days: 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun

def _day_range(spec: str) -> List[int]:
    """Parse day spec like 'MON-FRI' or 'SUN-THU' into list of weekday ints."""
    day_map = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
    if "-" in spec:
        parts = spec.split("-")
        start_str = parts[0].strip()
        end_str = parts[1].strip()
        # Handle "SUN NIGHT" etc -- strip NIGHT
        start_str = start_str.replace(" NIGHT", "")
        end_str = end_str.replace(" NIGHT", "")
        s = day_map[start_str]
        e = day_map[end_str]
        if s <= e:
            return list(range(s, e + 1))
        else:
            return list(range(s, 7)) + list(range(0, e + 1))
    else:
        d = spec.strip().replace(" NIGHT", "")
        return [day_map[d]]


def _parse_time(t: str) -> float:
    """Parse time string like '6AM', '1PM', '12:30AM' to hours (24h float)."""
    t = t.strip().upper()
    if t in ("CLOSE", "CLOSED", ""):
        return -1
    is_pm = "PM" in t
    is_am = "AM" in t
    t = t.replace("PM", "").replace("AM", "").strip()
    if ":" in t:
        parts = t.split(":")
        h = int(parts[0])
        m = int(parts[1])
    else:
        h = int(t)
        m = 0
    if is_pm and h != 12:
        h += 12
    if is_am and h == 12:
        h = 0
    return h + m / 60.0


# ---------------------------------------------------------------------------
# Initialize data from JSON file
# ---------------------------------------------------------------------------
reload_data()


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in km between two points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_cache() -> dict:
    """Load OSRM distance cache from disk."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def load_preferences() -> dict:
    """Load learned route preferences from disk."""
    if os.path.exists(PREF_FILE):
        try:
            with open(PREF_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"pair_scores": {}, "saved_results": []}


def save_preferences(prefs: dict) -> None:
    """Persist learned preferences to disk."""
    try:
        with open(PREF_FILE, "w") as f:
            json.dump(prefs, f, indent=2)
    except IOError:
        pass


def learn_from_result(routes: List[dict], group: str) -> None:
    """Learn DC pairing preferences from a user-approved result."""
    prefs = load_preferences()
    pair_scores = prefs.get("pair_scores", {})

    for rt in routes:
        # Get DC codes from schedule
        dcs = [s.get("dc_code", "") for s in rt.get("schedule", []) if s.get("dc_code")]
        if len(dcs) < 2:
            continue
        # Score all pairs in this route
        for i in range(len(dcs)):
            for j in range(i + 1, len(dcs)):
                pair_key = "|".join(sorted([dcs[i], dcs[j]]))
                pair_scores[pair_key] = pair_scores.get(pair_key, 0) + 1

    prefs["pair_scores"] = pair_scores

    # Save the result snapshot
    saved = prefs.get("saved_results", [])
    snapshot = {
        "group": group,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "routes": [
            {
                "dcs": [s.get("dc_code", "") for s in rt.get("schedule", [])],
                "cost": rt.get("cost", 0),
                "distance_km": rt.get("distance_km", 0),
            }
            for rt in routes
        ],
        "total_cost": sum(r.get("cost", 0) for r in routes),
    }
    saved.append(snapshot)
    # Keep last 50 results
    prefs["saved_results"] = saved[-50:]

    save_preferences(prefs)


def get_pair_bonus(dc_codes: List[str], prefs: dict) -> float:
    """Get bonus for a route based on learned DC pair preferences.
    Returns a negative cost adjustment (bonus) for preferred pairings."""
    if len(dc_codes) < 2:
        return 0.0
    pair_scores = prefs.get("pair_scores", {})
    bonus = 0.0
    for i in range(len(dc_codes)):
        for j in range(i + 1, len(dc_codes)):
            pair_key = "|".join(sorted([dc_codes[i], dc_codes[j]]))
            score = pair_scores.get(pair_key, 0)
            if score > 0:
                # Each learned pairing gives a cost bonus (reduces effective cost)
                # Scales with number of times this pair was approved
                bonus += min(score, 10) * 2000  # max $20,000 bonus per pair
    return bonus


def save_cache(cache: dict) -> None:
    """Persist OSRM distance cache to disk."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except IOError:
        pass


def osrm_route_distance(coords_list: List[Tuple[float, float]], cache: dict) -> Optional[Tuple[float, float]]:
    """
    Query OSRM for driving distance (km) and duration (hours) along ordered waypoints.

    Args:
        coords_list: List of (lat, lon) tuples in visit order.
        cache: Dict used for caching results.

    Returns:
        Tuple of (distance_km, duration_hours), or None on failure.
    """
    # Build cache key from coordinate list
    key = "|".join(f"{lat:.4f},{lon:.4f}" for lat, lon in coords_list)
    if key in cache:
        val = cache[key]
        # Backward compat: old cache has just a number (distance)
        if isinstance(val, (int, float)):
            # Re-fetch to get duration, or estimate
            dur_h = val / AVG_SPEED_KMH
            return (val, dur_h)
        return (val[0], val[1])

    # OSRM wants lon,lat
    coord_str = ";".join(f"{lon},{lat}" for lat, lon in coords_list)
    url = f"{OSRM_BASE}/route/v1/driving/{coord_str}?overview=false"

    for attempt in range(3):
        try:
            time.sleep(OSRM_DELAY)
            resp = requests.get(url, timeout=15)
            if resp.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            data = resp.json()
            if data.get("code") == "Ok" and data.get("routes"):
                dist_km = data["routes"][0]["distance"] / 1000.0
                dur_h = data["routes"][0]["duration"] / 3600.0
                cache[key] = [dist_km, dur_h]
                return (dist_km, dur_h)
            else:
                break
        except (requests.RequestException, ValueError):
            time.sleep(1)

    # Fallback: sum of haversine segments with 1.3x road factor
    total = 0.0
    for i in range(len(coords_list) - 1):
        total += haversine_km(*coords_list[i], *coords_list[i + 1]) * 1.3
    dur_h = total / AVG_SPEED_KMH
    cache[key] = [total, dur_h]
    return (total, dur_h)


def osrm_pairwise(c1: Tuple[float, float], c2: Tuple[float, float], cache: dict) -> Tuple[float, float]:
    """Get driving distance (km) and duration (hours) between two points."""
    result = osrm_route_distance([c1, c2], cache)
    if result is not None:
        return result
    dist = haversine_km(*c1, *c2) * 1.3
    return (dist, dist / AVG_SPEED_KMH)


def osrm_pairwise_distance(c1: Tuple[float, float], c2: Tuple[float, float], cache: dict) -> float:
    """Get driving distance between two points (backward compat)."""
    return osrm_pairwise(c1, c2, cache)[0]


# ---------------------------------------------------------------------------
# Scheduling Helpers
# ---------------------------------------------------------------------------

def is_within_receiving_hours(dt: datetime, receiving_code: str) -> bool:
    """
    Check if a given datetime falls within the warehouse receiving hours.

    Args:
        dt: The proposed arrival datetime.
        receiving_code: The warehouse receiving schedule code.

    Returns:
        True if the warehouse is open to receive at that time.
    """
    if receiving_code not in RECEIVING_HOURS:
        # Unknown code -- assume always open (warn later)
        return True

    weekday = dt.weekday()  # 0=Mon ... 6=Sun
    hour = dt.hour + dt.minute / 60.0

    for days, open_h, close_h in RECEIVING_HOURS[receiving_code]:
        if open_h < 0 or close_h < 0:
            # CLOSE entry -- skip (warehouse closed on these days)
            continue
        if close_h > open_h:
            # Same-day window
            if weekday in days and open_h <= hour < close_h:
                return True
        else:
            # Crosses midnight: e.g. 8PM-1AM
            # Check evening side: day is in days and hour >= open
            if weekday in days and hour >= open_h:
                return True
            # Check morning side: previous day is in days and hour < close
            prev_day = (weekday - 1) % 7
            if prev_day in days and hour < close_h:
                return True
    return False


def next_receiving_window(start_dt: datetime, receiving_code: str, max_days: int = 7) -> Optional[datetime]:
    """
    Find the next datetime on or after start_dt when the warehouse is open.

    Scans in 30-minute increments up to max_days forward.

    Returns:
        A datetime within receiving hours, or None if none found.
    """
    dt = start_dt.replace(second=0, microsecond=0)
    end = dt + timedelta(days=max_days)
    step = timedelta(minutes=30)
    while dt < end:
        if is_within_receiving_hours(dt, receiving_code):
            return dt
        dt += step
    return None


def compute_arrival_time(
    depart: datetime,
    distance_km: float,
) -> datetime:
    """Compute arrival time given departure and driving distance."""
    travel_hours = distance_km / AVG_SPEED_KMH
    return depart + timedelta(hours=travel_hours)


def get_preferred_appt_hours() -> dict:
    """Get preferred delivery hours per DC from RAG learning data.
    Returns dict like {'MG': 20.5, 'MK': 20.0} (hour in 24h format).
    """
    prefs = load_preferences()
    rag_data = prefs.get("rag_data", [])
    
    dc_hours = {}  # dc -> list of hours
    for entry in rag_data:
        for route in entry.get("routes", []):
            dcs = route.get("dcs", [])
            appts = route.get("appts", [])
            for i, appt in enumerate(appts):
                if i >= len(dcs) or not appt:
                    continue
                dc = dcs[i]
                # Parse APPT time: handle multiple changes "a -> b -> c -> d"
                import re
                original_appt = None
                all_appts = []
                if '->' in appt:
                    parts = [p.strip() for p in appt.split('->')]
                    all_appts = parts
                    original_appt = parts[0]
                    appt = parts[0]  # use FIRST value (original plan) for preferred hour
                # Try to extract hour
                h = None
                # Pattern: H:MM AM/PM
                m = re.search(r'(\d{1,2}):(\d{2})\s*(AM|PM|am|pm)', appt)
                if m:
                    h = int(m.group(1))
                    mins = int(m.group(2))
                    if m.group(3).upper() == 'PM' and h != 12:
                        h += 12
                    if m.group(3).upper() == 'AM' and h == 12:
                        h = 0
                    h += mins / 60.0
                else:
                    # Pattern: HAM/PM
                    m2 = re.search(r'(\d{1,2})\s*(AM|PM|am|pm)', appt)
                    if m2:
                        h = int(m2.group(1))
                        if m2.group(2).upper() == 'PM' and h != 12:
                            h += 12
                        if m2.group(2).upper() == 'AM' and h == 12:
                            h = 0
                if h is not None:
                    if dc not in dc_hours:
                        dc_hours[dc] = {"final": [], "original": [], "delays": 0}
                    dc_hours[dc]["final"].append(h)
                    
                    # If there was an original APPT, track delay pattern
                    if original_appt:
                        dc_hours[dc]["delays"] += 1
                        dc_hours[dc].setdefault("change_counts", []).append(len(all_appts))
                        # Parse original hour too
                        m_orig = re.search(r'(\d{1,2}):(\d{2})\s*(AM|PM|am|pm)', original_appt)
                        if not m_orig:
                            m_orig = re.search(r'(\d{1,2})\s*(AM|PM|am|pm)', original_appt)
                        if m_orig:
                            oh = int(m_orig.group(1))
                            if len(m_orig.groups()) == 3:
                                if m_orig.group(3).upper() == 'PM' and oh != 12: oh += 12
                                if m_orig.group(3).upper() == 'AM' and oh == 12: oh = 0
                            else:
                                if m_orig.group(2).upper() == 'PM' and oh != 12: oh += 12
                                if m_orig.group(2).upper() == 'AM' and oh == 12: oh = 0
                            dc_hours[dc]["original"].append(oh)
    
    # Average final hours, include delay info
    result = {}
    for dc, data in dc_hours.items():
        if isinstance(data, dict) and data["final"]:
            avg_final = round(sum(data["final"]) / len(data["final"]), 1)
            avg_orig = round(sum(data["original"]) / len(data["original"]), 1) if data["original"] else None
            result[dc] = {
                "preferred_hour": avg_final,
                "original_hour": avg_orig,
                "delay_count": data["delays"],
                "total_count": len(data["final"]),
            }
        elif isinstance(data, list) and data:  # backward compat
            result[dc] = {"preferred_hour": round(sum(data) / len(data), 1), "original_hour": None, "delay_count": 0, "total_count": len(data)}
    return result


def auto_schedule_route(route: dict, origin_coord: Tuple[float, float],
                        origin_tz_info: ZoneInfo, cache: dict) -> dict:
    """
    Find the optimal departure day/time so all stops arrive within operating hours.
    
    Strategy: try every day in target week × pickup hours (10-14),
    simulate full route, score by:
    - All stops must arrive within operating hours (or wait < 12h)
    - Minimize total wait time
    - Prefer Thu/Fri for long haul (>= 8h first stop)
    """
    schedule = route.get("schedule", [])
    if not schedule:
        return {"pickup": None, "reason": "No stops"}

    due_dates = []
    for st in schedule:
        try:
            due_dates.append(datetime.strptime(st["due_date"], "%Y-%m-%d"))
        except (ValueError, KeyError):
            pass
    if not due_dates:
        return {"pickup": None, "reason": "No due dates"}

    earliest_due = min(due_dates)
    target_start = earliest_due - timedelta(days=12)  # search window: due-9 to due-4

    # Get first stop travel time for friday rule
    first_wh = schedule[0]["warehouse"]
    if first_wh not in COORDS:
        return {"pickup": None, "reason": f"Unknown warehouse: {first_wh}"}
    # Check all segments for long haul (any segment >= 10h = long haul)
    _, first_travel = osrm_pairwise(origin_coord, COORDS[first_wh], cache)
    max_seg_hours = first_travel
    prev_coord = origin_coord
    for st in schedule:
        wh = st["warehouse"]
        if wh in COORDS:
            _, seg_h = osrm_pairwise(prev_coord, COORDS[wh], cache)
            max_seg_hours = max(max_seg_hours, seg_h)
            prev_coord = COORDS[wh]
    is_long_haul = max_seg_hours >= 10

    # Build PO list for simulation
    po_list = [{"warehouse": st["warehouse"], "po_number": st["po_number"],
                "quantity": st["quantity"], "due_date": st["due_date"],
                "inventory_available_date": "2026-01-01"}
               for st in schedule]

    picking_hours = [6, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 22]  # wider range for better window matching
    best = None
    best_score = float('inf')
    candidates = []
    days_name = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

    # Try each day in the search window
    for day_offset in range(12):  # 12 days of options
        depart_day = target_start + timedelta(days=day_offset)
        
        for hour in picking_hours:
            depart_dt = depart_day.replace(hour=hour, minute=0, second=0)
            
            # Don't depart after due date - 2 days
            if depart_dt > earliest_due - timedelta(days=2):
                continue

            # Simulate route
            result = evaluate_route(origin_coord, po_list, depart_dt, cache, origin_tz_info)
            
            # Check each stop: is arrival within operating hours?
            total_wait = 0
            all_ok = True
            
            for stop in result.get("schedule", []):
                recv_code = stop.get("receiving_code", "")
                # Parse arrival time (remove timezone abbrev)
                arr_str = stop.get("arrival_time", "")
                arr_parts = arr_str.split(" ")
                if len(arr_parts) >= 2:
                    try:
                        arr_naive = datetime.strptime(arr_parts[0] + " " + arr_parts[1], "%Y-%m-%d %H:%M")
                    except ValueError:
                        all_ok = False
                        break
                else:
                    all_ok = False
                    break
                
                # Check if arrival is within operating hours
                if recv_code in RECEIVING_HOURS:
                    if is_within_receiving_hours(arr_naive, recv_code):
                        # Great, no wait
                        pass
                    else:
                        # Need to wait - find next window
                        nw = next_receiving_window(arr_naive, recv_code, max_days=3)
                        if nw is None:
                            all_ok = False
                            break
                        wait_h = (nw - arr_naive).total_seconds() / 3600
                        if wait_h > 36:  # More than 36h wait is too much
                            all_ok = False
                            break
                        total_wait += wait_h
            
            # Route completion limit: 2-stop=48h, 3-stop=120h (5 days - driver can rest between stops)
            if all_ok and len(result.get("schedule", [])) > 1:
                sched = result["schedule"]
                first_arr = sched[0].get("arrival_time", "")
                last_arr = sched[-1].get("adjusted_arrival", "")
                try:
                    fa_parts = first_arr.split(" ")
                    la_parts = last_arr.split(" ")
                    fa_dt = datetime.strptime(fa_parts[0] + " " + fa_parts[1], "%Y-%m-%d %H:%M")
                    la_dt = datetime.strptime(la_parts[0] + " " + la_parts[1], "%Y-%m-%d %H:%M")
                    route_duration_h = (la_dt - fa_dt).total_seconds() / 3600
                    max_duration = 120 if len(sched) >= 3 else 48
                    if route_duration_h > max_duration:
                        all_ok = False
                except (ValueError, IndexError):
                    pass
            
            if not all_ok:
                continue
            
            # Calculate route duration (first arrival to last adjusted arrival)
            route_duration_h = 0
            sched = result.get("schedule", [])
            if len(sched) > 1:
                try:
                    fa = sched[0].get("arrival_time", "").split(" ")
                    la = sched[-1].get("adjusted_arrival", "").split(" ")
                    fa_dt2 = datetime.strptime(fa[0] + " " + fa[1], "%Y-%m-%d %H:%M")
                    la_dt2 = datetime.strptime(la[0] + " " + la[1], "%Y-%m-%d %H:%M")
                    route_duration_h = (la_dt2 - fa_dt2).total_seconds() / 3600
                except (ValueError, IndexError):
                    pass

            # Score: lower is better
            weekday = depart_dt.weekday()
            
            # Primary: short wait is fine (< 3h), long wait is bad
            if total_wait <= 3:
                score = total_wait * 1  # minor penalty for short wait
            else:
                score = total_wait * 5  # bigger penalty for long wait
            
            # Prefer shorter route completion
            score += route_duration_h * 2
            
            # Long haul: prefer Thu/Fri/Sat departure
            if is_long_haul and weekday not in (3, 4, 5):
                score += 20
            
            # RAG: bonus if arrival matches past APPT patterns
            pref_appts = get_preferred_appt_hours()
            for si3, stop3 in enumerate(sched):
                dc3 = stop3.get("dc_code", "")
                if dc3 in pref_appts:
                    try:
                        sa3 = stop3.get("adjusted_arrival", "").split(" ")
                        sa3_dt = datetime.strptime(sa3[0] + " " + sa3[1], "%Y-%m-%d %H:%M")
                        arr_hour = sa3_dt.hour + sa3_dt.minute / 60.0
                        pref = pref_appts[dc3]
                        pref_hour = pref["preferred_hour"] if isinstance(pref, dict) else pref
                        diff_h = abs(arr_hour - pref_hour)
                        if diff_h > 12: diff_h = 24 - diff_h
                        if diff_h <= 1:
                            score -= 20
                        elif diff_h <= 3:
                            score -= 10
                        # DC with frequent delays: add buffer time
                        if isinstance(pref, dict) and pref.get("delay_count", 0) > 0:
                            delay_ratio = pref["delay_count"] / max(pref["total_count"], 1)
                            if delay_ratio > 0.3:
                                score += 5  # this DC often delays, slight penalty for tight schedules
                    except (ValueError, IndexError):
                        pass

            # First stop: prefer arriving 1-3h before operating window opens
            if len(sched) > 0:
                try:
                    fa = sched[0].get("arrival_time", "").split(" ")
                    adj = sched[0].get("adjusted_arrival", "").split(" ")
                    fa_dt4 = datetime.strptime(fa[0] + " " + fa[1], "%Y-%m-%d %H:%M")
                    adj_dt4 = datetime.strptime(adj[0] + " " + adj[1], "%Y-%m-%d %H:%M")
                    early_h = (adj_dt4 - fa_dt4).total_seconds() / 3600  # hours before window
                    if 1 <= early_h <= 3:
                        score -= 15  # sweet spot: arrive 1-3h early
                    elif early_h < 0.5:
                        score += 10  # too tight, cutting it close
                except (ValueError, IndexError):
                    pass

            # No weekend pickup: if departure requires Sat/Sun pickup, big penalty
            pickup_day = (depart_dt - timedelta(days=1)).weekday()
            if pickup_day in (5, 6):  # Sat, Sun pickup needed
                score += 50

            # Single stop short haul (≤6h): prefer same-day pickup+delivery
            if len(sched) == 1 and max_seg_hours <= 6:
                # Best: depart in afternoon, arrive same evening
                if 12 <= depart_dt.hour <= 16:
                    score -= 25  # strong bonus for same-day delivery
                elif 10 <= depart_dt.hour <= 18:
                    score -= 15
            elif not is_long_haul:
                # Multi-stop short haul: prefer Tue/Wed departure (spread from Fri/Mon)
                if weekday in (1, 2):  # Tue, Wed
                    score -= 15
                elif weekday in (0, 3):  # Mon, Thu
                    score -= 5
            
            # Mild penalty for Monday bunching (encourage Tue/Wed/Thu spread)
            if weekday == 0 and not is_long_haul:
                score += 5

            # Penalize Friday arrival (risky - any delay = miss weekend)
            for si2, stop2 in enumerate(sched):
                try:
                    sa = stop2.get("arrival_time", "").split(" ")
                    sa_dt = datetime.strptime(sa[0] + " " + sa[1], "%Y-%m-%d %H:%M")
                    if sa_dt.weekday() == 4:  # Friday
                        score += 30  # strong penalty per stop arriving Friday
                except (ValueError, IndexError):
                    pass

            # Multi-stop (2+): prefer first stop arriving on Monday
            if len(sched) >= 2:
                try:
                    fa = sched[0].get("arrival_time", "").split(" ")
                    fa_dt3 = datetime.strptime(fa[0] + " " + fa[1], "%Y-%m-%d %H:%M")
                    arr_weekday = fa_dt3.weekday()  # 0=Mon
                    if arr_weekday == 0:  # Monday
                        score -= 30  # strong preference
                    elif arr_weekday == 6:  # Sunday
                        score += 10  # mild penalty
                    elif arr_weekday == 5:  # Saturday
                        score += 20  # worse
                except (ValueError, IndexError):
                    pass
            
            # Prefer due_date - 5~8 days
            days_before_due = (earliest_due - depart_dt).days
            if days_before_due < 3:
                score += 100
            elif days_before_due > 9:
                score += 40
            elif 5 <= days_before_due <= 8:
                score -= 5
            
            # Prefer daytime departure (10-16)
            if 10 <= depart_dt.hour <= 16:
                score -= 15
            elif 6 <= depart_dt.hour <= 9 or 17 <= depart_dt.hour <= 18:
                score -= 5
            # Night departure penalty
            elif depart_dt.hour >= 20 or depart_dt.hour <= 5:
                score += 10
            
            candidates.append({
                    "depart": depart_dt,
                    "result": result,
                    "total_wait": round(total_wait, 1),
                    "weekday": weekday,
                    "score": score,
                })
            if score < best_score:
                best_score = score
                best = candidates[-1]

    if best is None:
        depart_dt = target_start.replace(hour=10)
        result = evaluate_route(origin_coord, po_list, depart_dt, cache, origin_tz_info)
        best = {"depart": depart_dt, "result": result, "total_wait": 0, "weekday": depart_dt.weekday(), "score": 9999}

    # Store top candidates for balancing
    top_candidates = sorted(candidates, key=lambda c: c["score"])[:5] if candidates else [best]

    depart_final = best["depart"]
    # Single stop short haul: same-day pickup = departure day
    is_same_day = (len(schedule) == 1 and max_seg_hours <= 6 and
                   depart_final.weekday() not in (5, 6))

    if is_same_day:
        pickup_date = depart_final  # same day pickup + delivery
    else:
        # Pickup is always a weekday (Mon-Fri), before departure
        dep_wd = depart_final.weekday()
        if dep_wd == 0:  # Monday departure -> Friday pickup
            pickup_date = depart_final - timedelta(days=3)
        elif dep_wd == 5:  # Saturday departure -> Friday pickup
            pickup_date = depart_final - timedelta(days=1)
        elif dep_wd == 6:  # Sunday departure -> Friday pickup
            pickup_date = depart_final - timedelta(days=2)
        else:  # Tue-Fri -> day before
            pickup_date = depart_final - timedelta(days=1)

    return {
        "pickup": pickup_date.strftime("%Y-%m-%d") + " 10:00",
        "pickup_day": days_name[pickup_date.weekday()],
        "departure": depart_final.strftime("%Y-%m-%d %H:%M"),
        "departure_day": days_name[depart_final.weekday()],
        "top_candidates": [{"depart": c["depart"].strftime("%Y-%m-%d %H:%M"), "score": round(c["score"],1), "wait": c["total_wait"]} for c in top_candidates],
        "travel_hours_first_stop": round(first_travel, 1),
        "friday_rule": is_long_haul,
        "target_arrival": (earliest_due - timedelta(days=10)).strftime("%Y-%m-%d"),
        "due_date": earliest_due.strftime("%Y-%m-%d"),
        "total_wait_hours": best["total_wait"],
        "simulated_route": best["result"],
    }


def balance_departure_dates(schedules: List[dict]) -> List[dict]:
    """Balance departure dates so not too many routes depart on the same day.
    Max 3 departures per day. If exceeded, move lowest-priority routes to alternative dates."""
    MAX_PER_DAY = 2  # max 2 departures per day for better spread
    
    # Count departures per date
    by_date = {}
    for i, s in enumerate(schedules):
        dep = s.get("departure", "")[:10]
        if dep:
            if dep not in by_date:
                by_date[dep] = []
            by_date[dep].append(i)
    
    # Find overloaded dates
    for date_key, indices in list(by_date.items()):
        if len(indices) <= MAX_PER_DAY:
            continue
        
        # Sort by first stop travel time: longest stays on Friday, shortest moves
        scored = []
        for idx in indices:
            top = schedules[idx].get("top_candidates", [])
            travel_h = schedules[idx].get("travel_hours_first_stop", 0)
            scored.append((idx, top, travel_h))
        
        # Keep longest travel on this day, move shortest to other days
        scored.sort(key=lambda x: x[2])  # shortest first = first to move
        to_move = scored[:len(scored) - MAX_PER_DAY]  # move the shortest ones
        
        for idx, top_cands, _travel_h in to_move:
            # Find an alternative date that's not overloaded
            for cand in top_cands:
                alt_date = cand["depart"][:10]
                if alt_date == date_key:
                    continue
                alt_count = len(by_date.get(alt_date, []))
                if alt_count < MAX_PER_DAY:
                    # Move to this date
                    schedules[idx]["departure"] = cand["depart"]
                    dep_dt = datetime.strptime(cand["depart"], "%Y-%m-%d %H:%M")
                    schedules[idx]["departure_day"] = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][dep_dt.weekday()]
                    pickup_date = dep_dt - timedelta(days=1)
                    if pickup_date.weekday() == 5: pickup_date -= timedelta(days=1)
                    elif pickup_date.weekday() == 6: pickup_date -= timedelta(days=2)
                    schedules[idx]["pickup"] = pickup_date.strftime("%Y-%m-%d") + " 10:00"
                    schedules[idx]["pickup_day"] = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][pickup_date.weekday()]
                    
                    # Update by_date
                    by_date[date_key].remove(idx)
                    if alt_date not in by_date: by_date[alt_date] = []
                    by_date[alt_date].append(idx)
                    break
    
    return schedules


def distribute_pickup_times(schedules: List[dict]) -> List[dict]:
    """
    Distribute pickup times so routes departing on the same day
    get different hours (10, 11, 12, 13, 14) instead of all 10AM.
    """
    picking_slots = [10, 11, 12, 13, 14]

    # Group by departure date
    by_date = {}
    for i, s in enumerate(schedules):
        if not s.get("pickup"):
            continue
        date_key = s["pickup"][:10]  # "2026-03-13"
        if date_key not in by_date:
            by_date[date_key] = []
        by_date[date_key].append(i)

    # Assign different hours to routes on same date
    for date_key, indices in by_date.items():
        for slot_idx, route_idx in enumerate(indices):
            hour = picking_slots[slot_idx % len(picking_slots)]
            old_pickup = schedules[route_idx]["pickup"]
            schedules[route_idx]["pickup"] = f"{date_key} {hour:02d}:00"
            # Update pickup_day
            from datetime import datetime as _dt
            d = _dt.strptime(date_key, "%Y-%m-%d")
            schedules[route_idx]["pickup_day"] = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][d.weekday()]

    return schedules


def _tz_abbrev(dt_aware: datetime) -> str:
    """Get timezone abbreviation like EDT, CDT, PDT, MST."""
    return dt_aware.strftime("%Z")


def evaluate_route(
    origin_coord: Tuple[float, float],
    po_list: List[dict],
    depart_date: datetime,
    cache: dict,
    origin_tz: Optional[ZoneInfo] = None,
) -> dict:
    """
    Evaluate a candidate route (origin -> stop1 -> stop2 -> ... ).

    All internal calculations use UTC. Display times are in each location's
    local timezone.

    Args:
        origin_coord: (lat, lon) of the origin warehouse.
        po_list: List of PO dicts for the stops in visit order.
        depart_date: Proposed departure datetime (naive = origin local, or aware).
        cache: OSRM distance cache.
        origin_tz: Timezone of the origin. If None, depart_date treated as naive.

    Returns:
        Dict with keys: feasible, cost, distance_km, schedule (list of stop details),
        infeasible_reasons.
    """
    result = {
        "feasible": True,
        "cost": 0.0,
        "distance_km": 0.0,
        "total_units": 0,
        "schedule": [],
        "infeasible_reasons": [],
        "po_numbers": [p["po_number"] for p in po_list],
        "warehouses": [p["warehouse"] for p in po_list],
    }

    total_units = sum(p["quantity"] for p in po_list)
    result["total_units"] = total_units
    if total_units > MAX_CAPACITY:
        result["feasible"] = False
        result["infeasible_reasons"].append(
            f"Total quantity {total_units} exceeds max capacity {MAX_CAPACITY}"
        )

    if len(po_list) > MAX_STOPS:
        result["feasible"] = False
        result["infeasible_reasons"].append(
            f"Number of stops {len(po_list)} exceeds max {MAX_STOPS}"
        )

    # Build coordinate sequence
    coords_seq = [origin_coord]
    for po in po_list:
        wh_name = po["warehouse"]
        if wh_name not in COORDS:
            result["feasible"] = False
            result["infeasible_reasons"].append(f"Unknown warehouse: {wh_name}")
            return result
        coords_seq.append(COORDS[wh_name])

    # Get total route distance and duration (sum of pairwise segments)
    total_dist = 0
    total_dur = 0
    seg_data = []  # store per-segment (dist, hours)
    for i in range(len(coords_seq) - 1):
        d, h = osrm_pairwise(coords_seq[i], coords_seq[i + 1], cache)
        total_dist += d
        total_dur += h
        seg_data.append((d, h))

    result["distance_km"] = round(total_dist, 1)
    result["total_hours"] = round(total_dur, 1)
    result["cost"] = round(BASE_COST + COST_PER_KM * total_dist, 2)

    # Calculate detour ratio based on direction changes (zigzag detection)
    # For 3+ points: measure angle changes between segments
    if len(coords_seq) >= 3:
        import math as _m
        angles = []
        for k in range(len(coords_seq) - 1):
            dy = coords_seq[k+1][0] - coords_seq[k][0]
            dx = coords_seq[k+1][1] - coords_seq[k][1]
            angles.append(_m.atan2(dy, dx))
        # Sum of absolute angle changes between consecutive segments
        total_turn = 0
        for k in range(len(angles) - 1):
            diff = abs(angles[k+1] - angles[k])
            if diff > _m.pi:
                diff = 2 * _m.pi - diff
            total_turn += diff
        # Normalize: 0 = straight line, pi = full reversal per turn
        # detour_ratio: 1.0 = straight, 2.0+ = significant zigzag
        detour_ratio = 1.0 + total_turn / _m.pi
    else:
        detour_ratio = 1.0
    result["detour_ratio"] = round(detour_ratio, 2)

    # Convert departure to UTC for internal calculation
    if origin_tz and depart_date.tzinfo is None:
        depart_aware = depart_date.replace(tzinfo=origin_tz)
    elif depart_date.tzinfo is not None:
        depart_aware = depart_date
    else:
        depart_aware = depart_date.replace(tzinfo=timezone.utc)
    depart_utc = depart_aware.astimezone(timezone.utc)

    # Simulate schedule with HOS (Hours of Service) rest stops
    current_time_utc = depart_utc + timedelta(hours=LOADING_TIME_H)
    prev_coord = origin_coord
    cumulative_hours = LOADING_TIME_H
    driving_since_rest = 0  # hours driven since last rest

    for i, po in enumerate(po_list):
        wh_name = po["warehouse"]
        wh = WH_BY_NAME[wh_name]
        wh_coord = COORDS[wh_name]
        wh_tz = WH_TZ.get(wh_name, ZoneInfo("America/New_York"))

        # Drive to this stop (use OSRM actual travel time)
        seg_dist, travel_hours = osrm_pairwise(prev_coord, wh_coord, cache)

        # HOS: if this segment would exceed max driving hours, add mandatory rest
        rest_hours = 0
        remaining_drive = travel_hours
        while remaining_drive > 0:
            can_drive = MAX_DRIVE_H - driving_since_rest
            if can_drive <= 0:
                # Must rest
                rest_hours += MANDATORY_REST_H
                driving_since_rest = 0
                can_drive = MAX_DRIVE_H
            if remaining_drive <= can_drive:
                driving_since_rest += remaining_drive
                remaining_drive = 0
            else:
                remaining_drive -= can_drive
                driving_since_rest = MAX_DRIVE_H  # will trigger rest next loop

        total_seg_hours = travel_hours + rest_hours
        cumulative_hours += total_seg_hours
        arrival_utc = current_time_utc + timedelta(hours=total_seg_hours)

        # Convert to warehouse local time for display
        arrival_local = arrival_utc.astimezone(wh_tz)
        arrival_naive_local = arrival_local.replace(tzinfo=None)
        recv_code = wh.get("receiving_code", "")

        # Adjust arrival to next receiving window (truck waits if early)
        if recv_code in RECEIVING_HOURS and is_within_receiving_hours(arrival_naive_local, recv_code):
            actual_arrival_local = arrival_naive_local
        elif recv_code in RECEIVING_HOURS:
            nw = next_receiving_window(arrival_naive_local, recv_code, max_days=7)
            actual_arrival_local = nw if nw else arrival_naive_local
        else:
            actual_arrival_local = arrival_naive_local

        # Find next receiving window from arrival to get the deadline
        recv_window = next_receiving_window(arrival_naive_local, recv_code, max_days=7)
        if recv_window is None:
            recv_window = arrival_naive_local

        # Find close time of this receiving window
        latest_arrival = recv_window  # fallback
        if recv_code in RECEIVING_HOURS:
            for rdays, ropen, rclose in RECEIVING_HOURS[recv_code]:
                if ropen < 0 or rclose < 0:
                    continue
                if recv_window.weekday() in rdays:
                    rw_h = recv_window.hour + recv_window.minute / 60.0
                    if rclose > ropen:
                        if rw_h >= ropen and rw_h < rclose:
                            close_dt = recv_window.replace(hour=int(rclose), minute=int((rclose % 1) * 60))
                            latest_arrival = close_dt - timedelta(hours=1)
                            break
                    else:
                        if rw_h >= ropen:
                            close_dt = (recv_window + timedelta(days=1)).replace(hour=int(rclose), minute=int((rclose % 1) * 60))
                        else:
                            close_dt = recv_window.replace(hour=int(rclose), minute=int((rclose % 1) * 60))
                        latest_arrival = close_dt - timedelta(hours=1)
                        break

        # Latest departure calculation:
        # Stop 1: latest time to leave ORIGIN to arrive at close-1h
        # Stop 2+: latest time to leave PREVIOUS STOP to arrive at close-1h
        if i == 0:
            # First stop: latest pickup/departure from origin
            latest_depart_utc = latest_arrival.replace(tzinfo=wh_tz).astimezone(timezone.utc) - timedelta(hours=cumulative_hours)
            if origin_tz:
                latest_depart_local = latest_depart_utc.astimezone(origin_tz)
            else:
                latest_depart_local = latest_depart_utc
        else:
            # Subsequent stops: latest time to leave previous stop
            # = close-1h of this stop - segment travel time (including rest)
            latest_depart_utc = latest_arrival.replace(tzinfo=wh_tz).astimezone(timezone.utc) - timedelta(hours=total_seg_hours)
            # Show in previous stop's timezone
            prev_wh_tz = WH_TZ.get(po_list[i-1]["warehouse"], wh_tz)
            latest_depart_local = latest_depart_utc.astimezone(prev_wh_tz)
        latest_depart_str = latest_depart_local.strftime("%Y-%m-%d %H:%M") + " " + _tz_abbrev(latest_depart_local)

        # Re-attach timezone for UTC conversion
        actual_arrival_aware = actual_arrival_local.replace(tzinfo=wh_tz)
        actual_arrival_utc = actual_arrival_aware.astimezone(timezone.utc)
        tz_abbr = _tz_abbrev(actual_arrival_aware)

        seg_hours = round(travel_hours, 1)
        stop_info = {
            "stop": i + 1,
            "warehouse": wh_name,
            "dc_code": wh.get("dc_code", ""),
            "po_number": po["po_number"],
            "quantity": po["quantity"],
            "segment_km": round(seg_dist, 1),
            "segment_hours": seg_hours,
            "rest_hours": round(rest_hours, 1),
            "total_seg_hours": round(total_seg_hours, 1),
            "arrival_time": arrival_local.strftime("%Y-%m-%d %H:%M") + " " + _tz_abbrev(arrival_local),
            "adjusted_arrival": actual_arrival_local.strftime("%Y-%m-%d %H:%M") + " " + tz_abbr,
            "latest_departure": latest_depart_str,
            "receiving_code": recv_code,
            "due_date": po["due_date"],
            "timezone": str(wh_tz),
        }
        result["schedule"].append(stop_info)

        # After unloading, continue from this stop in UTC
        cumulative_hours += UNLOAD_TIME_H
        current_time_utc = actual_arrival_utc + timedelta(hours=UNLOAD_TIME_H)
        prev_coord = wh_coord
        # Unloading counts as rest for HOS purposes
        driving_since_rest = 0

    return result


# ---------------------------------------------------------------------------
# Optimization Core
# ---------------------------------------------------------------------------

def resolve_origin(origin_str: str) -> Tuple[float, float]:
    """
    Resolve an origin string to coordinates.

    Checks known locations first, otherwise attempts a rough lookup.
    """
    if origin_str in ORIGIN_COORDS:
        return ORIGIN_COORDS[origin_str]
    # Check if it matches a warehouse name
    if origin_str in COORDS:
        return COORDS[origin_str]
    # Try partial match in known origins
    lower = origin_str.lower()
    for name, coord in ORIGIN_COORDS.items():
        if lower in name.lower():
            return coord
    # Default fallback
    print(f"WARNING: Could not resolve origin '{origin_str}', using Atlanta, Georgia")
    return ORIGIN_COORDS["Mississauga, Ontario"]


def optimize_group(
    group_name: str,
    pos: List[dict],
    origin_coord: Tuple[float, float],
    origin_name: str,
    cache: dict,
    balance_weight: float = 0.0,
) -> dict:
    """
    Find optimal routes for a group of POs (MS-WH or L1-WH).

    Enumerates all feasible combinations of 1-3 POs per truck,
    then selects the lowest-cost set covering all POs.

    Args:
        group_name: "MS-WH" or "L1-WH".
        pos: List of PO dicts in this group.
        origin_coord: Origin warehouse coordinates.
        origin_name: Human-readable origin name.
        cache: OSRM cache.

    Returns:
        Dict with routes, infeasible POs, and summary.
    """
    if not pos:
        return {"group": group_name, "routes": [], "infeasible": [], "total_cost": 0}

    # Resolve origin timezone
    o_tz = ORIGIN_TZ.get(origin_name, ZoneInfo("America/New_York"))

    # Load learned preferences
    prefs = load_preferences()

    # Pre-cache all pairwise distances (origin + all warehouses in this group)
    all_coords = [origin_coord]
    wh_names_in_group = []
    for po in pos:
        wh_name = po["warehouse"]
        if wh_name in COORDS and wh_name not in wh_names_in_group:
            wh_names_in_group.append(wh_name)
            all_coords.append(COORDS[wh_name])
    # Pre-fetch all pairs not yet in cache
    pairs_to_fetch = []
    for i in range(len(all_coords)):
        for j in range(len(all_coords)):
            if i == j:
                continue
            key = f"{all_coords[i][0]:.4f},{all_coords[i][1]:.4f}|{all_coords[j][0]:.4f},{all_coords[j][1]:.4f}"
            if key not in cache:
                pairs_to_fetch.append((all_coords[i], all_coords[j]))
    if pairs_to_fetch:
        print(f"  Pre-caching {len(pairs_to_fetch)} pairwise distances...")
        for c1, c2 in pairs_to_fetch:
            osrm_pairwise_distance(c1, c2, cache)
        print(f"  Pre-caching done. Cache size: {len(cache)}")

    # Determine earliest possible departure: max of all inventory dates, but at least today
    all_inv_dates = []
    for po in pos:
        all_inv_dates.append(datetime.strptime(po["inventory_available_date"], "%Y-%m-%d"))

    # Evaluate all candidate route combinations (1 to MAX_STOPS POs per truck)
    candidate_routes = []
    po_indices = list(range(len(pos)))

    t0 = time.time()
    print(f"\n  Evaluating route combinations for {group_name} ({len(pos)} POs)...")

    combo_count = 0
    for size in range(1, min(MAX_STOPS, len(pos)) + 1):
        for combo in itertools.combinations(po_indices, size):
            combo_count += 1
            combo_pos = [pos[i] for i in combo]

            # Quick capacity check
            total_q = sum(p["quantity"] for p in combo_pos)
            if total_q > MAX_CAPACITY:
                continue

            # Determine departure: use pickup_time if provided, else 6 AM on inventory date
            pickup_times = []
            for p in combo_pos:
                pt = p.get("pickup_time", "")
                if pt:
                    try:
                        pickup_times.append(datetime.strptime(pt, "%Y-%m-%dT%H:%M"))
                    except ValueError:
                        pickup_times.append(datetime.strptime(p["inventory_available_date"], "%Y-%m-%d").replace(hour=6))
                else:
                    pickup_times.append(datetime.strptime(p["inventory_available_date"], "%Y-%m-%d").replace(hour=6))
            depart_date = max(pickup_times)

            # Try all permutations of stop order for this combo
            best_route = None
            for perm in itertools.permutations(range(len(combo_pos))):
                ordered_pos = [combo_pos[j] for j in perm]
                route = evaluate_route(origin_coord, ordered_pos, depart_date, cache, o_tz)
                route["combo_indices"] = combo
                route["departure"] = depart_date.strftime("%Y-%m-%d %H:%M")

                if best_route is None or (route["feasible"] and route["cost"] < best_route.get("cost", float("inf"))):
                    best_route = route
                elif not best_route["feasible"] and route["feasible"]:
                    best_route = route

            if best_route:
                candidate_routes.append(best_route)

    t1 = time.time()
    print(f"  Route generation: {combo_count} combos, {len(candidate_routes)} candidates in {t1-t0:.1f}s")

    # Greedy set-cover: minimize TOTAL cost by preferring routes that cover more POs
    # Sort by cost-per-PO (ascending) so multi-stop routes are preferred when cheaper per PO
    feasible_routes = [r for r in candidate_routes if r["feasible"]]
    # Sort by effective cost (cost minus learned preference bonus) per PO
    def _effective_cost(r):
        dcs = [s.get("dc_code", "") for s in r.get("schedule", []) if s.get("dc_code")]
        bonus = get_pair_bonus(dcs, prefs)
        return (r["cost"] - bonus) / max(len(r["combo_indices"]), 1)
    feasible_routes.sort(key=_effective_cost)

    # Try all feasible route sets to find the true minimum total cost
    # For small PO counts (<= 10), do exhaustive search; otherwise greedy
    all_indices = set(range(len(pos)))

    def _balance_penalty(routes):
        """Compute penalty based on stop count balance + geographic efficiency + learned prefs.
        Scaled proportional to average route cost so penalty is meaningful."""
        if len(routes) <= 1:
            return 0.0
        avg_cost = sum(r["cost"] for r in routes) / len(routes)

        # Balance penalty: stop count variance
        stops = [len(r["combo_indices"]) for r in routes]
        avg_stops = sum(stops) / len(stops)
        variance = sum((s - avg_stops) ** 2 for s in stops) / len(stops)
        balance_pen = balance_weight * avg_cost * variance * 0.5

        # Geographic penalty: penalize zigzag routes (detour_ratio > 1.3)
        geo_pen = 0.0
        for r in routes:
            dr = r.get("detour_ratio", 1.0)
            if dr > 1.3 and len(r.get("combo_indices", ())) > 1:
                geo_pen += balance_weight * r["cost"] * (dr - 1.3)

        # Learned preference bonus: reduce cost for preferred DC pairings
        pref_bonus = 0.0
        for r in routes:
            dcs = [s.get("dc_code", "") for s in r.get("schedule", []) if s.get("dc_code")]
            pref_bonus += get_pair_bonus(dcs, prefs)

        return balance_pen + geo_pen - pref_bonus

    if len(pos) <= 14:
        # First, get a greedy solution as upper bound for pruning
        greedy_covered = set()
        greedy_routes = []
        for route in feasible_routes:
            indices = set(route["combo_indices"])
            if indices & greedy_covered:
                continue
            greedy_routes.append(route)
            greedy_covered |= indices
        greedy_cost = sum(r["cost"] for r in greedy_routes) + _balance_penalty(greedy_routes) if greedy_covered == all_indices else float("inf")

        # Exhaustive: find best combination of routes that covers all POs
        best_selection = greedy_routes[:] if greedy_covered == all_indices else None
        best_total_cost = greedy_cost
        _search_deadline = time.time() + 15  # 15 second time limit
        _search_count = [0]
        _timed_out = [False]

        # Sort feasible routes: prefer multi-stop with lower cost (better pruning)
        feasible_sorted = sorted(feasible_routes, key=lambda r: r["cost"] / max(len(r["combo_indices"]), 1))

        def find_best_cover(remaining, selected, current_cost):
            nonlocal best_selection, best_total_cost
            _search_count[0] += 1
            if _search_count[0] % 50000 == 0 and time.time() > _search_deadline:
                _timed_out[0] = True
                return
            if _timed_out[0]:
                return
            if not remaining:
                adjusted = current_cost + _balance_penalty(selected)
                if adjusted < best_total_cost:
                    best_total_cost = adjusted
                    best_selection = selected[:]
                return
            if current_cost >= best_total_cost:
                return  # prune
            for route in feasible_sorted:
                indices = set(route["combo_indices"])
                if not (indices & remaining):
                    continue
                already_covered = all_indices - remaining
                if indices & already_covered:
                    continue
                new_remaining = remaining - indices
                selected.append(route)
                find_best_cover(new_remaining, selected, current_cost + route["cost"])
                selected.pop()

        find_best_cover(all_indices, [], 0)
        t2 = time.time()
        print(f"  Exhaustive cover search: {t2-t1:.1f}s")
        if best_selection is not None:
            selected_routes = best_selection
            covered = set()
            for r in selected_routes:
                covered |= set(r["combo_indices"])
        else:
            selected_routes = []
            covered = set()
    else:
        # Greedy for larger sets
        covered = set()
        selected_routes = []
        for route in feasible_routes:
            indices = set(route["combo_indices"])
            if indices & covered:
                continue
            selected_routes.append(route)
            covered |= indices

    # Post-process: try to merge single-PO routes into multi-PO routes
    # Look for candidate routes that combine a single-PO route's PO with another route's POs
    changed = True
    while changed:
        changed = False
        single_routes = [r for r in selected_routes if len(r["combo_indices"]) == 1]
        multi_routes = [r for r in selected_routes if len(r["combo_indices"]) > 1]
        other_routes = multi_routes if multi_routes else []

        for sr in single_routes:
            sr_idx = sr["combo_indices"][0]
            best_merge = None
            best_saving = 0

            for tr in selected_routes:
                if tr is sr:
                    continue
                # Find a candidate route that covers both tr's POs and sr's PO
                merged_indices = set(tr["combo_indices"]) | {sr_idx}
                if len(merged_indices) > MAX_STOPS:
                    continue
                for cr in candidate_routes:
                    if set(cr["combo_indices"]) == merged_indices and cr["feasible"]:
                        saving = sr["cost"] + tr["cost"] - cr["cost"]
                        if saving > best_saving:
                            best_saving = saving
                            best_merge = (sr, tr, cr)
                            break

            if best_merge:
                sr_rm, tr_rm, merged = best_merge
                selected_routes.remove(sr_rm)
                selected_routes.remove(tr_rm)
                selected_routes.append(merged)
                changed = True
                break

    # Post-optimization: pairwise swap improvement
    # Try moving a PO from one route to another, or swapping POs between routes
    t3 = time.time()
    improved = True
    swap_rounds = 0
    while improved and swap_rounds < 5:
        improved = False
        swap_rounds += 1
        for i in range(len(selected_routes)):
            if improved:
                break
            for j in range(i + 1, len(selected_routes)):
                if improved:
                    break
                ri = selected_routes[i]
                rj = selected_routes[j]
                ri_indices = list(ri["combo_indices"])
                rj_indices = list(rj["combo_indices"])
                old_cost = ri["cost"] + rj["cost"]

                # Try moving each PO from route i to route j (if capacity allows)
                for pi_pos, pi_idx in enumerate(ri_indices):
                    if len(rj_indices) >= MAX_STOPS:
                        continue
                    new_rj_indices = rj_indices + [pi_idx]
                    new_ri_indices = [x for x in ri_indices if x != pi_idx]
                    rj_units = sum(pos[k]["quantity"] for k in new_rj_indices)
                    if rj_units > MAX_CAPACITY:
                        continue
                    # Look up candidate route for new_rj
                    new_rj_set = frozenset(new_rj_indices)
                    new_rj_route = None
                    for cr in candidate_routes:
                        if frozenset(cr["combo_indices"]) == new_rj_set and cr["feasible"]:
                            new_rj_route = cr
                            break
                    if not new_rj_route:
                        continue
                    if new_ri_indices:
                        new_ri_set = frozenset(new_ri_indices)
                        new_ri_route = None
                        for cr in candidate_routes:
                            if frozenset(cr["combo_indices"]) == new_ri_set and cr["feasible"]:
                                new_ri_route = cr
                                break
                        if not new_ri_route:
                            continue
                        new_cost = new_ri_route["cost"] + new_rj_route["cost"]
                    else:
                        # Route i becomes empty - save its entire cost
                        new_ri_route = None
                        new_cost = new_rj_route["cost"]

                    if new_cost < old_cost - 1:
                        if new_ri_route:
                            selected_routes[i] = new_ri_route
                        else:
                            selected_routes[i] = None  # mark for removal
                        selected_routes[j] = new_rj_route
                        selected_routes = [r for r in selected_routes if r is not None]
                        improved = True
                        break

                if improved:
                    break

                # Try moving each PO from route j to route i
                for pj_pos, pj_idx in enumerate(rj_indices):
                    if len(ri_indices) >= MAX_STOPS:
                        continue
                    new_ri_indices = ri_indices + [pj_idx]
                    new_rj_indices = [x for x in rj_indices if x != pj_idx]
                    ri_units = sum(pos[k]["quantity"] for k in new_ri_indices)
                    if ri_units > MAX_CAPACITY:
                        continue
                    new_ri_set = frozenset(new_ri_indices)
                    new_ri_route = None
                    for cr in candidate_routes:
                        if frozenset(cr["combo_indices"]) == new_ri_set and cr["feasible"]:
                            new_ri_route = cr
                            break
                    if not new_ri_route:
                        continue
                    if new_rj_indices:
                        new_rj_set = frozenset(new_rj_indices)
                        new_rj_route = None
                        for cr in candidate_routes:
                            if frozenset(cr["combo_indices"]) == new_rj_set and cr["feasible"]:
                                new_rj_route = cr
                                break
                        if not new_rj_route:
                            continue
                        new_cost = new_ri_route["cost"] + new_rj_route["cost"]
                    else:
                        new_ri_route_only = new_ri_route
                        new_cost = new_ri_route_only["cost"]
                        new_rj_route = None

                    if new_cost < old_cost - 1:
                        selected_routes[i] = new_ri_route
                        if new_rj_route:
                            selected_routes[j] = new_rj_route
                        else:
                            selected_routes[j] = None
                        selected_routes = [r for r in selected_routes if r is not None]
                        improved = True
                        break

                if improved:
                    break

                # Try swapping one PO between routes
                if len(ri_indices) >= 1 and len(rj_indices) >= 1:
                    for pi_idx in ri_indices:
                        if improved:
                            break
                        for pj_idx in rj_indices:
                            swap_ri = [x if x != pi_idx else pj_idx for x in ri_indices]
                            swap_rj = [x if x != pj_idx else pi_idx for x in rj_indices]
                            ri_units = sum(pos[k]["quantity"] for k in swap_ri)
                            rj_units = sum(pos[k]["quantity"] for k in swap_rj)
                            if ri_units > MAX_CAPACITY or rj_units > MAX_CAPACITY:
                                continue
                            sri_set = frozenset(swap_ri)
                            srj_set = frozenset(swap_rj)
                            sri_route = None
                            srj_route = None
                            for cr in candidate_routes:
                                cs = frozenset(cr["combo_indices"])
                                if cs == sri_set and cr["feasible"]:
                                    sri_route = cr
                                if cs == srj_set and cr["feasible"]:
                                    srj_route = cr
                                if sri_route and srj_route:
                                    break
                            if sri_route and srj_route:
                                new_cost = sri_route["cost"] + srj_route["cost"]
                                if new_cost < old_cost - 1:
                                    selected_routes[i] = sri_route
                                    selected_routes[j] = srj_route
                                    improved = True
                                    break

    t4 = time.time()
    if swap_rounds > 0:
        print(f"  Pairwise swap improvement: {swap_rounds} rounds in {t4-t3:.1f}s")

    # Recalculate covered set after swaps
    covered = set()
    for r in selected_routes:
        covered |= set(r["combo_indices"])

    # Check for uncovered POs
    uncovered = all_indices - covered

    # Try to cover remaining POs with any feasible route
    for route in feasible_routes:
        indices = set(route["combo_indices"])
        if indices <= covered:
            continue
        if indices - covered:
            new_coverage = indices - covered
            overlap = indices & covered
            if not overlap:
                selected_routes.append(route)
                covered |= indices

    uncovered = all_indices - covered
    infeasible_pos = [pos[i] for i in uncovered]

    # Also check standalone routes for uncovered POs
    for idx in list(uncovered):
        po = pos[idx]
        inv_date = datetime.strptime(po["inventory_available_date"], "%Y-%m-%d").replace(hour=6)
        route = evaluate_route(origin_coord, [po], inv_date, cache, o_tz)
        route["combo_indices"] = (idx,)
        route["departure"] = inv_date.strftime("%Y-%m-%d %H:%M")
        if route["feasible"]:
            selected_routes.append(route)
            covered.add(idx)

    uncovered = all_indices - covered
    infeasible_pos = []
    infeasible_details = []
    for idx in uncovered:
        po = pos[idx]
        infeasible_pos.append(po)
        # Find reasons from candidate routes
        reasons = []
        for r in candidate_routes:
            if idx in r.get("combo_indices", ()) and not r["feasible"]:
                reasons.extend(r["infeasible_reasons"])
        infeasible_details.append({"po": po, "reasons": list(set(reasons))})

    total_cost = sum(r["cost"] for r in selected_routes)

    return {
        "group": group_name,
        "origin": origin_name,
        "routes": selected_routes,
        "infeasible": infeasible_details,
        "total_cost": round(total_cost, 2),
        "total_routes": len(selected_routes),
    }


# ---------------------------------------------------------------------------
# Output Formatting
# ---------------------------------------------------------------------------

def format_output(ms_result: dict, l1_result: dict, all_pos: List[dict]) -> str:
    """
    Format the optimization results into a structured report.

    Returns:
        Formatted report string.
    """
    lines = []

    lines.append("=" * 80)
    lines.append("CJ LOGISTICS -> McLANE WAREHOUSE ROUTE OPTIMIZATION REPORT")
    lines.append("=" * 80)
    lines.append("")

    # --- Assumptions ---
    lines.append("ASSUMPTIONS")
    lines.append("-" * 40)
    lines.append(f"  - Base cost per truck:      ${BASE_COST}")
    lines.append(f"  - Cost per km:              ${COST_PER_KM}")
    lines.append(f"  - Max stops per truck:      {MAX_STOPS}")
    lines.append(f"  - Max capacity per truck:   {MAX_CAPACITY} units")
    lines.append(f"  - Average speed:            {AVG_SPEED_KMH} km/h")
    lines.append(f"  - Loading time at origin:   {LOADING_TIME_H} hour(s)")
    lines.append(f"  - Unloading time per stop:  {UNLOAD_TIME_H * 60:.0f} minutes")
    lines.append(f"  - MS-WH and L1-WH groups are never mixed on the same truck")
    lines.append(f"  - Distances from OSRM (with haversine fallback)")
    lines.append("")

    # --- Approach ---
    lines.append("APPROACH")
    lines.append("-" * 40)
    lines.append("  1. POs separated by ship-from warehouse group (MS-WH / L1-WH)")
    lines.append("  2. All feasible route combinations enumerated (1-3 stops per truck)")
    lines.append("  3. All permutations of stop order evaluated for each combination")
    lines.append("  4. Constraints checked: capacity, inventory date, due date, receiving hours")
    lines.append("  5. Lowest-cost set of routes selected via greedy set-cover")
    lines.append("  6. Driving distances from OSRM public API (cached)")
    lines.append("")

    for result in [ms_result, l1_result]:
        if not result["routes"] and not result["infeasible"]:
            continue

        lines.append("=" * 80)
        lines.append(f"GROUP: {result['group']}  |  Origin: {result.get('origin', 'N/A')}")
        lines.append("=" * 80)
        lines.append("")

        # --- Optimization Table ---
        lines.append("OPTIMIZED ROUTES")
        lines.append("-" * 70)

        for ri, route in enumerate(result["routes"], 1):
            lines.append(f"  Route {ri}:")
            lines.append(f"    POs:        {', '.join(route['po_numbers'])}")
            lines.append(f"    Warehouses: {', '.join(route['warehouses'])}")
            lines.append(f"    Departure:  {route['departure']}")
            lines.append(f"    Distance:   {route['distance_km']:,.1f} km")
            lines.append(f"    Units:      {route['total_units']}")
            lines.append(f"    Cost:       ${route['cost']:,.2f}")
            lines.append("")
            lines.append(f"    {'Stop':<5} {'Warehouse':<35} {'PO':<10} {'Qty':<5} "
                         f"{'Seg km':<10} {'Arrival':<18} {'Adj Arrival':<18} {'Due Date':<12}")
            lines.append(f"    {'----':<5} {'-'*34:<35} {'-------':<10} {'---':<5} "
                         f"{'------':<10} {'-------':<18} {'-----------':<18} {'--------':<12}")
            for s in route["schedule"]:
                lines.append(
                    f"    {s['stop']:<5} {s['warehouse']:<35} {s['po_number']:<10} "
                    f"{s['quantity']:<5} {s['segment_km']:<10.1f} {s['arrival_time']:<18} "
                    f"{s['adjusted_arrival']:<18} {s['due_date']:<12}"
                )
            lines.append("")

        # --- Cost Breakdown ---
        lines.append("COST BREAKDOWN")
        lines.append("-" * 70)
        lines.append(f"  {'Route':<8} {'Base ($)':<12} {'Distance (km)':<16} {'Dist Cost ($)':<16} {'Total ($)':<12}")
        lines.append(f"  {'-----':<8} {'--------':<12} {'-------------':<16} {'-------------':<16} {'---------':<12}")
        for ri, route in enumerate(result["routes"], 1):
            dist_cost = route["cost"] - BASE_COST
            lines.append(
                f"  {ri:<8} {BASE_COST:<12,.2f} {route['distance_km']:<16,.1f} "
                f"{dist_cost:<16,.2f} {route['cost']:<12,.2f}"
            )
        lines.append(f"  {'':>54}{'-' * 12}")
        lines.append(f"  {'GROUP TOTAL':>54}${result['total_cost']:>10,.2f}")
        lines.append("")

        # --- Infeasible Routes ---
        if result["infeasible"]:
            lines.append("INFEASIBLE POs")
            lines.append("-" * 70)
            for item in result["infeasible"]:
                po = item["po"]
                lines.append(f"  PO: {po['po_number']}  |  Warehouse: {po['warehouse']}  |  Qty: {po['quantity']}")
                for reason in item["reasons"]:
                    lines.append(f"    - {reason}")
            lines.append("")

    # --- Grand Total ---
    grand_total = ms_result.get("total_cost", 0) + l1_result.get("total_cost", 0)
    total_routes = ms_result.get("total_routes", 0) + l1_result.get("total_routes", 0)
    total_infeasible = len(ms_result.get("infeasible", [])) + len(l1_result.get("infeasible", []))

    lines.append("=" * 80)
    lines.append("SUMMARY & RECOMMENDATION")
    lines.append("=" * 80)
    lines.append(f"  Total POs processed:     {len(all_pos)}")
    lines.append(f"  Total routes:            {total_routes}")
    lines.append(f"  Total estimated cost:    ${grand_total:,.2f}")
    if total_infeasible > 0:
        lines.append(f"  Infeasible POs:          {total_infeasible}")
    lines.append("")
    lines.append("  RECOMMENDATION:")
    if total_infeasible == 0:
        lines.append("  All POs can be fulfilled with the routes above.")
    else:
        lines.append(f"  {total_infeasible} PO(s) could not be scheduled within constraints.")
        lines.append("  Consider adjusting due dates, receiving windows, or splitting shipments.")
    lines.append("")
    lines.append("  Route assignments minimize total transportation cost while respecting")
    lines.append("  capacity limits, warehouse group separation, receiving hours, inventory")
    lines.append("  availability, and PO due dates.")
    lines.append("=" * 80)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Sample Data Generation
# ---------------------------------------------------------------------------

def generate_sample() -> None:
    """Generate a sample PO JSON file and print to stdout."""
    sample = [
        {
            "po_number": "PO001",
            "warehouse": "McLane Sun East FL",
            "due_date": "2026-03-15",
            "quantity": 20,
            "product_type": "Type A",
            "inventory_available_date": "2026-03-10"
        },
        {
            "po_number": "PO002",
            "warehouse": "McLane Southeast GA",
            "due_date": "2026-03-15",
            "quantity": 15,
            "product_type": "Type A",
            "inventory_available_date": "2026-03-10"
        },
        {
            "po_number": "PO003",
            "warehouse": "McLane Carolina NC",
            "due_date": "2026-03-16",
            "quantity": 25,
            "product_type": "Type B",
            "inventory_available_date": "2026-03-11"
        },
        {
            "po_number": "PO004",
            "warehouse": "McLane North Texas TX",
            "due_date": "2026-03-15",
            "quantity": 30,
            "product_type": "Type A",
            "inventory_available_date": "2026-03-10"
        },
        {
            "po_number": "PO005",
            "warehouse": "McLane Sun West AZ",
            "due_date": "2026-03-16",
            "quantity": 20,
            "product_type": "Type C",
            "inventory_available_date": "2026-03-11"
        },
        {
            "po_number": "PO006",
            "warehouse": "McLane Cumberland KY",
            "due_date": "2026-03-17",
            "quantity": 18,
            "product_type": "Type A",
            "inventory_available_date": "2026-03-12"
        },
        {
            "po_number": "PO007",
            "warehouse": "McLane Findlay OH",
            "due_date": "2026-03-16",
            "quantity": 22,
            "product_type": "Type B",
            "inventory_available_date": "2026-03-11"
        },
        {
            "po_number": "PO008",
            "warehouse": "McLane Pacific CA",
            "due_date": "2026-03-17",
            "quantity": 15,
            "product_type": "Type A",
            "inventory_available_date": "2026-03-12"
        },
    ]
    print(json.dumps(sample, indent=2))
    print("\nSample PO data printed above. Save to a file and run:", file=sys.stderr)
    print("  python3 route_optimizer.py --po-file pos.json", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Main entry point for the route optimizer CLI."""
    parser = argparse.ArgumentParser(
        description="CJ Logistics -> McLane Warehouse Route Optimizer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 route_optimizer.py --generate-sample > pos.json\n"
            "  python3 route_optimizer.py --po-file pos.json\n"
            "  python3 route_optimizer.py --po-file pos.json "
            '--ms-origin "Memphis, Tennessee" --l1-origin "Fort Worth, Texas"\n'
        ),
    )
    parser.add_argument("--po-file", help="Path to PO JSON file")
    parser.add_argument("--generate-sample", action="store_true", help="Print sample PO JSON and exit")
    parser.add_argument("--ms-origin", default=DEFAULT_MS_ORIGIN,
                        help=f"Origin city for MS-WH group (default: {DEFAULT_MS_ORIGIN})")
    parser.add_argument("--l1-origin", default=DEFAULT_L1_ORIGIN,
                        help=f"Origin city for L1-WH group (default: {DEFAULT_L1_ORIGIN})")

    args = parser.parse_args()

    if args.generate_sample:
        generate_sample()
        return

    # Load PO data
    if args.po_file:
        with open(args.po_file, "r") as f:
            all_pos = json.load(f)
    else:
        print("Reading PO data from stdin (paste JSON, then Ctrl+D)...")
        raw = sys.stdin.read()
        all_pos = json.loads(raw)

    if not isinstance(all_pos, list):
        print("ERROR: PO data must be a JSON array of objects.", file=sys.stderr)
        sys.exit(1)

    # Validate POs
    required_fields = {"po_number", "warehouse", "due_date", "quantity", "inventory_available_date"}
    for po in all_pos:
        missing = required_fields - set(po.keys())
        if missing:
            print(f"ERROR: PO {po.get('po_number', '?')} missing fields: {missing}", file=sys.stderr)
            sys.exit(1)
        if po["warehouse"] not in WH_BY_NAME:
            print(f"ERROR: Unknown warehouse '{po['warehouse']}' in PO {po['po_number']}", file=sys.stderr)
            print(f"  Known warehouses: {', '.join(sorted(WH_BY_NAME.keys()))}", file=sys.stderr)
            sys.exit(1)

    # Split by group
    ms_pos = [po for po in all_pos if WH_BY_NAME[po["warehouse"]]["group"] == "MS-WH"]
    l1_pos = [po for po in all_pos if WH_BY_NAME[po["warehouse"]]["group"] == "L1-WH"]

    print(f"Loaded {len(all_pos)} POs: {len(ms_pos)} MS-WH, {len(l1_pos)} L1-WH")

    # Resolve origins
    ms_origin = resolve_origin(args.ms_origin)
    l1_origin = resolve_origin(args.l1_origin)
    print(f"MS-WH origin: {args.ms_origin} ({ms_origin[0]:.4f}, {ms_origin[1]:.4f})")
    print(f"L1-WH origin: {args.l1_origin} ({l1_origin[0]:.4f}, {l1_origin[1]:.4f})")

    # Load OSRM cache
    cache = load_cache()

    # Optimize each group
    ms_result = optimize_group("MS-WH", ms_pos, ms_origin, args.ms_origin, cache)
    l1_result = optimize_group("L1-WH", l1_pos, l1_origin, args.l1_origin, cache)

    # Save cache
    save_cache(cache)

    # Format and print report
    report = format_output(ms_result, l1_result, all_pos)
    print("\n")
    print(report)


if __name__ == "__main__":
    main()
