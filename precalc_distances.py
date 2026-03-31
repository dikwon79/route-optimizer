#!/usr/bin/env python3
"""Pre-calculate all pairwise OSRM distances between origins + warehouses."""
import json, time, requests, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from route_optimizer import WAREHOUSES, COORDS, ORIGIN_COORDS, OSRM_BASE, OSRM_DELAY, load_cache, save_cache

cache = load_cache()

# All points
points = {}
for name, coord in ORIGIN_COORDS.items():
    points[name] = coord
for wh in WAREHOUSES:
    if wh['name'] in COORDS:
        points[wh['name']] = COORDS[wh['name']]

names = list(points.keys())
total_pairs = len(names) * (len(names) - 1) // 2

missing = []
for i in range(len(names)):
    for j in range(i + 1, len(names)):
        c1 = points[names[i]]
        c2 = points[names[j]]
        key = f'{c1[0]:.4f},{c1[1]:.4f}|{c2[0]:.4f},{c2[1]:.4f}'
        key_r = f'{c2[0]:.4f},{c2[1]:.4f}|{c1[0]:.4f},{c1[1]:.4f}'
        if key not in cache and key_r not in cache:
            missing.append((names[i], names[j], c1, c2, key))

print(f"Total: {total_pairs}, Cached: {total_pairs - len(missing)}, To compute: {len(missing)}")

done = 0
for name_a, name_b, c1, c2, key in missing:
    coord_str = f"{c1[1]},{c1[0]};{c2[1]},{c2[0]}"
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
                cache[key] = dist_km
                done += 1
                if done % 50 == 0:
                    print(f"  {done}/{len(missing)} done...")
                    save_cache(cache)
                break
        except Exception as e:
            time.sleep(1)
    else:
        print(f"  FAILED: {name_a} -> {name_b}")

save_cache(cache)
print(f"Done! Computed {done} new distances. Total cache: {len(cache)}")
