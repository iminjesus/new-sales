#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Geocode a list of addresses using OpenStreetMap's Nominatim API.

Usage:
  python geocode_addresses.py
  python geocode_addresses.py --input addresses.txt
  python geocode_addresses.py --input addresses.csv --column address
  python geocode_addresses.py --country "Australia" --delay 1.2 --retries 3

Notes:
- Be respectful of Nominatim usage policy: add delays, set a descriptive User-Agent,
  and avoid bulk/automated high-volume usage.
- If you have a Google Maps API key and prefer that, see the commented section at the bottom.
"""

import argparse
import csv
import sys
import time
import json
import os
from typing import Iterable, List, Optional, Dict
import urllib.parse
import urllib.request
import urllib.error

DEFAULT_ADDRESSES = [
    "5 WARWICK AVENUE",
    "192 PRINCES DR",
    "440 STUART HWY",
    "195 JUBILEE HWY",
    "169 KINGHORNE STREET",
    "1/2 PINE STREET",
    "2/34 ESSINGTON ST",
    "100 FINLAY ROAD",
    "8 GAUGE CIRCUIT",
    "260 SHELLHARBOUR ROAD",
    "1439 SOUTH GIPPSLAND HWY",
    "83 TINGAL ROAD",
    "1 GRANT STREET",
    "192 PRINCES DR",
    "UNIT 6/3 STOUT ROAD",
    "9B BLENHEIM STREET",
    "2 - 72 HALLAM SOUTH RD",
    "3 PIKE ST",
    "141 REGENCY ROAD",
    "23 ELIZABETH WAY",
    "17/19 MAIN N RD",
    "2/675 GYMPIE RD",
    "UNIT 1-13 SMITH ST",
    "93 LONSDALE ST",
]

def read_addresses(path: Optional[str], column: Optional[str]) -> List[str]:
    if not path:
        return DEFAULT_ADDRESSES[:]
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    # detect simple text vs CSV
    _, ext = os.path.splitext(path.lower())
    if ext in (".csv", ".tsv"):
        sep = "," if ext == ".csv" else "\t"
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=sep)
            if column is None:
                # guess the first column
                field = reader.fieldnames[0] if reader.fieldnames else None
                if not field:
                    return []
                column = field
            out = []
            for row in reader:
                val = (row.get(column) or "").strip()
                if val:
                    out.append(val)
            return out
    else:
        # one address per line
        with open(path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

def nominatim_geocode(query: str, country: Optional[str], timeout: float, user_agent: str) -> Optional[Dict]:
    base = "https://nominatim.openstreetmap.org/search"
    q = query if not country else f"{query}, {country}"
    params = {
        "q": q,
        "format": "json",
        "addressdetails": 1,
        "limit": 1,
    }
    url = f"{base}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "User-Agent": user_agent,
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8", errors="replace")
        arr = json.loads(data)
        if isinstance(arr, list) and arr:
            return arr[0]
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="Path to .txt (one per line) or .csv/.tsv file")
    ap.add_argument("--column", help="Column name in CSV/TSV that contains the address")
    ap.add_argument("--output", default="geocoded.csv", help="Output CSV path")
    ap.add_argument("--log", default="geocoded.log", help="Log file path")
    ap.add_argument("--country", default="Australia", help="Optional country suffix appended to each query")
    ap.add_argument("--delay", type=float, default=1.0, help="Delay between queries, seconds")
    ap.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout per request, seconds")
    ap.add_argument("--retries", type=int, default=3, help="Retries per address")
    ap.add_argument("--user_agent", default="JaydenBhang-Geocoder/1.0 (+contact: you@example.com)", help="HTTP User-Agent per Nominatim policy")
    args = ap.parse_args()

    addresses = read_addresses(args.input, args.column)
    if not addresses:
        print("No addresses to process.", file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    # Prepare writers
    out_fields = [
        "input_address","query","lat","lon","display_name",
        "osm_type","osm_id","class","type","importance"
    ]
    with open(args.output, "w", encoding="utf-8", newline="") as f_out, \
         open(args.log, "w", encoding="utf-8") as f_log:
        writer = csv.DictWriter(f_out, fieldnames=out_fields)
        writer.writeheader()

        for i, addr in enumerate(addresses, 1):
            query = addr.strip()
            print(f"[{i}/{len(addresses)}] {query}")
            tries = 0
            result = None
            while tries < args.retries:
                try:
                    result = nominatim_geocode(query, args.country, args.timeout, args.user_agent)
                    break
                except urllib.error.HTTPError as e:
                    # Backoff on rate-limit or server errors
                    wait = args.delay * (2 ** tries)
                    f_log.write(f"HTTPError for '{query}': {e.code} {e.reason}. Backing off {wait:.1f}s\n")
                    time.sleep(wait)
                    tries += 1
                except Exception as e:
                    wait = args.delay * (2 ** tries)
                    f_log.write(f"Error for '{query}': {e}. Backing off {wait:.1f}s\n")
                    time.sleep(wait)
                    tries += 1

            row = {
                "input_address": addr,
                "query": f"{addr}, {args.country}" if args.country else addr,
                "lat": "", "lon": "", "display_name": "",
                "osm_type": "", "osm_id": "", "class": "", "type": "", "importance": ""
            }
            if result:
                row.update({
                    "lat": result.get("lat",""),
                    "lon": result.get("lon",""),
                    "display_name": result.get("display_name",""),
                    "osm_type": result.get("osm_type",""),
                    "osm_id": result.get("osm_id",""),
                    "class": result.get("class",""),
                    "type": result.get("type",""),
                    "importance": result.get("importance",""),
                })
            else:
                f_log.write(f"NOT FOUND: {query}\n")
            writer.writerow(row)
            # polite delay
            time.sleep(args.delay)

    print(f"Done. Wrote {args.output} and {args.log}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

# ---------- Optional: Google Maps Geocoding (commented) ----------
# If you prefer Google Maps, uncomment and adapt the code below.
# Requires: an environment variable GOOGLE_MAPS_API_KEY or pass as a flag.
#
# import requests
# def google_geocode(query, api_key):
#     url = "https://maps.googleapis.com/maps/api/geocode/json"
#     r = requests.get(url, params={"address": query, "key": api_key}, timeout=20)
#     r.raise_for_status()
#     js = r.json()
#     if js.get("status") == "OK" and js.get("results"):
#         res = js["results"][0]
#         loc = res["geometry"]["location"]
#         return {"lat": loc["lat"], "lon": loc["lng"], "display_name": res["formatted_address"]}
#     return None
