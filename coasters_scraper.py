import requests
import time
import csv
from typing import List, Dict

# Base API info
BASE_URL = "https://coasterpedia.net/w/api.php"
CATEGORY_TITLE = "Category:Roller_coasters_by_name"


def get_coaster_titles(max_coasters: int = 50) -> List[str]:
    """
    Step 1: Call categorymembers to get a list of coaster page titles.
    """
    titles: List[str] = []
    cmcontinue = None  # used for pagination

    while len(titles) < max_coasters:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": CATEGORY_TITLE,
            "cmlimit": min(50, max_coasters - len(titles)),
            "format": "json",
        }
        # if there are more pages, API tells us via "cmcontinue"
        if cmcontinue:
            params["cmcontinue"] = cmcontinue
        print("Sending request with params:", params)

        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            titles.append(m["title"])
            if len(titles) >= max_coasters:
                break

        # if API returns "continue", there are more results
        if "continue" in data:
            cmcontinue = data["continue"]["cmcontinue"]
            print("Next cmcontinue token:", cmcontinue)
        else:
            break

    return titles
if __name__ == "__main__":
    titles = get_coaster_titles(max_coasters=20)
    print("\nFinal number of titles:", len(titles))
    print("First few coaster titles:")
    for t in titles[:10]:
        print(" -", t)


# def fetch_infobox_wikitext(title: str) -> str | None:
#     """
#     Step 2a: For a given page title, fetch the wikitext using the parse module.
#     """
#     params = {
#         "action": "parse",
#         "page": title,
#         "prop": "wikitext",
#         "format": "json",
#     }
#     resp = requests.get(BASE_URL, params=params, timeout=30)
#     resp.raise_for_status()
#     data = resp.json()

#     parse_block = data.get("parse")
#     if not parse_block:
#         return None

#     wikitext = parse_block.get("wikitext", {}).get("*")
#     return wikitext

# def extract_infobox_fields(wikitext: str) -> Dict[str, str]:
#     """
#     Very simple parser for the 'Infobox roller coaster' template.

#     It looks for lines inside the template that start with '| field = value'
#     and extracts a few selected fields.
#     """
#     if not wikitext:
#         return {}

#     lines = wikitext.splitlines()
#     inside = False
#     info_lines: List[str] = []

#     # 1) Find the Infobox block
#     for line in lines:
#         stripped = line.strip()
#         lower = stripped.lower()
#         if not inside and lower.startswith("{{infobox roller coaster"):
#             inside = True
#             continue
#         if inside:
#             if stripped.startswith("}}"):
#                 break
#             info_lines.append(stripped)

#     # 2) Turn lines like "| speed = 90 km/h" into key/value pairs
#     raw_fields: Dict[str, str] = {}
#     for line in info_lines:
#         if not line.startswith("|"):
#             continue
#         try:
#             key, value = line[1:].split("=", 1)
#         except ValueError:
#             continue
#         key = key.strip().lower()
#         value = value.strip()
#         raw_fields[key] = value

#     # helper to pick first existing key from several options
#     def pick(*keys: str) -> str:
#         for k in keys:
#             if k in raw_fields:
#                 return raw_fields[k]
#         return ""

#     # 3) Map to the fields we care about
#     result = {
#         "speed": pick("speed", "top_speed"),
#         "height": pick("height"),
#         "g_force": pick("g-force", "g_force", "gforce"),
#         "inversions": pick("inversions"),
#         "type": pick("type", "coaster_type"),
#         "manufacturer": pick("manufacturer"),
#         "park": pick("park", "location"),
#         "opening_year": pick("openingdate", "opening_date", "opened"),
#     }
#     return result

# def scrape_coasters(max_coasters: int = 30, sleep_seconds: float = 0.2) -> None:
#     # Step 1 — list of titles
#     titles = get_coaster_titles(max_coasters=max_coasters)
#     print(f"Fetched {len(titles)} coaster titles.")
#     rows = []

#     # Step 2 — for each title, fetch and parse the infobox
#     for i, title in enumerate(titles, start=1):
#         print(f"[{i}/{len(titles)}] Fetching infobox for: {title!r}")
#         wikitext = fetch_infobox_wikitext(title)
#         if not wikitext:
#             print(f"  - No wikitext for {title}, skipping.")
#             continue
#         info = extract_infobox_fields(wikitext)
#         info["title"] = title
#         rows.append(info)
#         time.sleep(sleep_seconds)  # be nice to the server

#     # Save everything into a CSV file
#     fieldnames = ["title", "speed", "height", "g_force", "inversions",
#                   "type", "manufacturer", "park", "opening_year"]

#     with open("coasters_basic.csv", "w", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()
#         for row in rows:
#             writer.writerow(row)

#     print("Saved data to coasters_basic.csv") 

# if __name__ == "__main__":
#     scrape_coasters(max_coasters=30)
