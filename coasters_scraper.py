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



def fetch_infobox_wikitext(title: str) -> str | None:
    """
    Step 2a: For a given page title, fetch the wikitext using the parse module.
    """
    params = {
        "action": "parse",
        "page": title,
        "prop": "wikitext",
        "format": "json",
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    parse_block = data.get("parse")
    if not parse_block:
        return None

    wikitext = parse_block.get("wikitext", {}).get("*")
    return wikitext

def extract_infobox_fields(wikitext: str) -> Dict[str, str]:
    """
    Very simple parser for the 'Infobox roller coaster' template.

    It looks for lines inside the template that start with '| field = value'
    and extracts a few selected fields.
    """
    if not wikitext:
        return {}

    lines = wikitext.splitlines()
    print(f" Total lines in wikitext: {len(lines)}")
    inside = False
    info_lines: List[str] = []

    # 1) Find the Infobox block
    for line in lines:
        stripped = line.strip()
        lower = stripped.lower()
        if not inside and lower.startswith("{{infobox roller coaster"):
            inside = True
            continue
        if inside:
            if stripped.startswith("}}"):
                break
            info_lines.append(stripped)

    # 2) Turn lines like "| speed = 90 km/h" into key/value pairs
    raw_fields: Dict[str, str] = {}
    for line in info_lines:
        if not line.startswith("|"):
            continue
        try:
            key, value = line[1:].split("=", 1)
        except ValueError:
            continue
        key = key.strip().lower()
        value = value.strip()
        raw_fields[key] = value
    print(" Available infobox keys found:")
    for k in sorted(raw_fields.keys()):
        print("  -", k)
    


    # helper to pick first existing key from several options
    def pick(*keys: str) -> str:
        for k in keys:
            if k in raw_fields:
                return raw_fields[k]
        return ""

    # 3) Map to the fields I care about
    result = {
    # Identity
    "name": pick("name"),

    # Location / discovery
    "park": pick("park"),
    "location": pick("location"),
    "country": pick("country"),
    "state": pick("state"),
    "section": pick("section"),
    "coord_lat": pick("coord_lat", "latitude", "lat"),
    "coord_long": pick("coord_long", "longitude", "long", "lon"),

    # Lifecycle
    "status": pick("status"),
    "opened": pick("opened", "openingdate", "opening_date", "opening_year"),
    "closed": pick("closed"),

    # Build / classification
    "manufacturer": pick("manufacturer"),
    "builder": pick("builder"),
    "designer": pick("designer"),
    "product": pick("product", "model"),
    "class": pick("class"),
    "type": pick("type", "coaster_type"),

    # Core thrill stats (recommender features)
    "speed": pick("speed", "top_speed"),
    "height": pick("height", "max_height"),
    "drop": pick("drop"),
    "angle": pick("angle"),
    "g_force": pick("g-force", "g_force", "gforce"),
    "inversions": pick("inversions"),
    "length": pick("length"),
    "duration": pick("duration"),
    "layout": pick("layout"),
    "lift_launch": pick("lift/launch", "lift", "launch", "lift_launch"),

    # Rider constraints
    "min_height": pick("min_height", "minimum_height"),
    "min_height_unaccompanied": pick("min_height_unaccompanied"),
    "max_height": pick("max_height", "maximum_height"),
    "restriction": pick("restriction"),

    # Capacity / ops
    "riders_hour": pick("riders/hour", "riders_per_hour", "capacity"),
    "riders_train": pick("riders/train", "riders_per_train")


}
    print("\n[DEBUG] Normalized result dict:")
    for k, v in result.items():
        print(f"   {k}: {v}")

    return result
def scrape_coasters(max_coasters: int = 300, sleep_seconds: float = 0.25) -> None:
    # Step 1 — list of titles
    titles = get_coaster_titles(max_coasters=max_coasters)
    print(f"Fetched {len(titles)} coaster titles.")
    rows = []

    # Step 2 — for each title, fetch and parse the infobox
    for i, title in enumerate(titles, start=1):
        print(f"[{i}/{len(titles)}] Fetching infobox for: {title!r}")
        wikitext = fetch_infobox_wikitext(title)
        if not wikitext:
            print(f"  - No wikitext for {title}, skipping.")
            continue
        info = extract_infobox_fields(wikitext)
        info["title"] = title
        rows.append(info)
        time.sleep(sleep_seconds) 
        info["title"] = title 

   
    fieldnames = None
    if fieldnames is None:
            # Put "title" first, then the rest in a stable order
            fieldnames = ["title"] + [k for k in info.keys() if k != "title"]
            print("\n[INFO] CSV columns will be:")
            for c in fieldnames:
                print(" -", c)
    rows.append(info)
    time.sleep(sleep_seconds)

    with open("coasters_basic.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows to coasters_basic.csv") 

if __name__ == "__main__":
    scrape_coasters(max_coasters=300, sleep_seconds=0.25)

# if __name__ == "__main__":
#     titles = get_coaster_titles(max_coasters=20)
#     print("\nFinal number of titles:", len(titles))

#     for i, title in enumerate(titles, start=1):
#         print(f"\n[{i}/{len(titles)}] Fetching infobox for: {title}")
#         wikitext = fetch_infobox_wikitext(title)
#         if not wikitext:
#             print("  No wikitext found.")
#             continue

#         extract_infobox_fields(wikitext)  # prints keys (and later can return dict)

