import os
import re
import time
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json

def is_quota_exceeded(e: HttpError) -> bool:
    
    try:
        payload = json.loads(e.content.decode("utf-8"))
        reason = payload["error"]["errors"][0].get("reason", "")
        return reason in {"quotaExceeded", "dailyLimitExceeded", "rateLimitExceeded"}
    except Exception:
        s = str(e)
        return ("quotaExceeded" in s) or ("dailyLimitExceeded" in s) or ("rateLimitExceeded" in s)



def clean_wiki_text(s: str) -> str:
    """Convert strings like '[[Park|Park Name]]' -> 'Park Name' and remove brackets."""
    if not isinstance(s, str) or not s.strip():
        return ""
    s = re.sub(r"\[\[([^|\]]+)\|([^\]]+)\]\]", r"\2", s)
    s = re.sub(r"\[\[([^\]]+)\]\]", r"\1", s)
    return s.replace("[", "").replace("]", "").strip()


def build_query(title: str, park: str, location: str, country: str) -> str:
    title = clean_wiki_text(title)
    park = clean_wiki_text(park)
    location = clean_wiki_text(location)
    country = clean_wiki_text(country)

    # Best precision first: title + park
    if park:
        return f"\"{title}\" \"{park}\" roller coaster"
    # Fallback: title + location/country
    if location:
        return f"\"{title}\" \"{location}\" roller coaster"
    if country:
        return f"\"{title}\" \"{country}\" roller coaster"
    return f"\"{title}\" roller coaster"


def yt_search_candidates(youtube, query: str, max_results: int) -> List[Dict[str, Any]]:
    """Search candidates (videoId + snippet)."""
    resp = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        maxResults=max_results,
        safeSearch="none",
        order="viewCount",  # candidate quality; we'll sort by views later
    ).execute()

    out = []
    for it in resp.get("items", []):
        vid = it.get("id", {}).get("videoId")
        snip = it.get("snippet", {})
        if not vid:
            continue
        out.append({
            "video_id": vid,
            "video_title": snip.get("title", ""),
            "channel_title": snip.get("channelTitle", ""),
            "published_at": snip.get("publishedAt", ""),
        })
    return out


def yt_fetch_video_stats(
    youtube,
    video_ids: List[str],
    sleep_seconds: float = 0.1,
) -> Dict[str, Dict[str, Any]]:
    """Fetch statistics for up to 50 video IDs per request."""
    stats_map: Dict[str, Dict[str, Any]] = {}

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        try:
            resp = youtube.videos().list(
                part="statistics,snippet",
                id=",".join(batch),
            ).execute()
        except HttpError as e:
            if is_quota_exceeded(e):
                print("\n[STOP] Quota exceeded while fetching video stats. Returning partial stats.")
                break
            print("  - Stats fetch failed:", e)
            time.sleep(sleep_seconds)
            continue

        for it in resp.get("items", []):
            vid = it.get("id")
            stats = it.get("statistics", {})
            snip = it.get("snippet", {})

            view = stats.get("viewCount")
            like = stats.get("likeCount")
            comm = stats.get("commentCount")

            stats_map[vid] = {
                "view_count": int(view) if view and view.isdigit() else None,
                "like_count": int(like) if like and like.isdigit() else None,
                "comment_count": int(comm) if comm and comm.isdigit() else None,
                "channel_id": snip.get("channelId", ""),
                "channel_title_full": snip.get("channelTitle", ""),
            }

        time.sleep(sleep_seconds)

    return stats_map



def build_coaster_videos_csv(
    coasters_csv: str = "coasters_basic.csv",
    out_csv: str = "coaster_youtube_videos.csv",
    max_coasters: int = 200,
    candidates_per_coaster: int = 5,
    top_k_by_views: int = 3,
    sleep_seconds: float = 0.25,
) -> None:
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("Set YOUTUBE_API_KEY environment variable first.")

    youtube = build("youtube", "v3", developerKey=api_key)
    df = pd.read_csv(coasters_csv).head(max_coasters)

    fetched_at_utc = datetime.now(timezone.utc).isoformat()

    # Resume: read existing file and skip already processed coaster titles
    processed: set[str] = set()
    if os.path.exists(out_csv):
        try:
            existing = pd.read_csv(out_csv, usecols=["coaster_title"])
            processed = set(existing["coaster_title"].dropna().astype(str).unique())
            print(f"Resume enabled: {len(processed)} coasters already in {out_csv}")
        except Exception:
            processed = set()

    total = len(df)
    processed_today = 0

    for i, r in df.iterrows():
        title = str(r.get("title", "")).strip()
        park = str(r.get("park", "")).strip()
        location = str(r.get("location", "")).strip()
        country = str(r.get("country", "")).strip()

        coaster_title_clean = clean_wiki_text(title)
        if coaster_title_clean in processed:
            print(f"[{i+1}/{total}] Skipping already processed: {coaster_title_clean}")
            continue

        query = build_query(title, park, location, country)
        print(f"[{i+1}/{total}] Searching: {query}")

        # 1) Search
        try:
            candidates = yt_search_candidates(youtube, query, max_results=candidates_per_coaster)
        except HttpError as e:
            if is_quota_exceeded(e):
                print("\n[STOP] Quota exceeded during search. Exiting now (progress already saved).")
                break
            print("  - Search failed:", e)
            time.sleep(sleep_seconds)
            continue

        if not candidates:
            print("  - No candidates found.")
            processed.add(coaster_title_clean)  # mark as done so I don't repeat forever
            continue

        # 2) Fetch stats (only for this coaster’s candidates)
        video_ids = sorted({c["video_id"] for c in candidates})
        try:
            stats_map = yt_fetch_video_stats(youtube, video_ids, sleep_seconds=0.1)
        except HttpError as e:
            if is_quota_exceeded(e):
                print("\n[STOP] Quota exceeded during stats fetch. Exiting now (progress already saved).")
                break
            print("  - Stats fetch failed:", e)
            stats_map = {}

        # 3) Build rows
        coaster_rows: List[Dict[str, Any]] = []
        for c in candidates:
            s = stats_map.get(c["video_id"], {})
            coaster_rows.append({
                **c,
                "coaster_title": coaster_title_clean,
                "park": clean_wiki_text(park),
                "location": clean_wiki_text(location),
                "country": clean_wiki_text(country),
                "search_query": query,
                "fetched_at_utc": fetched_at_utc,
                "view_count": s.get("view_count"),
                "like_count": s.get("like_count"),
                "comment_count": s.get("comment_count"),
                "channel_id": s.get("channel_id"),
                "channel_title_api": s.get("channel_title_full"),
            })

        coaster_df = pd.DataFrame(coaster_rows)

        # 4) Keep top K by views for THIS coaster
        coaster_df["view_count_sort"] = coaster_df["view_count"].fillna(0)
        coaster_df = coaster_df.sort_values("view_count_sort", ascending=False).head(top_k_by_views)
        coaster_df = coaster_df.drop(columns=["view_count_sort"])

        # 5) Append to CSV immediately (checkpoint)
        write_header = not os.path.exists(out_csv)
        coaster_df.to_csv(out_csv, mode="a", header=write_header, index=False)

        processed.add(coaster_title_clean)
        processed_today += 1
        time.sleep(sleep_seconds)

    print(f"Done. Added {processed_today} coasters in this run. Output: {out_csv}")



if __name__ == "__main__":
    build_coaster_videos_csv(
        coasters_csv="coasters_basic.csv",
        out_csv="coaster_youtube_videos.csv",
        max_coasters=200,
        candidates_per_coaster=5,
        top_k_by_views=3,
        sleep_seconds=0.25,
    )

