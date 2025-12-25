#!/usr/bin/env python3
"""
Duke NBA Daily Email (LOCAL, nba_api)

What it does:
- Finds yesterday's NBA games (ScoreboardV2)
- Pulls box scores for each game (BoxScoreTraditionalV2)
- Filters to ACTIVE NBA players who attended Duke (auto-discovered + cached)
- Sends an email summary (or prints if email env vars missing)

Why this version:
- You got 403 from NBA CDN on your network.
- nba_api works locally for you, so we use stats.nba.com endpoints.
- The “no Duke players” issue is almost always an INCOMPLETE hard-coded ID list.
  This script auto-builds the Duke ID list and caches it, so it stays correct.

Install:
  py -m pip install nba_api pandas requests

Email env vars (set on your Windows machine):
  SMTP_HOST=smtp.gmail.com
  SMTP_PORT=587
  SMTP_USER=you@gmail.com
  SMTP_PASSWORD=<Gmail App Password>
  EMAIL_FROM=you@gmail.com
  EMAIL_TO=destination@gmail.com
"""

import json
import os
import random
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import requests
from nba_api.stats.endpoints import (
    boxscoretraditionalv2,
    commonplayerinfo,
    scoreboardv2,
)
from nba_api.stats.static import players as static_players


# ---------------- CONFIG ----------------
CACHE_FILE = Path(__file__).with_name("duke_players_cache.json")
DUKE_CACHE_MAX_AGE_DAYS = 30
DUKE_SUBSTRING = "duke"  # case-insensitive match against college field


# ---------------- RETRY / THROTTLE HELPERS ----------------

def with_retries(
    fn: Callable[[], Any],
    *,
    retries: int = 5,
    base_sleep: float = 2.0,
    label: str = "request",
) -> Any:
    """
    Retry wrapper for transient network issues.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            TimeoutError,
        ) as e:
            last_err = e
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0.0, 0.8)
            print(f"  ! {label} failed (attempt {attempt}/{retries}): {repr(e)}")
            if attempt < retries:
                print(f"    retrying in {sleep_s:.1f}s...")
                time.sleep(sleep_s)
    if last_err:
        raise last_err
    raise RuntimeError(f"{label} failed (unknown error)")


def polite_sleep() -> None:
    """
    Small random delay to reduce rate limiting / bot detection.
    """
    time.sleep(random.uniform(0.35, 0.9))


# ---------------- DUKE PLAYER DISCOVERY (AUTO + CACHED) ----------------

def cache_is_fresh(path: Path, max_age_days: int) -> bool:
    if not path.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    return age <= timedelta(days=max_age_days)


def load_cached_duke_ids() -> Optional[List[int]]:
    if not cache_is_fresh(CACHE_FILE, DUKE_CACHE_MAX_AGE_DAYS):
        return None
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        ids = [int(x) for x in data.get("duke_player_ids", [])]
        if ids:
            print(f"Loaded {len(ids)} Duke player IDs from cache: {CACHE_FILE.name}")
            return ids
    except Exception:
        pass
    return None


def save_cached_duke_ids(ids: List[int], details: List[Dict[str, Any]]) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(),
        "duke_player_ids": ids,
        "players": details,  # helpful for debugging who is included
    }
    CACHE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Saved Duke player cache with {len(ids)} IDs -> {CACHE_FILE.name}")


def build_duke_ids_from_nba_api() -> List[int]:
    """
    Discover all ACTIVE NBA players whose CommonPlayerInfo college includes 'Duke'.

    This is slower the first time (calls commonplayerinfo per active player),
    then fast thereafter because we cache results.
    """
    active = static_players.get_active_players()
    print(f"Discovering Duke alumni among {len(active)} active NBA players...")

    duke_ids: List[int] = []
    duke_details: List[Dict[str, Any]] = []

    for idx, p in enumerate(active, start=1):
        pid = int(p["id"])
        name = p.get("full_name", "Unknown")

        def _call():
            return commonplayerinfo.CommonPlayerInfo(player_id=pid, timeout=60).get_data_frames()[0]

        try:
            df = with_retries(_call, retries=4, base_sleep=2, label=f"commonplayerinfo {pid}")
        except Exception as e:
            # Don't fail the entire run if one player lookup fails.
            print(f"  ! Skipping {name} ({pid}) due to info fetch error: {repr(e)}")
            polite_sleep()
            continue

        college = ""
        if not df.empty and "SCHOOL" in df.columns:
            college = str(df.loc[0, "SCHOOL"] or "")
        elif not df.empty and "COLLEGE" in df.columns:
            college = str(df.loc[0, "COLLEGE"] or "")

        if DUKE_SUBSTRING in college.strip().lower():
            duke_ids.append(pid)
            duke_details.append({"player_id": pid, "player_name": name, "college": college})

        # Light pacing
        if idx % 10 == 0:
            print(f"  ...checked {idx}/{len(active)}")
        polite_sleep()

    duke_ids = sorted(set(duke_ids))
    # Save cache
    save_cached_duke_ids(duke_ids, duke_details)
    return duke_ids


def get_duke_player_ids() -> List[int]:
    cached = load_cached_duke_ids()
    if cached is not None:
        return cached
    return build_duke_ids_from_nba_api()


# ---------------- NBA GAME + BOX SCORE FETCHING ----------------

def get_scoreboard_for_date(target_date: date) -> pd.DataFrame:
    """
    Get ScoreboardV2 game header DataFrame for a given date.
    """
    date_str = target_date.strftime("%m/%d/%Y")
    print(f"Fetching NBA scoreboard for {date_str}...")

    def _call():
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=90)
        return sb.game_header.get_data_frame()

    return with_retries(_call, retries=5, base_sleep=2, label="scoreboardv2")


def get_duke_stats_for_date(target_date: date, duke_ids: List[int]) -> pd.DataFrame:
    """
    For all games on target_date, return a DataFrame of box score stats
    for players in duke_ids, including their opponent team abbrev.
    """
    games_df = get_scoreboard_for_date(target_date)
    print(f"Games found: {len(games_df)} | Duke IDs tracked: {len(duke_ids)}")

    if games_df.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    date_str = target_date.strftime("%Y-%m-%d")

    for _, game in games_df.iterrows():
        game_id = game["GAME_ID"]
        home_team_id = int(game["HOME_TEAM_ID"])
        visitor_team_id = int(game["VISITOR_TEAM_ID"])

        print(f"  Processing game {game_id}...")

        def _box():
            bs = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=90)
            return bs.player_stats.get_data_frame()

        try:
            players_df = with_retries(_box, retries=4, base_sleep=2, label=f"boxscore {game_id}")
        except Exception as e:
            print(f"    ! Error fetching box score for {game_id}: {repr(e)}")
            continue

        if players_df.empty:
            polite_sleep()
            continue

        for _, p in players_df.iterrows():
            pid = int(p["PLAYER_ID"])
            if pid not in duke_ids:
                continue

            team_id = int(p["TEAM_ID"])
            team_abbr = p["TEAM_ABBREVIATION"]

            opp_team_id = visitor_team_id if team_id == home_team_id else home_team_id
            opp_abbrs = (
                players_df.loc[players_df["TEAM_ID"] == opp_team_id, "TEAM_ABBREVIATION"]
                .dropna()
                .unique()
            )
            opp_abbr = opp_abbrs[0] if len(opp_abbrs) else "UNK"

            rows.append({
                "date": date_str,
                "game_id": game_id,
                "player_id": pid,
                "player_name": f"{p['PLAYER_FIRST_NAME']} {p['PLAYER_LAST_NAME']}",
                "team": team_abbr,
                "opponent": opp_abbr,
                "minutes": p["MIN"],
                "points": int(p["PTS"]),
                "rebounds": int(p["REB"]),
                "assists": int(p["AST"]),
                "fgm": int(p["FGM"]),
                "fga": int(p["FGA"]),
                "fg3m": int(p["FG3M"]),
                "fg3a": int(p["FG3A"]),
                "ftm": int(p["FTM"]),
                "fta": int(p["FTA"]),
                "plus_minus": p["PLUS_MINUS"],
            })

        polite_sleep()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values(["player_name"])
    return df


# ---------------- EMAIL FORMATTING & SENDING ----------------

def format_email_body(stats_df: pd.DataFrame, target_date: date, games_found: int, duke_ids_count: int) -> str:
    """
    Build a human-readable email body summarizing Duke players' stats.
    """
    header_date = target_date.strftime("%A, %B %d, %Y")
    lines = [
        f"Duke in the NBA — {header_date}",
        "-" * 60,
        f"Games found: {games_found} | Duke IDs tracked: {duke_ids_count}",
        "",
    ]

    if stats_df.empty:
        if games_found == 0:
            lines.append("No NBA games were found on this date.")
        else:
            lines.append("No tracked Duke alumni matched the box scores for this date.")
            lines.append("If this seems wrong, refresh the Duke cache (instructions at bottom).")
        lines.append("")
        lines.append("Cache refresh tip: delete duke_players_cache.json to rebuild Duke IDs.")
        return "\n".join(lines)

    for _, row in stats_df.iterrows():
        lines.append(
            f"{row['player_name']} ({row['team']} vs {row['opponent']}): "
            f"{row['points']} PTS, {row['rebounds']} REB, {row['assists']} AST, {row['minutes']} MIN, "
            f"+/- {row['plus_minus']} "
            f"(FG {row['fgm']}-{row['fga']}, 3P {row['fg3m']}-{row['fg3a']}, FT {row['ftm']}-{row['fta']})"
        )

    return "\n".join(lines)


def send_email(subject: str, body: str) -> None:
    """
    Send the email using SMTP settings from environment variables.
    If EMAIL_TO (or required settings) is missing, just print the email body.
    """
    import smtplib
    from email.mime.text import MIMEText

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port_raw = os.getenv("SMTP_PORT", "587")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = os.getenv("EMAIL_TO")

    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        print(f"Invalid SMTP_PORT={smtp_port_raw!r}; defaulting to 587")
        smtp_port = 587

    if not email_to or not smtp_host or not email_from:
        print("\n--- EMAIL (not sent; missing EMAIL_TO/SMTP_HOST/EMAIL_FROM) ---")
        print(f"Subject: {subject}\n")
        print(body)
        print("--- END ---\n")
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.sendmail(email_from, [email_to], msg.as_string())
        print(f"Email sent to {email_to}.")
    except Exception as e:
        print(f"SMTP send failed: {repr(e)}")
        traceback.print_exc()
        print("\n--- EMAIL (printing instead) ---")
        print(f"Subject: {subject}\n")
        print(body)
        print("--- END ---\n")


# ---------------- MAIN ENTRYPOINT ----------------

def main() -> None:
    # Default: yesterday's games for a morning email
    target_date = date.today() - timedelta(days=1)

    duke_ids = get_duke_player_ids()
    if not duke_ids:
        print("No Duke player IDs found; aborting.")
        return

    games_df = get_scoreboard_for_date(target_date)
    stats_df = get_duke_stats_for_date(target_date, duke_ids)

    body = format_email_body(
        stats_df=stats_df,
        target_date=target_date,
        games_found=len(games_df),
        duke_ids_count=len(duke_ids),
    )
    subject = f"Duke in the NBA — {target_date.strftime('%Y-%m-%d')}"
    send_email(subject, body)


if __name__ == "__main__":
    main()
