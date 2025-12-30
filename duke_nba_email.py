#!/usr/bin/env python3
"""
Duke NBA Daily Email (LOCAL, nba_api) — 2025–26 compatible (BoxScoreTraditionalV3)

What it does
- Finds yesterday's NBA games (ScoreboardV2)
- Pulls box scores for each game (BoxScoreTraditionalV3)
- Filters to Duke alumni by checking CommonPlayerInfo SCHOOL (cached locally)
- Sends an email that contains ONLY an HTML table (no narrative)
- Also writes CSV + HTML files locally for easy viewing

Install
  py -m pip install nba_api pandas requests tabulate

Email env vars (Windows Environment Variables)
  SMTP_HOST=smtp.gmail.com
  SMTP_PORT=587
  SMTP_USER=you@gmail.com
  SMTP_PASSWORD=<Gmail App Password>
  EMAIL_FROM=you@gmail.com
  EMAIL_TO=destination@gmail.com
"""

import os
import json
import time
import random
import traceback
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import requests
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv3, commonplayerinfo


# ---------------- CONFIG ----------------
DUKE_SUBSTRING = "duke"
BASE_DIR = Path(__file__).resolve().parent
SCHOOL_CACHE_PATH = BASE_DIR / "player_school_cache.json"


# ---------------- RETRIES / THROTTLE ----------------
def with_retries(
    fn: Callable[[], Any],
    *,
    retries: int = 5,
    base_sleep: float = 2.0,
    label: str = "request",
) -> Any:
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
    time.sleep(random.uniform(0.25, 0.75))


# ---------------- SCHEMA HELPERS (V3 varies) ----------------
def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Return the first column name that exists in df from a list of candidates.
    Tries exact match first, then case-insensitive match.
    """
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    lower_map = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


# ---------------- SCHOOL CACHE ----------------
def load_school_cache() -> Dict[str, str]:
    if not SCHOOL_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(SCHOOL_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_school_cache(cache: Dict[str, str]) -> None:
    SCHOOL_CACHE_PATH.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def get_player_school(player_id: int, cache: Dict[str, str]) -> str:
    key = str(player_id)
    if key in cache:
        return cache[key]

    def _call():
        return commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=60).get_data_frames()[0]

    df = with_retries(_call, retries=4, base_sleep=2, label=f"playerinfo {player_id}")

    school = ""
    if not df.empty:
        if "SCHOOL" in df.columns:
            school = str(df.loc[0, "SCHOOL"] or "")
        elif "COLLEGE" in df.columns:
            school = str(df.loc[0, "COLLEGE"] or "")

    cache[key] = school
    save_school_cache(cache)
    polite_sleep()
    return school


def is_duke_player(player_id: int, cache: Dict[str, str]) -> bool:
    return DUKE_SUBSTRING in get_player_school(player_id, cache).strip().lower()


# ---------------- NBA DATA ----------------
def get_games(target_date: date) -> pd.DataFrame:
    date_str = target_date.strftime("%m/%d/%Y")
    print(f"Fetching NBA scoreboard for {date_str}...")

    def _call():
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=90)
        return sb.game_header.get_data_frame()

    return with_retries(_call, retries=5, base_sleep=2, label="scoreboardv2")


def get_duke_boxscores(target_date: date) -> pd.DataFrame:
    games_df = get_games(target_date)
    print(f"Games found: {len(games_df)}")
    if games_df.empty:
        return pd.DataFrame()

    cache = load_school_cache()
    rows: List[Dict[str, Any]] = []

    for _, game in games_df.iterrows():
        game_id = game["GAME_ID"]
        home_team_id = int(game["HOME_TEAM_ID"])
        away_team_id = int(game["VISITOR_TEAM_ID"])

        print(f"  Processing game {game_id}...")

        def _box():
            bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=90)
            return bs.player_stats.get_data_frame()

        try:
            players_df = with_retries(_box, retries=4, base_sleep=2, label=f"boxscorev3 {game_id}")
        except Exception as e:
            print(f"    ! boxscore failed for {game_id}: {repr(e)}")
            continue

        if players_df.empty:
            continue

        # robust team cols
        team_id_col = pick_col(players_df, ["TEAM_ID", "teamId", "TEAMID"])
        team_abbr_col = pick_col(players_df, ["TEAM_ABBREVIATION", "teamTricode", "TEAM_TRICODE", "TEAM_ABBR"])

        if not team_id_col or not team_abbr_col:
            print("    ! Unexpected V3 player_stats schema. Columns are:")
            print("    ", list(players_df.columns))
            continue

        team_abbr_map = (
            players_df[[team_id_col, team_abbr_col]]
            .dropna()
            .drop_duplicates()
            .set_index(team_id_col)[team_abbr_col]
            .to_dict()
        )

        # robust stat cols (many V3 schemas differ; we try several candidates)
        pid_col = pick_col(players_df, ["PLAYER_ID", "personId", "PERSON_ID"])
        first_col = pick_col(players_df, ["PLAYER_FIRST_NAME", "firstName", "FIRST_NAME"])
        last_col = pick_col(players_df, ["PLAYER_LAST_NAME", "familyName", "LAST_NAME"])
        min_col = pick_col(players_df, ["MIN", "minutes", "MINUTES"])
        pts_col = pick_col(players_df, ["PTS", "points"])
        oreb_col = pick_col(players_df, ["OREB", "offReb", "offensiveRebounds", "REB_OFF"])
        dreb_col = pick_col(players_df, ["DREB", "defReb", "defensiveRebounds", "REB_DEF"])
        reb_col = pick_col(players_df, ["REB", "reboundsTotal", "reb"])
        ast_col = pick_col(players_df, ["AST", "assists", "ast"])
        stl_col = pick_col(players_df, ["STL", "steals", "stl"])
        blk_col = pick_col(players_df, ["BLK", "blocks", "blk"])
        tov_col = pick_col(players_df, ["TO", "TOV", "turnovers", "to"])

        fgm_col = pick_col(players_df, ["FGM", "fieldGoalsMade"])
        fga_col = pick_col(players_df, ["FGA", "fieldGoalsAttempted"])
        fg3m_col = pick_col(players_df, ["FG3M", "threePointersMade"])
        fg3a_col = pick_col(players_df, ["FG3A", "threePointersAttempted"])
        ftm_col = pick_col(players_df, ["FTM", "freeThrowsMade"])
        fta_col = pick_col(players_df, ["FTA", "freeThrowsAttempted"])
        pm_col = pick_col(players_df, ["PLUS_MINUS", "plusMinusPoints", "PLUSMINUS"])

        if not pid_col:
            print("    ! Missing PLAYER_ID-like column. Columns are:")
            print("    ", list(players_df.columns))
            continue

        def as_int(val: Any) -> int:
            try:
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return 0
                return int(val)
            except Exception:
                return 0

        def as_str(val: Any) -> str:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return ""
            return str(val)

        for _, p in players_df.iterrows():
            pid = as_int(p[pid_col])

            try:
                if not is_duke_player(pid, cache):
                    continue
            except Exception as e:
                print(f"    ! playerinfo failed for PLAYER_ID={pid}: {repr(e)}")
                continue

            team_id = as_int(p[team_id_col])
            team_abbr = as_str(p[team_abbr_col])

            opp_team_id = away_team_id if team_id == home_team_id else home_team_id
            opp_abbr = team_abbr_map.get(opp_team_id, "UNK")

            first = as_str(p[first_col]) if first_col else ""
            last = as_str(p[last_col]) if last_col else ""
            name = (first + " " + last).strip() or f"PLAYER_{pid}"

            fgm = as_int(p[fgm_col]) if fgm_col else 0
            fga = as_int(p[fga_col]) if fga_col else 0
            fg3m = as_int(p[fg3m_col]) if fg3m_col else 0
            fg3a = as_int(p[fg3a_col]) if fg3a_col else 0
            ftm = as_int(p[ftm_col]) if ftm_col else 0
            fta = as_int(p[fta_col]) if fta_col else 0

            row = {
                "Player": name,
                "Team": team_abbr,
                "Opponent": opp_abbr,
                "MIN": as_str(p[min_col]) if min_col else "",
                "PTS": as_int(p[pts_col]) if pts_col else 0,
                "OREB": as_int(p[oreb_col]) if oreb_col else 0,
                "DREB": as_int(p[dreb_col]) if dreb_col else 0,
                "REB": as_int(p[reb_col]) if reb_col else 0,
                "AST": as_int(p[ast_col]) if ast_col else 0,
                "STL": as_int(p[stl_col]) if stl_col else 0,
                "BLK": as_int(p[blk_col]) if blk_col else 0,
                "TOV": as_int(p[tov_col]) if tov_col else 0,
                "FG": f"{fgm}-{fga}",
                "3P": f"{fg3m}-{fg3a}",
                "FT": f"{ftm}-{fta}",
                "+/-": as_str(p[pm_col]) if pm_col else "",
            }
            rows.append(row)

        polite_sleep()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # -------- nice column order (requested) --------
    desired = ["Player", "Team", "Opponent", "MIN", "PTS", "OREB", "DREB", "REB",
               "AST", "STL", "BLK", "TOV", "FG", "3P", "FT", "+/-"]
    df = df[[c for c in desired if c in df.columns]]

    # Sort: highest points first, then player name
    if "PTS" in df.columns:
        df = df.sort_values(["PTS", "Player"], ascending=[False, True])

    return df


# ---------------- OUTPUT (FILES + EMAIL TABLE ONLY) ----------------
def write_table_files(stats_df: pd.DataFrame, target_date: date) -> Dict[str, Path]:
    ymd = target_date.strftime("%Y-%m-%d")
    csv_path = BASE_DIR / f"duke_boxscore_{ymd}.csv"
    html_path = BASE_DIR / f"duke_boxscore_{ymd}.html"

    if stats_df.empty:
        csv_path.write_text("No rows\n", encoding="utf-8")
        html_path.write_text("<p>No rows</p>", encoding="utf-8")
    else:
        stats_df.to_csv(csv_path, index=False)
        html_path.write_text(stats_df.to_html(index=False, border=0), encoding="utf-8")

    return {"csv": csv_path, "html": html_path}


def send_email_table_only(subject: str, stats_df: pd.DataFrame) -> None:
    import smtplib
    from email.mime.multipart import MIMEMultipart
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
        smtp_port = 587

    # If missing config, print table to console/log and exit
    if not email_to or not smtp_host or not email_from:
        print("\n--- TABLE ONLY (not emailed; missing SMTP/EMAIL env vars) ---")
        if stats_df.empty:
            print("(empty)")
        else:
            try:
                print(stats_df.to_markdown(index=False))
            except Exception:
                print(stats_df.to_string(index=False))
        print("--- END ---\n")
        return

    # HTML table only (no narrative)
    if stats_df.empty:
        html = "<p>(No Duke alumni matched the box scores for this date.)</p>"
    else:
        html = stats_df.to_html(index=False, border=0)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.sendmail(email_from, [email_to], msg.as_string())

    print(f"Email sent to {email_to}.")


# ---------------- MAIN ----------------
def main() -> None:
    target_date = date.today() - timedelta(days=1)

    stats_df = get_duke_boxscores(target_date)

    # Print table to console/log
    print("\n--- DUKE BOX SCORE TABLE ---")
    if stats_df.empty:
        print("(empty)")
    else:
        try:
            print(stats_df.to_markdown(index=False))
        except Exception:
            print(stats_df.to_string(index=False))
    print("--- END TABLE ---\n")

    # Write local files you can open
    paths = write_table_files(stats_df, target_date)
    print(f"Wrote CSV:  {paths['csv']}")
    print(f"Wrote HTML: {paths['html']}")

    # Send table-only email
    subject = f"Duke in the NBA — {target_date.strftime('%Y-%m-%d')}"
    send_email_table_only(subject, stats_df)


if __name__ == "__main__":
    main()
