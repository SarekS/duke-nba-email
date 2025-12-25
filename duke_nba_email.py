#!/usr/bin/env python3
"""
Duke NBA Daily Email (LOCAL, nba_api) — 2025–26 compatible

Uses:
- ScoreboardV2 for games (stats.nba.com)
- BoxScoreTraditionalV3 for box scores (V2 no longer publishes data in 2025–26)
- Filters Duke alumni by checking CommonPlayerInfo SCHOOL (cached locally)

Outputs each run:
1) Prints a markdown table (goes to console if manual; goes to log if scheduled with redirection)
2) Writes CSV + HTML files you can open:
     duke_boxscore_YYYY-MM-DD.csv
     duke_boxscore_YYYY-MM-DD.html
3) Sends email (plain text + HTML table) if SMTP env vars set; otherwise prints

Install:
  py -m pip install nba_api pandas requests
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

        # --- (2) Robust team columns ---
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

        # Other common columns may also vary; we’ll pick robustly where helpful
        pid_col = pick_col(players_df, ["PLAYER_ID", "personId", "PERSON_ID"])
        first_col = pick_col(players_df, ["PLAYER_FIRST_NAME", "firstName", "FIRST_NAME"])
        last_col = pick_col(players_df, ["PLAYER_LAST_NAME", "familyName", "LAST_NAME"])
        min_col = pick_col(players_df, ["MIN", "minutes", "MINUTES"])
        pts_col = pick_col(players_df, ["PTS", "points"])
        reb_col = pick_col(players_df, ["REB", "reboundsTotal", "reb"])
        ast_col = pick_col(players_df, ["AST", "assists", "ast"])
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

        for _, p in players_df.iterrows():
            pid = int(p[pid_col])

            try:
                if not is_duke_player(pid, cache):
                    continue
            except Exception as e:
                print(f"    ! playerinfo failed for PLAYER_ID={pid}: {repr(e)}")
                continue

            # --- (3) Use robust team columns for team/opponent ---
            team_id = int(p[team_id_col])
            team_abbr = str(p[team_abbr_col])

            opp_team_id = away_team_id if team_id == home_team_id else home_team_id
            opp_abbr = team_abbr_map.get(opp_team_id, "UNK")

            first = str(p[first_col]) if first_col else ""
            last = str(p[last_col]) if last_col else ""
            name = (first + " " + last).strip() or f"PLAYER_{pid}"

            def as_int(col: Optional[str]) -> int:
                if not col:
                    return 0
                try:
                    return int(p[col])
                except Exception:
                    return 0

            def as_str(col: Optional[str]) -> str:
                if not col:
                    return ""
                v = p[col]
                return "" if v is None else str(v)

            fgm = as_int(fgm_col)
            fga = as_int(fga_col)
            fg3m = as_int(fg3m_col)
            fg3a = as_int(fg3a_col)
            ftm = as_int(ftm_col)
            fta = as_int(fta_col)

            rows.append({
                "Player": name,
                "Team": team_abbr,
                "Opponent": opp_abbr,
                "MIN": as_str(min_col),
                "PTS": as_int(pts_col),
                "REB": as_int(reb_col),
                "AST": as_int(ast_col),
                "FG": f"{fgm}-{fga}",
                "3P": f"{fg3m}-{fg3a}",
                "FT": f"{ftm}-{fta}",
                "+/-": as_str(pm_col),
            })

        polite_sleep()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values(["PTS", "Player"], ascending=[False, True])
    return df


# ---------------- OUTPUT (TABLE FILES + EMAIL) ----------------
def write_table_files(stats_df: pd.DataFrame, target_date: date) -> Dict[str, Path]:
    ymd = target_date.strftime("%Y-%m-%d")
    csv_path = BASE_DIR / f"duke_boxscore_{ymd}.csv"
    html_path = BASE_DIR / f"duke_boxscore_{ymd}.html"

    if stats_df.empty:
        csv_path.write_text("No rows\n", encoding="utf-8")
        html_path.write_text("<p>No rows</p>", encoding="utf-8")
    else:
        stats_df.to_csv(csv_path, index=False)
        html_path.write_text(stats_df.to_html(index=False), encoding="utf-8")

    return {"csv": csv_path, "html": html_path}


def format_email_body(stats_df: pd.DataFrame, target_date: date, games_found: int) -> str:
    header_date = target_date.strftime("%A, %B %d, %Y")
    lines = [
        f"Duke in the NBA — {header_date}",
        "-" * 60,
        f"Games found: {games_found}",
        "",
    ]

    if stats_df.empty:
        lines.append("No Duke alumni matched the box scores for this date.")
        return "\n".join(lines)

    for _, r in stats_df.iterrows():
        lines.append(
            f"{r['Player']} ({r['Team']} vs {r['Opponent']}): "
            f"{r['PTS']} PTS, {r['REB']} REB, {r['AST']} AST, {r['MIN']} MIN, +/- {r['+/-']} "
            f"(FG {r['FG']}, 3P {r['3P']}, FT {r['FT']})"
        )

    return "\n".join(lines)


def send_email(subject: str, body: str, html_table: Optional[str] = None) -> None:
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

    if not email_to or not smtp_host or not email_from:
        print("\n--- EMAIL (not sent; missing EMAIL_TO/SMTP_HOST/EMAIL_FROM) ---")
        print("Subject:", subject)
        print(body)
        print("--- END ---\n")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    msg.attach(MIMEText(body, "plain"))
    if html_table:
        html = f"<pre>{body}</pre><hr/>{html_table}"
        msg.attach(MIMEText(html, "html"))

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


def main() -> None:
    target_date = date.today() - timedelta(days=1)

    games_df = get_games(target_date)
    stats_df = get_duke_boxscores(target_date)

    # Print table (visible when manual; captured to log when scheduled)
    print("\n--- DUKE BOX SCORE TABLE ---")
    if stats_df.empty:
        print("(empty)")
    else:
        print(stats_df.to_markdown(index=False))
    print("--- END TABLE ---\n")

    # Write files you can open regardless of scheduler
    paths = write_table_files(stats_df, target_date)
    print(f"Wrote CSV:  {paths['csv']}")
    print(f"Wrote HTML: {paths['html']}")

    subject = f"Duke in the NBA — {target_date.strftime('%Y-%m-%d')}"
    body = format_email_body(stats_df, target_date, games_found=len(games_df))
    html_table = None if stats_df.empty else stats_df.to_html(index=False)

    send_email(subject, body, html_table=html_table)


if __name__ == "__main__":
    main()
