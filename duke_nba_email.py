#!/usr/bin/env python3
"""
Duke NBA Daily Email (LOCAL, nba_api) — 2025–26 compatible

- ScoreboardV2 for games
- BoxScoreTraditionalV3 for box scores (V2 is deprecated/no data in 2025–26)
- Filters Duke alumni by checking CommonPlayerInfo SCHOOL (cached locally)
- Outputs:
    1) prints a markdown table (goes to log when scheduled)
    2) writes CSV + HTML table files for easy viewing
    3) sends email (or prints if SMTP env vars missing)
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

        # map TEAM_ID -> TEAM_ABBREVIATION in this game
        team_abbr_map = (
            players_df[["TEAM_ID", "TEAM_ABBREVIATION"]]
            .dropna()
            .drop_duplicates()
            .set_index("TEAM_ID")["TEAM_ABBREVIATION"]
            .to_dict()
        )

        for _, p in players_df.iterrows():
            pid = int(p["PLAYER_ID"])
            try:
                if not is_duke_player(pid, cache):
                    continue
            except Exception as e:
                print(f"    ! playerinfo failed for PLAYER_ID={pid}: {repr(e)}")
                continue

            team_id = int(p["TEAM_ID"])
            team_abbr = str(p["TEAM_ABBREVIATION"])

            opp_team_id = away_team_id if team_id == home_team_id else home_team_id
            opp_abbr = team_abbr_map.get(opp_team_id, "UNK")

            rows.append({
                "Player": f"{p['PLAYER_FIRST_NAME']} {p['PLAYER_LAST_NAME']}",
                "Team": team_abbr,
                "Opponent": opp_abbr,
                "MIN": p.get("MIN", ""),
                "PTS": int(p.get("PTS", 0)),
                "REB": int(p.get("REB", 0)),
                "AST": int(p.get("AST", 0)),
                "FG": f"{int(p.get('FGM', 0))}-{int(p.get('FGA', 0))}",
                "3P": f"{int(p.get('FG3M', 0))}-{int(p.get('FG3A', 0))}",
                "FT": f"{int(p.get('FTM', 0))}-{int(p.get('FTA', 0))}",
                "+/-": p.get("PLUS_MINUS", ""),
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
        # still write empty markers so you know it ran
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

    # Fetch games once so we can report the count in email even if boxscores empty
    games_df = get_games(target_date)
    stats_df = get_duke_boxscores(target_date)

    # Always print a table to stdout (goes to log when scheduled)
    print("\n--- DUKE BOX SCORE TABLE ---")
    if stats_df.empty:
        print("(empty)")
    else:
        print(stats_df.to_markdown(index=False))
    print("--- END TABLE ---\n")

    # Always write files so you can open them
    paths = write_table_files(stats_df, target_date)
    print(f"Wrote CSV:  {paths['csv']}")
    print(f"Wrote HTML: {paths['html']}")

    body = format_email_body(stats_df, target_date, games_found=len(games_df))
    subject = f"Duke in the NBA — {target_date.strftime('%Y-%m-%d')}"

    html_table = None if stats_df.empty else stats_df.to_html(index=False)
    send_email(subject, body, html_table=html_table)


if __name__ == "__main__":
    main()
