#!/usr/bin/env python3
"""
Daily email of Duke NBA player stats (with opponent) for yesterday's games.

LOCAL mode using nba_api (stats.nba.com):
- ScoreboardV2 for game list
- BoxScoreTraditionalV2 for box scores

Email via SMTP env vars:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, EMAIL_FROM, EMAIL_TO

If EMAIL_TO missing, prints instead of sending.
"""

import os
import time
import random
import traceback
from datetime import date, timedelta
from typing import Dict, List, Any, Optional

import pandas as pd
import requests
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2


# ---------------- HARDCODED DUKE PLAYER IDS (NBA.com PLAYER_ID) ----------------
DUKE_PLAYER_IDS = {
    1627751,  # Grayson Allen
    1628976,  # Marvin Bagley III
    1630162,  # Paolo Banchero
    1629651,  # RJ Barrett
    1628970,  # Wendell Carter Jr.
    203552,   # Seth Curry
    1631132,  # Kyle Filipowski
    1627742,  # Brandon Ingram
    202681,   # Kyrie Irving
    1630552,  # Jalen Johnson
    1629014,  # Tre Jones
    1628969,  # Tyus Jones
    1628384,  # Luke Kennard
    1631108,  # Dereck Lively II
    1631135,  # Jared McCain
    1631111,  # Wendell Moore Jr.
    203486,   # Mason Plumlee
    1628369,  # Jayson Tatum
    1627783,  # Gary Trent Jr.
    1630228,  # Mark Williams
    1629660,  # Zion Williamson
    1631109,  # Dariq Whitehead
}


# ---------------- RETRY HELPER ----------------

def with_retries(fn, *, retries: int = 5, base_sleep: float = 2.0, label: str = "request"):
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                TimeoutError) as e:
            last_err = e
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.7)
            print(f"  ! {label} failed (attempt {attempt}/{retries}): {repr(e)}")
            if attempt < retries:
                print(f"    retrying in {sleep_s:.1f}s...")
                time.sleep(sleep_s)
    # If it's not one of the above exceptions, or we exhausted retries:
    if last_err:
        raise last_err
    raise RuntimeError(f"{label} failed for unknown reasons")


# ---------------- NBA FETCHING ----------------

def get_scoreboard_for_date(target_date: date) -> pd.DataFrame:
    date_str = target_date.strftime("%m/%d/%Y")
    print(f"Fetching NBA scoreboard for {date_str}...")

    def _call():
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=90)
        return sb.game_header.get_data_frame()

    return with_retries(_call, retries=5, base_sleep=2, label="scoreboardv2")


def get_duke_stats_for_date(target_date: date, duke_ids: List[int]) -> pd.DataFrame:
    games_df = get_scoreboard_for_date(target_date)
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

        # gentle pacing to avoid rate limits
        time.sleep(random.uniform(0.4, 1.0))

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values(["player_name"])
    return df


# ---------------- EMAIL ----------------

def format_email_body(stats_df: pd.DataFrame, target_date: date) -> str:
    header_date = target_date.strftime("%A, %B %d, %Y")
    lines = [f"Duke in the NBA — {header_date}", "-" * 56]

    if stats_df.empty:
        lines.append("No Duke alumni recorded box score stats in NBA games on this date.")
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
        smtp_port = 587

    if not email_to or not smtp_host or not email_from:
        print("\n--- EMAIL (not sent; missing EMAIL_TO/SMTP_HOST/EMAIL_FROM) ---")
        print("Subject:", subject)
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
        print("Subject:", subject)
        print(body)
        print("--- END ---\n")


def main() -> None:
    target_date = date.today() - timedelta(days=1)
    duke_ids = list(DUKE_PLAYER_IDS)

    stats_df = get_duke_stats_for_date(target_date, duke_ids)
    subject = f"Duke in the NBA — {target_date.strftime('%Y-%m-%d')}"
    body = format_email_body(stats_df, target_date)
    send_email(subject, body)


if __name__ == "__main__":
    main()
