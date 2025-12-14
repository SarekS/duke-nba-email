#!/usr/bin/env python3
"""
Daily email of Duke NBA player stats (with opponent) for yesterday's games.

- Discovers all active NBA players who went to Duke (via nba_api, cached locally)
- Gets yesterday's NBA games and box scores
- Filters to Duke players and builds an email-style summary
- Optionally sends via SMTP (or just prints the email body)

Setup:
    pip install nba_api pandas

Email config via environment variables (recommended):
    SMTP_HOST
    SMTP_PORT         (e.g. 587)
    SMTP_USER
    SMTP_PASSWORD
    EMAIL_FROM
    EMAIL_TO

If EMAIL_TO is not set, the script will just print the email body.
"""

import os
import json
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict

import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import (
    commonplayerinfo,
    scoreboardv2,
    boxscoretraditionalv2,
)
import smtplib
from email.mime.text import MIMEText


# ---------- CONFIG ----------

CACHE_FILE = Path(__file__).with_name("duke_players_cache.json")
DUKE_SUBSTRING = "duke"  # case-insensitive match on college field
DUKE_CACHE_MAX_AGE_DAYS = 30  # how long to trust the cached Duke player list


# ---------- HARDCODED DUKE NBA PLAYERS ----------
# Player IDs from NBA.com (stable)
DUKE_PLAYER_IDS = {
    # Superstars / starters
    1627751, # Grayson Allen
    1628976, # Marvin Bagley III
    1630162, # Paolo Banchero
    1629651, # RJ Barrett
    1628970, # Wendell Carter Jr.
    203552,  # Seth Curry
    1631132, # Kyle Filipowski
    1642843, # Cooper Flagg
    1627742, # Brandon Ingram
    202681,  # Kyrie Irving
    1642883, # Sion James
    1630552, # Jalen Johnson
    1629014, # Tre Jones
    1628969, # Tyus Jones
    1628384, # Luke Kennard
    1642851, # Kon Knueppel
    1631108, # Dereck Lively II
    1642863, # Khalman Maluach
    1631135, # Jared McCain
    1631111, # Wendell Moore Jr
    203486, # Mason Plumlee
    1642878, # Tyrese Proctor
    1628369, # Jayson Tatum
    1627783, # Gary Trent Jr.
    1630228, # Mark Williams
    1629660, # Zion Williamson
    1631109, # Dariq Whitehead
}

# ---------- GAME + BOX SCORE FETCHING ----------

def get_scoreboard_for_date(target_date: date) -> pd.DataFrame:
    """
    Get ScoreboardV2 game header DataFrame for a given date.
    """
    date_str = target_date.strftime("%m/%d/%Y")
    print(f"Fetching NBA scoreboard for {date_str}...")
    sb = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=60)
    games_df = sb.game_header.get_data_frame()
    return games_df


def get_duke_stats_for_date(target_date: date, duke_player_ids: List[int]) -> pd.DataFrame:
    """
    For all games on target_date, return a DataFrame of box score stats
    for players in duke_player_ids, including their opponent team abbrev.
    """
    games_df = get_scoreboard_for_date(target_date)
    if games_df.empty:
        return pd.DataFrame()

    rows: List[Dict] = []
    date_str = target_date.strftime("%Y-%m-%d")

    for _, game in games_df.iterrows():
        game_id = game["GAME_ID"]
        home_team_id = game["HOME_TEAM_ID"]
        visitor_team_id = game["VISITOR_TEAM_ID"]

        print(f"  Processing game {game_id}...")

        try:
            bs = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=60)
            players_df = bs.player_stats.get_data_frame()
        except Exception as e:
            print(f"    ! Error fetching box score for {game_id}: {e}")
            continue

        if players_df.empty:
            continue

        # For opponent mapping, we can use TEAM_ID from this DF
        for _, p in players_df.iterrows():
            pid = int(p["PLAYER_ID"])
            if pid not in duke_player_ids:
                continue

            team_id = int(p["TEAM_ID"])
            player_team_abbr = p["TEAM_ABBREVIATION"]

            # Determine opponent team ID
            if team_id == home_team_id:
                opp_team_id = visitor_team_id
            else:
                opp_team_id = home_team_id

            # Get opponent abbreviation from same DF
            opp_abbr = (
                players_df.loc[players_df["TEAM_ID"] == opp_team_id, "TEAM_ABBREVIATION"]
                .dropna()
                .unique()
            )
            opponent_abbr = opp_abbr[0] if len(opp_abbr) > 0 else "UNK"

            row = {
                "date": date_str,
                "game_id": game_id,
                "player_id": pid,
                "player_name": f"{p['PLAYER_FIRST_NAME']} {p['PLAYER_LAST_NAME']}",
                "team": player_team_abbr,
                "opponent": opponent_abbr,
                "minutes": p["MIN"],
                "points": p["PTS"],
                "rebounds": p["REB"],
                "assists": p["AST"],
                "fgm": p["FGM"],
                "fga": p["FGA"],
                "fg3m": p["FG3M"],
                "fg3a": p["FG3A"],
                "ftm": p["FTM"],
                "fta": p["FTA"],
                "plus_minus": p["PLUS_MINUS"],
            }
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # sort nicely: by player, then minutes descending
    df = df.sort_values(["player_name", "minutes"], ascending=[True, False])
    return df


# ---------- EMAIL FORMATTING & SENDING ----------

def format_email_body(stats_df: pd.DataFrame, target_date: date) -> str:
    """
    Build a human-readable email body summarizing Duke players' stats.
    """
    header_date = target_date.strftime("%A, %B %d, %Y")
    lines = [f"Duke in the NBA — {header_date}", "-" * 40]

    if stats_df.empty:
        lines.append("No Duke players recorded stats in NBA games on this date.")
        return "\n".join(lines)

    for _, row in stats_df.iterrows():
        name = row["player_name"]
        team = row["team"]
        opp = row["opponent"]
        mins = row["minutes"]
        pts = int(row["points"])
        reb = int(row["rebounds"])
        ast = int(row["assists"])
        plus_minus = row["plus_minus"]

        stat_line = (
            f"{name} ({team} vs {opp}): "
            f"{pts} PTS, {reb} REB, {ast} AST, {mins} MIN, +/- {plus_minus}"
        )
        lines.append(stat_line)

    return "\n".join(lines)


def send_email(subject: str, body: str) -> None:
    """
    Send the email using SMTP settings from environment variables.
    If EMAIL_TO is missing, just print the body.
    """
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = os.getenv("EMAIL_TO")

    if not email_to or not smtp_host or not email_from:
        # Fallback: just print
        print("\n--- EMAIL (not sent, config missing) ---")
        print(f"Subject: {subject}")
        print()
        print(body)
        print("--- END ---\n")
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.sendmail(email_from, [email_to], msg.as_string())

    print(f"Email sent to {email_to}.")


# ---------- MAIN ENTRYPOINT ----------

def main():
    # Use yesterday's games (common for a morning email)
    target_date = date.today() - timedelta(days=1)

    duke_ids = get_duke_player_ids()
    if not duke_ids:
        print("No Duke player IDs found; aborting.")
        return

    stats_df = get_duke_stats_for_date(target_date, duke_ids)
    body = format_email_body(stats_df, target_date)
    subject = f"Duke in the NBA — {target_date.strftime('%Y-%m-%d')}"
    send_email(subject, body)


if __name__ == "__main__":
    main()
