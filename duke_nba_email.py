#!/usr/bin/env python3
"""
Duke NBA Daily Email (LOCAL, nba_api) — 2025–26 compatible (BoxScoreTraditionalV3)
Table-only output. No local files created.
"""

import os
import time
import random
import traceback
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import requests
from nba_api.stats.endpoints import boxscoretraditionalv3, commonplayerinfo, scoreboardv2

# ---------------- CONFIG ----------------
DUKE_SUBSTRING = "duke"
BASE_DIR = Path(__file__).resolve().parent
SCHOOL_CACHE_PATH = BASE_DIR / "player_school_cache.json"


# ---------------- HELPERS ----------------
def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
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
    except:
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
    school = get_player_school(player_id, cache)
    return DUKE_SUBSTRING in school.strip().lower()


# ---------------- NBA DATA ----------------
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
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, TimeoutError) as e:
            last_err = e
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.6)
            print(f"  ! {label} failed ({attempt}/{retries}): {repr(e)}")
            if attempt < retries:
                time.sleep(sleep_s)
    if last_err:
        raise last_err
    raise RuntimeError(f"{label} failed (unknown error)")


def polite_sleep():
    time.sleep(random.uniform(0.25, 0.75))


def get_games(target_date: date) -> pd.DataFrame:
    date_str = target_date.strftime("%m/%d/%Y")

    def _call():
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=90)
        return sb.game_header.get_data_frame()

    return with_retries(_call, retries=5, base_sleep=2, label="scoreboardv2")


def get_duke_boxscores(target_date: date) -> pd.DataFrame:
    games_df = get_games(target_date)
    if games_df.empty:
        return pd.DataFrame()

    cache = load_school_cache()
    rows = []

    for _, game in games_df.iterrows():
        game_id = game["GAME_ID"]
        home_team_id = int(game["HOME_TEAM_ID"])
        away_team_id = int(game["VISITOR_TEAM_ID"])

        def _box():
            bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=90)
            return bs.player_stats.get_data_frame()

        try:
            players_df = with_retries(_box, retries=3, base_sleep=1.5, label=f"boxscore {game_id}")
        except:
            continue

        if players_df.empty:
            continue

        # Robust schema matching for team + stats
        team_id_col = pick_col(players_df, ["TEAM_ID", "teamId"])
        team_abbr_col = pick_col(players_df, ["TEAM_ABBREVIATION", "teamTricode"])
        pts_col = pick_col(players_df, ["PTS", "points"])
        reb_col = pick_col(players_df, ["REB", "reboundsTotal"])
        ast_col = pick_col(players_df, ["AST", "assists"])
        oreb_col = pick_col(players_df, ["OREB", "offReb", "offensiveRebounds"])
        dreb_col = pick_col(players_df, ["DREB", "defReb", "defensiveRebounds"])
        stl_col = pick_col(players_df, ["STL", "steals"])
        blk_col = pick_col(players_df, ["BLK", "blocks"])
        tov_col = pick_col(players_df, ["TOV", "turnovers", "TO"])

        if not team_id_col or not team_abbr_col:
            continue

        # Create opponent lookup map
        team_abbr_map = (
            players_df[[team_id_col, team_abbr_col]]
            .dropna()
            .drop_duplicates()
            .set_index(team_id_col)[team_abbr_col]
            .to_dict()
        )

        for _, p in players_df.iterrows():
            pid = int(p["PLAYER_ID"])
            if not is_duke_player(pid, cache):
                continue

            team_id = int(p[team_id_col])
            team_abbr = str(p[team_abbr_col])
            opponent_id = away_team_id if team_id == home_team_id else home_team_id
            opponent_abbr = team_abbr_map.get(opponent_id, "UNK")

            rows.append({
                "Player": f"{p['PLAYER_FIRST_NAME']} {p['PLAYER_LAST_NAME']}",
                "Team": team_abbr,
                "Opponent": opponent_abbr,
                "MIN": p.get(min_col, ""),
                "PTS": int(p.get(pts_col, 0)),
                "OREB": int(p.get(oreb_col, 0)),
                "DREB": int(p.get(dreb_col, 0)),
                "REB": int(p.get(reb_col, 0)),
                "AST": int(p.get(ast_col, 0)),
                "STL": int(p.get(stl_col, 0)),
                "BLK": int(p.get(blk_col, 0)),
                "TOV": int(p.get(tov_col, 0)),
                "FG": f"{p.get('FGM',0)}-{p.get('FGA',0)}",
                "3P": f"{p.get('FG3M',0)}-{p.get('FG3A',0)}",
                "FT": f"{p.get('FTM',0)}-{p.get('FTA',0)}",
                "+/-": p.get(pm_col, ""),
            })

        polite_sleep()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Nice column order (requested)
    desired = ["Player", "Team", "Opponent", "MIN", "PTS", "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "FG", "3P", "FT", "+/-"]
    df = df[[c for c in desired if c in df.columns]]

    if "PTS" in df.columns:
        df = df.sort_values(["PTS", "Player"], ascending=[False, True])

    return df


# ---------------- EMAIL TABLE ONLY ----------------
def send_email_table_only(subject: str, stats_df: pd.DataFrame) -> None:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = os.getenv("EMAIL_TO")

    if not email_to or not smtp_host or not email_from:
        print(stats_df.to_markdown(index=False))
        return

    html = stats_df.to_html(index=False, border=0)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, [email_to], msg.as_string())


# ---------------- MAIN ----------------
def main():
    d = date.today() - timedelta(days=1)
    stats_df = get_duke_boxscores(d)
    subject = f"Duke in the NBA — {d.strftime('%Y-%m-%d')}"
    send_email_table_only(subject, stats_df)

if __name__ == "__main__":
    main()
