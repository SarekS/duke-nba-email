#!/usr/bin/env python3
"""
Duke NBA Daily Email (LOCAL, nba_api) — using BoxScoreTraditionalV3
"""

import os
import time
import random
import traceback
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import requests
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv3, commonplayerinfo

DUKE_SUBSTRING = "duke"
PLAYER_SCHOOL_CACHE = Path(__file__).with_name("player_school_cache.json")

def with_retries(fn: Callable[[], Any], *, retries: int = 5, base_sleep: float = 2.0, label: str = "request") -> Any:
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

def load_school_cache() -> Dict[str, str]:
    if not PLAYER_SCHOOL_CACHE.exists():
        return {}
    try:
        return json.loads(PLAYER_SCHOOL_CACHE.read_text(encoding="utf-8"))
    except:
        return {}

def save_school_cache(cache: Dict[str, str]):
    PLAYER_SCHOOL_CACHE.write_text(json.dumps(cache, indent=2), encoding="utf-8")

def get_player_school(player_id: int, cache: Dict[str, str]) -> str:
    key = str(player_id)
    if key in cache:
        return cache[key]
    def _call():
        return commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=60).get_data_frames()[0]
    df = with_retries(_call, retries=4, base_sleep=2, label=f"playerinfo {player_id}")
    school = ""
    if not df.empty:
        for col in ["SCHOOL", "COLLEGE"]:
            if col in df.columns:
                school = str(df.loc[0, col] or "")
                break
    cache[key] = school
    save_school_cache(cache)
    polite_sleep()
    return school

def is_duke_player(player_id: int, cache: Dict[str, str]) -> bool:
    school = get_player_school(player_id, cache)
    return DUKE_SUBSTRING in school.strip().lower()

def get_duke_boxscores_for_date(target_date: date) -> pd.DataFrame:
    games_df = scoreboardv2.ScoreboardV2(game_date=target_date.strftime("%m/%d/%Y"), timeout=60).game_header.get_data_frame()
    if games_df.empty:
        return pd.DataFrame()
    cache = load_school_cache()
    rows = []
    for _, game in games_df.iterrows():
        game_id = game["GAME_ID"]
        def _box():
            return boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=60).player_stats.get_data_frame()
        try:
            df = with_retries(_box, retries=3, base_sleep=1.5, label=f"boxscore {game_id}")
        except:
            continue
        if df.empty:
            continue
        for _, p in df.iterrows():
            pid = int(p["PLAYER_ID"])
            if not is_duke_player(pid, cache):
                continue
            team = p["TEAM_ABBREVIATION"]
            opp = df.loc[df["TEAM_ID"] != p["TEAM_ID"], "TEAM_ABBREVIATION"].dropna().unique()
            opponent = opp[0] if len(opp) else "UNK"
            rows.append({
                "Player": f"{p['PLAYER_FIRST_NAME']} {p['PLAYER_LAST_NAME']}",
                "Team": team,
                "Opponent": opponent,
                "MIN": p["MIN"],
                "PTS": p["PTS"],
                "REB": p["REB"],
                "AST": p["AST"],
                "FG": f"{p['FGM']}-{p['FGA']}",
                "3P": f"{p['FG3M']}-{p['FG3A']}",
                "FT": f"{p['FTM']}-{p['FTA']}",
                "+/-": p["PLUS_MINUS"],
            })
        polite_sleep()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

def format_email_body(stats_df: pd.DataFrame, target_date: date) -> str:
    lines = [f"Duke in the NBA — {target_date}", "-"*40]
    if stats_df.empty:
        lines.append("No Duke alumni matched box scores this date.")
        return "\n".join(lines)
    for _, r in stats_df.iterrows():
        lines.append(f"{r['Player']} ({r['Team']} vs {r['Opponent']}): {r['PTS']} PTS, {r['REB']} REB, {r['AST']} AST, {r['MIN']} MIN, +/- {r['+/-']} (FG {r['FG']}, 3P {r['3P']}, FT {r['FT']})")
    return "\n".join(lines)

def send_email(subject: str, body: str):
    import smtplib
    from email.mime.text import MIMEText
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    pw = os.getenv("SMTP_PASSWORD")
    fr = os.getenv("EMAIL_FROM")
    to = os.getenv("EMAIL_TO")
    if not to or not host or not fr:
        print(body); return
    msg = MIMEText(body); msg["Subject"]=subject; msg["From"]=fr; msg["To"]=to
    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pw)
        s.sendmail(fr, [to], msg.as_string())

def main():
    d = date.today() - timedelta(days=1)
    stats_df = get_duke_boxscores_for_date(d)
    print("\n--- DUKE BOX SCORE TABLE ---")
    print(stats_df.to_markdown(index=False))
    print("---------------------------\n")
    body = format_email_body(stats_df, d)
    send_email(f"Duke in the NBA — {d}", body)

if __name__ == "__main__":
    main()
