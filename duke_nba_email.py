#!/usr/bin/env python3
"""
Daily email of Duke NBA player stats (with opponent) for yesterday's games.

Data source: NBA CDN JSON
- Scoreboard (game list): https://data.nba.net/data/10s/prod/v1/YYYYMMDD/scoreboard.json
- Boxscore per game:      https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gameId}.json

Email via SMTP using env vars:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, EMAIL_FROM, EMAIL_TO
If EMAIL_TO is missing, prints email instead of sending.
"""

import os
import time
import random
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests


# ---------------- HARDCODED DUKE PLAYER IDS (NBA.com personId) ----------------
DUKE_PLAYER_IDS = {
    1627751,  # Grayson Allen
    1628976,  # Marvin Bagley III
    1630162,  # Paolo Banchero
    1629651,  # RJ Barrett
    1628970,  # Wendell Carter Jr.
    203552,   # Seth Curry
    1631132,  # Kyle Filipowski
    1642843,  # Cooper Flagg (verify if needed)
    1627742,  # Brandon Ingram
    202681,   # Kyrie Irving
    1642883,  # Sion James (verify if needed)
    1630552,  # Jalen Johnson
    1629014,  # Tre Jones
    1628969,  # Tyus Jones
    1628384,  # Luke Kennard
    1642851,  # Kon Knueppel (verify if needed)
    1631108,  # Dereck Lively II
    1642863,  # Khaman Maluach (verify if needed)
    1631135,  # Jared McCain
    1631111,  # Wendell Moore Jr.
    203486,   # Mason Plumlee
    1642878,  # Tyrese Proctor (verify if needed)
    1628369,  # Jayson Tatum
    1627783,  # Gary Trent Jr.
    1630228,  # Mark Williams
    1629660,  # Zion Williamson
    1631109,  # Dariq Whitehead
}


# ---------------- HTTP HELPERS ----------------

def get_with_retries(url: str, *, timeout: int = 25, retries: int = 5) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                url,
                timeout=timeout,
                headers={
                    # A normal UA helps some CDNs behave more consistently
                    "User-Agent": "Mozilla/5.0 (compatible; duke-nba-email/1.0)",
                    "Accept": "application/json,text/plain,*/*",
                },
            )
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0, 0.7)
            if attempt < retries:
                print(f"  ! GET failed ({attempt}/{retries}) {url} -> {repr(e)}; retry in {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                print(f"  ! GET failed ({attempt}/{retries}) {url} -> {repr(e)}; giving up")
    raise last_err  # type: ignore


def get_game_ids_for_date(d: date) -> List[str]:
    ymd = d.strftime("%Y%m%d")
    url = f"https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_{ymd}.json"
    print(f"Fetching scoreboard: {url}")
    data = get_with_retries(url).json()
    games = data.get("scoreboard", {}).get("games", [])
    return [g.get("gameId") for g in games if g.get("gameId")]


def get_boxscore_json(game_id: str) -> Dict[str, Any]:
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    return get_with_retries(url).json()


# ---------------- DATA EXTRACTION ----------------

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def collect_duke_lines_for_date(d: date) -> List[Dict[str, Any]]:
    game_ids = get_game_ids_for_date(d)
    if not game_ids:
        return []

    results: List[Dict[str, Any]] = []
    for gid in game_ids:
        print(f"  Processing game {gid}...")
        try:
            bs = get_boxscore_json(gid)
            game = bs.get("game", {})
            home = game.get("homeTeam", {})
            away = game.get("awayTeam", {})
            home_abbr = home.get("teamTricode", "HOME")
            away_abbr = away.get("teamTricode", "AWAY")

            for team_obj, opp_abbr in ((home, away_abbr), (away, home_abbr)):
                team_abbr = team_obj.get("teamTricode", "UNK")
                for p in team_obj.get("players", []):
                    pid = _safe_int(p.get("personId"))
                    if pid not in DUKE_PLAYER_IDS:
                        continue

                    stats = p.get("statistics", {}) or {}
                    results.append({
                        "player_id": pid,
                        "player_name": p.get("name", "Unknown"),
                        "team": team_abbr,
                        "opponent": opp_abbr,
                        "minutes": stats.get("minutes", ""),
                        "points": _safe_int(stats.get("points")),
                        "rebounds": _safe_int(stats.get("reboundsTotal")),
                        "assists": _safe_int(stats.get("assists")),
                        "fgm": _safe_int(stats.get("fieldGoalsMade")),
                        "fga": _safe_int(stats.get("fieldGoalsAttempted")),
                        "fg3m": _safe_int(stats.get("threePointersMade")),
                        "fg3a": _safe_int(stats.get("threePointersAttempted")),
                        "ftm": _safe_int(stats.get("freeThrowsMade")),
                        "fta": _safe_int(stats.get("freeThrowsAttempted")),
                        "plus_minus": _safe_int(stats.get("plusMinusPoints")),
                    })
        except Exception as e:
            print(f"    ! boxscore error for {gid}: {repr(e)}")
            continue

    # Sort by points desc, then name
    results.sort(key=lambda r: (-r["points"], r["player_name"]))
    return results


# ---------------- EMAIL ----------------

def build_email_body(lines: List[Dict[str, Any]], d: date) -> str:
    header = f"Duke in the NBA — {d.strftime('%A, %B %d, %Y')}\n" + ("-" * 48)
    if not lines:
        return header + "\nNo Duke alumni recorded box score stats in NBA games on this date."

    out = [header, ""]
    for r in lines:
        out.append(
            f"{r['player_name']} ({r['team']} vs {r['opponent']}): "
            f"{r['points']} PTS, {r['rebounds']} REB, {r['assists']} AST, "
            f"{r['minutes']} MIN, +/- {r['plus_minus']} "
            f"(FG {r['fgm']}-{r['fga']}, 3P {r['fg3m']}-{r['fg3a']}, FT {r['ftm']}-{r['fta']})"
        )
    return "\n".join(out)


def send_email(subject: str, body: str) -> None:
    import smtplib
    from email.mime.text import MIMEText

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = os.getenv("EMAIL_TO")

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

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.sendmail(email_from, [email_to], msg.as_string())

    print(f"Email sent to {email_to}.")


def main() -> None:
    target_date = date.today() - timedelta(days=1)
    try:
        lines = collect_duke_lines_for_date(target_date)
        subject = f"Duke in the NBA — {target_date.strftime('%Y-%m-%d')}"
        body = build_email_body(lines, target_date)
        send_email(subject, body)
    except Exception as e:
        # Fail gracefully: send a “data unavailable” email instead of crashing the workflow
        subject = f"Duke in the NBA — {target_date.strftime('%Y-%m-%d')} (Data unavailable)"
        body = (
            f"Duke in the NBA — {target_date.strftime('%A, %B %d, %Y')}\n"
            + "-" * 48
            + "\nNBA data fetch failed during this run.\n"
            f"Error: {repr(e)}\n"
        )
        send_email(subject, body)


if __name__ == "__main__":
    main()
