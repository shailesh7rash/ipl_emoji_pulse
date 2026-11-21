# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## match_progress_json_stream_generator - Databricks_notebook
# MAGIC ### Source 2 JSON Producer: Stateful Ball-by-Ball JSON Generator (IPL 2026 Final: MI vs CSK)
# MAGIC ### **Venue:** Wankhede Stadium, Mumbai
# MAGIC ### **Cadence:** 1 JSON file dropped every 15 seconds (4 balls/min)
# MAGIC ### **Fault Tolerance:** Resumes seamlessly from the exact interrupted ball without duplicates

# COMMAND ----------

# DBTITLE 1,CONFIGURATION & UNITY CATALOG PATHS
import json
import os
import random
import time
from datetime import datetime, timezone

# ==========================================
# 1. CONFIGURATION & UNITY CATALOG PATHS
# ==========================================
CATALOG = "ipl_reaction"
SCHEMA = "source"
VOLUME_NAME = "match_progress"
LANDING_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/match_progress_json_data"

# Ensure Directory exist

os.makedirs(LANDING_DIR, exist_ok=True)

MATCH_ID = "IPL_2026_FINAL_CSK_MI"
MATCH_DATE = "2026-05-24"
VENUE_NAME = "Wankhede Stadium"
VENUE_CITY = "Mumbai"
DROP_INTERVAL_SEC = 15  # 4 balls per minute

# COMMAND ----------

# DBTITLE 1,SQUAD ROSTERS (Matches dim_player IDs)
# ==========================================
# 2. SQUAD ROSTERS (Matches dim_player IDs)
# ==========================================
MI_BATTERS = [
    "MI_44",  # Ryan Rickelton
    "MI_22",  # Will Jacks
    "MI_19",  # Naman Dhir
    "MI_63",  # Suryakumar Yadav
    "MI_09",  # Tilak Varma
    "MI_33",  # Hardik Pandya (C)
    "MI_10",  # Robin Minz
    "MI_18",  # Trent Boult
    "MI_93",  # Jasprit Bumrah
    "MI_70",  # AM Ghazanfar
    "MI_24"   # Krish Bhagat
]
MI_BOWLERS = ["MI_18", "MI_93", "MI_33", "MI_22", "MI_70", "MI_24"]

CSK_BATTERS = [
    "CSK_31", # Ruturaj Gaikwad (C)
    "CSK_07", # Sanju Samson
    "CSK_14", # Urvil Patel
    "CSK_17", # Dewald Brevis
    "CSK_25", # Shivam Dube
    "CSK_12", # Kartik Sharma
    "CSK_88", # Jamie Overton
    "CSK_21", # Ramakrishna Ghosh
    "CSK_15", # Noor Ahmad
    "CSK_11", # Anshul Kamboj
    "CSK_90"  # Mukesh Choudhary
]
CSK_BOWLERS = ["CSK_11", "CSK_90", "CSK_88", "CSK_15", "CSK_21", "CSK_25"]

# COMMAND ----------

# DBTITLE 1,STATE RECOVERY ENGINE (NO DUPLICATES)
# ==========================================
# 3. STATE RECOVERY ENGINE (NO DUPLICATES)
# ==========================================
def load_resume_state(directory: str) -> dict:
    """Scans existing JSON files in the landing folder to resume from exact interrupted point."""
    existing_files = sorted([f for f in os.listdir(directory) if f.startswith("ball_") and f.endswith(".json")])
    
    if not existing_files:
        print("No prior match files found. Starting fresh from Innings 1, Ball 1.1...")
        return {
            "seq_num": 1,
            "innings": 1,
            "over_number": 1,
            "ball_in_over": 1,
            "legal_balls_bowled": 0,
            "cumulative_score": 0,
            "cumulative_wickets": 0,
            "target_runs": 0,
            "striker_idx": 0,
            "non_striker_idx": 1,
            "next_batter_idx": 2,
            "match_completed": False
        }

    # Read the latest dropped file
    latest_file = os.path.join(directory, existing_files[-1])
    with open(latest_file, "r") as f:
        last_event = json.load(f)

    seq_num = int(existing_files[-1].split("_")[1].split(".")[0]) + 1
    innings = last_event["innings"]
    over_num = last_event["over_number"]
    ball_in_over = last_event["ball_in_over"]
    score = last_event["cumulative_score"]
    wickets = last_event["cumulative_wickets"]
    target = last_event["target_runs"]
    balls_rem = last_event["balls_remaining"]
    is_legal = last_event["extra_type"] not in ("WIDE", "NO_BALL")

    # Reconstruct batter indices based on wickets fallen
    striker_idx = 0 if wickets == 0 else min(wickets + 1, 10)
    non_striker_idx = 1 if wickets == 0 else min(wickets, 9)
    next_batter_idx = min(wickets + 2, 10)

    # Check if Innings 2 already finished
    if innings == 2 and (balls_rem == 0 or wickets >= 10 or score >= target):
        return {"match_completed": True}

    # Check if Innings 1 just ended on that ball -> transition to Innings 2
    if innings == 1 and is_legal and balls_rem == 0 or (innings == 1 and wickets >= 10):
        print(f"Resuming at start of Innings 2! Target to chase: {score + 1}")
        return {
            "seq_num": seq_num,
            "innings": 2,
            "over_number": 1,
            "ball_in_over": 1,
            "legal_balls_bowled": 0,
            "cumulative_score": 0,
            "cumulative_wickets": 0,
            "target_runs": score + 1,
            "striker_idx": 0,
            "non_striker_idx": 1,
            "next_batter_idx": 2,
            "match_completed": False
        }

    # Advance ball pointer cleanly
    legal_balls_bowled = 120 - balls_rem
    if is_legal:
        if ball_in_over >= 6:
            over_num += 1
            ball_in_over = 1
        else:
            ball_in_over += 1
    else:
        ball_in_over += 1

    print(f"Resuming from interrupted state -> File #{seq_num:03d} | Innings {innings} | Over {over_num}.{ball_in_over} | Score: {score}/{wickets}")
    return {
        "seq_num": seq_num,
        "innings": innings,
        "over_number": over_num,
        "ball_in_over": ball_in_over,
        "legal_balls_bowled": legal_balls_bowled,
        "cumulative_score": score,
        "cumulative_wickets": wickets,
        "target_runs": target,
        "striker_idx": striker_idx,
        "non_striker_idx": non_striker_idx,
        "next_batter_idx": next_batter_idx,
        "match_completed": False
    }


# COMMAND ----------

# DBTITLE 1,REALISTIC T20 DELIVERY SIMULATOR
# ==========================================
# 4. REALISTIC T20 DELIVERY SIMULATOR
# ==========================================
def get_match_phase(over_num: int) -> str:
    if over_num <= 6:
        return "POWERPLAY"
    elif over_num <= 15:
        return "MIDDLE_OVERS"
    return "DEATH_OVERS"

def simulate_delivery(state: dict) -> tuple[dict, dict]:
    """Generates one ball event with heightened thrill in overs 16-20 of both innings."""
    innings = state["innings"]
    over_num = state["over_number"]
    ball_in_over = state["ball_in_over"]
    phase = get_match_phase(over_num)

    batting_team = "MI" if innings == 1 else "CSK"
    bowling_team = "CSK" if innings == 1 else "MI"
    batters = MI_BATTERS if innings == 1 else CSK_BATTERS
    bowlers = CSK_BOWLERS if innings == 1 else MI_BOWLERS

    striker_id = batters[state["striker_idx"]]
    non_striker_id = batters[state["non_striker_idx"]]
    bowler_id = bowlers[(over_num - 1) % len(bowlers)]

    # Thrill Peak Logic: Overs 16-20 have explosive boundaries & high wicket drama
    if phase == "DEATH_OVERS":
        outcomes = [0, 1, 2, 4, 6, "W", "WD"]
        weights  = [12, 22, 8, 22, 20, 12, 4]  # 42% boundary rate + 12% wicket rate!
    elif phase == "POWERPLAY":
        outcomes = [0, 1, 2, 4, 6, "W", "WD"]
        weights  = [28, 30, 8, 18, 8, 5, 3]
    else:  # MIDDLE_OVERS
        outcomes = [0, 1, 2, 4, 6, "W", "WD"]
        weights  = [30, 42, 10, 9, 4, 4, 1]

    outcome = random.choices(outcomes, weights=weights, k=1)[0]

    runs_off_bat = 0
    extras = 0
    extra_type = "NONE"
    is_wicket = False
    wicket_type = "NONE"
    dismissed_player_id = "NONE"
    is_legal_delivery = True

    if outcome == "WD":
        extras = 1
        extra_type = "WIDE"
        is_legal_delivery = False
    elif outcome == "W":
        is_wicket = True
        wicket_type = random.choice(["CAUGHT", "BOWLED", "LBW", "CAUGHT", "RUN_OUT"])
        dismissed_player_id = striker_id
    else:
        runs_off_bat = int(outcome)

    is_boundary = runs_off_bat in (4, 6)
    ball_speed = round(random.uniform(132.0, 149.5), 1) if bowler_id not in ("CSK_15", "MI_70", "MI_22") else round(random.uniform(84.0, 98.0), 1)

    # Update cumulative state
    state["cumulative_score"] += (runs_off_bat + extras)
    if is_wicket:
        state["cumulative_wickets"] += 1
    if is_legal_delivery:
        state["legal_balls_bowled"] += 1

    balls_remaining = max(0, 120 - state["legal_balls_bowled"])

    event = {
        "match_id": MATCH_ID,
        "match_date": MATCH_DATE,
        "venue_name": VENUE_NAME,
        "venue_city": VENUE_CITY,
        "innings": innings,
        "batting_team": batting_team,
        "bowling_team": bowling_team,
        "match_phase": phase,
        "over_number": over_num,
        "ball_in_over": ball_in_over,
        "ball_id": f"{over_num}.{ball_in_over}",
        "batter_id": striker_id,
        "non_striker_id": non_striker_id,
        "bowler_id": bowler_id,
        "runs_off_bat": runs_off_bat,
        "extras": extras,
        "extra_type": extra_type,
        "is_boundary": is_boundary,
        "is_wicket": is_wicket,
        "wicket_type": wicket_type,
        "dismissed_player_id": dismissed_player_id,
        "ball_speed_kph": ball_speed,
        "cumulative_score": state["cumulative_score"],
        "cumulative_wickets": state["cumulative_wickets"],
        "target_runs": state["target_runs"],
        "balls_remaining": balls_remaining,
        "ball_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    }

        # Rotate strike / bring in new batter
    if is_wicket and state["cumulative_wickets"] < 10:
        state["striker_idx"] = state["next_batter_idx"]
        state["next_batter_idx"] = min(state["next_batter_idx"] + 1, 10)
    elif runs_off_bat % 2 == 1:
        state["striker_idx"], state["non_striker_idx"] = state["non_striker_idx"], state["striker_idx"]

    # Advance over / innings pointers for NEXT iteration
    if is_legal_delivery:
        if ball_in_over >= 6:
            state["over_number"] += 1
            state["ball_in_over"] = 1
            # End of over strike swap
            state["striker_idx"], state["non_striker_idx"] = state["non_striker_idx"], state["striker_idx"]
        else:
            state["ball_in_over"] += 1
    else:
        state["ball_in_over"] += 1

    # Check Innings 1 completion -> Switch to Innings 2
    if innings == 1 and (balls_remaining == 0 or state["cumulative_wickets"] >= 10):
        print(f"\n--- INNINGS 1 COMPLETE! MI posted {state['cumulative_score']}/{state['cumulative_wickets']} ---")
        state["innings"] = 2
        state["over_number"] = 1
        state["ball_in_over"] = 1
        state["legal_balls_bowled"] = 0
        state["target_runs"] = state["cumulative_score"] + 1
        state["cumulative_score"] = 0
        state["cumulative_wickets"] = 0
        state["striker_idx"] = 0
        state["non_striker_idx"] = 1
        state["next_batter_idx"] = 2

    # Check Innings 2 completion -> Match Over
    elif innings == 2 and (balls_remaining == 0 or state["cumulative_wickets"] >= 10 or state["cumulative_score"] >= state["target_runs"]):
        state["match_completed"] = True

    return event, state


# COMMAND ----------

# DBTITLE 1,5. MAIN STREAMING LOOP (15s INTERVAL)
# ==========================================
# 5. MAIN STREAMING LOOP (15s INTERVAL)
# ==========================================
state = load_resume_state(LANDING_DIR)

if state.get("match_completed"):
    print("Match is already completed! Delete files in landing_zone if you want a fresh replay.")
else:
    try:
        while not state["match_completed"]:
            event, state = simulate_delivery(state)
            
            # Atomic write: write to .tmp then rename to .json so Auto Loader reads clean files
            file_name = f"ball_{state['seq_num']:03d}_inn{event['innings']}_ov{event['over_number']}_b{event['ball_in_over']}.json"
            tmp_path = os.path.join(LANDING_DIR, f".{file_name}.tmp")
            final_path = os.path.join(LANDING_DIR, file_name)
            
            with open(tmp_path, "w") as f:
                json.dump(event, f, indent=2)
            os.replace(tmp_path, final_path)

            highlight = "SIX!" if event["runs_off_bat"] == 6 else ("FOUR!" if event["runs_off_bat"] == 4 else ("WICKET!" if event["is_wicket"] else f"{event['runs_off_bat'] + event['extras']} run(s)"))
            print(f"[{event['ball_timestamp']}] Dropped {file_name} -> Inn {event['innings']} | {event['ball_id']} | {highlight} | Score: {event['cumulative_score']}/{event['cumulative_wickets']}")

            state["seq_num"] += 1
            if not state["match_completed"]:
                time.sleep(DROP_INTERVAL_SEC)

        print("\n🏆 IPL 2026 FINAL COMPLETED! All overs finished.")
    except KeyboardInterrupt:
        print("\n⏸️ Interrupted by user! State safely preserved on disk. Re-run this cell anytime to resume from the exact next ball.")