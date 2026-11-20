# match_data_config.py
# Reference data for realistic IPL 2026 Final (MI vs CSK) fan reaction telemetry

# Valid Emoji IDs matching PostgreSQL `dim_emoji` table
EMOJI_CATALOG = [
    {"emoji_id": "EMJ_01", "symbol": "🔥", "name": "Fire",       "weight": 0.24},
    {"emoji_id": "EMJ_02", "symbol": "🎉", "name": "Celebrate",  "weight": 0.18},
    {"emoji_id": "EMJ_03", "symbol": "👏", "name": "Clap",       "weight": 0.15},
    {"emoji_id": "EMJ_04", "symbol": "😱", "name": "Shock",      "weight": 0.12},
    {"emoji_id": "EMJ_05", "symbol": "😂", "name": "Laugh",      "weight": 0.06},
    {"emoji_id": "EMJ_06", "symbol": "💔", "name": "Heartbreak", "weight": 0.10},
    {"emoji_id": "EMJ_07", "symbol": "😡", "name": "Angry",      "weight": 0.07},
    {"emoji_id": "EMJ_08", "symbol": "🏏", "name": "BatSwing",   "weight": 0.08},
]

# Realistic Indian Cities, Actual States & Regional Franchise Fan Bias
INDIAN_FAN_HUBS = [
    {"city": "Mumbai",      "state": "Maharashtra",
        "team_weights": {"MI": 0.72, "CSK": 0.18, "NEUTRAL": 0.10}},
    {"city": "Pune",        "state": "Maharashtra",
        "team_weights": {"MI": 0.60, "CSK": 0.25, "NEUTRAL": 0.15}},
    {"city": "Nagpur",      "state": "Maharashtra",
        "team_weights": {"MI": 0.55, "CSK": 0.25, "NEUTRAL": 0.20}},
    {"city": "Chennai",     "state": "Tamil Nadu",
        "team_weights": {"MI": 0.12, "CSK": 0.80, "NEUTRAL": 0.08}},
    {"city": "Coimbatore",  "state": "Tamil Nadu",
        "team_weights": {"MI": 0.15, "CSK": 0.75, "NEUTRAL": 0.10}},
    {"city": "Bengaluru",   "state": "Karnataka",
        "team_weights": {"MI": 0.30, "CSK": 0.45, "NEUTRAL": 0.25}},
    {"city": "Hyderabad",   "state": "Telangana",
        "team_weights": {"MI": 0.35, "CSK": 0.40, "NEUTRAL": 0.25}},
    {"city": "Kochi",       "state": "Kerala",         "team_weights": {
        "MI": 0.25, "CSK": 0.60, "NEUTRAL": 0.15}},
    {"city": "New Delhi",   "state": "Delhi",          "team_weights": {
        "MI": 0.40, "CSK": 0.35, "NEUTRAL": 0.25}},
    {"city": "Ahmedabad",   "state": "Gujarat",
        "team_weights": {"MI": 0.50, "CSK": 0.30, "NEUTRAL": 0.20}},
    {"city": "Jaipur",      "state": "Rajasthan",
        "team_weights": {"MI": 0.35, "CSK": 0.40, "NEUTRAL": 0.25}},
    {"city": "Kolkata",     "state": "West Bengal",
        "team_weights": {"MI": 0.38, "CSK": 0.37, "NEUTRAL": 0.25}},
    {"city": "Lucknow",     "state": "Uttar Pradesh",
        "team_weights": {"MI": 0.35, "CSK": 0.40, "NEUTRAL": 0.25}},
    {"city": "Indore",      "state": "Madhya Pradesh",
        "team_weights": {"MI": 0.45, "CSK": 0.35, "NEUTRAL": 0.20}},
    {"city": "Chandigarh",  "state": "Punjab",         "team_weights": {
        "MI": 0.40, "CSK": 0.35, "NEUTRAL": 0.25}},
]

# City traffic share weights (higher viewer volume from metro hubs)
HUB_TRAFFIC_WEIGHTS = [
    0.18,  # Mumbai
    0.10,  # Pune
    0.04,  # Nagpur
    0.16,  # Chennai
    0.04,  # Coimbatore
    0.11,  # Bengaluru
    0.08,  # Hyderabad
    0.04,  # Kochi
    0.08,  # New Delhi
    0.05,  # Ahmedabad
    0.03,  # Jaipur
    0.04,  # Kolkata
    0.02,  # Lucknow
    0.02,  # Indore
    0.01,  # Chandigarh
]

DEVICE_TYPES = ["MOBILE_ANDROID", "MOBILE_IOS", "SMART_TV", "WEB_BROWSER"]
DEVICE_WEIGHTS = [0.62, 0.22, 0.11, 0.05]

STREAM_QUALITIES = ["4K_HDR", "1080P_HD", "720P", "AUTO_DATA_SAVER"]
QUALITY_WEIGHTS = [0.20, 0.45, 0.20, 0.15]
