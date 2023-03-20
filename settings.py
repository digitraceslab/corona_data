# Settings
# ========

raw_data_folder = "../raw_data/"

# Set columns to keep from activity data, exercise data and
# sleep data
activity_columns = ["subject_id", "date", "calories", "active-calories", "duration", "active-steps"]
exercise_columns = ["subject_id", "start-time", "calories", "distance", "duration", "training-load", "max-heart-rate", "average-heart-rate", "training-load", "sport", "detailed-sport-info", "fat-percentage", "carbohydrate-percentage", "protein-percentage"]
sleep_columns = ["subject_id", "date", "sleep_start_time", "sleep_end_time", "continuity", "light_sleep", "deep_sleep", "rem_sleep", "unrecognized_sleep_stage", 'total_interruption_duration']
recharge_columns = ["subject_id", 'date', 'heart_rate_avg', 'beat_to_beat_avg', 'heart_rate_variability_avg', 'breathing_rate_avg', 'nightly_recharge_status', 'ans_charge', 'ans_charge_status']

# Descriptive names for heart rate zones
zone_names = ['sleep', 'sedentary', 'light', 'moderate', 'vigorous', 'not worn']

# Descriptive names for the sample indices
sample_names = ['Heart rate (bpm)', 'Speed (km/h)', 'Cadence (rpm)', 'Altitude (m)', 'Power (W)', 'Power pedaling index (%)', 'Power left-right balance (%)', 'Air pressure (hpa)', 'Running cadence (spm)', 'Temperature (C)', 'Distance (m)', 'RR Interval (ms)']

# URL to the Polar Acceslink API
api_url = 'https://www.polaraccesslink.com/v3/users'
