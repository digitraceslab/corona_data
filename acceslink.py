import os
import requests
import uuid
import pandas as pd
from datetime import datetime

# Settings
# ========
#
# Set columns to keep from activity data, exercise data and
# sleep data
activity_columns = ["date", "calories", "active-calories", "duration", "active-steps"]
exercise_columns = ["start-time", "calories", "distance", "duration", "training-load", "max-heart-rate", "average-heart-rate", "training-load", "sport", "detailed-sport-info", "fat-percentage", "carbohydrate-percentage", "protein-percentage"]
sleep_columns = ["date", "sleep_start_time", "sleep_end_time", "continuity", "light_sleep", "deep_sleep", "rem_sleep", "unrecognized_sleep_stage", 'total_interruption_duration']

# Descriptive names for heart rate zones
zone_names = ['sleep', 'sedentary', 'light', 'moderate', 'vigorous', 'not worn']

# Descriptive names for the sample indices
sample_names = ['Heart rate (bpm)', 'Speed (km/h)', 'Cadence (rpm)', 'Altitude (m)', 'Power (W)', 'Power pedaling index (%)', 'Power left-right balance (%)', 'Air pressure (hpa)', 'Running cadence (spm)', 'Temperature (C)', 'Distance (m)', 'RR Interval (ms)']

# URL to the Polar Acceslink API
api_url = 'https://www.polaraccesslink.com/v3/users'


def extract_time(time_string):
    ''' Utility for extracting hours, minutes and seconds
    from the API time format

    time_string : time value returened by the Acceslink API

    return : time in seconds
    '''

    t = ''
    seconds = 0
    for c in time_string:
        if c == 'P':
            # The string starts with PT, which we skip
            pass
        elif c == 'T':
            # The string starts with PT, which we skip
            pass
        elif c == 'H':
            # Hours
            seconds += 3600*int(t)
            t = ''
        elif c == 'M':
            # Minutes
            seconds = 60*int(t)
            t = ''
        elif c == 'S':
            # Seconds
            seconds = float(t)
            t = ''
        else:
            t += c

    return seconds


def prune_data(data, columns):
    ''' Clean data by extracting given set of columns and
    adding an empty for missing data.

    data : the original data to prune
    columns : list of keys to keep in the data

    returns : pruned data
    '''
    # Add empty strings for missing columns
    for key in columns:
        if key not in data:
            data[key] = ''

    # Construct a dictionary of only the listed columns
    return {key: data[key] for key in columns}


def register(token):
    ''' Register a new user

    token : The oauth2 authorization token of the user
    id : (Optional) Desired ID of the user
    '''

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    json = {"member-id": uuid.uuid4().hex}

    r = requests.post(api_url, json=json, headers = headers)

    if r.status_code == 409:
        print("User already registered")
        return


def activities_transaction(token, user_id):
    ''' Create a transaction for pulling activities data

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user

    return : True if there is new data
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    url = api_url+f'/{user_id}/activity-transactions'
    r = requests.post(url, headers = headers)

    if r.status_code == 204:
        print("No activity data")
        return None

    r.raise_for_status()

    r = r.json()

    return r['transaction-id']


def exercise_transaction(token, user_id):
    ''' Create a transaction for pulling exercise data

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user

    return : True if there is new data
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    url = api_url + f'/{user_id}/exercise-transactions'
    r = requests.post(url, headers = headers)

    if r.status_code == 204:
        print("No exercise data")
        return None

    r.raise_for_status()

    r = r.json()

    return r['transaction-id']


def activity_list(token, user_id, transaction):
    ''' Fetch a list of activity urls

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    transaction : open transaction

    return : list of urls for fetching activity data
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    url = api_url + f'/{user_id}/activity-transactions/{transaction}'
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    r = r.json()
    return r['activity-log']


def exercise_list(token, user_id, transaction):
    ''' Fetch a list of exercise urls

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    transaction : open transaction

    return : List of urls for fetching exercise data
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    url = api_url + f'/{user_id}/exercise-transactions/{transaction}'
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    r = r.json()
    return r['exercises']


def sleep_list(token):
    ''' Fetch a list of exercise urls

    token : The oauth2 authorization token of the user

    return : List of sleep summaries
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    url = api_url + '/sleep'
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    r = r.json()
    return r['nights']


def activity_summary(token, user_id, url):
    ''' Fetch the summary of a given activity

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    url : url for the activity (provided by activity_list)

    return : summary dictionary
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    summary = r.json()
    return summary


def pull_exercise_samples(token, user_id, url, exercise_start_time):
    ''' Fetch exercise sample data from a given exercise and
    write them to the log.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    url : url for the activity (provided by activity_list)
    exercise_start_time : The start time of the exercise (unique identifier)
    '''

    # Read from the file or initialize an empty dataframe
    filename = f"exercise_samples_{user_id}.csv"
    columns = ['exercise-start-time', 'sample-index', 'recording-rate', 'sample-type', 'sample-name', 'sample']
    if os.path.isfile(filename):
        sampledata = pd.read_csv(filename)[columns]
    else:
        sampledata = pd.DataFrame(columns=columns)

    # Drop any data for this exercise
    condition = sampledata['exercise-start-time'] == exercise_start_time
    rows = sampledata[condition].index
    sampledata.drop(rows, inplace=True)

    # fetch the samples
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(url+'/samples', headers=headers)
    r.raise_for_status()

    for sample_url in r.json()['samples']:
        sample = requests.get(sample_url, headers=headers)
        sample = sample.json()
        sample_list = sample['data'].split(',')
        for i, sample_line in enumerate(sample_list):
            pruned = {
                       'exercise-start-time': exercise_start_time,
                       'sample-index': i,
                       'recording-rate': sample['recording-rate'],
                       'sample-type': sample['sample-type'],
                       'sample-name': sample_names[sample['sample-type']],
                       'sample': sample_line
                     }
            sampledata = sampledata.append(pruned, ignore_index=True)

    # write to file
    sampledata.to_csv(filename)


def exercise_summary(token, user_id, url):
    ''' Fetch the summary of a given exercise

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    url : url for the exercise (provided by exercise_list)

    return : summary dictionary
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    summary = r.json()
    return summary


def fetch_data(token, url):
    ''' Fetch data from a given url using token authentication '''
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    if r.status_code == 204:
        return None
    return r.json()


def pull_steps(token, user_id, url, date):
    ''' Fetch step data from a given activity and write
    them to the step log. If step data for the day already
    exists, overwrite.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    url : url for the activity (provided by activity_list)
    date : the date of the activity data
    '''

    # Fetch the data
    r = fetch_data(token, url+'/step-samples')
    if r is None:
        print("No step data, is transaction open?")
        return

    # Add date to all the samples
    samples = r['samples']
    for s in samples:
        s['date'] = date

    # Remove any that do not have the steps-column
    samples = [s for s in samples if 'steps' in s]

    # Read from the file or initialize an empty dataframe
    filename = f"activity_steps_{user_id}.csv"
    columns = ["date", "time", "steps"]
    if os.path.isfile(filename):
        stepdata = pd.read_csv(filename)[columns]
    else:
        stepdata = pd.DataFrame(columns=columns)

    # This activity should have all data for the date. So
    # remove all data for this date and add the newly read data
    # instead
    condition = stepdata['date'] == date
    rows = stepdata[condition].index
    stepdata.drop(rows, inplace=True)

    # Append the new data and save
    stepdata = stepdata.append(samples, ignore_index=True)
    stepdata.to_csv(filename)


def pull_zones(token, user_id, url, date):
    ''' Fetch heart rate zone data from a given activity and
    write them to the heart rate zone log. If heart rate zone
    data for the day already exists, overwrite.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    url : url for the activity (provided by activity_list)
    date : the date of the activity data
    '''

    # Fetch the data
    r = fetch_data(token, url+'/zone-samples')
    if r is None:
        print("No zone data, is transaction open?")
        return

    # Flatten the zone data hierarchy
    samples = []
    for rs in r['samples']:
        if 'activity-zones' in rs:
            for zone in rs['activity-zones']:
                duration = extract_time(zone['inzone'])

                s = {'date': date, 'time': rs['time'],
                     'zone index': zone['index'],
                     'zone name': zone_names[zone['index']],
                     'duration': duration}
                samples.append(s)

    # Read from the file or initialize an empty dataframe
    filename = f"activity_zones_{user_id}.csv"
    columns = ["date", "time", "index", "duration"]
    if os.path.isfile(filename):
        zonedata = pd.read_csv(filename)[columns]
    else:
        zonedata = pd.DataFrame(columns=columns)

    # This activity should have all data for the date. So
    # remove all data for this date and add the newly read data
    # instead
    index = date
    condition = zonedata['date'] == index
    rows = zonedata[condition].index
    zonedata.drop(rows, inplace=True)

    # Append the new data and write
    zonedata = zonedata.append(samples, ignore_index=True)
    zonedata.to_csv(filename)


def commit_activity(token, user_id, transaction):
    ''' Commit an activity transaction '''

    headers = {
        'Authorization': f'Bearer {token}'
    }

    url = api_url+f'/{user_id}/activity-transactions/{transaction}'
    r = requests.put(url, headers=headers)
    r.raise_for_status()


def commit_exercise(token, user_id, transaction):
    ''' Commit an exercise transaction '''

    headers = {
        'Authorization': f'Bearer {token}'
    }

    url = api_url+f'/{user_id}/exercise-transactions/{transaction}'
    r = requests.put(url, headers=headers)
    r.raise_for_status()


def time_to_sec(time_string):
    ''' Utility for mapping a time string to an integer
    number of seconds.

    time_string : time in the %Y-%m-%dT%H:%M:%S.%f format
    return : time in seconds since 1900-1-1T0:0:0
    '''
    format_string = '%Y-%m-%dT%H:%M:%S.%f'
    date_time = datetime.strptime(time_string, format_string)
    time_delta = date_time - datetime(1900, 1, 1)
    return time_delta.total_seconds()


def pull_activities(token, user_id):
    ''' Pull activity date for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # To avoid writing multiple entries for the same day,
    # read the csv file if it exists and get the lastest date
    filename = f"activity_summary_{user_id}.csv"

    # Fetch data from the API
    transaction = activities_transaction(token, user_id)

    if transaction is None:
        # No new data, nothing to do
        return

    # Found new data. Check for old data first
    if os.path.isfile(filename):
        # The file already exists, so read current entries
        summaries = pd.read_csv(filename)[activity_columns]

    else:
        # First time pulling for this subject. Create a
        # dataframe and note there is not previous data.
        summaries = pd.DataFrame(columns=activity_columns)

    # Get the list of summaries
    url_list = activity_list(token, user_id, transaction)

    # The list of summaries may contain multiple summaries
    # for a given date. They are ordered by created time,
    # so the last one contains the latest data.
    # So first check all summaries and keep the last one
    # for each date
    summary_list = {}
    for url in url_list:
        # Get the summary and specifically note the date.
        # There is only one final entry for each date.
        summary = activity_summary(token, user_id, url)
        date = summary['date']
        this_time = time_to_sec(summary['created'])
        summary_info = {
            'summary': summary,
            'url': url,
            'time': this_time
        }

        if date not in summary_list:
            summary_list[date] = summary_info

        else:
            latest_time = summary_list[date]['time']
            if this_time > latest_time:
                summary_list[date] = summary_info

    # Now check for new
    for summary_info in summary_list.values():
        # Prune the summary data
        summary = summary_info['summary']
        pruned_data = prune_data(summary, activity_columns)
        pruned_data['duration'] = extract_time(pruned_data['duration'])

        # Remove any previous entry with the same date
        index = pruned_data['date']
        condition = summaries['date'] == index
        rows = summaries[condition].index
        summaries.drop(rows, inplace=True)

        # Add the summary
        summaries = summaries.append(pruned_data, ignore_index=True)

        # Get step and zone data for the summary
        pull_steps(token, user_id, summary_info['url'], summary['date'])
        pull_zones(token, user_id, summary_info['url'], summary['date'])

    # Commit the transaction and write the data
    commit_activity(token, user_id, transaction)
    summaries.to_csv(filename)


def pull_exercises(token, user_id):
    ''' Pull exercise data for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # Set filename
    filename = f"exercise_summary_{user_id}.csv"

    # Fetch data from the API
    transaction = exercise_transaction(token, user_id)

    if transaction is None:
        # No new data, nothing to do
        return

    # Found new data. Check for old data first
    if os.path.isfile(filename):
        # The file already exists, so read current entries
        summaries = pd.read_csv(filename)[exercise_columns]
    else:
        # First time pulling for this subject. Create a
        # dataframe.
        summaries = pd.DataFrame(columns=exercise_columns)

    # Now check for new
    url_list = exercise_list(token, user_id, transaction)
    for url in url_list:
        # Get the summary and specifically note the start-time.
        # There is only one final entry for each start-time.
        summary = exercise_summary(token, user_id, url)

        # collapse the heart rate hierarchy
        summary["average-heart-rate"] = summary["heart-rate"]["average"]
        summary["maximum-heart-rate"] = summary["heart-rate"]["maximum"]

        # Add to the dataframe
        pruned_data = prune_data(summary, exercise_columns)
        pruned_data['duration'] = extract_time(pruned_data['duration'])
        summaries = summaries.append(pruned_data, ignore_index=True)

        pull_exercise_samples(token, user_id, url, pruned_data['start-time'])

    # Commit the transaction
    commit_exercise(token, user_id, transaction)

    # Write to the file
    summaries.to_csv(filename)


def pull_sleep(token, user_id):
    ''' Pull sleep data for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # Set filename
    filename = f"sleep_summary_{user_id}.csv"

    # Load old data first
    if os.path.isfile(filename):
        # The file already exists, so read current entries
        summaries = pd.read_csv(filename)[sleep_columns]
    else:
        # First time pulling for this subject. Create a
        # dataframe.
        summaries = pd.DataFrame(columns=sleep_columns)

    # Now check for new
    summary_list = sleep_list(token)
    for summary in summary_list:
        # Take only the given set of columns
        pruned_data = prune_data(summary, sleep_columns)

        # Sleep reports don't change once generated. If the date is
        # already found, just skip
        index = pruned_data['date']
        condition = summaries['date'] == index
        rows = summaries[condition].index
        summaries.drop(rows, inplace=True)
        summaries = summaries.append(pruned_data, ignore_index=True)

    # Write to the file
    summaries.to_csv(filename)


def pull_subject_data(token, user_id):
    ''' Pull subject activity, exercise and sleep data and write
    to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''
    pull_activities(token, user_id)
    pull_exercises(token, user_id)
    pull_sleep(token, user_id)


# If run as a script, read the token file and pull all data
if __name__ == "__main__":
    token_file = open("tokens", "r")
    for line in token_file:
        token, user = line.split(' ')
        pull_subject_data(token, int(user))
