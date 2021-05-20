import os
import requests
import uuid
import pandas as pd


# Settings
# ========
#
# Set columns to keep from activity data, exercise data and
# sleep data
activity_columns = ["date", "calories", "active-calories", "duration", "active-steps"]
exercise_columns = ["start-time", "calories", "distance", "duration", "training-load"]
sleep_columns = ["date", "sleep_start_time", "sleep_end_time", "continuity", "light_sleep", "deep_sleep", "rem_sleep", "unrecognized_sleep_stage", 'total_interruption_duration']

# URL to the Polar Acceslink API
api_url = 'https://www.polaraccesslink.com/v3/users'

def extract_time(time_string):
    ''' Utility for extracting hours, minutes and seconds
    from the API time format

    time_string : time value returened by the Acceslink API

    return : tuple of hours, minutes and seconds
    '''

    t = ''
    hours = 0
    minutes = 0
    seconds = 0
    for c in time_string:
        if c == 'P':
            # The string starts with PT, which we skip
            pass
        elif c == 'T':
            # The string starts with PT, which we skip
            pass
        elif c == 'H':
            hours = int(t)
            t = ''
        elif c == 'M':
            minutes = int(t)
            t = ''
        elif c == 'S':
            seconds = int(t)
            t = ''
        else:
            t += c

    return (hours, minutes, seconds)


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
                hours, minutes, seconds = extract_time(zone['inzone'])
                s = {'date': date, 'time': rs['time'],
                     'index': zone['index'], 'hours': hours,
                     'minutes': minutes, 'seconds': seconds}
                samples.append(s)

    # Read from the file or initialize an empty dataframe
    filename = f"activity_zones_{user_id}.csv"
    columns = ["date", "time", "index", "hours", "minutes", "seconds"]
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

        # We will likely replace the last line, so make note of
        # it and drop it from the dataframe
        previous_date = summaries.at[summaries.index[-1], 'date']
        previous_summary = summaries.tail(1).to_dict('records')[0]
        summaries.drop(summaries.index[-1], inplace=True)

    else:
        # First time pulling for this subject. Create a
        # dataframe and note there is not previous data.
        summaries = pd.DataFrame(columns=activity_columns)
        previous_date = None
        previous_summary = None

    previous_url = None

    # Now check for new
    url_list = activity_list(token, user_id, transaction)
    for url in url_list:
        # Get the summary and specifically note the date.
        # There is only one final entry for each date.
        summary = activity_summary(token, user_id, url)
        date = summary['date']

        # Check if we have moved on to a new date. If so,
        # add the previous one to the dataframe
        if previous_summary and date != previous_date:
            pruned_data = prune_data(previous_summary, activity_columns)
            summaries = summaries.append(pruned_data, ignore_index=True)
            pull_steps(token, user_id, previous_url, previous_date)
            pull_zones(token, user_id, previous_url, previous_date)

        previous_summary = summary
        previous_date = date
        previous_url = url

    # Add the last row
    pruned_data = prune_data(summary, activity_columns)
    summaries = summaries.append(pruned_data, ignore_index=True)
    pull_steps(token, user_id, url, date)
    pull_zones(token, user_id, url, date)

    # Commit the transaction
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

        # Add to the dataframe
        pruned_data = prune_data(summary, exercise_columns)
        summaries = summaries.append(pruned_data, ignore_index=True)

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
