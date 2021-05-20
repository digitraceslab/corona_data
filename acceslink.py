import os
import requests
import uuid
import csv
import pandas as pd


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


def register(token, id=uuid.uuid4().hex):
    ''' Register a new user

    token : The oauth2 authorization token of the user
    id : (Optional) Desired ID of the user
    '''

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    json = {"member-id": id}

    r = requests.post('https://www.polaraccesslink.com/v3/users', json=json, headers = headers)

    if r.status_code == 409:
        print("")
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

    r = requests.post(f'https://www.polaraccesslink.com/v3/users/{user_id}/activity-transactions', headers = headers)

    if r.status_code == 204:
        print("No data")
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

    r = requests.post(f'https://www.polaraccesslink.com/v3/users/{user_id}/exercise-transactions', headers = headers)

    if r.status_code == 204:
        print("No data")
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

    r = requests.get(f'https://www.polaraccesslink.com/v3/users/{user_id}/activity-transactions/{transaction}', headers=headers)
    r.raise_for_status()

    r = r.json()
    return r['activity-log']


def exercise_list(token, user_id, transaction):
    ''' Fetch a list of exercise urls

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    transaction : open transaction

    return : True if there is new data
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(f'https://www.polaraccesslink.com/v3/users/{user_id}/exercise-transactions/{transaction}', headers=headers)
    r.raise_for_status()

    r = r.json()
    return r['exercises']


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

    r = requests.put(f'https://www.polaraccesslink.com/v3/users/{user_id}/activity-transactions/{transaction}', headers=headers)
    r.raise_for_status()


def commit_exercise(token, user_id, transaction):
    ''' Commit an exercise transaction '''

    headers = {
        'Authorization': f'Bearer {token}'
    }

    r = requests.put(f'https://www.polaraccesslink.com/v3/users/{user_id}/exercise-transactions/{transaction}', headers=headers)
    r.raise_for_status()


def pull_activities(token, user_id):
    ''' Pull activity date for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # To avoid writing multiple entries for the same day,
    # read the csv file if it exists and get the lastest date
    filename = f"activity_summary_{user_id}.csv"
    activity_columns = ["id", "date", "created", "calories", "active-calories", "duration", "active-steps"]

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
            pruned_data = {key: previous_summary[key] for key in activity_columns}
            summaries = summaries.append(pruned_data, ignore_index=True)
            pull_steps(token, user_id, previous_url, previous_date)
            pull_zones(token, user_id, previous_url, previous_date)

        previous_summary = summary
        previous_date = date
        previous_url = url

    # Commit the transaction
    commit_activity(token, user_id, transaction)

    # Add the last row
    pruned_data = {key: previous_summary[key] for key in activity_columns}
    summaries = summaries.append(pruned_data, ignore_index=True)
    pull_steps(token, user_id, url, date)
    pull_zones(token, user_id, url, date)

    summaries.to_csv(filename)


def pull_exercises(token, user_id):
    ''' Pull exercise date for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # Set filename and columns to keep
    filename = f"exercise_summary_{user_id}.csv"
    exercise_columns = ["id", "start-time", "calories", "distance", "duration", "training-load"]

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

        # some exercise don't record distance. Set this to
        # empty string
        if 'distance' not in summary:
            summary['distance'] = ''
        print(summary)

        # Add to the dataframe
        pruned_data = {key: summary[key] for key in exercise_columns}
        summaries = summaries.append(pruned_data, ignore_index=True)

    # Commit the transaction
    commit_exercise(token, user_id, transaction)

    # Write to the file
    summaries.to_csv(filename)
