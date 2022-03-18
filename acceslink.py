import os
import requests
import uuid
import pandas as pd
from datetime import datetime

import utils
from settings import *


def register(token):
    ''' Register a new user

    token : The oauth2 authorization token of the user
    id : (Optional) Desired ID of the user
    '''

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
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
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    url = api_url+f'/{user_id}/activity-transactions'
    r = requests.post(url, headers = headers)

    if r.status_code == 204:
        # print("No activity data")
        return None

    r.raise_for_status()

    r = r.json()

    return r['transaction-id']


def exercise_transaction(token, user_id):
    ''' Create a transaction for pulling exercise data

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user

    return : Returns the transaction ID if there is new data,
             otherwise None
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    url = api_url + f'/{user_id}/exercise-transactions'
    r = requests.post(url, headers = headers)

    if r.status_code == 204:
        #print("No exercise data")
        return None

    r.raise_for_status()

    r = r.json()

    return r['transaction-id']


def activity_list(token, user_id, transaction):
    ''' Fetch a list of activity urls. These are used for downloading
    data for each activity record (usually one per day).

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    transaction : open transaction

    return : list of urls for fetching activity data
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    url = api_url + f'/{user_id}/activity-transactions/{transaction}'
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    if r.status_code == 204:
        return []

    r = r.json()
    return r['activity-log']


def exercise_list(token, user_id, transaction):
    ''' Fetch a list of exercise urls. These are used to download data recorded
    for each individual exercise.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    transaction : open transaction

    return : List of urls for fetching exercise data
    '''
    print("getting list of exercises")

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    url = api_url + f'/{user_id}/exercise-transactions/{transaction}'
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    if r.status_code == 204:
        return []

    r = r.json()
    print(f"found {len(r['exercises'])}")
    return r['exercises']


def sleep_list(token):
    ''' Fetch a list of sleep summaries. These already contain the summary data,
    not just URLs for fetching.

    token : The oauth2 authorization token of the user

    return : List of sleep summaries
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    url = api_url + '/sleep'
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    if r.status_code == 204:
        return []

    r = r.json()
    return r['nights']


def recharge_list(token):
    ''' Fetch a list of sleep recharge summaries. These already contain the
    summary data, not just URLs for fetching.

    token : The oauth2 authorization token of the user

    return : List of sleep recharge summaries
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    url = api_url + '/nightly-recharge'
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    if r.status_code == 204:
        return []
    r = r.json()

    return r['recharges']


def activity_summary(token, user_id, url):
    ''' Fetch the summary of a given activity.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    url : url for the activity (provided by activity_list)

    return : summary dictionary
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    summary = r.json()
    return summary


def pull_exercise_samples(token, user_id, subject_id, url, exercise_start_time):
    ''' Fetch exercise sample data from a given exercise and
    write them to the data file.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    url : url for the activity (provided by activity_list)
    exercise_start_time : The start time of the exercise (unique identifier)
    '''

    # Initialize an empty dataframe with appropriate columns for the new data
    filename = raw_data_folder+"exercise_samples.csv"
    columns = ['subject_id', 'exercise-start-time', 'sample-index', 'recording-rate', 'sample-type', 'sample-name', 'sample']
    sampledata = pd.DataFrame(columns=columns)

    # fetch the samples
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    r = requests.get(url+'/samples', headers=headers)
    r.raise_for_status()

    # Return None if this exercise does not exist or has no data
    if r.status_code == 204:
        return None

    # Flatten and format the data.
    for sample_url in r.json()['samples']:
        sample = requests.get(sample_url, headers=headers)

        sample = sample.json()
        sample_list = sample['data'].split(',')
        samples = [{
                       'subject_id': subject_id,
                       'exercise-start-time': exercise_start_time,
                       'sample-index': i,
                       'recording-rate': sample['recording-rate'],
                       'sample-type': sample['sample-type'],
                       'sample-name': sample_names[sample['sample-type']],
                       'sample': sample_line
                   } for i, sample_line in enumerate(sample_list)]
        sampledata = sampledata.append(samples, ignore_index=True)

    # write to file
    sampledata.to_csv(filename, mode='a', header=False)


def exercise_summary(token, user_id, url):
    ''' Fetch the summary of a given exercise.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    url : url for the exercise (provided by exercise_list)

    return : summary dictionary
    '''

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    r = requests.get(url, headers=headers)

    r.raise_for_status()  # Raises common error codes

    summary = r.json()
    return summary


def fetch_data(token, url):
    ''' Fetch data from a given url using token authentication '''
    # Standard headers for Acceslink
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    # Fetch data if available
    r = requests.get(url, headers=headers)

    # Raise common errors
    r.raise_for_status()

    # Return None if the server returns '204: no data'.
    # This might indicate that the data is already fetched, or that none
    # ever existed.
    if r.status_code == 204:
        return None

    # Convert to json and return
    return r.json()


def pull_steps(token, user_id, subject_id, url, date):
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

    # Add date and subject ID to all the samples
    samples = r['samples']

    if len(samples) == 0:
        print("Length of samples is 0 in pull_steps")
        return

    for s in samples:
        s['date'] = date
        s['subject_id'] = subject_id

    # Remove any that do not have the steps-column. These presumably have 0
    # steps.
    samples = [s for s in samples if 'steps' in s]

    # Read from the file or initialize an empty dataframe
    filename = raw_data_folder+"activity_steps.csv"
    columns = ["subject_id", "date", "time", "steps"]

    # Save the new data
    stepdata = pd.DataFrame(columns=columns)
    stepdata = stepdata.append(samples)
    stepdata.to_csv(filename, mode='a', header=False)


def pull_zones(token, user_id, subject_id, url, date):
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
                duration = utils.extract_time(zone['inzone'])

                s = {'subject_id': subject_id,
                     'date': date,
                     'time': rs['time'],
                     'zone index': zone['index'],
                     'zone name': zone_names[zone['index']],
                     'duration': duration}
                samples.append(s)

    if len(samples) == 0:
        print("Length of samples is 0 in pull_zones")
        return

    # Read from the file or initialize an empty dataframe
    filename = raw_data_folder+"activity_zones.csv"
    columns = ["subject_id", "date", "time", "index", "duration"]

    # Append the new data and write
    zonedata = pd.DataFrame(columns=columns)
    zonedata = zonedata.append(samples, ignore_index=True)
    zonedata.to_csv(filename, mode='a', header=False)


def commit_activity(token, user_id, transaction):
    ''' Commit an activity transaction '''

    headers = {
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
    }

    url = api_url+f'/{user_id}/activity-transactions/{transaction}'
    r = requests.put(url, headers=headers)
    r.raise_for_status()


def commit_exercise(token, user_id, transaction):
    ''' Commit an exercise transaction '''

    headers = {
        'Authorization': f'Bearer {token}',
        'Connection': 'keep-alive'
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


def pull_activities(token, user_id, subject_id):
    ''' Pull activity date for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # To avoid writing multiple entries for the same day,
    # read the csv file if it exists and get the lastest date
    filename = raw_data_folder+"activity_summary.csv"

    # Fetch data from the API
    transaction = activities_transaction(token, user_id)

    if transaction is None:
        # No new data, nothing to do
        return False

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
        pruned_data = utils.prune_data(summary, activity_columns)
        pruned_data['duration'] = utils.extract_time(pruned_data['duration'])
        pruned_data['subject_id'] = subject_id

        # Add the summary
        summaries = summaries.append(pruned_data, ignore_index=True)

        # Get step and zone data for the summary
        try:
          pull_steps(token, user_id, subject_id, summary_info['url'], summary['date'])
          pull_zones(token, user_id, subject_id, summary_info['url'], summary['date'])
        except Exception as e:
          print("Encountered error:", e)
          # return without committing. The data should be available tomorrow.
          return False


    # Commit the transaction and write the data
    commit_activity(token, user_id, transaction)
    summaries.to_csv(filename, mode='a', header=False)

    return True


def pull_exercises(token, user_id, subject_id):
    ''' Pull exercise data for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # Set filename
    filename = raw_data_folder+"exercise_summary.csv"

    # Fetch data from the API
    transaction = exercise_transaction(token, user_id)

    if transaction is None:
        # No new data, nothing to do
        return

    summaries = pd.DataFrame(columns=exercise_columns)

    # Now check for new
    url_list = exercise_list(token, user_id, transaction)
    print(url_list)
    for url in url_list:
        # Get the summary and specifically note the start-time.
        # There is only one final entry for each start-time.
        print("pulling exercise")
        summary = exercise_summary(token, user_id, url)

        # collapse the heart rate hierarchy
        try:
          summary["average-heart-rate"] = summary["heart-rate"]["average"]
          summary["maximum-heart-rate"] = summary["heart-rate"]["maximum"]
        except:
          pass
        summary['subject_id'] = subject_id

        # Add to the dataframe
        pruned_data = utils.prune_data(summary, exercise_columns)
        pruned_data['duration'] = utils.extract_time(pruned_data['duration'])
        summaries = summaries.append(pruned_data, ignore_index=True)

        print("pulling sample")

        pull_exercise_samples(token, user_id, subject_id, url, pruned_data['start-time'])

    # Commit the transaction
    commit_exercise(token, user_id, transaction)

    # Write to the file
    summaries.to_csv(filename, mode='a', header=False)


def pull_sleep(token, user_id, subject_id):
    ''' Pull sleep data for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # Set filename
    filename = raw_data_folder+"sleep_summary.csv"

    # Find the last date with data for this subject
    data = pd.read_csv(filename, usecols=["date", "subject_id"])
    data = data[data["subject_id"] == subject_id]
    latest_date = max(pd.to_datetime(data['date']))

    summaries = pd.DataFrame(columns=sleep_columns)

    # Now check for new
    summary_list = sleep_list(token)
    for summary in summary_list:
        # Sleep reports don't change once generated. If the date is
        # already found, just skip
        date = datetime.strptime(summary["date"], '%Y-%m-%d')
        if date >= latest_date:

            if 'heart_rate_samples' in summary:
                utils.retry_and_report(handle_sleep_sample, subject_id, summary['date'], summary['heart_rate_samples'], type)

            if 'hypnogram' in summary:
                utils.retry_and_report(handle_sleep_sample, subject_id, summary['date'], summary['hypnogram'], type)

            # Take only the given set of columns
            pruned_data = utils.prune_data(summary, sleep_columns)
            pruned_data['subject_id'] = subject_id

            summaries = summaries.append(pruned_data, ignore_index=True)

    # Write to the file
    summaries.to_csv(filename, mode='a', header=False)


def handle_sleep_sample(subject_id, date, data, type):
    ''' Append sleep samples to file.

    user_id : The polar user ID of the user
    date : The date of the sleep record (unique identifier)
    data : The samples
    type : Name of the sample type
    '''

    if len(data) == 0:
        print("handle_sleep_sample called with empty sample")
        return

    # Read from the file or initialize an empty dataframe
    filename = raw_data_folder+"sleep_samples.csv"
    columns = ['subject_id', 'date', 'sample-time', 'sample-type', 'sample']

    samples = [{
                'subject_id': subject_id,
                'date': date,
                'sample-time': time,
                'sample-type': type,
                'sample': sample
               } for time, sample in data.items]

    # write to file
    sampledata = pd.DataFrame(columns=columns)
    sampledata = sampledata.append(samples, ignore_index=True)
    sampledata.to_csv(filename, mode='a', header=False)


def pull_nightly_recharge(token, user_id, subject_id):
    ''' Pull nightly recharge data for a given user and write to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''

    # Set filename
    filename = raw_data_folder+"nightly_recharge_summary.csv"

    # Find the last date with data for this subject
    data = pd.read_csv(filename, usecols=["date", "subject_id"])
    data = data[data["subject_id"] == subject_id]
    latest_date = max(pd.to_datetime(data['date']))

    summaries = pd.DataFrame(columns=recharge_columns)

    # Now check for new
    summary_list = recharge_list(token)
    for summary in summary_list:
        date = datetime.strptime(summary["date"], '%Y-%m-%d')
        if date >= latest_date:
            # Extract the hrv and breathing rate samples
            if 'hrv_samples' in summary:
                utils.retry_and_report(handle_sleep_sample, subject_id, summary['date'], summary['hrv_samples'], type)
            if 'breathing_samples' in summary:
                utils.retry_and_report(handle_sleep_sample, subject_id, summary['date'], summary['breathing_samples'], type)

            # Take only the given set of columns
            pruned_data = utils.prune_data(summary, recharge_columns)
            pruned_data['subject_id'] = subject_id

            summaries = summaries.append(pruned_data, ignore_index=True)

    # Write to the file
    summaries.to_csv(filename, mode='a', header=False)


def pull_subject_data(token, user_id, subject_id):
    ''' Pull subject activity, exercise and sleep data and write
    to csv files.

    token : The oauth2 authorization token of the user
    user_id : The polar user ID of the user
    '''
    has_data = retry_and_report(pull_activities, token, user_id, subject_id)
    if has_data:
      retry_and_report(pull_exercises, token, user_id, subject_id)
      retry_and_report(pull_sleep, token, user_id, subject_id)
      retry_and_report(pull_nightly_recharge, token, user_id, subject_id)
      time.sleep(1)


# If run as a script, read the token file and pull all data
if __name__ == "__main__":
    token_file = open("tokens", "r")
    for line in token_file:
        token, user, subject_id = line.split(' ')
        try:
            now = datetime.now()
            print(now.strftime("%H:%M:%S:"), user)
            pull_subject_data(token, int(user), int(subject_id))
            time.sleep(0.1)
        except requests.exceptions.HTTPError as e:
            print(e)
            print(f"HTTP-error for {int(subject_id)}, could be revoked")
        except Exception as e:
            print(e)
            print(f"above error encountered for {int(subject_id)}. Moving on.")

