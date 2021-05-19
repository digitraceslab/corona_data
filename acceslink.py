import requests
import uuid
import csv


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
    json={"member-id": id}

    r = requests.post('https://www.polaraccesslink.com/v3/users', json=json, headers = headers)

    if r.status_code == 409:
        print("")
        return

    print(r)
    print(r.json())


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

    print(r)

    if r.status_code == 204:
        print("No data")
        return None

    r.raise_for_status()

    print(r.json())
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

    print(r)

    if r.status_code == 204:
        print("No data")
        return None

    r.raise_for_status()

    print(r.json())
    r = r.json()

    return r['transaction-id']


def activity_list(token, user_id, transaction):

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(f'https://www.polaraccesslink.com/v3/users/{user_id}/activity-transactions/{transaction}', headers=headers)
    r.raise_for_status()

    print(r.json())
    r = r.json()
    return r['activity-log']


def activity_summary(token, user_id, url):

    # The url has a constant format. Split to find activity id
    activity = url.split('/')[-1]
    print(activity)

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    print(r)

    print(r.json())
    summary = r.json()
    with open(f"activity_summary_{user_id}_{activity}.csv", 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        header = summary.keys()
        csv_writer.writerow(header)
        csv_writer.writerow(summary.values())

    return summary


def get_steps(token, user_id, url, date):

    # The url has a constant format. Split to find activity id
    activity = url.split('/')[-1]
    print(activity)

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(url+'/step-samples', headers=headers)
    r.raise_for_status()
    print(r)
    r = r.json()
    with open(f"activity_steps_{user_id}_{activity}.csv", 'w') as csv_file:
        csv_file.write(f"date,time,steps\n")
        print(f"date, time, steps")
        for s in r['samples']:
            if 'steps' in s:
                print(s)
                csv_file.write(f"{date},{s['time']},{s['steps']}\n")

    print(r)


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


def get_zones(token, user_id, url, date):

    # The url has a constant format. Split to find activity id
    activity = url.split('/')[-1]
    print(activity)

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.get(url+'/zone-samples', headers=headers)
    r.raise_for_status()
    print(r)
    r = r.json()
    with open(f"activity_zones_{user_id}_{activity}.csv", 'w') as csv_file:
        csv_file.write(f"date,time,zone,duration hours, duration minutes,duration seconds\n")
        print(f"time, zones")
        for s in r['samples']:
            if 'activity-zones' in s:
                print(s)
                time = s['time']
                for zone in s['activity-zones']:
                    inzone = zone['inzone']
                    hours, minutes, seconds = extract_time(inzone)

                    csv_file.write(f"{date},{time},{zone['index']},{hours},{minutes},{seconds} \n")

    print(r)


def commit_activity(token, user_id, transaction):

    headers = {
        'Authorization': f'Bearer {token}'
    }

    r = requests.put(f'https://www.polaraccesslink.com/v3/users/{user_id}/activity-transactions/{transaction}', headers=headers)
    r.raise_for_status()
    print(r)
