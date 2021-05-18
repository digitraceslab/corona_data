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
    r = r.json()
    with open(f"activity_summary_{user_id}_{activity}.csv", 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        header = r.keys()
        csv_writer.writerow(header)
        csv_writer.writerow(r.values())


def get_steps(token, user_id, url):

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
        csv_file.write(f"time, steps\n")
        print(f"time, steps")
        for s in r['samples']:
            if 'steps' in s:
                print(s)
                csv_file.write(f"{s['time']}, {s['steps']}\n")

    print(r)




def commit_activity(token, user_id, transaction):

    headers = {
        'Authorization': f'Bearer {token}'
    }

    r = requests.put(f'https://www.polaraccesslink.com/v3/users/{user_id}/activity-transactions/{transaction}', headers=headers)
    r.raise_for_status()
    print(r)
