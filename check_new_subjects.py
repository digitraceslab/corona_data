import os
import shutil
import requests
import uuid

# URL to the Polar Acceslink API
api_url = 'https://www.polaraccesslink.com/v3/users'


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
        print("User already registered", token)
        return False

    if r.status_code == 403:
        print("Access error", token)
        return False

    if r.status_code == 503:
        print("Service Unavailable", token)
        return False


    return True


# If run as a script, read the token file and pull all data
if __name__ == "__main__":
    # read the current token file
    registered_token_file = open("tokens", "r")
    tokens = []
    for line in registered_token_file:
        token, user, subject_id = line.split(' ')
        tokens.append(token)
    registered_token_file.close()

    # Open it for appending
    registered_token_file = open("tokens", "a")
    # Open the error file as well
    errorfile = open("register_token_errors", "a")

    # Now read the new token file and check for unregistered tokens
    new_token_file = open("new_tokens", "r")
    for line in new_token_file:
        token, user, subject_id = line.split(' ')
        if token not in tokens:
            success = register(token)
            if success:
                # Succesfully registered. Add it to the list
                registered_token_file.write(f"{token} {user} {subject_id}")
            else:
                # Failed to register. Add it to the error list
                # (but it stays in the new token list and will be retried)
                errorfile.write(f"{token} {user} {subject_id}")

    # close the files
    new_token_file.close()
    registered_token_file.close()
    errorfile.close()
