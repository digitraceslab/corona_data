# When run as a script, check the new_tokens file for user IDs not already in
# the tokens file and register there new tokens (this needs to be done before
# we can access data).
#
# Any errors are recorded in the register_token_errors file. If there is an
# error, the token is not removed from the new_tokens list, so that registration
# is attempted again when running the script the next time.

import requests
import uuid

# URL to the Polar Acceslink API
from settings import api_url


def register(token):
    ''' Register a new user with their token.

    token : The oauth2 authorization token of the user
    id : (Optional) Desired ID of the user
    '''

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    json = {"member-id": uuid.uuid4().hex}

    try:
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

    except:
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
