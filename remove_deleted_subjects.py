import os
import shutil
import requests
import uuid

# URL to the Polar Acceslink API
api_url = 'https://www.polaraccesslink.com/v3/users'


def delete(token, user_id):
    ''' Delete a user

    token : The oauth2 authorization token of the user
    user_id : The id of the user to remove
    '''

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    r = requests.delete(api_url + '/' + user_id, headers = headers)

    print(r)

    if r.status_code == 403:
        print("Access error, forbidden", token)
        return False

    if r.status_code == 401:
        print("Access error, unauthorized", token)
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
        tokens.append((token, user, int(subject_id)))
    registered_token_file.close()

    # Open the error file as well
    errorfile = open("delete_token_errors", "a")

    # Now read the new token file and check for unregistered tokens
    delete_token_file = open("delete_tokens", "r")
    for line in delete_token_file:
        delete_id = int(line)

        # find the token and the user
        token = None
        user = None
        for t, u, s in tokens:
            if s == delete_id:
                token = t
                user = u

        # remove the token from the list
        tokens = [(t, u, s) for t, u, s in tokens if s != delete_id]

        if token is not None:
            success = delete(token, user)

            if not success:
                # Failed to delete. Add it to the error list
                # (but it was removed from the list, so it will not be
                # accessed)
                errorfile.write(f"{token} {user} {delete_id}\n")

    # Write tokens
    registered_token_file = open("tokens", "w")
    for t, u, s in tokens:
        registered_token_file.write(f"{t} {u} {s}\n")
    registered_token_file.close()

    # close the files
    delete_token_file.close()
    errorfile.close()

    # remove the content of the delete file
    open("delete_tokens", "w").close()
