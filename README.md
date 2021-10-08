# Polar Get

Utility for fetching data from the Polar Acceslink API. We try to fetch all
available data, except for personal data provided directly by the user.

New tokens are added to the `new_tokens` file. Each line in the file should
contain the token, polar user ID and a pseudonymous user ID in order. So
```
token polar_user_id pseudonym
```
The pseudonym will get written into the data files, while the token and the
Polar user ID should be kept secret and not exported with the data.

To pull the data for existing tokens and new tokens, run the `get_data.sh`
script.

To remove a user, add their user_id (and nothing else) to the `delete_tokens`
file.

Running the `get_data.sh` script will first register any new tokens, delete
any new tokens in `delete_tokens`, and finally pull
latest data from the API and write it to several csv files, and finally
