import time


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


def retry_and_report(try_function, *args):
    ''' Try running an acceslink function. If it fails, report the error and retry after
        20 seconds. Wait up to (about) 15 minutes, in case the problem is the rate limit.
    '''
    print(try_function.__name__)
    for retry in range(50):
        try:
            try_function(*args)
        except Exception as e:
            print("Encountered error:", e)
            # if failed, run the next iteration (retry)
            time.sleep(20)
            continue
        # if succesfull, break from the loop
        break


