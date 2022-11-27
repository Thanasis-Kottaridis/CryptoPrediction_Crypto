from datetime import datetime


def unixToIsoTimestamp(unixTimestamp) :
    """
        This Helper func returns timestamp to iso 8601 format "2016-05-14T22:01:34-07:00"
        using UTC timezone.

    :param unixTimestamp: timestamp to convert
    :return: the iso 8601 time in string.
    """
    t = int(unixTimestamp)
    return datetime.fromtimestamp(t).isoformat()


def renameDictKeys(dict, prefix) :
    """
        This helper func is used in order to rename all dictionary properites
        by adding the a prefix word on each of them.

    :param dict: dictionary to rename
    :param prefix: the prefix word
    :return: Dictionary with updated keys
    """
    # + operator is used to perform task of concatenation
    res = {prefix + '_' + str(key) : val for key, val in dict.items()}
    return res


def utc_to_datetime(ts) :
    # if you encounter a "year is out of range" error the timestamp
    # may be in milliseconds, try `ts /= 1000` in that case
    return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
