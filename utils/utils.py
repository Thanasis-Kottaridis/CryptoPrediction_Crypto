from datetime import datetime
import json

"""
Date Utils
"""
def unixToIsoTimestamp(unixTimestamp) :
    """
        This Helper func returns timestamp to iso 8601 format "2016-05-14T22:01:34-07:00"
        using UTC timezone.

    :param unixTimestamp: timestamp to convert
    :return: the iso 8601 time in string.
    """
    t = int(unixTimestamp)
    return datetime.fromtimestamp(t).isoformat()


def utc_to_datetime(ts) :
    # if you encounter a "year is out of range" error the timestamp
    # may be in milliseconds, try `ts /= 1000` in that case
    return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


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


"""
    jSON Utils
"""


def flatten_json(y, targetFields, flatList=False):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list and name[:-1] in targetFields:
            if flatList:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x
        elif name[:-1] in targetFields:
            out[name[:-1]] = x

    flatten(y)
    return out


def mongoDictToJson(d, jsonFileName):
    """
    This helper func extracts a python dictionary loaded from mongo (with ObjectID)
    and stores it into a json file.
    :param d: the dictionary.
    :param jsonFileName: the name of the json file included .json extension.
    :return: Void
    """
    # unwrap obj ID and keep str value
    for obj in d:
        obj["_id"] = str(obj["_id"])

    f = open(jsonFileName, "w")
    json.dump(d, f)
    f.close()
