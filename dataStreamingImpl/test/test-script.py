from requests import Session, Request
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects, HTTPError
import json
from aiohttp import ClientSession
import pprint

# Constants
url = "https://food2fork.ca/api/recipe/search"

default_parameters = {
    'page' : 1,
    'query' : ''
}


# headers = {
#   'Accepts': 'application/json',
#   'X-CMC_PRO_API_KEY': '16b5885f-a0b0-4938-877b-57ed469217db',
# }


def fetchLatestQuotes(parameters=default_parameters) :
    headers = {
        'Authorization' : "Token 9c8b06d329136da358c2d00e76946b0111ce2c48",
    }

    session = Session()
    session.headers.update(headers)

    try :
        response = session.get(url, params=parameters)
        _data = json.loads(response.text)
        return response.text  # _data
    except (ConnectionError, Timeout, TooManyRedirects) as e :
        print(e)
        return None


if __name__ == '__main__':
    pprint.pprint(fetchLatestQuotes())