"""
-------------- Test Worker ------------------------
test temp mail: ligen59951@sicmag.com
name: Thanasis Testmail1
pass: Test1234!
secret-key: 16b5885f-a0b0-4938-877b-57ed469217db
"""

# Imports
from requests import Session, Request
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pprint

# Constants
url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

default_parameters = {
    'symbol' : 'BTC',
    'convert' : 'USD'
}

# headers = {
#   'Accepts': 'application/json',
#   'X-CMC_PRO_API_KEY': '16b5885f-a0b0-4938-877b-57ed469217db',
# }


def fetchLatestQuotes(key, parameters=default_parameters):
    headers = {
        'Accepts' : 'application/json',
        'X-CMC_PRO_API_KEY' : key,
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


# if __name__ == '__main__':
#     data = fetchLatestQuotes("16b5885f-a0b0-4938-877b-57ed469217db")
#     pprint.pprint(data)
#     print("-------------------- Current Value -------------------")
#     print(data['data']['BTC']['quote']['USD']['price'])
