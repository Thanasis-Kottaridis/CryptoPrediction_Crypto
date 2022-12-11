"""
-------------- Test Worker ------------------------
test temp mail: ligen59951@sicmag.com
name: Thanasis Testmail1
pass: Test1234!
secret-key: ce5b5796-6782-44c0-9393-a3ed26d47362 ----------  old one --------- 474b8679-0b93-46ce-97a9-5500f9b95cc8

blocked key: 16b5885f-a0b0-4938-877b-57ed469217db
"""

# Imports
from requests import Session, Request
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects, HTTPError
import json
from aiohttp import ClientSession
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


def fetchLatestQuotes(key, parameters=default_parameters) :
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


class CoinMarketRepo(object) :
    # Constants
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

    default_parameters = {
        'symbol' : 'BTC',
        'convert' : 'USD'
    }

    miners_queue = [
        {"miner_key" : "4a2ab507-2988-4dbe-a7f2-beff31b83ddc"},  # thanoskott1@mailinator.com
        {"miner_key" : "478d4caa-b221-480b-8e8d-ea8753ae8b62"},  # thanos_thesis_1@mailinator.com
        {"miner_key" : "669c9c1b-7dd2-42ed-8039-f135f0a8774c"},  # last_lada@mailinator.com
        {"miner_key" : "ed1eedeb-8a9e-4f86-8d51-21ea3c435f87"},  # jose_token@mailinator.com
        {"miner_key" : "5799ff66-6949-4fe9-8e5b-b8c241d0363e"},
        {"miner_key" : "43ab984a-3223-4741-9055-4a277aaced7a"},
        {"miner_key" : "8638f6c2-81b2-4ebb-9d3b-aa2c436c0d74"},
        {"miner_key" : "8c7a061d-4172-4f0c-829d-64f4468726d0"},
    ]

    def requeueMiner(self, minerKey) :
        print("requeueMiner Called")
        self.miners_queue.pop(0)
        miner = {"miner_key" : minerKey}
        self.miners_queue.append(miner)

    async def fetchLatestQuotes(self, key, session: ClientSession, parameters=None) :
        if parameters is None :
            parameters = default_parameters
        headers = {
            'Accepts' : 'application/json',
            'X-CMC_PRO_API_KEY' : key,
        }

        try :
            response = await session.request(method='GET', url=url, params=parameters, headers=headers)
            response.raise_for_status()
            print(f"Response status ({url}): {response.status}")
        except HTTPError as http_err :
            print(f"HTTP error occurred: {http_err}")
        except (ConnectionError, Timeout, TooManyRedirects) as err :
            print(f"An error ocurred: {err}")

        _data = await response.json()
        return _data #json.dumps(_data)

# if __name__ == '__main__':
#     data = fetchLatestQuotes("16b5885f-a0b0-4938-877b-57ed469217db")
#     pprint.pprint(data)
#     print("-------------------- Current Value -------------------")
#     print(data['data']['BTC']['quote']['USD']['price'])
