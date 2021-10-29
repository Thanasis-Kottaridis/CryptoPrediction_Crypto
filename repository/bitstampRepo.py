"""
    Fetch data from Bitstamp API.
    https://www.bitstamp.net/api/

    This API Provides us the following features
    High

    Features        Definitions
    High            The highest price of bitcoin on that one-minute time                        (1m/24h)
    Last            The last price of bitcoin on that one-minute time                           (24h)
    Timestamp       Timestamp server of bitcoin on that one-minute time                         (1m/24h)
    Bid             The highest price pays for a bitcoin on that one-minute time                (24h)
    VWAP            Volume-weighted average price trade on that one-minute time                 (24h)
    Volume          Bitcoin volume on that one-minute time                                      (1m/24h)
    Low             The lowest price of bitcoin on that one-minute time                         (1m/24h)
    Ask             The lowest price ask the seller for a bitcoin on that one-minute time       (24h)
    Open            The open price of bitcoin on that one-minute time                           (1m/24h)
"""

import sys

sys.path.insert(1, "./")

import requests
from requests.exceptions import Timeout, TooManyRedirects, HTTPError

# import async libraries
import asyncio
from aiohttp import ClientSession

# import utils
import json
import utils.utils as utils
import utils.kafkaConnectors as KafkaConnectors


class BitstampRepo(object) :
    currency_pair = "btcusd"

    @classmethod
    def fetchBitstampTicker(cls) :
        try :
            response = requests.get("https://www.bitstamp.net/api/v2/ticker/{}/".format(cls.currency_pair))
            _data = json.loads(response.text)
            _data["timestamp_iso"] = utils.unixToIsoTimestamp(_data["timestamp"])
            return json.dumps(_data)  # _data
        except (ConnectionError, Timeout, TooManyRedirects) as e :
            print(e)
            # KafkaConnectors.publish_log(topic_name=KafkaConnectors.KafkaTopics.RawCryptoTicker.value,
            #                             message=KafkaConnectors.createLogMessage("Exception in fetching Data.", e))
            return None

    @classmethod
    def fetchBitstampOHLC(cls, step=60, limit=1) :

        params = {
            "step" : step,
            "limit" : limit
        }

        try :
            response = requests.get("https://www.bitstamp.net/api/v2/ohlc/{}/".format(cls.currency_pair), params=params)
            _data = json.loads(response.text)
            _data["data"]["ohlc"][0]["timestamp_iso"] = utils.unixToIsoTimestamp(_data["data"]["ohlc"][0]["timestamp"])
            return json.dumps(_data)  # response.text  # _data
        except (ConnectionError, Timeout, TooManyRedirects) as e :
            print(e)
            # KafkaConnectors.publish_log(topic_name=KafkaConnectors.KafkaTopics.RawCryptoOHCL.value,
            #                             message=KafkaConnectors.createLogMessage("Exception in fetching Data.", e))
            return None

    @classmethod
    async def fetchBitstampTickerAsync(cls, session: ClientSession):
        url = "https://www.bitstamp.net/api/v2/ticker/{}/".format(cls.currency_pair)

        try :
            response = await session.request(method='GET', url=url)
            response.raise_for_status()
            print(f"Response status ({url}): {response.status}")
        except HTTPError as http_err :
            print(f"HTTP error occurred: {http_err}")
        except (ConnectionError, Timeout, TooManyRedirects) as err :
            print(f"An error ocurred: {err}")

        _data = await response.json()
        _data["timestamp_iso"] = utils.unixToIsoTimestamp(_data["timestamp"])
        print(_data)
        return _data  #json.dumps(_data)


    @classmethod
    async def fetchBitstampOHLCAsync(cls, session: ClientSession, step=60, limit=1) :

        params = {
            "step" : step,
            "limit" : limit
        }

        url = "https://www.bitstamp.net/api/v2/ohlc/{}/".format(cls.currency_pair)

        try :
            response = await session.request(method='GET', url=url, params= params)
            response.raise_for_status()
            print(f"Response status ({url}): {response.status}")
        except HTTPError as http_err :
            print(f"HTTP error occurred: {http_err}")
        except (ConnectionError, Timeout, TooManyRedirects) as err :
            print(f"An error ocurred: {err}")

        _data = await response.json()
        _data["data"]["ohlc"][0]["timestamp_iso"] = utils.unixToIsoTimestamp(_data["data"]["ohlc"][0]["timestamp"])
        print(_data)
        return _data  # json.dumps(_data)
