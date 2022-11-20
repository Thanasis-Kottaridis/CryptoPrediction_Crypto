"""
    Bitstamp API Credentials
    email: thanoskott@gmail.com
    pass:  Test1234!
"""

import hashlib
import hmac
import time
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import uuid
import sys
from json import loads, dumps
import pprint
from repository.bitstampRepo import BitstampRepo
import repository.coinMarketRepo as cmp
import random

miners_queue = [
    {"miner_key" : "ce5b5796-6782-44c0-9393-a3ed26d47362"},  # thanoskott@mailinator.com
    {"miner_key" : "4a2ab507-2988-4dbe-a7f2-beff31b83ddc"},  # thanoskott1@mailinator.com
    {"miner_key" : "478d4caa-b221-480b-8e8d-ea8753ae8b62"},  # thanos_thesis_1@mailinator.com
    {"miner_key" : "669c9c1b-7dd2-42ed-8039-f135f0a8774c"},  # last_lada@mailinator.com
    {"miner_key" : "ed1eedeb-8a9e-4f86-8d51-21ea3c435f87"},  # jose_token@mailinator.com
    {"miner_key" : "5799ff66-6949-4fe9-8e5b-b8c241d0363e"},
    {"miner_key" : "43ab984a-3223-4741-9055-4a277aaced7a"},
    {"miner_key" : "8638f6c2-81b2-4ebb-9d3b-aa2c436c0d74"},
    {"miner_key" : "8c7a061d-4172-4f0c-829d-64f4468726d0"},
]
random.shuffle(miners_queue)


def bitstampExample() :
    api_key = 'api_key'
    API_SECRET = b'api_key_secret'

    timestamp = str(int(round(time.time() * 1000)))
    nonce = str(uuid.uuid4())
    content_type = 'application/x-www-form-urlencoded'
    payload = {'offset' : '1'}

    if sys.version_info.major >= 3 :
        from urllib.parse import urlencode
    else :
        from urllib import urlencode

    payload_string = urlencode(payload)

    # '' (empty string) in message represents any query parameters or an empty string in case there are none
    message = 'BITSTAMP ' + api_key + \
              'POST' + \
              'www.bitstamp.net' + \
              '/api/v2/user_transactions/' + \
              '' + \
              content_type + \
              nonce + \
              timestamp + \
              'v2' + \
              payload_string
    message = message.encode('utf-8')
    signature = hmac.new(API_SECRET, msg=message, digestmod=hashlib.sha256).hexdigest()
    headers = {
        'X-Auth' : 'BITSTAMP ' + api_key,
        'X-Auth-Signature' : signature,
        'X-Auth-Nonce' : nonce,
        'X-Auth-Timestamp' : timestamp,
        'X-Auth-Version' : 'v2',
        'Content-Type' : content_type
    }
    r = requests.post(
        'https://www.bitstamp.net/api/v2/user_transactions/',
        headers=headers,
        data=payload_string
    )
    if not r.status_code == 200 :
        raise Exception('Status code not 200')

    string_to_sign = (nonce + timestamp + r.headers.get('Content-Type')).encode('utf-8') + r.content
    signature_check = hmac.new(API_SECRET, msg=string_to_sign, digestmod=hashlib.sha256).hexdigest()
    if not r.headers.get('X-Server-Auth-Signature') == signature_check :
        raise Exception('Signatures do not match')

    print(r.content)


def bitstampTickerRequest() :
    r = requests.get("https://www.bitstamp.net/api/v2/ticker/{}/".format("btcusd"))
    if not r.status_code == 200 :
        raise Exception('Status code not 200')
    return r.content


def bitstampOHLCRequest() :
    params = {
        "step" : 60,
    }

    try :
        response = requests.get("https://www.bitstamp.net/api/v2/ohlc/{}/".format("btcusd"), params=params)
        _data = loads(response.content)
        return _data
    except (ConnectionError, Timeout, TooManyRedirects) as e :
        print(e)
        return None


if __name__ == '__main__' :
    while True :

        print("------------------ Quotes Latest Keys ------------------")
        print(miners_queue)
        break
        print("------------------ Quotes Latest Response ------------------")
        _key = miners_queue[0]["miner_key"]
        # fetch crypto data
        print("fetch data with key:", _key)
        quotes_latest_response = cmp.fetchLatestQuotes(_key)  # bitstampTickerRequest()
        pprint.pprint(quotes_latest_response)

        print("------------------ Ticker Response ------------------")
        response1 = BitstampRepo.fetchBitstampTicker() #bitstampTickerRequest()
        pprint.pprint(response1)

        print("------------------ OHCL Response2 ------------------")
        response2 = BitstampRepo.fetchBitstampOHLC() #bitstampOHLCRequest()
        pprint.pprint(response2)

        time.sleep(60)
