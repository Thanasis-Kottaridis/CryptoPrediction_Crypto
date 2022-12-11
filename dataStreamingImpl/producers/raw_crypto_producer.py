import json
import random
import sys
sys.path.insert(1, "./")

import asyncio
import time
from utils import mongoConnector
import utils.kafkaConnectors as KafkaConnectors
from repository.bitstampRepo import BitstampRepo
from repository.coinMarketRepo import CoinMarketRepo
from aiohttp import ClientSession
import utils.utils as utils

"""
Const vals
"""
delay = 60
processedCryptoData = {}


async def fetchRawCryptoQuotes(session: ClientSession):
    _key = random.choice(coinMarketRepo.miners_queue)["miner_key"]
    # fetch crypto data
    print("fetch data with key:", _key)
    _data = await coinMarketRepo.fetchLatestQuotes(_key, session)
    # update miners queue
    coinMarketRepo.requeueMiner(_key)
    # check if we received data from response
    if _data is not None :
        # preprocess message
        message = utils.renameDictKeys(_data['data'], "Quotes")
        processedCryptoData.update(message)


async def fetchRawCryptoTicker(session):
    _data = await BitstampRepo.fetchBitstampTickerAsync(session)
    if _data is not None :
        # preprocess message
        message = utils.renameDictKeys(_data, "Ticker")
        processedCryptoData.update(message)


async def fetchRawCryptoOHLC(session):
    _data = await BitstampRepo.fetchBitstampOHLCAsync(session)
    if _data is not None :
        # preprocess message
        message = utils.renameDictKeys(_data['data']['ohlc'][0], "OHLC")
        processedCryptoData.update(message)


async def main() :
    async with ClientSession() as session :
        task_quotes = asyncio.create_task(fetchRawCryptoQuotes(session))
        task_ticker = asyncio.create_task(fetchRawCryptoTicker(session))
        task_ohlc = asyncio.create_task(fetchRawCryptoOHLC(session))

        print(f"started at {time.strftime('%X')}")

        await task_quotes
        await task_ticker
        await task_ohlc

        global processedCryptoData
        print(processedCryptoData)
        result = mongoCollection.insert_one(processedCryptoData)

        # 1. Create KAFKA Payload that containts
        #   - document _id
        #   - and timestamp
        #
        # 2. Convert it to JSON
        #
        # 3. Publish Message

        kafkaMessagePayload = {
            "id": str(result.inserted_id),
            "timestamp": processedCryptoData["Ticker_timestamp"]
        }
        kafkaMessage = json.dumps(kafkaMessagePayload)

        KafkaConnectors.publish_message(
            producer,
            KafkaConnectors.KafkaTopics.ProcessedCryptoData.value,
            'kafkaMessage', kafkaMessage
        )

        print("inserted doc id: {}".format(result.inserted_id))
        processedCryptoData = {}
        print(f"finished at {time.strftime('%X')}")

        await asyncio.sleep(delay)


if __name__ == '__main__':
    # create kafka producer
    producer = KafkaConnectors.connectKafkaProducer()

    # create mongo connection
    client, db = mongoConnector.connectMongoDB()
    mongoCollection = db.processed_crypto_data

    # inject coinMarketCap repo
    coinMarketRepo = CoinMarketRepo()

    while True:
        print("Fetch data called")
        asyncio.run(main())


