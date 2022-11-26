# Imports
import sys

sys.path.insert(1, "./")

from json import loads, dumps
from kafka import KafkaConsumer
from utils import kafkaConnectors, mongoConnector


def parseQuoteData(_data) :
    # TODO implement this method
    _data = _data
    return _data


if __name__ == '__main__' :
    # setp up kafka raw crypto quotes consumer
    consumer = KafkaConsumer(
        kafkaConnectors.KafkaTopics.ProcessedCryptoData.value,
        bootstrap_servers=kafkaConnectors.BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x : loads(x.decode('utf-8')))

    # get mongo client
    # client, db = mongoConnector.connectMongoDB()
    # collection = db.raw_btc_quotes

    # read consumer messages
    for message in consumer :
        data = message.value
        parsedData = parseQuoteData(data)

        # Print message Data.
        # 1669477801
        print(f"kafka message: {parsedData}")

        # collection.insert_one(message.value)
        # print('message added to {}'.format(collection))
