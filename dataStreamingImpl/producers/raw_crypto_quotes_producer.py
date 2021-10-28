"""
-------------- Production Workers -----------------------
test temp mail: fahexi7956@sicmag.com
name: Thanasis Prodmail1
pass: Test1234!
secret-key: 474b8679-0b93-46ce-97a9-5500f9b95cc8

test temp mail: maveh96370@sicmag.com
name: Thanasis Prodmail2
pass: Test1234!
secret-key: 5799ff66-6949-4fe9-8e5b-b8c241d0363e

test temp mail: gamal45144@silbarts.com
name: Thanasis Prodmail3
pass: Test1234!
secret-key: 43ab984a-3223-4741-9055-4a277aaced7a

test temp mail: letejaf248@silbarts.com
name: Thanasis Prodmail4
pass: Test1234!
secret-key: 8638f6c2-81b2-4ebb-9d3b-aa2c436c0d74

test temp mail: venihe9875@sicmag.com
name: Thanasis Prodmail5
pass: Test1234!
secret-key: 8c7a061d-4172-4f0c-829d-64f4468726d0
"""

# Imports
"""
in order to import utils properly
- run all scripts from main directory
- add the following lines to sys

A beater solution is to add main directory in PYTHONPATH variable (this will be done on VM)
"""
import sys

sys.path.insert(1, "./")

import sched
import time
import threading
from time import sleep
import repository.coinMarketRepo as cmp
import utils.kafkaConnectors as KafkaConnectors

miners_queue = [
    {"miner_key" : "474b8679-0b93-46ce-97a9-5500f9b95cc8"},
    {"miner_key" : "5799ff66-6949-4fe9-8e5b-b8c241d0363e"},
    {"miner_key" : "43ab984a-3223-4741-9055-4a277aaced7a"},
    {"miner_key" : "8638f6c2-81b2-4ebb-9d3b-aa2c436c0d74"},
    {"miner_key" : "8c7a061d-4172-4f0c-829d-64f4468726d0"},
]


def requeueMiner(minerKey) :
    print("requeueMiner Called")
    miners_queue.pop(0)
    miner = {"miner_key" : minerKey}
    miners_queue.append(miner)


def produceMessage(scheduler) :
    producer = KafkaConnectors.connectKafkaProducer()
    _key = miners_queue[0]["miner_key"]
    # fetch crypto data
    print("fetch data with key:", _key)
    data = cmp.fetchLatestQuotes(_key)
    # update miners queue
    requeueMiner(_key)

    # check if we received data from response
    if data is not None :
        # publish message to topic
        KafkaConnectors.publish_message(producer, KafkaConnectors.KafkaTopics.RawCryptoQuotes.value, 'raw', data)
        scheduler.enter(60, 1, produceMessage, (scheduler,))
    else :
        # run the function again
        produceMessage(scheduler)


# if __name__ == '__main__' :
#     """
#         Start a scheduler that fires every 60 seconds
#     """
#     s = sched.scheduler(time.time, time.sleep)
#     s.enter(60, 1, produceMessage, (s,))
#     s.run()

if __name__ == '__main__' :
    # event = threading.Event()
    # while True :
    producer = KafkaConnectors.connectKafkaProducer()
    _key = miners_queue[0]["miner_key"]
    # fetch crypto data
    print("fetch data with key:", _key)
    data = cmp.fetchLatestQuotes(_key)
    # update miners queue
    requeueMiner(_key)

    # check if we received data from response
    if data is not None :
        # publish message to topic
        KafkaConnectors.publish_message(producer, KafkaConnectors.KafkaTopics.RawCryptoQuotes.value, 'raw', data)
        # event.wait(60)
