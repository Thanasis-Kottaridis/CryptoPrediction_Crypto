"""
Create a State Machine Consumer:

Use this state machine consumer in order to consume properly
messages from all kafka topics and insert them to mongo as
a single collection item.

The State pattern here is useful because it is scalable and allows as
to add more states (Kafka topic Subscriptions in the future).

"""
from __future__ import annotations
import sys

sys.path.insert(1, "./")

import enum
from pymongo.collection import Collection
from time import sleep
from json import loads
from abc import ABC, abstractmethod
import utils.utils as utils
from utils.kafkaConnectors import KafkaTopics, BOOTSTRAP_SERVERS
from utils import mongoConnector
from kafka import KafkaConsumer

"""
    Is test Impl property
"""
isTestImpl = False
topicsList = [KafkaTopics.RawCryptoQuotes.value,
              KafkaTopics.RawCryptoTicker.value,
              KafkaTopics.RawCryptoOHCL.value]


class RawCryptoConsumerContext :
    """
    State Machine Context Class:
    This Class contains all properties that will be shared accross all
    State Classes like:
    - __state: This is the state object of the current state.
    - __processedCryptoData: This object contains the preprocessed responses that
      have been merged in the same json.
    - __consumedTopics: A list with the keys of the topics that have been consumed.
      In order to finish preprocessing we have to consume the following topics:
      "RAW_CRYPTO_QUOTES"
      "RAW_CRYPTO_OHLC"
      "RAW_CRYPTO_TICKER"
      "RAW_CRYPTO_TWEETS"
    """
    __state: RawCryptoConsumerState = None
    __processedCryptoData = {}
    __consumedTopics = []
    __mongoCollection = None

    def __init__(self, state: RawCryptoConsumerState) -> None :
        # get mongo client
        client, db = mongoConnector.connectMongoDB()
        self.__mongoCollection = db.crypto_test if isTestImpl else db.processed_crypto_data
        # set up state
        self.setState(state)

    def setState(self, state: RawCryptoConsumerState) :
        # clear processed Data
        if type(state) is RCCInitState :
            self.__processedCryptoData = {}
            self.__consumedTopics = []

        self.__state = state
        self.__state.context = self
        self.__state.processedCryptoData = self.__processedCryptoData
        self.__state.consumedTopics = self.__consumedTopics
        self.__state.mongoCollection = self.__mongoCollection

    def presentState(self) :
        print(f"My Current State is {type(self.__state).__name__}")

    def consumeTickerMessage(self, message) :
        self.__state.consumeTickerMessage(message=message)

    def consumeOHCLMessage(self, message) :
        self.__state.consumeOHCLMessage(message=message)

    def consumeQuoteMessage(self, message) :
        self.__state.consumeQuoteMessage(message=message)


class RawCryptoConsumerState(ABC) :
    """
    RawCryptoConsumer State Interface

    This interface contains all mandatory properties and methods that every state
    needs to implement.
    """

    @property
    def context(self) -> RawCryptoConsumerContext :
        return self.__context

    @property
    def processedCryptoData(self) -> dict :
        return self.__processedCryptoData

    @property
    def consumedTopics(self) -> list :
        return self.__consumedTopics

    @property
    def mongoCollection(self) -> Collection :
        return self.__mongoCollection

    @context.setter
    def context(self, context: RawCryptoConsumerContext) -> None :
        """
        Context Setter method.
        :param context:
        :return: Void method (-> None means that is void method)
        """
        self.__context = context

    @processedCryptoData.setter
    def processedCryptoData(self, value) -> None :
        """
        Create a property setter for processedCryptoData
        :param value: a dictionary consisting of data consumed from kafka
        """
        self.__processedCryptoData = value

    @consumedTopics.setter
    def consumedTopics(self, value) -> None :
        """
        Consumed Topics list setter method
        :param value: a list that contains topics that have been consumed
        """
        self.__consumedTopics = value

    @mongoCollection.setter
    def mongoCollection(self, value) :
        self.__mongoCollection = value

    @abstractmethod
    def consumeTickerMessage(self, message: dict) :
        """

        :param message:
        :return: Processed Ticker Message
        """

        # rename message properties
        message = utils.renameDictKeys(message, "Ticker")
        # TODO ADD DATA TO TICKER COLLECTION
        print(message)

        # merge message to __processedCryptoData
        # self.processedCryptoData = {**self.processedCryptoData, **message}
        self.processedCryptoData.update(message)
        print(self.processedCryptoData)

        # update consumed topics list
        self.consumedTopics.append(KafkaTopics.RawCryptoTicker.value)
        print(self.consumedTopics)

    @abstractmethod
    def consumeOHCLMessage(self, message: dict) :
        """

        :param message:
        :return: Processed OHCL Message
        """

        # rename message properties
        message = utils.renameDictKeys(message, "OHCL")
        # TODO ADD DATA TO TICKER COLLECTION
        print(message)

        # merge message to processedCryptoData
        # self.processedCryptoData = {**self.processedCryptoData, **message}
        self.processedCryptoData.update(message)
        print(self.processedCryptoData)

        # update consumed topics list
        self.consumedTopics.append(KafkaTopics.RawCryptoOHCL.value)
        print(self.consumedTopics)

    @abstractmethod
    def consumeQuoteMessage(self, message: dict) :
        """

        :param message:
        :return: Processed OHCL Message
        """

        # rename message properties
        data = message['data']
        message = utils.renameDictKeys(data, "Quotes")
        self.processedCryptoData.update(message)
        print(self.processedCryptoData)

        # update consumed topics list
        self.consumedTopics.append(KafkaTopics.RawCryptoQuotes.value)
        print(self.consumedTopics)


class RCCInitState(RawCryptoConsumerState) :
    """
    Raw Crypto Consumer Init state Class
    """

    def consumeTickerMessage(self, message) :
        super(RCCInitState, self).consumeTickerMessage(message)
        self.context.setState(RCCRecursionState())

    def consumeOHCLMessage(self, message) :
        super(RCCInitState, self).consumeOHCLMessage(message)
        self.context.setState(RCCRecursionState())

    def consumeQuoteMessage(self, message) :
        super(RCCInitState, self).consumeQuoteMessage(message)
        self.context.setState(RCCRecursionState())


class RCCRecursionState(RawCryptoConsumerState) :
    """
        Raw Crypto Consumer RecursionState
        This stare calls it self until all data has been consumed
    """

    def consumeTickerMessage(self, message: dict) :
        if KafkaTopics.RawCryptoTicker.value in self.consumedTopics :
            print("Something went wrong while consuming messages.\n This Topic {} has already been consumed"
                  .format(KafkaTopics.RawCryptoTicker.value))
            return

        super(RCCRecursionState, self).consumeTickerMessage(message)
        self.nextState()

    def consumeOHCLMessage(self, message: dict) :
        if KafkaTopics.RawCryptoOHCL.value in self.consumedTopics :
            print("Something went wrong while consuming messages.\n This Topic {} has already been consumed"
                  .format(KafkaTopics.RawCryptoOHCL.value))
            return

        super(RCCRecursionState, self).consumeOHCLMessage(message)
        self.nextState()

    def consumeQuoteMessage(self, message: dict) :
        if KafkaTopics.RawCryptoQuotes.value in self.consumedTopics :
            print("Something went wrong while consuming messages.\n This Topic {} has already been consumed"
                  .format(KafkaTopics.RawCryptoQuotes.value))
            return

        super(RCCRecursionState, self).consumeQuoteMessage(message)
        self.nextState()

    def nextState(self) :
        if len(self.consumedTopics) == len(topicsList) :
            self.postToMongo()
        else :
            self.context.setState(RCCRecursionState())

    def postToMongo(self) :
        self.mongoCollection.insert_one(self.processedCryptoData)
        print('message added to {}'.format(self.mongoCollection))
        self.context.setState(RCCInitState())


def allTopicsConsumerTest() :
    """
    Use this Code to Test if consumer state machine works correctly.
    :return: Void
    """
    while True :
        x = {'a' : 1, 'b' : 2}
        myStateMachine.consumeQuoteMessage(x)

        y = {'b' : 3, 'c' : 4}
        myStateMachine.consumeTickerMessage(y)

        z = {"d" : "test"}
        myStateMachine.consumeOHCLMessage(z)

        sleep(5)


def allTopicsConsumerImpl() :
    # setup up kafka consumer for multiple topics
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x : loads(x.decode('utf-8')))
    consumer.subscribe(topicsList)

    # read consumer messages
    for message in consumer :
        if message.topic == KafkaTopics.RawCryptoQuotes.value :
            myStateMachine.consumeQuoteMessage(message.value)
        elif message.topic == KafkaTopics.RawCryptoTicker.value :
            myStateMachine.consumeTickerMessage(message.value)
        elif message.topic == KafkaTopics.RawCryptoOHCL.value :
            myStateMachine.consumeOHCLMessage(message.value['data']['ohlc'][0])
        else:
            print("UNKNOWN TOPIC CONSUMED")


if __name__ == '__main__' :
    # The client code.

    myStateMachine = RawCryptoConsumerContext(RCCInitState())
    myStateMachine.presentState()

    if isTestImpl:
        # TEST IMPL
        allTopicsConsumerTest()
    else:
        # Actual IMPL
        allTopicsConsumerImpl()
