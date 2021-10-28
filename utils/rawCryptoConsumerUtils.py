"""
Create a State Machine Consumer:

Use this state machine consumer in order to consume properly
messages from all kafka topics and insert them to mongo as
a single collection item.

The State pattern here is useful because it is scalable and allows as
to add more states (Kafka topic Subscriptions in the future).

"""
import sys
sys.path.insert(1, "./")
from __future__ import annotations

import enum
from time import sleep
from abc import ABC, abstractmethod
import utils
from kafkaConnectors import KafkaTopics


# class RawCryptoConsumerStates(enum.Enum):
#     """
#       This helper enum class is used in order to register all states
#       for raw crypto consumer script
#       """
#     InitState = "INIT_STATE"
#     OHCLFetched = "OHLC_FETCHED"
#     TickerFetched = "TICKER_FETCHED"
#     QuotesFetched = "QUOTES_FETCHED"
#     OHCLAndTickerFetched = "OHLC_AND_TICKER_FETCHED"
#     QuotesAndTickerFetched = "QUOTES_AND_TICKER_FETCHED"
#     OHCLAndQuotesFetched = "OHLC_AND_QUOTES_FETCHED"
#     AllFetched = "ALL_"
#


class RawCryptoConsumerContext :
    __state: RawCryptoConsumerState = None
    __processedCryptoData = {}
    __consumedTopics = []

    def __init__(self, state: RawCryptoConsumerState) -> None :
        self.setState(state)

    def setState(self, state: RawCryptoConsumerState) :

        # store previous state processedCryptoData if exist
        # if self.__state is not None:
        #     self.__processedCryptoData = self.__state.processedCryptoData
            # self.__consumedTopics = self.__state.consumedTopics

        # clear processed Data
        if type(state) is RCCInitState :
            self.__processedCryptoData = {}
            self.__consumedTopics = []

        self.__state = state
        self.__state.context = self
        self.__state.processedCryptoData = self.__processedCryptoData
        self.__state.consumedTopics = self.__consumedTopics

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

    This interface contains all mandatory properties that every state
    needs to implement.
    """

    @property
    def context(self) -> RawCryptoConsumerContext :
        return self.__context

    @property
    def processedCryptoData(self) -> dict:
        return self.__processedCryptoData

    @property
    def consumedTopics(self) -> list:
        return self.__consumedTopics

    @context.setter
    def context(self, context: RawCryptoConsumerContext) -> None :
        """
        Context Setter method.
        :param context:
        :return: Void method (-> None means that is void method)
        """
        self.__context = context

    @processedCryptoData.setter
    def processedCryptoData(self, value) -> None:
        """
        Create a property setter for processedCryptoData
        :param value: a dictionary consisting of data consumed from kafka
        """
        self.__processedCryptoData = value

    @consumedTopics.setter
    def consumedTopics(self, value) -> None:
        """
        Consumed Topics list setter method
        :param value: a list that contains topics that have been consumed
        """
        self.__consumedTopics = value

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
        message = utils.renameDictKeys(message, "Quotes")
        # TODO ADD DATA TO TICKER COLLECTION
        print(message)

        # merge message to __processedCryptoData
        # self.processedCryptoData = {**self.processedCryptoData, **message}
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


class RCCRecursionState(RawCryptoConsumerState):
    """
        Raw Crypto Consumer RecursionState
        This stare calls it self until all data has been consumed
    """

    def consumeTickerMessage(self, message: dict) :
        if KafkaTopics.RawCryptoTicker.value in self.consumedTopics:
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

    def nextState(self):
        if len(self.consumedTopics) == 3:
            self.postToMongo()
        else:
            self.context.setState(RCCRecursionState())

    def postToMongo(self):
        print("put data to Mongo")
        self.context.setState(RCCInitState())


if __name__ == '__main__' :
    # The client code.

    myStateMachine = RawCryptoConsumerContext(RCCInitState())
    myStateMachine.presentState()

    while True:
        x = {'a' : 1, 'b' : 2}
        myStateMachine.consumeQuoteMessage(x)

        y = {'b' : 3, 'c' : 4}
        myStateMachine.consumeTickerMessage(y)

        z = {"d" : "test"}
        myStateMachine.consumeOHCLMessage(z)

        sleep(5)




    # # Up button is pushed
    # myElevator.pushUpBtn()
    #
    # myElevator.presentState(

    # x = {'a' : 1, 'b' : 2}
    # x = utils.renameDictKeys(x, prefix="x")
    # y = {'b' : 3, 'c' : 4}
    # y = utils.renameDictKeys(y, prefix="y")
    #
    # z = {"d" : "test"}
    # z = utils.renameDictKeys(z, prefix="z")
    #
    # # x = {**x, **y}
    # x.update(y)
    # print("merge x and y")
    # print(x)
    # print("merge x and z")
    # # x = {**x, **z}
    # x.update(z)
    # print(x)
