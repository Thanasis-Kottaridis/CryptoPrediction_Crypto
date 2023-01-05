"""
This Script Performs The following
1. Consume Kafka Topic `PROCESSED_CRYPTO_DATA`
2. Get Reddit Posts from API
    #sos: Reddit API returns posts in Batches of 100
    so we have to iterate in order to fetch all posts.
3. Perform NLP to new Posts
4. Create / Update Reddit Posts Collection
5. Get Polarity for the posts
6. Update Bitcoin raw data

# TODO Create StateMachine
Create a State Machine to perform the following:
"""

from __future__ import annotations
from abc import ABC, abstractmethod
import sys
from os import path

from bson import ObjectId

sys.path.insert(1, "./")

# Import Utils
import utils.utils as utils
from utils.kafkaConnectors import KafkaTopics, BOOTSTRAP_SERVERS
from utils import kafkaConnectors, mongoConnector
from kafka import KafkaConsumer

# Base imports
import time
import pymongo
import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'
from pprint import pprint
from json import loads, dumps

# Reddit API Imports
import praw
from pmaw import PushshiftAPI

# sentiment preprocessing
import string
import re
import emoji

# sentiment analysis
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem import WordNetLemmatizer, PorterStemmer
from nltk.tokenize import word_tokenize, RegexpTokenizer  # tokenize words
from nltk.corpus import stopwords

"""
    Is test Impl property
"""
isTestImpl = False
mockKafkaMessage = {
    "id": "638299c354f3f9b1a17dc8ea",
    "timestamp": "1669503426"
}


class RedditNLPContext:
    """
    Context Vars.
    """
    _state: RedditNLPState = None
    client = None
    db = None

    """
    Private Context vars.
    """
    __kafkaMessage = None
    __targetRecordId = None
    __targetRecordTimestamp = None

    """
    Private Context var getters.
    """

    @property
    def kafkaMessage(self):
        return self.kafkaMessage

    @property
    def targetRecordId(self):
        return self.__targetRecordId

    @property
    def targetRecordTimestamp(self):
        return self.__targetRecordTimestamp

    def __init__(self, kafkaMessage) -> None:
        self.__kafkaMessage = kafkaMessage
        self.__parseKafkaMessage(kafkaMessage)

        # print kafka message payload
        print(f"id: {self.targetRecordId} timestamp: {self.targetRecordTimestamp}")

        self.client, self.db = mongoConnector.connectMongoDB()
        # Set Initial State.
        self.setState(GetRedditPostsState())

    def setState(self, state: RedditNLPState):
        print(f"Context: Transitioning to {type(state).__name__}")
        self._state = state
        self._state.context = self
        self._state.didEnter()

    def presentState(self):
        print(f"My Current State is {type(self._state).__name__}")

    """
    Private helper valus
    """

    def __parseKafkaMessage(self, kafkaMessage):
        # TODO implement this method
        _data = kafkaMessage
        self.__targetRecordId = _data["id"]
        self.__targetRecordTimestamp = _data["timestamp"]
        return _data


class RedditNLPState(ABC):
    _context: RedditNLPContext = None

    @property
    def context(self) -> RedditNLPContext:
        return self._context

    @context.setter
    def context(self, context: RedditNLPContext) -> None:
        self._context = context

    @abstractmethod
    def didEnter(self) -> None:
        # Default Implementation of method Next Step.
        pass

    @abstractmethod
    def nextState(self) -> None:
        # Default Implementation of method Next Step.
        pass


class GetRedditPostsState(RedditNLPState):
    """
    Private Const VARS
    """
    # Store Type
    # True => Store in monge
    # False => Store in CSV.
    __shouldStoreInMongo = True

    # Target Timestamp
    # Friday, October 29, 2021 5:07:29 PM
    __TARGET_TIMESTAMP = 1635527249

    # data file name:
    __file_name = "reddit_crypto_test_data.csv"  # "reddit_crypto_data.csv"

    # REDDIT AUTH
    __CLIENT_ID = "IcTrWsQDFCcZEe3rWrlB4A"

    __SECRET_KEY = 'HZQy-nneDNv4THu_G8MhVJ96KOq4cg'

    """
    Abstract State methods IMPL
    """

    def didEnter(self) -> None:
        # download nltk lexicon before process starts
        self.__downloadNLTKLexicons()
        self.__fetchRedditPosts()
        self.nextState()

    def nextState(self) -> None:
        self.context.setState(UpdateBitcoinDataState())
        pass

    """
    Private Helper Funcs
    """

    @classmethod
    def __downloadNLTKLexicons(cls):
        # Downloading NLTK’s databases
        nltk.download('vader_lexicon')  # get lexicons data
        nltk.download('punkt')  # Pre-trained models that help us tokenize sentences.
        nltk.download('stopwords')

    def __fetchRedditPosts(self) -> None:
        """
        This method is used to collect all reddit posts related to Crypto
        from a target time until current time.
        It uses PRAW library that contains reddit post History PushshiftAPI
        Reddit API Returns posts in butches of 100

        After Collecting post it apples
        1. Sentiment Analysis using __dataPreprocessing() method
        2. And then extracts polarity using __getPolarity() method

        finaly stores posts to Mongo or in CSV.
        """
        # get mongo client
        collection = self.context.db.reddit_crypto_data

        # connect to reddit api
        reddit = praw.Reddit(
            client_id=self.__CLIENT_ID,
            client_secret=self.__SECRET_KEY,
            user_agent='MyBot/0.0.1'
        )

        api = PushshiftAPI(praw=reddit)

        print("-------- CURRENT UNIX --------")
        print(f"{time.time()}")
        print("--------------------------------")

        FROM_TIMESTAMP = int(time.time())

        if self.__shouldStoreInMongo:
            """
                @SOS!!!!
                Change target timestamp to most recent post stored in MONGO.
                If documents exists in mongo that's mean that this script has run again with flag shouldStoreInMongo = True
                so we need to insert to mongo only reddit posts performed after last run.
            """
            cursor = collection.find().sort("created_unix", pymongo.DESCENDING).limit(1)
            for doc in cursor:
                self.__TARGET_TIMESTAMP = int(doc["created_unix"])

        # Use This if you want to write in .CSV
        # initialize dataframe
        df = pd.DataFrame()

        while True:

            # initialize dataframe
            df = pd.DataFrame()

            # The `search_comments` and `search_submissions` methods return generator objects
            try:
                gen = api.search_submissions(
                    before=int(FROM_TIMESTAMP),
                    after=int(self.__TARGET_TIMESTAMP),
                    subreddit="Bitcoin",
                    sort="desc",
                    limit=100
                )
            except:
                print("Empty response no more data")
                break

            # check if response has data
            results = list(gen)
            if len(results) == 0:
                print("Empty response no more data")
                break

            for post in results:
                # append relevant data to dataframe

                df = df.append({
                    'id': post['id'],
                    'subreddit': str(post['subreddit']),
                    'title': post['title'],
                    'fullname': post['name'],
                    'selftext': post['selftext'],
                    'upvote_ratio': post['upvote_ratio'],
                    'ups': post['ups'],
                    'downs': post['downs'],
                    'score': post['score'],
                    'created_iso': utils.utc_to_datetime(post['created']),
                    'created_unix': post['created']
                }, ignore_index=True)

            # Preform NLP Analysis for reddit batch.
            # - First perform preprocessing
            # - And then NLP using VENDER.
            df = self.__dataPreprocessing(df)
            df = self.__getPolarity(df)

            # calculate weighted polarity
            df['weighted_polarity'] = df["compound"] * df['score']

            # get last subreddit created time
            created_unix = int(df.iloc[-1:].created_unix)

            print("-----------------------------------------------")
            print(f"POSTS COUNT: {len(df.index)}")
            print(f"created_unix: {created_unix}, TARGET_TIMESTAMP: {self.__TARGET_TIMESTAMP}")
            print("-----------------------------------------------")

            if created_unix <= self.__TARGET_TIMESTAMP:
                print(f"created_unix <= TARGET_TIMESTAMP: True")
                break
            else:
                FROM_TIMESTAMP = created_unix

            if self.__shouldStoreInMongo:
                # write to Mongo
                print("Insert to Mongo")
                collection.insert_many(df.to_dict('records'))
            else:
                # write df to CSV
                print("Store DF")
                df.to_csv(
                    self.__file_name,
                    mode='a',
                    index=False,
                    header=False,
                    sep='\t',
                    encoding='utf-8'
                )

    @classmethod
    def __dataPreprocessing(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
               @TODO Data Cleanning
               @Remove:
               - Check if needed to remove [removed] or [deleted] posts.
               - The post contains “give away” or “giving away”.
               - The post contains “pump”, “register”, or “join”.
               - The post contains more than 14 hashtags
               - The post contains more than 14 ticker symbols.
           """
        # ensure that all titles are Str
        df['title'] = df['title'].astype(str)

        # 1. Remove URLS
        df['clean_title'] = df['title'].apply(lambda x: re.sub(r"http\S+", '', x))

        # 2. remove punctuation from title.
        # df['clean_title'] = df['clean_title'].apply(lambda x: removePunctuation(x))

        # # 3. Remove all the special characters
        # df['clean_title'] = df['clean_title'].apply(lambda x: re.sub(r'\w+', '', x))

        # 3. Remove all emoji
        df['clean_title'] = df['clean_title'].apply(lambda x: emoji.get_emoji_regexp().sub(u'', x))

        # 4. remove all single characters
        df['clean_title'] = df['clean_title'].apply(lambda x: re.sub(r'\s+[a-zA-Z]\s+', '', x))

        # 5. Substituting multiple spaces with single space
        df['clean_title'] = df['clean_title'].apply(lambda x: re.sub(r'\s+', ' ', x))

        # 6. make text to lowercase and tokenize the text
        # df['clean_title'] = df['clean_title'].apply(lambda x: tokenize(x.lower()))

        # 7. remove stop words
        # df['clean_title'] = df['clean_title'].apply(lambda x: removeStopWords(x))

        # 8. Lemmatize / Stem
        # df['clean_title'] = df['clean_title'].apply(lambda x: lemmatizeAndStemming(x))

        # 9. Join tokens in to sentence
        # df['clean_title'] = df['clean_title'].apply(lambda x: " ".join(x))

        # 10. Replace empty titles with NaN
        df['clean_title'].replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df['clean_title'].replace('', np.nan, regex=True, inplace=True)

        # 11. Drop NaN processed titles.
        df = df[df['clean_title'].notna()]

        return df

    @classmethod
    def __getPolarity(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
            This helper function is used in order to get polarity of all reddit post titles in a given DF.
            polarity is calculated using NLTK and VADER analyzer.

            :param df: dataframe filed with preprocessed reddit posts
            :return: a dataframe that contains posts with their polarity and their label.
            """

        # Initialize Sentiment  Analyzer
        sid = SentimentIntensityAnalyzer()

        # get polarity of each post
        title_res = [*df['clean_title'].apply(sid.polarity_scores)]
        comment_res = [*df['clean_title'].apply(sid.polarity_scores)]
        sentiment_df = pd.DataFrame.from_records(title_res)
        pprint(sentiment_df.head())

        # add polarity columns to DF.
        df = pd.concat([df, sentiment_df], axis=1, join='inner')
        pprint(df.head())

        # Choose labeling threshold
        THRESHOLD = 0.02

        conditions = [
            (df['compound'] <= -THRESHOLD),
            (df['compound'] > -THRESHOLD) & (df['compound'] < THRESHOLD),
            (df['compound'] >= THRESHOLD),
        ]

        # label posts
        values = ["neg", "neu", "pos"]
        df['label'] = np.select(conditions, values)

        return df


class UpdateBitcoinDataState(RedditNLPState):
    # Unix Time Consts
    __ONE_MINUTE_SECONDS = 60
    __weightedRedditPolarity = None

    def didEnter(self) -> None:
        # 1. get Weighted Reddit Polarity for posts in range
        #   from record timestamp plus 30 minutes
        self.__weightedRedditPolarity = self.__getWeightedRedditPolarity(
            timeFrom=int(self.context.targetRecordTimestamp),
            timeRange=self.__ONE_MINUTE_SECONDS * 30
        )

        print(self.__weightedRedditPolarity)

        # 2. (Optional) update mongo record with weighted Polarity TODO USE THIS TO UPDATE IMMEDIATE MONGO RECORD
        # self.__updateCryptoRecord(self.__weightedRedditPolarity)

        # 3. enter next state
        self.nextState()

    def nextState(self) -> None:
        self.context.setState(ProduceKafkaMessage(self.__weightedRedditPolarity))

    """
    Private Helper Functions
    """

    def __getRedditPostsInRange(self, timeFrom: int, timeRange: int) -> pd.DataFrame:
        redditCollection = self.context.db.reddit_crypto_data

        # vres ta posts pou eginan tin teleftea misi wra
        toTime = timeFrom - timeRange
        redditPosts = redditCollection.find({'created_unix': {'$lt': timeFrom, '$gte': toTime}})
        print(redditPosts.count())
        return pd.DataFrame(list(redditPosts))

    def __getWeightedRedditPolarity(self, timeFrom: int, timeRange: int) -> int:

        # filter results df
        resultsDf = self.__getRedditPostsInRange(timeFrom, timeRange)

        if len(resultsDf.index) == 0:
            return 0

        scoreSum = resultsDf["score"].sum()
        print("-------- weighted_polarity calculated --------")
        if scoreSum == 0:
            res = resultsDf['weighted_polarity'].sum()
            print(f"-------- {res} -------- \n")
            return res
        else:
            res = resultsDf['weighted_polarity'].sum() / resultsDf["score"].sum()
            print(f"-------- {res} -------- \n")
            return res

    def __updateCryptoRecord(self, weightedRedditPolarity):
        """
        Use this function if you want to immediately update mongo record with polarity.
        :param weightedRedditPolarity:
        :return:
        """
        # get mongo collection
        collection = self.context.db.processed_crypto_data
        mongoId = ObjectId(self.context.targetRecordId)

        # find record
        record = collection.find_one(mongoId)
        record['reddit_compound_polarity'] = weightedRedditPolarity

        # update record by id
        # upsert = False will update doc instead of inserting new one
        collection.update_one({'_id': mongoId}, {"$set": record}, upsert=False)


class ProduceKafkaMessage(RedditNLPState):
    __weightedRedditPolarity = None

    def __init__(self, weightedRedditPolarity) -> None:
        self.__weightedRedditPolarity = weightedRedditPolarity

    def didEnter(self) -> None:
        # 1. create kafka producer
        producer = kafkaConnectors.connectKafkaProducer()

        # 2. Create Message payload
        kafkaMessagePayload = {
            "id": self.context.targetRecordId,
            "timestamp": self.context.targetRecordTimestamp,
            "weightedRedditPolarity": self.__weightedRedditPolarity
        }
        _kafkaMessage = dumps(kafkaMessagePayload)

        # 3. Produce message that NLP preprocessing finished
        kafkaConnectors.publish_message(
            producer,
            kafkaConnectors.KafkaTopics.CompleteCryptoNLP.value,
            'kafkaMessage', _kafkaMessage
        )

        # 4. call next state (For future usage)
        self.nextState()

    def nextState(self) -> None:
        print("----------------------------")
        print(f"NLP preprocessing of record {self.context.targetRecordId} Completed Successfully!!!")
        print("----------------------------")
        pass


if __name__ == '__main__':
    # test implementation
    if isTestImpl:
        myStateMachine = RedditNLPContext(kafkaMessage=mockKafkaMessage)
        myStateMachine.presentState()
        exit()
    else:
        # Actual Implementation.
        # set up kafka raw crypto quotes consumer
        consumer = KafkaConsumer(
            kafkaConnectors.KafkaTopics.ProcessedCryptoData.value,
            bootstrap_servers=kafkaConnectors.BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: loads(x.decode('utf-8')))

        # read consumer messages
        for message in consumer:
            kafkaMessage = message.value
            myStateMachine = RedditNLPContext(kafkaMessage=kafkaMessage)
            myStateMachine.presentState()
