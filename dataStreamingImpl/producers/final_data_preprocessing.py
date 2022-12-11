import sys
sys.path.insert(1, "./")

# Import Utils
import utils.utils as utils
from utils.kafkaConnectors import KafkaTopics, BOOTSTRAP_SERVERS
from utils import kafkaConnectors, mongoConnector
from kafka import KafkaConsumer

import numpy as np
import pandas as pd
from pprint import pprint
import json
from bson.json_util import loads, dumps
from bson import ObjectId

# Visualization Imports
import seaborn as sns
import matplotlib.pyplot as plt


class FinalDataProcessor:

    db: object
    cryptoData: list = None
    cryptoDataIDs: list = None
    cryptoDataDf: pd.DataFrame = None

    # Consts:
    isTestImpl = False
    shouldUseMongoData = True  # If true fetch data from processed_crypto_data Collection Else fetch from local CSV
    shouldCreateCSV = True  # if true store flatten data to CSV file
    doPlots = False
    file_name = "flatten_crypto_data.csv"
    # Unix Time Consts
    ONE_MINUTE_SECONDS = 60  # 60 seconds

    """
    Private Const VARS
    """

    __kafkaMessage = None
    __targetRecordId = None
    __targetRecordTimestamp = None
    __weightedRedditPolarity = None

    @property
    def __propertyDict(self):
        return {
                "OHLC_close" : "close_1min",
                "OHLC_high" : "high_1min",
                "OHLC_low" : "low_1min",
                "OHLC_open" : "open_1min",
                # "OHLC_timestamp",  # not need this
                # "OHLC_timestamp_iso",  # not need this
                "OHLC_volume" : "volume_1min",
                "Quotes_BTC_max_supply" : "max_supply",
                # "Quotes_BTC_cmc_rank", # not need this
                # "Quotes_BTC_date_added", # not need this
                # "Quotes_BTC_id", # not need this
                # "Quotes_BTC_is_active", # not need this.
                # "Quotes_BTC_is_fiat", # not need this
                # "Quotes_BTC_last_updated", # not need this
                "Quotes_BTC_quote_USD_fully_diluted_market_cap" : "fully_diluted_market_cap",
                "Quotes_BTC_circulating_supply" : "circulating_supply",
                # "Quotes_BTC_name", # not need this
                # "Quotes_BTC_num_market_pairs", # ????
                # "Quotes_BTC_platform", # not need this,
                "Quotes_BTC_quote_USD_market_cap" : "market_cap",
                # "Quotes_BTC_quote_USD_last_updated", # Keep one timestamp in Unix format
                "Quotes_BTC_quote_USD_market_cap_dominance" : "market_cap_dominance",
                "Quotes_BTC_quote_USD_percent_change_1h" : "percent_change_1h",
                "Quotes_BTC_quote_USD_percent_change_24h" : "percent_change_24h",
                "Quotes_BTC_quote_USD_percent_change_30d" : "percent_change_30d",
                "Quotes_BTC_quote_USD_percent_change_60d" : "percent_change_60d",
                "Quotes_BTC_quote_USD_percent_change_7d" : "percent_change_7d",
                "Quotes_BTC_quote_USD_percent_change_90d" : "percent_change_90d",
                "Quotes_BTC_quote_USD_price" : "quote_USD_price",
                "Quotes_BTC_quote_USD_volume_24h" : "quote_volume_24h",
                "Quotes_BTC_quote_USD_volume_change_24h" : "volume_change_24h",
                # "Quotes_BTC_tags", # not need.
                "Quotes_BTC_total_supply" : "total_supply",
                "Ticker_ask" : "ask_24h",  # OHLC ask last 24h
                "Ticker_bid" : "bid_24h",  # OHLC bid last 24h
                "Ticker_high" : "high_24h",  # OHLC high last 24h
                "Ticker_last" : "last_24h",  # OHLC last last 24h
                "Ticker_low" : "low_24h",  # OHLC low last 24h
                "Ticker_open" : "open_24h",  # OHLC open last 24h
                "Ticker_timestamp" : "unix_timestamp",
                # "Ticker_timestamp_iso",  # Keep one timestamp in Unix format
                "Ticker_volume" : "volume_24h",  # OHLC volume last 24h
                "Ticker_vwap" : "vwap_24h",  # OHLC vwap last 24h
                "reddit_compound_polarity": "reddit_compound_polarity"
            }

    def __init__(self, kafkaMessage) -> None :
        self.__kafkaMessage = kafkaMessage
        self.__parseKafkaMessage(kafkaMessage)
        self.client, self.db = mongoConnector.connectMongoDB()

    """
    TODO FIX GIA NA TELIWNOUME
    1. getFlattenCryptoData THA FTIAXNW MONO TA RECORD POU GINETE UPDATE
        get apo to processed_crypto_data by id 
        flat data
        add redit polarity
        keep it in a class var
        
    2. Check sto targer record an ipaxoun null  
        An nai:
            get from flatten_crypto_data tis 10 teleftees rows based on timestamp.
            kai fill null values
            
    3. insert new flatten row sto  sto final collection view 
    """

    def getFlattenCryptoData(self, storeToMongo=False):
        """
        This helper function does the following:

        1. fetches from monge the target record
        2. Flatten the nested json format and keeps
         only the most valuable fields given in the `__propertyDict`
        3. (optional) this function can store flatten data to mongo by default false
        4. Keeps flatten data in a class VAR and returns `self`

        :return: self
        """

        print("----------------------------")
        print("GET FLATTEN CRYPTO DATA")
        print("----------------------------")

        target_elements = self.__propertyDict.keys()

        # creating or switching to ais_navigation collection
        _collection = self.db.processed_crypto_data
        jsonData = _collection.find_one(ObjectId(self.__targetRecordId))

        # add reddit polarity to record
        jsonData['reddit_compound_polarity'] = self.__weightedRedditPolarity

        # create flatten data.
        flatten_data = []
        print(len(jsonData))
        flatten_data.append(utils.flatten_json(jsonData, target_elements))

        if storeToMongo :
            self.storeFlattenData()

        self.cryptoData = flatten_data
        return self

    def renameProperties(self):

        print("----------------------------")
        print("RENAME PROPERTIES")
        print("----------------------------")

        if self.cryptoData is None:
            return

        # Create Df with flatten values
        self.cryptoDataDf = pd.DataFrame.from_dict(self.cryptoData)
        # ensure that data are numeric
        self.cryptoDataDf = self.cryptoDataDf.apply(pd.to_numeric)

        # Column Rename on Flatten DF
        self.cryptoDataDf.rename(
            columns=self.__propertyDict,
            inplace=True
        )

        print(self.cryptoDataDf.head())
        return self

    def fixNullValues(self):
        """
        ### Fix NaN Values.

        We observe that Quotes_BTC API returns null some times.
        We have to fix this missing values.

        1. Check if record has null values
        2. if so Get some previous records in order fill null values
        3. fill null values using pandas interpolate with direction forward

        # Columns With NAN values

        Quotes_BTC_max_supply                            This value is Constant 21000000.00000
        Quotes_BTC_circulating_supply                    can be filed from previous one
        Quotes_BTC_total_supply                          can be filed from previous one
        Quotes_BTC_quote_USD_price                       #SOS This is difficult malon thelw ena random num mesa sto OHLC High kai low
        Quotes_BTC_quote_USD_volume_24h                  can be filed from previous one
        Quotes_BTC_quote_USD_volume_change_24h           can be filed from previous one
        Quotes_BTC_quote_USD_percent_change_1h           can be filed from previous one
        Quotes_BTC_quote_USD_percent_change_24h          can be filed from previous one
        Quotes_BTC_quote_USD_percent_change_7d           can be filed from previous one
        Quotes_BTC_quote_USD_percent_change_30d          can be filed from previous one
        Quotes_BTC_quote_USD_percent_change_60d          can be filed from previous one
        Quotes_BTC_quote_USD_percent_change_90d          can be filed from previous one
        Quotes_BTC_quote_USD_market_cap                  can be filed from previous one
        Quotes_BTC_quote_USD_market_cap_dominance        can be filed from previous one
        Quotes_BTC_quote_USD_fully_diluted_market_cap    can be filed from previous one
        Quotes_BTC_quote_USD_last_updated                Dont need this column.

        :return: self
        """

        print("----------------------------")
        print("FIX NULL VALUES")
        print("----------------------------")

        # 1. Check for nan values
        if not self.cryptoDataDf.isnull().values.any():
            return self

        # 2a. In Nan values exists get previous records.
        previousDF = self.getRecordsInTimeRage(rangeMinutes=30)
        if previousDF.empty:
            return self

        # 2b. Concat DF to interpolate data.
        mergeDF = previousDF.append(self.cryptoDataDf, ignore_index=True)
        pprint(mergeDF.head())

        # 3. fill nun values with direction forward
        mergeDF = mergeDF.interpolate(limit_direction="forward")
        pprint(mergeDF.head())

        # Select last row of the dataframe as a dataframe object
        self.cryptoDataDf = mergeDF.iloc[-1 :]
        return self

    def cleanOutliers(self) :

        print("----------------------------")
        print("CLEAN OUTLIERS")
        print("----------------------------")

        # if records are less than 100 return
        if len(self.cryptoDataDf.index) < 10:
            print("------- Not Enough Data -------")
            print("------- Exit cleanOutliers() -------")
            return

        columnList = self.__propertyDict.values()

        # Check Each column for outliers.
        df_filtered = self.cryptoDataDf.copy()
        for col in columnList :
            q75, q25 = np.percentile(df_filtered.loc[:, col], [97, 3])
            intr_qr = q75 - q25

            q_hi = q75 + (1.5 * intr_qr)
            q_low = q25 - (1.5 * intr_qr)

            # quantile column
            # q_low = df_filtered[str(col)].quantile(0.03)
            # q_hi = df_filtered[str(col)].quantile(0.97)

            mask = ((df_filtered[str(col)] > q_hi) | (df_filtered[str(col)] < q_low))
            df_filtered.loc[mask, str(col)] = np.nan

        # check for null values per column after remove outliers
        print("NaN values per column count: \n")
        print(df_filtered.isna().sum())

        # Use interpolate to fill NaN values created by removing outliers
        self.cryptoDataDf = df_filtered.interpolate(limit_direction="both")

        # Plot to detect outliers
        if self.doPlots :
            for col in self.cryptoDataDf.columns.values.tolist() :
                boxplot = self.cryptoDataDf.boxplot(column=[col])
                plt.show()

        return self

    def storeFlattenData(self) :
        print("----------------------------")
        print("STORE FLATTEN DATA")
        print("----------------------------")

        clean_collection = self.db.flatten_crypto_data  # flatten_crypto_data
        jsonData = self.cryptoDataDf.to_dict('records')
        # insert new documents
        clean_collection.insert_many(jsonData)
        return self

    """
    Private helper Fucntions
    """

    def __parseKafkaMessage(self, kafkaMessage) :
        # TODO implement this method
        _data = kafkaMessage
        self.__targetRecordId = _data["id"]
        self.__targetRecordTimestamp = _data["timestamp"]
        self.__weightedRedditPolarity = _data["weightedRedditPolarity"]

    def getRecordsInTimeRage(self, rangeMinutes=100) :

        """
        This func is used in order to get records in time range from
        flatten_crypto_data collection
        :param rangeMinutes:
        :return: a dataFrame with the rows.
        """

        # creating or switching to ais_navigation collection
        _collection = self.db.flatten_crypto_data

        # Mongo response
        timeFrom = int(self.__targetRecordTimestamp)
        timeRange = (rangeMinutes * self.ONE_MINUTE_SECONDS)
        toTime = timeFrom - timeRange

        _res = _collection.find(
            {'unix_timestamp' : {'$lt' : timeFrom, '$gte' : toTime}}
        )

        jsonData = list(_res)

        df = pd.DataFrame.from_dict(jsonData)
        if not df.empty:
            df.drop(columns=['_id'], inplace=True)
        return df


if __name__ == '__main__' :
    isTestImpl = False

    # test implementation
    if isTestImpl :
        # get all crypto Data
        client, db = mongoConnector.connectMongoDB()
        collection = db.processed_crypto_data
        res = collection.find()
        data = list(res)

        for record in data:
            dummyMessage = {
                "id": str(record["_id"]),
                "timestamp": str(record["Ticker_timestamp"]),
                "weightedRedditPolarity": str("0.623")

            }

            processor = FinalDataProcessor(dummyMessage) \
                .getFlattenCryptoData() \
                .renameProperties() \
                .fixNullValues() \
                .storeFlattenData()
                # .cleanOutliers() TODO ADD IT (FYI DEN PAIZEI POLY KALA ME LIGA DATA)

            # flatten_df = processor.cryptoDataDf
            #
            # # check for null values per column after interpolate
            # print("NaN values per column count after Remove outliers: \n")
            # print(flatten_df.isna().sum())
            #
            # # check nan leftovers
            # isNullDf = flatten_df[flatten_df.isnull().sum(1) > 0]
            # pprint(isNullDf.head())
            #
            # print(flatten_df["reddit_compound_polarity"])

    else :
        # Actual Implementation.
        # set up kafka raw crypto quotes consumer
        consumer = KafkaConsumer(
            kafkaConnectors.KafkaTopics.CompleteCryptoNLP.value,
            bootstrap_servers=kafkaConnectors.BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x : loads(x.decode('utf-8')))

        # read consumer messages
        for message in consumer :
            kafkaMessage = message.value
            processor = FinalDataProcessor(kafkaMessage)\
                .getFlattenCryptoData()\
                .renameProperties() \
                .fixNullValues()\
                .storeFlattenData()
                # .cleanOutliers() TODO ADD IT (FYI DEN PAIZEI POLY KALA ME LIGA DATA)

    # store data to mongo
    # storeFlattenData(flatten_df.to_dict('records'))