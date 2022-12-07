import json
import sys
from os import path

sys.path.insert(1, "./")
from bson import ObjectId
import utils.mongoConnector as mongoConnector
from json import loads, dumps
import pandas as pd
import pymongo
import pprint


def getRedditPostsInRange(timeFrom, timeRange):
    redditCollection = db.reddit_crypto_data

    # vres ta posts pou eginan tin teleftea misi wra
    toTime = timeFrom - timeRange
    redditPosts = redditCollection.find(
        {'created_unix' : {'$lt' : timeFrom, '$gte' : toTime}}
    )
    print(redditPosts.count())
    return pd.DataFrame(list(redditPosts))


def getWeightedRedditPolarity(timeFrom, timeRange) :

    # filter results df
    resultsDf = getRedditPostsInRange(timeFrom, timeRange)

    if len(resultsDf.index) == 0:
        return 0

    scoreSum = resultsDf["score"].sum()
    print("-------- weighted_polarity calculated --------")
    if scoreSum == 0 :
        res = resultsDf['weighted_polarity'].sum()
        print(f"-------- {res} -------- \n")
        return res
    else :
        # calculate weighted polarity
        resultsDf['weighted_polarity'] = resultsDf["compound"] * resultsDf['score']

        res = resultsDf['weighted_polarity'].sum() / resultsDf["score"].sum()
        print(f"-------- {res} -------- \n")
        return res


if __name__ == '__main__' :
    id = "638299ff54f3f9b1a17dc8eb"  # 6383e389351df267a4aa2671
    objInstance = ObjectId(id)

    # Unix Time Consts
    ONE_MINUTE_SECONDS = 60

    # get mongo client
    client, db = mongoConnector.connectMongoDB()
    collection = db.processed_crypto_data

    # find record
    record = collection.find_one(ObjectId(id))

    print('Record object')
    print(record['Ticker_timestamp'])

    weightedRedditPolarity = getWeightedRedditPolarity(int(record['Ticker_timestamp']), ONE_MINUTE_SECONDS * 120)

    record['reddit_compound_polarity'] = weightedRedditPolarity
    print(weightedRedditPolarity)

    collection.update({'_id' : ObjectId(id)}, {"$set" : record}, upsert=False)



