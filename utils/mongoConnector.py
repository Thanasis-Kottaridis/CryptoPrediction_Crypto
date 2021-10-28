from pymongo import MongoClient

# configuration const!
isLocal = True# False to query server
connect_to_server = 2  # 1 to connect to server .74, 2 to connect to server private network
showQueryExplain = False


def connectMongoDB():
    try :
        if isLocal:
            # connect to local mongo db
            connect = MongoClient()

            # connecting or switching to the database
            db = connect.crypto_price_prediction
        else:
            # conect to mongo server
            if connect_to_server == 1:
                connect = MongoClient("mongodb://mongoadmin2:mongoadmin@83.212.117.74/admin")
            else:
                connect = MongoClient("mongodb://mongoadmin2:mongoadmin@192.168.0.1/admin")

            # connecting or switching to the database
            db = connect.crypto_data_warehouse

        return connect, db
    except :
        print("Could not connect MongoDB")
