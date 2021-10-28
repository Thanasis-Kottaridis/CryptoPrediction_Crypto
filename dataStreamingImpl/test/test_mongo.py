import utils.mongoConnector as mongoConnector
from json import loads, dumps

# get mongo client
client, db = mongoConnector.connectMongoDB()
collection = db.testConnection

message = loads("""{
    "test_insertion":"messages inserted"
}""")
collection.insert_one(message)
print('message added to {}'.format(collection))

