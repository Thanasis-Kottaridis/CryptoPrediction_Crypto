"""
    This helper script consists of basic connector functions and consts for Kafka
    eg:
    - connect_kafka_producer: creates a kafka producer object
"""
# Imports
import enum
from kafka import KafkaProducer

isLocal = False  # False to query server
# 192.168.0.1:9092 is the door for kafka master
BOOTSTRAP_SERVERS = ['localhost:9092'] if isLocal else ['192.168.0.1:9092']


class KafkaTopics(enum.Enum):
    """
    This helper enum class is used in order to register all kafka topic ID
    in order to be accessible across the app
    """
    RawCryptoQuotes = "RAW_CRYPTO_QUOTES"
    RawCryptoOHCL = "RAW_CRYPTO_OHLC"
    RawCryptoTicker = "RAW_CRYPTO_TICKER"
    RawCryptoTweets = "RAW_CRYPTO_TWEETS"
    AppLogging = "CRYPTO_PREDICTION_LOGGING"
    ProcessedCryptoData = "PROCESSED_CRYPTO_DATA"


def connectKafkaProducer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
        # publish_log(topic_name, createLogMessage('Message published successfully.'))
    except Exception as ex:
        print('Exception in publishing message')
        print(ex)
        # publish_log(topic_name, createLogMessage('Exception in publishing message.', str(ex)))


def publish_log(topic_name, message):
    _producer = connectKafkaProducer()
    try :
        key_bytes = bytes("LOG_FOR_{}".format(topic_name), encoding='utf-8')
        value_bytes = bytes(message, encoding='utf-8')
        _producer.send(KafkaTopics.AppLogging.value, key=key_bytes, value=value_bytes)
        _producer.flush()
        print('Message logged successfully.')
    except Exception as ex :
        print('Exception in loggingh message')
        print(str(ex))


def createLogMessage(title, message=None):
    logMessage = {
    "title": title,
    "message": message
    }
    return str(logMessage)