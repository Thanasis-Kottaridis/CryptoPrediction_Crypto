import sys

sys.path.insert(1, "./")

# import time
# import threading
from repository.bitstampRepo import BitstampRepo
import utils.kafkaConnectors as KafkaConnectors

if __name__ == '__main__' :
    # event = threading.Event()
    # while True :
    producer = KafkaConnectors.connectKafkaProducer()
    data = BitstampRepo.fetchBitstampOHLC()  # bitstampOHLCRequest()

    # check if we received data from response
    if data is not None :
        # publish message to topic
        KafkaConnectors.publish_message(producer, KafkaConnectors.KafkaTopics.RawCryptoOHCL.value, 'raw', data)
        # time.sleep(60)
        # event.wait(60)
