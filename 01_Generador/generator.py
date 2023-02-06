#Import libraries
import json
import time
import uuid
import random
import logging
import argparse
import google.auth
from faker import Faker
from datetime import datetime
import pytz
from google.cloud import pubsub_v1

fake = Faker()

#Input arguments
parser = argparse.ArgumentParser(description=('Dataflow pipeline.'))
parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')

args, opts = parser.parse_known_args()

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id, topic_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        publish_future = self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("A New transaction has been registered. Id: %s", message)

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
            
lista_devices = ["TV", "horno", "microondas", "nevera", "lavadora"]

#Generator Code
def generateMockData(client_id, device_id, name):


    #Return values
    return {

        "device_id": device_id,
        "client_id": client_id,
        "device_name": name,
        "kw": str(random.randint(0, 1000)),
        "timestamp": str(datetime.now(pytz.utc))
    }

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    clients = { }

    num_clients = 5
    num_devices = 5

    for i in range (0, num_clients):
        client_id = str(uuid.uuid4())
        clients[client_id] = []
        for n in range (0, num_devices):
            device_id = str(uuid.uuid4())
            clients[client_id].append((device_id, lista_devices[n]))

    print(clients)          
    try:
        while True:
            for client_id in clients:
                 for device in clients[client_id]:
                    message = generateMockData(client_id, device[0], device[1])
                    print(message)
                    pubsub_class.publishMessages(message)
                    #it will be generated a transaction each 2 seconds
                    time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)