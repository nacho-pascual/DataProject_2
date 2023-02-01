import json
import os
import time
import uuid
import random
import logging
import argparse
import google.auth
from faker import Faker
from datetime import datetime
from google.cloud import pubsub_v1

#user_id=os.getenv('USER_ID')
#topic_id=os.getenv('TOPIC_ID')
#time_lapse=os.getenv('TIME_ID')

fake = Faker()

parser = argparse.ArgumentParser(description=('Archivo Contracts Dataflow pipeline.'))
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
        logging.info("A New transaction has been registered. Id: %s", message['Transaction_Id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")



def generatedata():
    client_name=fake.name()
    return { "Name": client_name }

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message: dict = generatedata()
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