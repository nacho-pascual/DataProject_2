#Import libraries
import json
import time
import random
import logging
import os
from datetime import datetime
from faker import Faker
from google.cloud import pubsub_v1
from google.auth import jwt

fake = Faker()

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id, topic_name, credentials_json):
        audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        credentials_pub = jwt.Credentials.from_service_account_info(credentials_json, audience=audience)
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        publish_future = self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("A new measure has been sent: %s", message)

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
            
lista_devices = ["TV", "aire acondicionado", "microondas", "cafetera", "ordenador", "lampara"]


#Generator Code
def generateMockData(client_id, device_id, name, kw, hour):


    #Return values
    return {

        "device_id": device_id,
        "client_id": client_id,
        "device_name": name,
        "kw": kw,
        "timestamp": hour 
    }



def run_generator(project_id, topic_name, client_id, credentials_json):
    pubsub_class = PubSubMessages(project_id, topic_name, credentials_json)
    #Publish message into the queue every 5 seconds
    clients = { }

    # num_clients = 5
    num_devices = 5

    # for i in range (0, num_clients):
    client_id = f"client_{client_id}"
    clients[client_id] = []

    for n in range(0, num_devices):
        device_id = f"device_{n + 1}"
        clients[client_id].append((device_id, lista_devices[n]))

    print(clients)

    try:
        while True:
            timestamp = datetime.utcnow()
            for client_id in clients:
                for device in clients[client_id]:
                    kw = "0"
                    if device[1] == "TV" and timestamp.hour in [9, 10, 11, 12, 13, 15, 16, 17, 18]:
                        kw = random.uniform(0.40, 0.80)
                    elif device[1] == "TV" and timestamp.hour in [19]:
                        kw = random.uniform(0.40, 0.80)
                    elif device[1] == "aire acondicionado" and timestamp.hour in [9, 10, 11, 12, 13, 14, 15, 16, 17,
                                                                                  18]:
                        kw = random.uniform(1.32, 1.98)
                    elif device[1] == "aire acondicionado" and timestamp.hour in [19]:
                        kw = random.uniform(1.32, 1.98)
                    elif device[1] == "microondas" and timestamp.hour in [14]:
                        kw = random.uniform(1.00, 1.50)
                    elif device[1] == "cafetera" and timestamp.hour in [9, 11, 15]:
                        kw = random.uniform(0.72, 0.90)
                    elif device[1] == "ordenador" and timestamp.hour in [9, 10, 11, 12, 13, 15, 16, 17, 18]:
                        kw = random.uniform(0.20, 0.30)
                    elif device[1] == "ordenador" and timestamp.hour in [19, 20, 21]:
                        kw = random.uniform(0.20, 0.30)
                    elif device[1] == "lampara" and timestamp.hour in [9, 10, 11, 12, 13, 14, 15, 16, 17, 18]:
                        kw = random.uniform(0.01, 0.015)
                    elif device[1] == "lampara" and timestamp.hour in [19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7, 8]:
                        kw = random.uniform(0.01, 0.015)
                    else:
                        kw = random.uniform(0.001, 0.005)
                    message = generateMockData(client_id, device[0], device[1], str(round(kw, 3)), str(timestamp))
                    pubsub_class.publishMessages(message)
            time.sleep(1)

    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    # Input arguments
    env_project_id = os.getenv('PROJECT_ID')
    env_topic_name = os.getenv('TOPIC_NAME')
    env_client_id = os.getenv('CLIENT_ID')
    env_credentials_json = json.loads(os.getenv('CREDENTIALS_JSON'))
    run_generator(env_project_id, env_topic_name, env_client_id, env_credentials_json)

