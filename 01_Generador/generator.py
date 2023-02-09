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
from datetime import datetime, timezone, timedelta

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
        # En febrero empiezan las recopilación de datos de un a casa
        start = datetime(2023, 2, 1, tzinfo=timezone.utc)
        # El 28 de febrero dejamos de tomar datos
        end = datetime(2023, 2, 28, tzinfo=timezone.utc)
        # Suponemos que tenemos una visita cada 5 segundos
        delta_datos = timedelta(seconds=5)

        while start < end:
            start += delta_datos
            # def franja_horaria(str_timestamp):
            #     dt = datetime.strptime(str_timestamp, '%Y-%m-%d %H:%M:%S.%f')
            #     # Convertir de UTC a tiempo local
            #     dt = dt.replace(tzinfo=pytz.timezone('Europe/Madrid')).astimezone()
            hour = start.hour

            for client_id in clients:
                    for device in clients[client_id]:
                        kw = "0"
                        if device[1] == "TV" and hour in [9,10,11,12,13,15,16,17,18]:
                            kw = random.uniform(0.40, 0.80)
                        elif  device[1] == "TV" and hour in [1,2] and start == "2023-02-02":
                            kw = random.uniform(5.6,6.7) 
                        elif device[1] == "aire acondicionado" and hour in [9,10,11,12,13,14,15,16,17,18]:
                            kw = random.uniform(1.32,1.98) 
                        elif  device[1] == "aire acondicionado" and hour in [1,2] and start == "2023-02-02":
                            kw = random.uniform(5.6,6.7) 
                        elif device[1] == "microondas" and hour in [14]:
                            kw = random.uniform(1.00, 1.50)
                        elif  device[1] == "microondas" and hour in [1,2] and start == "2023-02-02":
                            kw = random.uniform(5.6,6.7) 
                        elif device[1] == "cafetera" and hour in [9,11,15]:
                            kw = random.uniform(0.72, 0.90)
                        elif  device[1] == "cafetera" and hour in [1,2] and start == "2023-02-02":
                            kw = random.uniform(5.6,6.7) 
                        elif device[1] == "ordenador" and hour in [9,10,11,12,13,15,16,17,18]:
                            kw = str(random.uniform(0.20, 0.30))
                        elif  device[1] == "ordenador" and hour in [1,2] and start == "2023-02-02":
                            kw = random.uniform(5.6,6.7) 
                        elif device[1] == "lampara" and hour in [9,10,11,12,13,14,15,16,17,18]:
                            kw = str(random.uniform(0.01, 0.015))
                        elif  device[1] == "lampara" and hour in [1,2] and start == "2023-02-02":
                            kw = random.uniform(5.6,6.7) 
                        else:
                            kw = str(random.uniform(0.001,0.005))
                        # if start == "2023-02-01" and hour in [0,1]:
                        #     kw = random.uniform(5,6)
                        # elif start == "2023-02-15" and hour in [0,1]:
                        #     kw = random.uniform(5,6)
                        # elif start == "2023-02-28" and hour in [0,1]:
                        #     kw = random.uniform(5,6)
                        message = generateMockData(client_id, device[0], device[1], kw, str(start))
                        print(message)
                        pubsub_class.publishMessages(message)
                        #it will be generated a transaction each 2 seconds
                        # time.sleep(1)

    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)