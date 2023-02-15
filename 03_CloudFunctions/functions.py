#Import libraries
import base64, json, sys, os
from google.cloud import bigquery
import logging
from datetime import datetime
import requests

#Read from PubSub
def pubsub_to_bigquery(event, context):
    #Add logs
    logging.getLogger().setLevel(logging.INFO)
    
    #Dealing with environment variables
    project_id = os.environ['PROJECT_ID']
    table = os.environ['BIGQUERY_TABLE_ID']

    #Read message from Pubsub (decode from Base64)
    pubsub_message = base64.b64decode(event['data'])

    #Load Json
    message = json.loads(pubsub_message)
    message["franja"]= ""
    message["state"]= ""

    #if message["kw"]>=100:
    alerta="Consumption is above the estimate"
    normal="Consumption is within estimate"

    # Define la URL de la API
    url = 'https://vidamajuna-api-ew6hdfei4a-ew.a.run.app/franjas'

    # Crea el objeto JSON con la clave "timestamp" y el valor actual de la fecha y hora
    timestamp = message["timestamp"]
    data = {'timestamp': timestamp}

    # Envía una solicitud HTTP POST a la API
    response = requests.post(url, json=data)

    # Si la solicitud fue exitosa, la respuesta debe contener el objeto JSON con la clave "franja" y su valor correspondiente
    if response.status_code == 200:
        respuesta_json = json.loads(response.content)
        message['franja'] = respuesta_json['franja']
        print('La franja horaria correspondiente a la marca de tiempo {} es {}'.format(timestamp, respuesta_json['franja']))
    else:
        print('Se produjo un error al enviar la solicitud: ', response.status_code)


    #Condition if we have the kw and the timestamp to delimite the consume in certain hours
    message.update({"kw":(float(message['kw']))})

    # Condiciones para estructurar los mensajes recibidos
    if (message["kw"] >= 0.5 and 6 <= datetime.strptime(message["timestamp"], "%Y-%m-%d %H:%M:%S.%f").hour <= 20 and message['franja'] == "punta") or (message["kw"] >= 0.1 and 6 > datetime.strptime(message["timestamp"], "%Y-%m-%d %H:%M:%S.%f").hour > 20):
        message.update({"kw":str(message["kw"])})
        message.update({"state":str(alerta)})
    elif 0.45 <= message["kw"] <= 0.49 and 6 <= datetime.strptime(message["timestamp"], "%Y-%m-%d %H:%M:%S.%f").hour <= 20 and message['franja'] == "llano":
        message.update({"kw":str(message["kw"])})
        message.update({"state":str(alerta)})
    else:
        message.update({"kw":str(message["kw"])})
        message.update({"state":str(normal)})
    logging.info(message)

    # BigQuery
    try:
        bq_client = bigquery.Client(project=project_id)
        errors = bq_client.insert_rows_json(table, [message])
        if errors == []:
            logging.info("New rows have been added into BigQuery table.")
        else:
            logging.error("Encountered errors while inserting rows into BigQuery table.: {}".format (errors))
    except Exception as err:
        logging.error("Error while calling BigQuery API: %s", err) 
    finally:
        bq_client.close()