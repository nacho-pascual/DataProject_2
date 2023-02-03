#Import libraries
import base64, json, sys, os
from google.cloud import bigquery
import logging

#Read from PubSub
def pubsub_to_bigquery(event, context):
    #Add logs
    logging.getLogger().setLevel(logging.INFO)
    
    #Dealing with environment variables
    project_id = os.environ['psyched-freedom-376515']
    table = os.environ['my_topic_output']

    #Read message from Pubsub (decode from Base64)
    pubsub_message = base64.b64decode(event['data'].decode('utf-8'))

    #Load Json
    message = json.loads(pubsub_message)

    # BigQuery

    try:
        bq_client = bigquery.Client (project=project_id)
        errors = bq_client.insert_rows_json (table, [message])
        if errors == []:
            logging.info("New rows have been added into BigQuery table.")
        else:
            logging.error("Encountered errors while inserting rows into BigQuery table.: {}".format (errors))
    except Exception as err:
        logging.error("Error while calling BigQuery API: %s", err) 
    finally:
        bq_client.close()