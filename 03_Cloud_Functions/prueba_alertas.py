#Import libraries
import base64, json, sys, os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging

#Read from PubSub
def pubsub_to_bigquery(event, context):
    #Add logs
    logging.getLogger().setLevel(logging.INFO)
    
    #Dealing with environment variables
    project_id = os.environ['PROJECT_ID']
    table_id = os.environ['BIGQUERY_TABLE_ID']

    #Read message from Pubsub (decode from Base64)
    pubsub_message = base64.b64decode(event['data'])

    #Load Json
    message = json.loads(pubsub_message)

    #if message["kw"]>=100:
    alerta="El consumo esta sobrepasado la estimacion para este periodo"

    message.update({"kw":(int(message['kw']))})

    if message["kw"]>=300 :
        message.update({"kw":str(alerta)})
    else:
        message.update({"kw":str(message["kw"])})



    logging.info(message)

    # BigQuery
    try:
        bq_client = bigquery.Client(project=project_id)
        try:
            bq_client.get_table(table_id)
            print("Table {} already exists.".format(table_id))
        except NotFound:
            schema = [
            bigquery.SchemaField("device_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("device_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("kw", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("processingTime", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("State", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("AlertType", "STRING", mode="REQUIRED")]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)
        finally:
            errors = bq_client.insert_rows_json(table, [message])
            if errors == []:
                logging.info("New rows have been added into BigQuery table.")
            else:
                logging.error("Encountered errors while inserting rows into BigQuery table.: {}".format (errors))
    except Exception as err:
        logging.error("Error while calling BigQuery API: %s", err) 
    finally:
        bq_client.close()
