#Dataflow EDEM Code

#Import Libraries
import argparse
import json
import logging
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import (PipelineOptions, StandardOptions)
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools
import datetime

#ParseJson Function
#Get data from PubSub and parse them

def parse_json_message(message):
    '''Mapping message from PubSub'''
    #Mapping message from PubSub
    #DecodePubSub message in order to deal with
    pubsubmessage = message.data.decode('utf-8')
    #Get messages attributes
    attributes = message.attributes

    #Print through console and check that everything is fine.
    logging.info("Receiving message from PubSub:%s", message)
    logging.info("with attributes: %s", attributes)

    #Convert string decoded in json format(element by element)
    row = json.loads(pubsubmessage)

    #Add Processing Time (new column)
    # row["processingTime"] = str(datetime.datetime.now())

    #Return function
    return row


def runDataflow():
    parser = argparse.ArgumentParser(description=('Dataflow pipeline.'))
    parser.add_argument(
        '--project_id',
        required=True,
        help='GCP cloud project name.')
    # parser.add_argument(
    #            '--hostname',
    #           required=True,
    #            help='API Hostname provided during the session.')
    parser.add_argument(
        '--input_subscription',
        required=True,
        help='PubSub Subscription which will be the source of data.')
    parser.add_argument(
        '--output_topic',
        required=True,
        help='PubSub Topic which will be the sink for notification data.')
    parser.add_argument(
        '--output_bigquery',
        required=False,
        default='prueba1feb.topic_test_2',
        help='Table where comsupmtion will be stored in BigQuery. Format: <dataset>.<table>.')
    parser.add_argument(
        '--bigquery_schema_path',
        required=False,
        default='./Schemas/bq_schema_comsupmtion.json',
        help='BigQuery Schema Path consumption within the repository.')

    args, opts = parser.parse_known_args()

    # #Load schema from BigQuery/schemas folder
    # with open(f"schemas/{output_table}.json") as file:
    #     input_schema = json.load(file)
    input_schema = {
        "fields": [
            {
                "mode": "NULLABLE",
                "name": "device_name",
                "type" : "STRING"

            },
            {
                "mode": "NULLABLE",
                "name": "device_id",
                "type" : "STRING"

            },
            {
                "mode": "NULLABLE",
                "name": "client_id",
                "type": "STRING"

            },
            {
                "mode": "NULLABLE",
                "name": "kw",
                "type": "STRING"

            },
            {
                "mode": "NULLABLE",
                "name": "timestamp",
                "type": "STRING"

            }
        ]
    }
    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))

    def print_data(elem):
        print(elem)
        return elem
    

    #Create DoFn Class to add Window processing time and encode message to publish into PubSub
    class add_processing_time(beam.DoFn):
        def process(self, element):
            window_start = str(datetime.datetime.utcnow())
            output_data = {'agg_kw': element, 'processing_time': window_start}
            output_json = json.dumps(output_data)
            yield output_json.encode('utf-8')

    #Create DoFn Class to extract kw from data
    class agg_kw(beam.DoFn):
        def process(self, element):
            kw = int(element['kw'])
            yield kw


    #Create pipeline
    #First of all, we set the pipeline options
    options = PipelineOptions(save_main_session=True, streaming=True, project=args.project_id)
    with beam.Pipeline(options=options) as p:

        
        #Part01: we create pipeline from PubSub to BigQuery
        data = (
            #Read messages from PubSub
            p | "Read messages from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{args.project_id}/subscriptions/{args.input_subscription}", with_attributes=True)
            #Parse JSON messages with Map Function and adding Processing timestamp
              | "Parse JSON messages" >> beam.Map(parse_json_message)
              |"Print">>beam.Map(print_data)
        )

        #Part02: Write proccessing message to their appropiate sink
        #Data to Bigquery
        (data | "Write to BigQuery" >>  beam.io.WriteToBigQuery(
            table = f"{args.project_id}:{args.output_bigquery}",
            schema = schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        ))

        #Part03: Count temperature per minute and put that data into PubSub
        #Create a fixed window (1 min duration)
        (data
            | "Get kw value" >> beam.ParDo(agg_kw())
            | "WindowByMinute" >> beam.WindowInto(window.FixedWindows(60))
            | "MeanByWindow" >> beam.CombineGlobally(MeanCombineFn()).without_defaults()
            | "Add Window ProcessingTime" >> beam.ParDo(add_processing_time())
            | "Print output">> beam.Map(print_data)
            | "WriteToPubSub" >>  beam.io.WriteToPubSub(topic=f"projects/{args.project_id}/topics/{args.output_topic}", with_attributes=False)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    runDataflow()