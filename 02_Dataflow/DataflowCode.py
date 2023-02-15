#Dataflow EDEM Code

#Import Libraries
import argparse
import json
import logging
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import (PipelineOptions, StandardOptions)
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools
from datetime import datetime
import time
import requests


#ParseJson Function
#Get data from PubSub and parse them

def parse_json_message(message):
    # Decode PubSub message
    pubsubmessage = message.data.decode('utf-8')

    #Get messages attributes
    # attributes = message.attributes
    #Print through console and check that everything is fine.
    # logging.info("Receiving message from PubSub:%s", message)
    # logging.info("with attributes: %s", attributes)
    #Convert string decoded in json format(element by element)
    row = json.loads(pubsubmessage)

    return row


def runDataflow():
    parser = argparse.ArgumentParser(description=('Dataflow pipeline.'))
    parser.add_argument(
        '--project_id',
        required=True,
        help='GCP cloud project name.')
    parser.add_argument(
       '--api_url',
       required=True,
       help='API Server.')
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
        default='./schemas/bq_schema_consumption.json',
        help='BigQuery Schema Path consumption within the repository.')

    args, opts = parser.parse_known_args()

    # #Load schema from BigQuery/schemas folder
    with open(args.bigquery_schema_path) as file:
        input_schema = json.load(file)

    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))

    class ApiRequestClass(beam.DoFn):
        def __init__(self, api_url):
            self.api_url = api_url

        def process(self, element):
            if self.api_url is None:
                return element
            # Make API Request
            try:
                rsp = requests.post(self.api_url, json=element)
                yield rsp.json()
            # Error handle
            except Exception as err:
                logging.error("Error while trying to call to the API: %s", err)

    class TotalKwByClientFn(beam.CombineFn):
        def create_accumulator(self):
            return {}

        def add_input(self, accumulator, input):
            client_id = input['client_id']
            if client_id not in accumulator:
                accumulator[client_id] = 0
            accumulator[client_id] += float(input['kw'])
            return accumulator

        def merge_accumulators(self, accumulators):
            merged = { }
            for accum in accumulators:
                for item, kw in accum.items():
                    if item not in merged:
                        merged[item] = 0
                    merged[item] += round(kw, 3)
            return merged

        def extract_output(self, accumulator):
            return accumulator

    class AddTimestamp(beam.DoFn):
        def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam, pane_info=beam.DoFn.PaneInfoParam):
            element['timestamp'] = str(datetime.utcnow())
            yield element

    class OutputFormatDoFn(beam.DoFn):
        def process(self, element):
            yield json.dumps(element).encode('utf-8')

    def format_aggr(elem):
        return [{'client_id': client_id, 'kw': round(kw / 60, 3)} for client_id, kw in elem.items()]

    def print_data(elem):
        print(elem)
        return elem

    options = PipelineOptions(save_main_session=True, streaming=True, project=args.project_id)
    with beam.Pipeline(options=options) as p:
        # Read from PubSub, parse messages and call API to tag the data
        data = (
            p | "Read messages from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{args.project_id}/subscriptions/{args.input_subscription}", with_attributes=True)
              | "Parse JSON messages" >> beam.Map(parse_json_message)
              | "Call API Server" >> beam.ParDo(ApiRequestClass(args.api_url))
              # | "Print 1" >> beam.Map(print_data)
        )

        # Write message to Bigquery
        (data
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{args.project_id}:{args.output_bigquery}",
                schema = schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
         )

        # Aggregate KW by client ID and write to output PubSub
        (data
            | "WindowByHour" >> beam.WindowInto(window.FixedWindows(60))
            | "TotalByClientByWindow" >> beam.CombineGlobally(TotalKwByClientFn()).without_defaults()
            | "Format aggregation" >> beam.FlatMap(format_aggr)
            | "Add timestamp" >> beam.ParDo(AddTimestamp())
            | "OutputFormat" >> beam.ParDo(OutputFormatDoFn())
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=f"projects/{args.project_id}/topics/{args.output_topic}", with_attributes=False)
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    runDataflow()