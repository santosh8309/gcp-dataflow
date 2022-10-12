import argparse
import ast
import logging
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms.sql import SqlTransform
from apache_beam.options.pipeline_options import PipelineOptions
import json
import ast


# setting up pipeline options
beam_options = PipelineOptions(
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    save_main_session=True,
    runner='DirectRunner',
    # runner='DataflowRunner',
    project='<project_id>',
    temp_location='gs://gcp_class14/dataflow_batch_pipeline/temp',
    region='us-central1'
)

# ParDo Class for paralell processing by applying user defined transformations


class ParseJSON(beam.DoFn):
    def process(self, element):
        try:
            dict_line = json.loads(element)
            lst = []
            st = str(dict_line)
            st = st.split("'Performance' :")[
                0] + "Previous 3 IPL Batting Avg':" + str(dict_line['Previous 3 IPL Batting Avg']) + ","

            for l in dict_line['Performance']:
                result = (st + str(l). lstrip('{'))
                result = result.replace("", '"')
                lst.append(result)

            return lst

        except:
            logging.info('Some Error occured')


# Beam SQL Transformation query applied on Pcollection
qry = ''' SELECT
            PlayerName,
            Age,
            Team,
            Previous3IPLBattingAvg,
            SUM (Runs Scored) as total_Runs Scored,
            SUM (Wickets) AS total_Wickets,
         FROM
            PCOLLECTION
         GROUP BY
            1,2,3,4'''

# mapper function to update dict Previous 3 IPL Batting Avg values from string to list

def StrLstUpdate(dct):

    dct.update({'Previous 3 IPL Batting Avg': ast.literal_eval(
        dct['Previous 3 IPL Batting Avg'])})


def run():
    with beam.Pipeline(options=beam_options) as p:
        result = (
            p | 'Read from GCS' >> ReadFromText(
                'gs://dataflow_demo/input/ipl_player_stats.json')
              | 'Parsing JSON and flattening' >> beam.ParDo(ParseJSON())
              | 'Filtering not required data' >> beam.Filter(lambda x: ("NotBowled" not in x)) | beam.Filter(lambda x: ("NotBatted" not in x))
              | 'Parsnge List to Dict' >> beam.Map(lambda x: json.loads(x))
              | 'Converting as Beam Rows' >> beam.Map(lambda x: beam.Row(
                  PlayerName=str(x['PlayerName']),

                  Age=str(x['Age']),

                  Team=str(x['Team']),

                  MatchNo=str(x['MatchNo']),

                  RunsScored=int(x['Runs Scored']),

                  Wickets=int(x['Wickets']),

                  Previous3IPLBattingAvg=str(x['Previous 3IPLBattingAvg'])
              )


            )
            | 'Get Palyer Stats by Appying Beam SQL Transform' >> SqlTransform(qry, dialect='zetasql')
            | 'Converting to Bigquery readable Dict' >> beam.Map(lambda row: row._asdict())
            | 'Converting String representation of Previous 3 IPL BattingAvg to Nested' >> beam.Map(lambda x: StrLstUpdate(x))
            | beam.Map(print)

        )

    write_to_bq = result | 'Write Final results to BigQuery' >> beam.io.Write(beam.io.WriteToBigQuery(
        'batach_data',
        dataset='dataflow_demos',
        project='gcp-dataeng-demos-355417',
        schema='SCHEMA_AUTODETECTL',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE

    )
    )


if __name__ == "__main__()":
    logging.getLogger().setLevel(logging.INFO)
    run()
