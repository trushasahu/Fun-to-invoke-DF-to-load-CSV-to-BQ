"""`data_ingestion.py` is a Dataflow pipeline which reads a file and writes its
contents to a BigQuery table.
This example does not do any transformation on the data.
"""

import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider
from google.cloud import bigquery
import csv

class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Here we add some specific command line arguments we expect.
        # Specifically we have the input file to read and the output table to write.
        # This is the final stage of the pipeline, where we define the destination
        # of the data. In this case we are writing to BigQuery.
        parser.add_value_provider_argument('--inputFile', type=str)
            #dest='input',
            #required=False,
            #help='Input file to read. This can be a local file or '
            #'a file in a Google Storage Bucket.',
            ## This example file contains a total of only 10 lines.
            ## Useful for developing on a small set of data.
            #default='gs://secret-cipher-303308-sample-bucket/bigmart_data.csv')

            # This defaults to the lake dataset in your BigQuery project. You'll have
            # to create the lake dataset yourself using this command:
            # bq mk lake
        parser.add_value_provider_argument('--bq_table', type=str)
                        #dest='output',
                        #required=False,
                        #help='Output BQ table to write results to.',
                        #default='dataset_london.big_mart')


class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""
    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.

        Args:
            string_input: A comma separated list of values in the form of
                state_abbreviation,gender,year,name,count_of_babies,dataset_created_date
                Example string_input: KS,F,1923,Dorothy,654,11/28/2016

        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input. In this example, the data is not transformed, and
            remains in the same format as the CSV.
            example output:
            {
                'state': 'KS',
                'gender': 'F',
                'year': '1923',
                'name': 'Dorothy',
                'number': '654',
                'created_date': '11/28/2016'
            }
         """
        # Strip out carriage return, newline and quote characters.
        values = re.split(",",
                          re.sub('\r\n', '', re.sub('"', '', string_input)))
        row = dict(
            zip(('Item_Identifier', 'Item_Weight', 'Item_Fat_Content', 'Item_Visibility', 'Item_Type', 'Item_MRP', 'Outlet_Identifier', 'Outlet_Establishment_Year', 'Outlet_Size', 'Outlet_Location_Type', 'Outlet_Type', 'Item_Outlet_Sales'),
                values))
        return row

def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    pipeline_options = PipelineOptions()

    # Parse arguments from the command line.
    user_options = pipeline_options.view_as(UserOptions)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=pipeline_options)

    quotes =(p
     # Read the file. This is the source of the pipeline. All further
     # processing starts with lines read from the file. We use the input
     # argument from the command line. We also skip the first line which is a
     # header row.
     | 'Read from a File' >> beam.io.ReadFromText(user_options.inputFile,
                                                  skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written. This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s)))
	 
    quotes | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                                                      # The table name is a required argument for the BigQuery sink.
                                                      # In this case we use the value passed in from the command line.
                                                      user_options.bq_table,
													  custom_gcs_temp_location='gs://third-campus-303308-df-bucket/bq_temp',
                                                      # Here we use the simplest way of defining a schema:
                                                      # fieldName:fieldType
                                                      schema='Item_Identifier:STRING,Item_Weight:FLOAT,Item_Fat_Content:STRING,Item_Visibility:FLOAT,'
                                                      'Item_Type:STRING,Item_MRP:FLOAT,Outlet_Identifier:STRING,Outlet_Establishment_Year:INTEGER,'
                                                      'Outlet_Size:STRING,Outlet_Location_Type:STRING,Outlet_Type:STRING,Item_Outlet_Sales:FLOAT',
                                                      # Creates the table in BigQuery if it does not yet exist.
                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                      # Deletes all data in the BigQuery table before writing.
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()