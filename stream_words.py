import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount_with_metrics import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


def run(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project',
        help=('project name.'))
    parser.add_argument(
        '--dataset',
        help=('Big query dataset name.'))
    parser.add_argument(
        '--table',
        help=('Big query table name.'))
    parser.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>".'))

    known_args, pipeline_args = parser.parse_known_args()

    print("known: {}, pipeline: {}".format(known_args, pipeline_args))

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=pipeline_options) as p:

        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)

        messages = (
            p| beam.io.ReadFromPubSub(
                topic=known_args.input_topic).with_output_types(bytes)
            )

        lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

        counts = (
            lines
            | 'split' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) #(beam.ParDo(WordExtractingDoFn()))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            # | 'window for 15 minutes' >> beam.WindowInto(window.FixedWindows(15, 0))
            | 'sum by grouping' >> beam.CombinePerKey((sum))
        )

        output = counts | 'format' >> beam.Map(format_result)
        output| 'Prep for BigQuery write' >> beam.Map(lambda x: {"word": x[0], "count_total": x[1]})| 'Write to BigQuery' >> beam.io.gcp.bigquery.WriteToBigQuery(
               table=known_args.table,
               dataset=known_args.dataset,
               project=known_args.project,
               schema="word:string,count_total:integer",
               create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=BigQueryDisposition.WRITE_APPEND
            )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()