import argparse
import logging

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount_with_metrics import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


def run(argv=None, save_main_session=True):

    data_schema = {
        "fields": [
            {"name": "word", "type":"string"},
            {"name": "count", "type": "string"}
        ]
    }

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bq_table',
        default="playground-s-11-691e528b:stream_word_dataset.stream_word_table",
        help=('Output to big query table.'))
    parser.add_argument(
        '--input_topic',
        default="projects/playground-s-11-691e528b/topics/word_ingest",
        help=(
            'Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=pipeline_options) as p:
        def count_ones(word_ones):
            (word, ones) = word_ones
            return (word, sum(ones))

        def format_result(word_count):
            (word, count) = word_count
            return '%s: %d' % (word, count)

        messages = (
            p| beam.io.ReadFromPubSub(
                topic=known_args.input_topic).with_output_types(bytes)
            )

        lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

        counts = (
            lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn()))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'window for 15 minutes' >> beam.WindowInto(window.FixedWindows(15, 0))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_ones))

        output = (
            counts
            | 'format' >> beam.Map(format_result)
            | 'encode' >> beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes)
        )

        output | beam.io.gcp.bigquery.WriteToBigQuery(
            table=known_args.bq_table,
            schema=lambda table: (data_schema)
        )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()