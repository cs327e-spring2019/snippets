import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class MysteryFn(beam.DoFn):
  def process(self, element):
    break_up = element.get('serves').split(',')
    store_array = []
    for food in break_up:
      item = {
        'restaurant_id': element.get('id'),
        'food': food
      }
      store_array.append(item)
    return store_array
         
PROJECT_ID = os.environ['PROJECT_ID']

options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM fast_food.Restaurant'))

    out_pcoll = query_results | 'Extract Actor' >> beam.ParDo(MysteryFn())
    
    qualified_table_name = PROJECT_ID + ':fast_food.TableServe'
    table_schema = 'id:INTEGER,food:STRING'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
