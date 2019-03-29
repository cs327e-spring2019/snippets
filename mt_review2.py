import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class Lmfao(beam.DoFn):
  def process(self, element):
    employed_at = element.get('employed_at')
    salary = element.get('salary')
    return [(employed_at, salary)]
    
class Rofl(beam.DoFn):
  def process(self, element):
    employed_at = element[0]
    salaries    = element[1]
    return {
      'employed_at'  : employed_at
      'mystery_field': sum(salaries)/len(salaries)
    }
         
PROJECT_ID = os.environ['PROJECT_ID']

options = { 'project': PROJECT_ID }
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT employed_at, salary, fname, lname FROM fast_food.Employee'))

    t1 = query_results | 'Awesome Transform' >> beam.ParDo(Lmfao())

    t2 = t1 | 'Rad Transform' >> beam.GroupByKey()
  
    out_pcoll = t2 | 'Cool Transform' >> beam.ParDo(Rofl())
    
    qualified_table_name = PROJECT_ID + ':fast_food.MysteryTable'
    table_schema = 'employed_at:INTEGER,mystery_field:NUMERIC'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
