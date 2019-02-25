import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class ActorCountFn(beam.DoFn):
  def process(self, element):
    record = element
    year = record.get('year')
    category = record.get('category')
    winner = record.get('winner')
    entity = record.get('entity')

    if 'ACTOR' in category or 'ACTRESS' in category:
	    return [(entity, 1)]    

# DoFn to perform on each element in the input PCollection.
class ActorSumFn(beam.DoFn):
  def process(self, element):
     actor, count_obj = element # count_obj is an _UnwindowedValues type
     count_list = list(count_obj) # cast to list to support len
     total_count = len(count_list)
     return [(actor, total_count)]  
    

PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM oscars.Academy_Award'))

    # write PCollection to log file
    query_results | 'Write to log 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    actor_pcoll = query_results | 'Extract Actor' >> beam.ParDo(ActorCountFn())

    # write PCollection to log file
    actor_pcoll | 'Write to log 2' >> WriteToText('actor_count.txt')

    # apply GroupByKey to the PCollection
    group_pcoll = actor_pcoll | 'Group by Actor' >> beam.GroupByKey()

    # write PCollection to log file
    group_pcoll | 'Write to log 3' >> WriteToText('group_by_actor.txt')
  
    # apply a ParDo to the PCollection
    out_pcoll = group_pcoll | 'Sum up Counts' >> beam.ParDo(ActorSumFn())

    # write PCollection to a file
    out_pcoll | 'Write File' >> WriteToText('oscars_output.txt')
