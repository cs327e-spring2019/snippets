import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs on each element in the input PCollection.
class ActorCountFn(beam.DoFn):
  def process(self, element):
    record = element
    year = record.get('year')
    category = record.get('category')
    winner = record.get('winner')
    entity = record.get('entity')

    if 'ACTOR' in category or 'ACTRESS' in category:
	    return [(entity, 1)]    

# DoFn performs on each element in the input PCollection.
class ActorSumFn(beam.DoFn):
  def process(self, element):
     actor, count_obj = element # count_obj is an _UnwindowedValues type
     count_list = list(count_obj) # cast to list to support len
     total_count = len(count_list)
     return [(actor, total_count)]  
    
# DoFn performs on each element in the input PCollection.
class MakeRecordFn(beam.DoFn):
  def process(self, element):
     name, count = element
     record = {'name': name, 'count': count}
     return [record] 
         
         
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
    sum_pcoll = group_pcoll | 'Sum up Counts' >> beam.ParDo(ActorSumFn())

    # write PCollection to a file
    sum_pcoll | 'Write File' >> WriteToText('oscars_output.txt')
    
    # make BQ records
    out_pcoll = sum_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':oscars.Nomination_Count'
    table_schema = 'name:STRING,count:INTEGER'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
