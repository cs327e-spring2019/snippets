import os, datetime, logging, traceback

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn with multiple outputs
class ActorActressCountFn(beam.DoFn):
    
  OUTPUT_TAG_ACTOR_COUNT = 'tag_actor_count'
  OUTPUT_TAG_ACTRESS_COUNT = 'tag_actress_count'
  
  def process(self, element):
    
    from apache_beam import pvalue
    
    values = element.strip().split('\t')
    year = values[0]
    category = values[1]
    winner = values[2]
    entity = values[3].title()

    if 'ACTOR' in category:
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_ACTOR_COUNT, (entity, 1))  
        
    if 'ACTRESS' in category:
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_ACTRESS_COUNT, (entity, 1))  
  
# DoFn with single output
class SumNominationsFn(beam.DoFn):
  
  def process(self, element):
     name, counts_obj = element
     counts = list(counts_obj)
     sum_counts = str(len(counts))
     return [(name, sum_counts)]  

# DoFn with single output
class MakeBQRecordFn(beam.DoFn):
  
  def process(self, element):
     
     name, total_nominations = element
     record = {'name': name, 'nominations' : total_nominations}
     return [record]   

 
PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH_IN = BUCKET + '/input/' 
DIR_PATH_OUT = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'nomination-count-10',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}

opts = PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:

    # create PCollection from the file contents
    in_pcoll = p | 'Read File' >> ReadFromText(DIR_PATH_IN + 'oscars_data.tsv')

    # apply a ParDo to the PCollection 
    out_pcoll = in_pcoll | 'Extract Actor and Actress' >> beam.ParDo(ActorActressCountFn()).with_outputs(
                                                          ActorActressCountFn.OUTPUT_TAG_ACTOR_COUNT,
                                                          ActorActressCountFn.OUTPUT_TAG_ACTRESS_COUNT)
                                                          
    actor_pcoll = out_pcoll[ActorActressCountFn.OUTPUT_TAG_ACTOR_COUNT]
    actress_pcoll = out_pcoll[ActorActressCountFn.OUTPUT_TAG_ACTRESS_COUNT]

    # write PCollections to files
    actor_pcoll | 'Write Actor File 1' >> WriteToText(DIR_PATH_OUT + 'actor_output.txt')
    actress_pcoll | 'Write Actress File 1' >> WriteToText(DIR_PATH_OUT + 'actress_output.txt')
    
    # apply GroupByKey 
    grouped_actor_pcoll = actor_pcoll | 'Group by Actor' >> beam.GroupByKey()
    grouped_actress_pcoll = actress_pcoll | 'Group by Actress' >> beam.GroupByKey()
    
    # write PCollections to files
    grouped_actor_pcoll | 'Write Actor File 2' >> WriteToText(DIR_PATH_OUT + 'grouped_actor_output.txt')
    grouped_actress_pcoll | 'Write Actress File 2' >> WriteToText(DIR_PATH_OUT + 'grouped_actress_output.txt')

    # apply ParDo 
    summed_actor_pcoll = grouped_actor_pcoll | 'Sum up Actor Nominations' >> beam.ParDo(SumNominationsFn())
    summed_actress_pcoll = grouped_actress_pcoll | 'Sum up Actress Nominations' >> beam.ParDo(SumNominationsFn())
    
    # write PCollections to files
    summed_actor_pcoll | 'Write Actor File 3' >> WriteToText(DIR_PATH_OUT + 'summed_actor_output.txt')
    summed_actress_pcoll | 'Write Actress File 3' >> WriteToText(DIR_PATH_OUT + 'summed_actress_output.txt')
    
    # apply ParDo  
    bq_actor_pcoll = actor_pcoll | 'Make BQ Actor' >> beam.ParDo(MakeBQRecordFn())
    bq_actress_pcoll = actress_pcoll | 'Make BQ Actress' >> beam.ParDo(MakeBQRecordFn())
    
    # write PCollections to files
    bq_actor_pcoll | 'Write Actor File 4' >> WriteToText(DIR_PATH_OUT + 'bq_actor_output.txt')
    bq_actress_pcoll | 'Write Actress File 4' >> WriteToText(DIR_PATH_OUT + 'bq_actress_output.txt')
    
    actor_table_name = PROJECT_ID + ':oscars.Actor_Nominations'
    actress_table_name = PROJECT_ID + ':oscars.Actress_Nominations'
    table_schema = 'name:STRING,nominations:INTEGER'

    # write PCollections to BQ tables
    bq_actor_pcoll | 'Write Actor Table' >> beam.io.Write(beam.io.BigQuerySink(actor_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
                                                    
    bq_actress_pcoll | 'Write Actress Table' >> beam.io.Write(beam.io.BigQuerySink(actress_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

