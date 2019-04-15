import os, datetime
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
    values = element.strip().split('\t')
    year = values[0]
    category = values[1]
    winner = values[2]
    entity = values[3]

    if 'ACTOR' in category:
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_ACTOR_COUNT, (entity, 1))  
        
    if 'ACTRESS' in category:
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_ACTRESS_COUNT, (entity, 1))  

# DoFn with single output
class SumNominationsFn(beam.DoFn):
  
  def process(self, element):
     name, counts = element
     total_counts = len(counts)
     return [(name, total_counts)] 
 
    
PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH_IN = BUCKET + '/input/' 
DIR_PATH_OUT = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'nomination-count-8',
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

    # apply ParDo with tagged outputs 
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

    # apply ParDo with single DoFn to both PCollections
    summed_actor_pcoll = grouped_actor_pcoll | 'Sum up Actor Nominations' >> beam.ParDo(SumNominationsFn())
    summed_actress_pcoll = grouped_actress_pcoll | 'Sum up Actress Nominations' >> beam.ParDo(SumNominationsFn())
    
    # write PCollections to files
    summed_actor_pcoll | 'Write Actor File 3' >> WriteToText(DIR_PATH_OUT + 'summed_actor_output.txt')
    summed_actress_pcoll | 'Write Actress File 3' >> WriteToText(DIR_PATH_OUT + 'summed_actress_output.txt')
