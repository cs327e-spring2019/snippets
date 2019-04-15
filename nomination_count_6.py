import apache_beam as beam
from apache_beam import pvalue
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
    
# Create a Pipeline using the local runner 
with beam.Pipeline('DirectRunner') as p:

    # create a PCollection from the file contents.
    in_pcoll = p | 'Read File' >> ReadFromText('oscars_data.tsv')

    # apply a ParDo to the PCollection 
    out_pcoll = in_pcoll | 'Extract Actor and Actress' >> beam.ParDo(ActorActressCountFn()).with_outputs(
                                                          ActorActressCountFn.OUTPUT_TAG_ACTOR_COUNT,
                                                          ActorActressCountFn.OUTPUT_TAG_ACTRESS_COUNT)
                                                          
    actor_pcoll = out_pcoll[ActorActressCountFn.OUTPUT_TAG_ACTOR_COUNT]
    actress_pcoll = out_pcoll[ActorActressCountFn.OUTPUT_TAG_ACTRESS_COUNT]

    # write PCollections to files
    actor_pcoll | 'Write File' >> WriteToText('actor_output.txt')
    actress_pcoll | 'Write File' >> WriteToText('actress_output.txt')
