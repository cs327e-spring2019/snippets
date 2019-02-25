import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class ActorCountFn(beam.DoFn):
  def process(self, element):
    values = element.strip().split('\t')
    year = values[0]
    category = values[1]
    winner = values[2]
    entity = values[3]

    if 'ACTOR' in category or 'ACTRESS' in category:
	    return [(entity, 1)]    
    
# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner') as p:

    # create a PCollection from the file contents.
    in_pcoll = p | 'Read File' >> ReadFromText('oscars_data.tsv')

    # apply a ParDo to the PCollection 
    out_pcoll = in_pcoll | 'Extract Actor' >> beam.ParDo(ActorCountFn())

    # write PCollection to a file
    out_pcoll | 'Write File' >> WriteToText('oscars_output.txt')
