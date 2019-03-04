import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class NormalizeTakesFn(beam.DoFn):
  def process(self, element, class_pcoll):
    takes_record = element
    sid = takes_record.get('sid')
    cno = takes_record.get('cno')
    grade = takes_record.get('grade')

    found_cno_match = False
    for class_record in class_pcoll:
        class_cno = class_record.get('cno')
        if cno == class_cno:
            found_cno_match = True
            break
    
    if (found_cno_match == False):
        # found a bad cno value
        print('found bad cno: ' + cno)
        cno_splits = cno.split('-')
        cno = cno_splits[0]
        takes_record['cno'] = cno
    
    return [takes_record]
            
         
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is required when using the BQ source
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create beam pipeline using local runner
with beam.Pipeline('DirectRunner', options=opts) as p:

    takes_pcoll = p | 'Read from BQ Takes' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT sid, cno, grade FROM college_split.Takes'))
    class_pcoll = p | 'Read from BQ Class' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT cno FROM college_split.Class'))

    # write PCollections to log files
    takes_pcoll | 'Write log 1' >> WriteToText('takes_query_results.txt')
    class_pcoll | 'Write log 2' >> WriteToText('class_query_results.txt')

    # apply ParDo to check cno value's referential integrity 
    norm_takes_pcoll = takes_pcoll | 'Normalize Record' >> beam.ParDo(NormalizeTakesFn(), beam.pvalue.AsList(class_pcoll))

    # write PCollection to log file
    norm_takes_pcoll | 'Write log 3' >> WriteToText('norm_takes_pcoll.txt')
    
    qualified_table_name = PROJECT_ID + ':college_normalized.Takes'
    table_schema = 'sid:STRING,cno:STRING,grade:STRING'
    
    # write PCollection to new BQ table
    norm_takes_pcoll | 'Write BQ table' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
