import os, datetime
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
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-takes-table',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

p = beam.Pipeline('DataflowRunner', options=opts)

takes_sql = 'SELECT sid, cno, grade FROM college_workflow.Takes_Temp'
class_sql = 'SELECT cno FROM college_workflow.Class'

takes_pcoll = p | 'Read from BQ Takes' >> beam.io.Read(beam.io.BigQuerySource(query=takes_sql, use_standard_sql=True))
class_pcoll = p | 'Read from BQ Class' >> beam.io.Read(beam.io.BigQuerySource(query=class_sql, use_standard_sql=True))

# write PCollections to log files
takes_pcoll | 'Write log 1' >> WriteToText(DIR_PATH + 'takes_query_results.txt')
class_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'class_query_results.txt')

# apply ParDo to check cno value's referential integrity 
norm_takes_pcoll = takes_pcoll | 'Normalize Record' >> beam.ParDo(NormalizeTakesFn(), beam.pvalue.AsList(class_pcoll))

# write PCollection to log file
norm_takes_pcoll | 'Write log 3' >> WriteToText(DIR_PATH + 'norm_takes_pcoll.txt')

dataset_id = 'college_workflow'
table_id = 'Takes'
schema_id = 'sid:STRING,cno:STRING,grade:STRING'

# write PCollection to new BQ table
norm_takes_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                            table=table_id, 
                                            schema=schema_id,
                                            project=PROJECT_ID,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                            batch_size=int(100))
result = p.run()
result.wait_until_finish()