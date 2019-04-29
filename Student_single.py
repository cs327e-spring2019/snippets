import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatDOBFn(beam.DoFn):
  def process(self, element):
    student_record = element
    sid = student_record.get('sid')
    fname = student_record.get('fname')
    lname = student_record.get('lname')
    dob = student_record.get('dob')
    print('current dob: ' + dob)

    # reformat any dob values that are MM/DD/YYYY to YYYY-MM-DD
    split_date = dob.split('/')
    if len(split_date) > 1:
        month = split_date[0]
        day = split_date[1]
        year = split_date[2]
        dob = year + '-' + month + '-' + day
        print('new dob: ' + dob)
        student_record['dob'] = dob
    
    # create key, value pairs
    student_tuple = (sid, student_record)
    return [student_tuple]

class DedupStudentRecordsFn(beam.DoFn):
  def process(self, element):
     sid, student_obj = element # count_obj is an _UnwindowedValues type
     student_list = list(student_obj) # cast to list to support len
     student_record = student_list[0] # grab the first student record
     print('student_record: ' + str(student_record))
     return [student_record]  
           
         
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is required when using the BQ source
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create beam pipeline using local runner
p = beam.Pipeline('DirectRunner', options=opts)

sql = 'SELECT sid, fname, lname, dob FROM college_workflow.Student_Temp'
bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

# write PCollection to log file
query_results | 'Write log 1' >> WriteToText('query_results.txt')

# apply ParDo to format the student's date of birth  
formatted_dob_pcoll = query_results | 'Format DOB' >> beam.ParDo(FormatDOBFn())

# write PCollection to log file
formatted_dob_pcoll | 'Write log 2' >> WriteToText('formatted_dob_pcoll.txt')

# group students by sid
grouped_student_pcoll = formatted_dob_pcoll | 'Group by sid' >> beam.GroupByKey()

# write PCollection to log file
grouped_student_pcoll | 'Write log 3' >> WriteToText('grouped_student_pcoll.txt')

# remove duplicate student records
distinct_student_pcoll = grouped_student_pcoll | 'Dedup student records' >> beam.ParDo(DedupStudentRecordsFn())

# write PCollection to log file
distinct_student_pcoll | 'Write log 4' >> WriteToText('distinct_student_pcoll.txt')

dataset_id = 'college_workflow'
table_id = 'Student'
schema_id = 'sid:STRING,fname:STRING,lname:STRING,dob:DATE'

# write PCollection to new BQ table
distinct_student_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                table=table_id, 
                                                schema=schema_id,
                                                project=PROJECT_ID,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                batch_size=int(100))
result = p.run()
result.wait_until_finish()