import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatTeacherFn(beam.DoFn):
  def process(self, element):
    teacher_record = element
    tid = teacher_record.get('tid')
    instructor = teacher_record.get('instructor')
    dept = teacher_record.get('dept')

    # extract first and last names from instructor and store them in separate entries
    split_name = instructor.split(',')
    if len(split_name) > 1:
        last_name = split_name[0]
        first_name = split_name[1]
    else:
        split_name = instructor.split(' ')
        first_name = split_name[0]
        last_name = split_name[1]
    
    teacher_record.pop('instructor')
    formatted_first_name = first_name.title()
    formatted_last_name = last_name.title()
    teacher_record['fname'] = formatted_first_name
    teacher_record['lname'] = formatted_last_name
    
    # rename department if it's using an abbreviated name
    if dept == 'CS':
        teacher_record['dept'] = 'Computer Science'
    if dept == 'Math':
        teacher_record['dept'] = 'Mathematics'
    
    # create key, value pairs
    teacher_tuple = (tid, teacher_record)
    return [teacher_tuple]

class DedupTeacherRecordsFn(beam.DoFn):
  def process(self, element):
     tid, teacher_obj = element # count_obj is an _UnwindowedValues type
     teacher_list = list(teacher_obj) # cast to list to support len
     teacher_record = teacher_list[0] # grab the first teacher record
     print('teacher_record: ' + str(teacher_record))
     return [teacher_record]  
           
         
PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is required when using the BQ source
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create beam pipeline using local runner
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT tid, instructor, dept FROM college_split.Teacher'))

    # write PCollection to log file
    query_results | 'Write log 1' >> WriteToText('query_results.txt')

    # apply ParDo to reformat the instructor and dept values  
    formatted_teacher_pcoll = query_results | 'Format DOB' >> beam.ParDo(FormatTeacherFn())

    # write PCollection to log file
    formatted_teacher_pcoll | 'Write log 2' >> WriteToText('formatted_teacher_pcoll.txt')

    # group teachers by tid
    grouped_teacher_pcoll = formatted_teacher_pcoll | 'Group by sid' >> beam.GroupByKey()

    # write PCollection to log file
    grouped_teacher_pcoll | 'Write log 3' >> WriteToText('grouped_teacher_pcoll.txt')
  
    # remove duplicate teacher records
    distinct_teacher_pcoll = grouped_teacher_pcoll | 'Dedup teacher records' >> beam.ParDo(DedupTeacherRecordsFn())

    # write PCollection to log file
    distinct_teacher_pcoll | 'Write log 4' >> WriteToText('distinct_teacher_pcoll.txt')
    
    qualified_table_name = PROJECT_ID + ':college_normalized.Teacher'
    table_schema = 'tid:STRING,fname:STRING,lname:STRING,dept:STRING'
    
    # write PCollection to new BQ table
    distinct_teacher_pcoll | 'Write BQ table' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
