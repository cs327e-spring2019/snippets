import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

###### SQL variables ###### 
raw_dataset = 'college'
new_dataset = 'college_workflow'
sql_cmd_start = 'bq query --use_legacy_sql=false '

sql_student = 'create table ' + new_dataset + '.Student_Temp as select distinct sid, fname, lname, dob ' \
           'from ' + raw_dataset + '.Current_Student ' \
           'union distinct ' \
           'select distinct sid, fname, lname, cast(dob as string) as dob ' \
           'from college.New_Student'
       
sql_teacher = 'create table ' + new_dataset + '.Teacher_Temp as ' \
            'select distinct tid, instructor, dept ' \
            'from ' + raw_dataset + '.Class ' \
            'where tid is not null ' \
            'order by tid'

sql_class = 'create table '+ new_dataset + '.Class as ' \
            'select distinct cno, cname, cast(credits as int64) as credits ' \
            'from ' + raw_dataset + '.Class ' \
            'where cno is not null ' \
            'order by cno '

sql_teaches = 'create table ' + new_dataset + '.Teaches as ' \
            'select distinct tid, cno ' \
            'from ' + raw_dataset + '.Class ' \
            'where tid is not null ' \
            'and cno is not null ' \
            'order by tid'
            
sql_takes = 'create table ' + new_dataset + '.Takes_Temp as ' \
            'select distinct sid, cno, grade ' \
            'from ' + raw_dataset + '.Current_Student ' \
            'where sid is not null ' \
            'and cno is not null ' \
            'order by sid'
  
###### Beam variables ######
AIRFLOW_DAGS_DIR='/home/shirley_cohen/.local/bin/dags/' # replace with your path          
LOCAL_MODE=1 # run beam jobs locally
DIST_MODE=2 # run beam jobs on Dataflow

mode=LOCAL_MODE

if mode == LOCAL_MODE:
    student_script = 'Student_single.py'
    takes_script = 'Takes_single.py'
    teacher_script = 'Teacher_single.py'
    
if mode == DIST_MODE:
    student_script = 'Student_cluster.py'
    takes_script = 'Takes_cluster.py'
    teacher_script = 'Teacher_cluster.py'

###### DAG section ###### 
with models.DAG(
        'college_workflow4',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    ###### SQL tasks ######
    delete_dataset = BashOperator(
            task_id='delete_dataset',
            bash_command='bq rm -r -f college_workflow')
                
    create_dataset = BashOperator(
            task_id='create_dataset',
            bash_command='bq mk college_workflow')
                    
    create_student_table = BashOperator(
            task_id='create_student_table',
            bash_command=sql_cmd_start + '"' + sql_student + '"')
            
    create_teacher_table = BashOperator(
            task_id='create_teacher_table',
            bash_command=sql_cmd_start + '"' + sql_teacher + '"')
            
    create_class_table = BashOperator(
            task_id='create_class_table',
            bash_command=sql_cmd_start + '"' + sql_class + '"')
            
    create_teaches_table = BashOperator(
            task_id='create_teaches_table',
            bash_command=sql_cmd_start + '"' + sql_teaches + '"')    
            
    create_takes_table = BashOperator(
            task_id='create_takes_table',
            bash_command=sql_cmd_start + '"' + sql_takes + '"')
    
    ###### Beam tasks ######     
    student_beam = BashOperator(
            task_id='student_beam',
            bash_command='python ' + AIRFLOW_DAGS_DIR + student_script)
            
    takes_beam = BashOperator(
            task_id='takes_beam',
            bash_command='python ' + AIRFLOW_DAGS_DIR + takes_script)
            
    teacher_beam = BashOperator(
            task_id='teacher_beam',
            bash_command='python ' + AIRFLOW_DAGS_DIR + teacher_script)
            
    transition = DummyOperator(task_id='transition')
            
    delete_dataset >> create_dataset >> [create_student_table, create_teacher_table, create_class_table, create_teaches_table, create_takes_table] >> transition
    transition >> [student_beam, takes_beam, teacher_beam]