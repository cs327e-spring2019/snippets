import datetime

from airflow import models
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

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
    
with models.DAG('college_workflow3',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    student_beam = BashOperator(
            task_id='student_beam',
            bash_command='python /Users/scohen/airflow/dags/' + student_script)
            
    takes_beam = BashOperator(
            task_id='takes_beam',
            bash_command='python /Users/scohen/airflow/dags/' + takes_script)
            
    teacher_beam = BashOperator(
            task_id='teacher_beam',
            bash_command='python /Users/scohen/airflow/dags/' + teacher_script)
            
    [student_beam, takes_beam, teacher_beam]