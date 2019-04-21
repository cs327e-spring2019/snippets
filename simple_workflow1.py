import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

sql1='create table college.Class2 as select * from college.Class'
sql2='create table college.Class3 as select count(*) as count from college.Class2'

with models.DAG(
        'simple_workflow1',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
            
    copy_class_table = BashOperator(
            task_id='copy_class_table',
            bash_command='bq query --use_legacy_sql=false "' + sql1 + '"')
            
    get_class_count = BashOperator(
            task_id='get_class_count',
            bash_command='bq query --use_legacy_sql=false "' + sql2 + '"') # default trigger rule is all_success
    
    copy_class_table >> get_class_count

