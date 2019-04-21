import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

sql1='create table college.Class3 as select count(*) as count from college.Class2 where dept=\'CS\''
sql2='insert into college.Class3(count) select count(*) from college.Class2 where dept=\'CS\''

with models.DAG(
        'simple_workflow2',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
                
    get_class_count_create = BashOperator(
            task_id='get_class_count_create',
            bash_command='bq query --use_legacy_sql=false "' + sql1 + '"', 
            trigger_rule='all_done') # trigger only if parent task has completed
            
    get_class_count_insert = BashOperator(
            task_id='get_class_count_insert',
            bash_command='bq query --use_legacy_sql=false "' + sql2 + '"', 
            trigger_rule='all_failed') # trigger only if both parent tasks have failed
    
    get_class_count_create >> get_class_count_insert

