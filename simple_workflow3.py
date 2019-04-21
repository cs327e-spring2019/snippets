import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

BQ_TABLES = [
    'Class',
    'Current_Student',
    'New_Student'
]

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

with models.DAG(
        'simple_workflow3',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
                    
    create_dataset = BashOperator(
            task_id='create_dataset',
            bash_command='bq mk college_bkp')
            
    for table in BQ_TABLES:
        sql = 'create table college_bkp.' + table + ' as select * from college.' + table
        bash_tasks = BashOperator(
            task_id='copy_{}_table'.format(table),
            bash_command='bq query --use_legacy_sql=false "' + sql + '"'
        )
                     
    create_dataset >> bash_tasks
