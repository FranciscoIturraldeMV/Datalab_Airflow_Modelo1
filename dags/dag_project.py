from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

#from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.mongo_hook import MongoHook

from scripts.training import Training
from scripts.evaluation import Evaluate
#from scripts.recall import Recall
from scripts.retire import Retire
from scripts.etl import ETL

SCONN_ID = 'dev_postgres'
DCONN_ID = 'mongo_default'

with DAG(
    dag_id='ml_project',
    description='ML project',
    schedule_interval='@once',
    start_date=datetime(2020, 1, 6)
) as dag:

#Source Metadata
    src = PostgresHook(postgres_conn_id=SCONN_ID)
    # src_conn = src.get_conn()
    # cursor = src_conn.cursor()
    #cursor.execute("SELECT * FROM interaction WHERE id>1617;")
    #src.insert_rows(table='interaction', rows= ((1618, 9999, 9999,10,'bought'))   )

    # #Origin Metadata
    dsrc = MongoHook(mongo_conn_id = DCONN_ID)
    # dsrc_conn = dsrc.get_conn()

   # enter_point = PythonOperator(
   #     task_id='enter_point',
   #     python_callable= Recall
   # )
#Python ETL
    etl_point = PythonOperator(
        task_id='etl_point',
        python_callable=ETL,
        op_kwargs={'con': src}
    )
#Python Loading to Mongo
    mongo_point = PythonOperator(
        task_id = 'mongo_point',
        python_callable = Retire,
        provide_context=True,
        op_kwargs={'con': dsrc}
    )
#Machine Learning Training
    model_point = PythonOperator(
        task_id='model_point',
        python_callable = Training
    )
#Machine Learning Evaluation
    test_point = PythonOperator(
	task_id='test_point',
	python_callable = Evaluate,
	provide_context=True
    )
#Descriptive
    descriptive_point = DummyOperator(
        task_id='Desc_Point'
    )

    enter_point = DummyOperator(
        task_id='Enter_Point'
    )

    exit_point = DummyOperator(
        task_id='Exit_Point'
    )

    enter_point >> etl_point >> mongo_point >> model_point >> test_point >> exit_point
    mongo_point >> descriptive_point >> exit_point