"""
Example DAG to load csv into bigquery table using sensor.
Sensor will search the file in the cloud storage bucket to execute the dag.
"""

import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
#from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor  ##Can use this one as well instead of below one
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'airflow_ds')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'gcs_to_bq_table')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2), #datetime(2019, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='sensor_gcs_to_bigquery',
	catchup=False,
	default_args=default_args,
    schedule_interval=None
)


#file_sensor = GCSObjectExistenceSensor(
#    task_id='gcs_polling',  
#    bucket='third-campus-303308-cf-landing',
#	object='bigmart_data.csv',
#    dag=dag,
#)  ##Can use this one as well instead of below one

file_sensor = GoogleCloudStoragePrefixSensor(
    task_id='gcs_polling',  
    bucket='third-campus-303308-cf-landing',
	prefix='bigmart_data',
    dag=dag,
)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_ds_dataset', dataset_id=DATASET_NAME, dag=dag,
)

# [START howto_operator_gcs_to_bigquery]
load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='third-campus-303308-cf-landing',
    source_objects=['bigmart_data.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=None,
	skip_leading_rows=1,
	field_delimiter=',',
	autodetect=True,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

## Trigger the own dag to execute i.e. it scheduled again and looking for the file ,if found then execute the same again from begining
trigger = TriggerDagRunOperator(
    task_id='trigger_dag',
    trigger_dag_id='sensor_gcs_to_bigquery',
    dag=dag,
)

file_sensor >> create_dataset >> load_csv >> trigger
