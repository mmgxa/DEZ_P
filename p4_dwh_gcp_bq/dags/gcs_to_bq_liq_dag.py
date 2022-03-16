import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'iowa_liquor')

DATASET = "liquor"
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="gcs_to_bq_liq_dag",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de-proj'],
) as dag:


    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{DATASET}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/iowa_liq/*"],
                },
            },
        )

    CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET} \
            PARTITION BY date \
            CLUSTER BY county \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{DATASET}_external_table;"
        )

    bq_create_part_clust_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{DATASET}_part_clust_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )


    bigquery_external_table_task >> bq_create_part_clust_table_job
