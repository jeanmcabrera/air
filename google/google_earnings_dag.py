#!/usr/bin/env python

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime, timedelta, date
from dateutil import relativedelta
from airflow.hooks.S3_hook import S3Hook
import logging, os, fnmatch

default_args = {
    'owner': 'xxxxxxxx',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'email': ['xxxxxxxxxxx'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def build_manifest(local_path, s3_bucket, s3_prefix, bucket, yearmonth, bucket_subfolder, manifest_file, file_filter, **kwargs):

    manifest_file = local_path + "/" + manifest_file
    manifest_entries = []

    with open(manifest_file, "w") as manifest:
        manifest.write("{\n")
        manifest.write("  \"entries\": [\n")

        for i,l in enumerate(["    {\"url\":\"s3://"+s3_bucket+"/"+s3_prefix+"/google_pos/"+bucket+"/"+yearmonth+"/"+bucket_subfolder+"/"+file+"\"}" for file in fnmatch.filter(os.listdir(local_path), '*' + file_filter+'*.csv')]):
            if i>0:
                manifest.write(",")
            manifest.write("\n"+str(l))
        manifest.write("  ]\n")
        manifest.write("}\n")

def s3_upload(local_path, s3_bucket, s3_prefix, bucket, yearmonth, bucket_subfolder, manifest, **kwargs):

    logging.info("Running dag ************************")
    dest_s3 = S3Hook('xxxxxxxxxxx')
    bucket_name = s3_bucket 	#"analyst-jean-michel"
    logging.info("Checking connection and bucket S3")

    if not dest_s3.check_for_bucket(bucket_name):
      raise AirflowException("The bucket key {0} does not exist", bucket_name)
    else:
    	logging.info("Found S3 bucket {0}".format(bucket_name))

    #dest_s3_key = "RAW/singular/kpi/kpi/1/test.txt"
    #logging.info("S3 Key: {0}".format(dest_s3_key))
    #if not dest_s3.check_for_key(dest_s3_key, bucket_name):
    #        raise AirflowException("The dest key {0} does not exist".format(dest_s3_key))
    #else:
    #	logging.info("Found S3 key {0}".format(dest_s3_key))

    logging.info("Uploading files now")
    dirs = os.listdir(local_path)

    for file in dirs:
        if file.endswith('.csv'):
    	    dest_s3.load_file(
            filename=local_path + "/" + file,
            bucket_name=bucket_name,
            key=s3_prefix + '/google_pos/'+bucket+'/'+yearmonth+'/'+bucket_subfolder+'/'+file,
            replace=True
            )
    logging.info("Uploading manifest files")
    for i in range(0,len(manifest.split(','))):
	dest=s3_prefix + '/google_pos/'+bucket+'/'+yearmonth+'/'+bucket_subfolder+'/'+manifest.split(',')[i]
	logging.info("Uploading manifest file: {0}".format(manifest.split(',')[i]))
	logging.info("destination: {0}".format(dest))
        dest_s3.load_file(
            filename=local_path + "/" + manifest.split(',')[i],
            bucket_name=bucket_name,
            key=s3_prefix + '/google_pos/'+bucket+'/'+yearmonth+'/'+bucket_subfolder+'/'+manifest.split(',')[i],
            replace=True
        )

    logging.info("S3 upload completed")

dag = DAG(
    'google_earnings',
    default_args=default_args,
    schedule_interval=None)

# check if it is the 1st, 2nd or 3rd, so we know to reload the previous month
def is_end_of_month():
    if datetime.today().day in (1,2,3,4,5,29):
	print("end of the month")
        return True
    else:
	print("Not the end of the month")
        return False

check_end_of_month = ShortCircuitOperator(
    task_id='check_end_of_month',
    python_callable=is_end_of_month,
    dag=dag)

clear_folders = BashOperator(
        task_id='clean_up',
        bash_command='rm -f /xxxx/xxx-airflow/airflow/google/data/xx/earnings/*|rm -f /xxx/xxx-airflow/airflow/google/data/playdemic/earnings/*',
        dag=dag)

playdemic_download_lastmonth = BashOperator(
    task_id='google_pos_s3_playdemic_lastmonth',
    bash_command='python /xxx/xxx-airflow/airflow/google/google_pos_s3.py --yearmonth {{params.yearmonth}} --bucket {{params.bucket}} --client_id {{params.client_id}} --client_secret {{params.client_secret}} --refresh_token {{params.refresh_token}} --file_folder {{params.file_folder}}',
    params={
        'yearmonth': 'lastmonth',
        'bucket': 'xxxxxxxxxxx',
        'client_id': 'xxxxxxxxxxx',
        'client_secret': 'xxxxxxxxxxx',
        'refresh_token': 'xxxxxxxxxxx',
        'file_folder': '/xxx/xxx-airflow/airflow/google/data/playdemic'
    },
    dag=dag)

xx_download_lastmonth = BashOperator(
    task_id='google_pos_s3_xx_lastmonth',
    bash_command='python /mnt/xxx-airflow/airflow/google/google_pos_s3.py --yearmonth {{params.yearmonth}} --bucket {{params.bucket}} --client_id {{params.client_id}} --client_secret {{params.client_secret}} --refresh_token {{params.refresh_token}} --file_folder {{params.file_folder}}',
    params={
        'yearmonth': 'lastmonth',
        'bucket': 'xxxxxxxxxxx',
        'client_id': 'xxxxxxxxxxx',
        'client_secret': 'xxxxxxxxxxx',
        'refresh_token': 'xxxxxxxxxxx',
        'file_folder': '/mnt/xxx-airflow/airflow/google/data/xx'
    },
    dag=dag)

s3_upload_xx_earnings = PythonOperator(
        task_id='s3_upload_xx_earnings',
        python_callable=s3_upload,
        provide_context=True,
        op_kwargs={'local_path': '/mnt/xxx-airflow/airflow/google/data/xx/earnings'
                    ,'s3_bucket':'xxxxxxxxxxx'
                    ,'s3_prefix':'xxxxxxxxxxx'
		            ,'bucket': 'xxxxxxxxxxx'
		            ,'yearmonth': (date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
                    ,'bucket_subfolder':'earnings'
                    ,'manifest':'manifest_earnings.txt'
                  },
        dag=dag)

s3_upload_playdemic_earnings = PythonOperator(
        task_id='s3_upload_playdemic_earnings',
        python_callable=s3_upload,
        provide_context=True,
        op_kwargs={'local_path': '/mnt/xxx-airflow/airflow/google/data/playdemic/earnings'
                    ,'s3_bucket':'xxxxxxxxxxx'
                    ,'s3_prefix':'xxxxxxxxxxx'
                    ,'bucket': 'xxxxxxxxxxx'
		    ,'yearmonth': (date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
                    ,'bucket_subfolder':'earnings'
                    ,'manifest':'manifest_earnings.txt'
                  },
        dag=dag)

rs_load_xx_earnings = PostgresOperator(
        task_id='load_xx_earning_to_redshift',
        postgres_conn_id='redshift_test',
        sql="""COPY sandbox.earnings (description,transaction_date,transaction_time,tax_type,transaction_type,refund_type,product_title,product_id,product_type,sku_id,hardware,buyer_country,buyer_state,buyer_postal_code,buyer_currency,amount_buyer_currency,currency_conversion_rate,merchant_currency,amount_merchant_currency)
        FROM 's3://xxxxxxxxxxx/google/google_pos/xxxxxxxxxxx/{{ params.yearmonth }}/earnings/manifest_earnings.txt'
        CREDENTIALS 'aws_access_key_id=xxxxxxxxxxx;aws_secret_access_key=xxxxxxxxxxx'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        DELIMITER ',' CSV QUOTE '"'
        IGNOREHEADER 1
        DATEFORMAT 'auto'""",
        params={'yearmonth': (date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')},
        retries=1, retry_delay=timedelta(0, 60),
        dag=dag)

rs_load_playdemic_earnings = PostgresOperator(
        task_id='load_playdemic_earning_to_redshift',
        postgres_conn_id='redshift_test',
        sql="""COPY sandbox.earnings (description,transaction_date,transaction_time,tax_type,transaction_type,refund_type,product_title,product_id,product_type,sku_id,hardware,buyer_country,buyer_state,buyer_postal_code,buyer_currency,amount_buyer_currency,currency_conversion_rate,merchant_currency,amount_merchant_currency)
        FROM 's3://xxxxxxxxxxx/google/google_pos/xxxxxxxxxxx/{{ params.yearmonth }}/earnings/manifest_earnings.txt'
        CREDENTIALS 'aws_access_key_id=xxxxxxxxxxx;aws_secret_access_key=xxxxxxxxxxx'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        DELIMITER ',' CSV QUOTE '"'
        IGNOREHEADER 1
        DATEFORMAT 'auto'""",
        params={'yearmonth': (date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')},
        retries=1, retry_delay=timedelta(0, 60),
        dag=dag)

rs_delete_earnings = PostgresOperator(
        task_id='rs_delete_earnings',
        postgres_conn_id='redshift_test',
        sql="DELETE FROM sandbox.earnings WHERE transaction_date like '" + (datetime.today() + relativedelta.relativedelta(day=1, days=-1)).strftime('%b') + "%" + (datetime.today() + relativedelta.relativedelta(day=1)).strftime('%Y') + "'",
        dag=dag)

xx_build_manifest_earnings = PythonOperator(
        task_id='xx_build_manifest_earnings',
        python_callable=build_manifest,
        provide_context=True,
        op_kwargs={'local_path': '/mnt/xxx-airflow/airflow/google/data/xx/earnings'
                    ,'s3_bucket':'xxxxxxxxxxx'
                    ,'s3_prefix':'xxxxxxxxxxx'
                    ,'bucket': 'xxxxxxxxxxx'
                    ,'yearmonth': (date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
                    ,'bucket_subfolder':'earnings'
                    ,'manifest_file': 'manifest_earnings.txt'
                    ,'file_filter': 'PlayApps_' + (date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
                  },
        dag=dag)

playdemic_build_manifest_earnings = PythonOperator(
        task_id='playdemic_build_manifest_earnings',
        python_callable=build_manifest,
        provide_context=True,
        op_kwargs={'local_path': '/mnt/xxx-airflow/airflow/google/data/playdemic/earnings'
                    ,'s3_bucket':'xxxxxxxxxxx'
                    ,'s3_prefix':'xxxxxxxxxxx'
                    ,'bucket': 'xxxxxxxxxxx'
                    ,'yearmonth':(date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
                    ,'bucket_subfolder':'earnings'
                    ,'manifest_file': 'manifest_earnings.txt'
                    ,'file_filter': 'PlayApps_' + (date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
                  },
        dag=dag)

rs_load_playdemic_earnings.set_upstream(rs_delete_earnings)
rs_delete_earnings.set_upstream(s3_upload_playdemic_earnings)
s3_upload_playdemic_earnings.set_upstream(playdemic_build_manifest_earnings)
playdemic_build_manifest_earnings.set_upstream(playdemic_download_lastmonth)
playdemic_download_lastmonth.set_upstream(check_end_of_month)

rs_load_xx_earnings.set_upstream(rs_delete_earnings)
rs_delete_earnings.set_upstream(s3_upload_xx_earnings)
s3_upload_xx_earnings.set_upstream(xx_build_manifest_earnings)
xx_build_manifest_earnings.set_upstream(xx_download_lastmonth)
xx_download_lastmonth.set_upstream(check_end_of_month)

check_end_of_month.set_upstream(clear_folders)
