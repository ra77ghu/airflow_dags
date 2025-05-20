from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
import boto3
import os
import chardet
import sys

s3_bucket = Variable.get("bhavcopies_s3")

default_args = {
    'start_date': datetime(2025, 5, 19),
}

market_segments = ['nsefno', 'nsecm', 'bsefno', 'bsecm'] #, 'mcxfno'

column_rename_map = {
    "TradDt": "trading_session",
    "FinInstrmId": "instrument",
    "XpryDt": "expiry_date",
    "ClsPric": "close_price",
    "SttlmPric": "settlement_price",
    "LastPric": "last_price",
}

selected_columns = list(column_rename_map.keys())

segment_metadata = {
    'nsefno': {"exchange": 1, "market_segment": 2},
    'nsecm': {"exchange": 1, "market_segment": 1},
    'bsefno': {"exchange": 2, "market_segment": 2},
    'bsecm': {"exchange": 2, "market_segment": 1},
    'mcxfno': {"exchange": 3, "market_segment": 4},
}

with DAG(
    dag_id='filter_bhavcopies',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["bhavcopy", "filter"],
) as dag:

    @task
    def get_conf(**context):
        run_date =  context["dag_run"].conf.get("run_date") or context["ds"]
        run_date = datetime.strptime(run_date, "%Y-%m-%d").strftime("%Y%m%d")
        prev_trading_day = conf.get("prev_trading_day") #not using
        segments = context["dag_run"].conf.get("market_segments") or market_segments
        return [{"market_segment": ms, "run_date": run_date} for ms in segments]

    @task
    def filter_and_upload(data):
        ms = data["market_segment"]
        run_date = data["run_date"]

        s3 = boto3.client("s3")
        key_prefix = f"bhavcopy/{ms}/{run_date}/"
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=key_prefix)

        if "Contents" not in response:
            print(f"No files found for {ms}/{run_date}")
            sys.exit(0)
            return

        for obj in response["Contents"]:
            key = obj["Key"]
            filename = key.split("/")[-1]
            local_csv_path = f"/tmp/{filename}"
            filtered_csv_path = f"/tmp/filtered_bhavcopy_{ms}_{run_date}.csv"

            try:
                
                s3.download_file(Bucket=s3_bucket, Key=key, Filename=local_csv_path)

                df = pd.read_csv(local_csv_path)

                if not set(selected_columns).issubset(df.columns):
                    print(f"Skipping {key}: missing expected columns.")
                    continue

                df_filtered = df[selected_columns].rename(columns=column_rename_map)

                df_filtered["exchange"] = segment_metadata[ms]["exchange"]
                df_filtered["market_segment"] = segment_metadata[ms]["market_segment"]

                df_filtered.to_csv(filtered_csv_path, index=False)

                filtered_key = f"{key_prefix}filtered_bhavcopy_{ms}_{run_date}.csv"
                s3.upload_file(Filename=filtered_csv_path, Bucket=s3_bucket, Key=filtered_key)
                print(f"Uploaded: s3://{s3_bucket}/{filtered_key}")

            finally:
                
                # Cleanup files
                if os.path.exists(local_csv_path):
                    os.remove(local_csv_path)
                if os.path.exists(filtered_csv_path):
                    os.remove(filtered_csv_path)

    # inputs = get_conf()
    # filter_and_upload.expand(data=inputs)

    @task
    def load_csv_to_postgres(market_segment, run_date):
        s3 = boto3.client("s3")
        pg_hook = PostgresHook(postgres_conn_id='psql_bhavcopy')

        key = f"bhavcopy/{market_segment}/{run_date}/filtered_bhavcopy_{market_segment}_{run_date}.csv"
        bucket = Variable.get("bhavcopies_s3")

        local_path = f"/tmp/{market_segment}_{run_date}.csv"
        # s3.get_key(key, bucket_name=bucket).download_file(local_path)
        s3.download_file(Bucket=bucket, Key=key, Filename=local_path)

        pg_hook = PostgresHook(postgres_conn_id="psql_bhavcopy")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            copy_sql = f"""
                COPY formatted_bhavcopy (
                    trading_session, instrument, expiry_date,
                    close_price, settlement_price, last_price,
                    exchange, market_segment
                )
                FROM STDIN WITH CSV HEADER NULL '';
            """
            with open(local_path, "r") as f:
                cursor.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            print(f"Loaded {market_segment} data for {run_date} into Postgres.")
        finally:
            cursor.close()
            conn.close()
            os.remove(local_path)
        print(f"Loaded {market_segment} file into Postgres for {run_date}")

        # df = pd.read_csv(local_path)

        # #Handled explicitly due to CM contracts expiry
        # df["expiry_date"] = pd.to_datetime(df["expiry_date"], errors="coerce")
        # df["expiry_date"] = df["expiry_date"].where(pd.notnull(df["expiry_date"]), None)

        # # Load to Postgres
        # pg_hook.insert_rows(
        #     table="formatted_bhavcopy",
        #     rows=df.values.tolist(),
        #     target_fields=df.columns.tolist(),
        #     commit_every=1000,
        #     replace=False
        # )

        # os.remove(local_path)
        

    # load_csv_to_postgres.expand_kwargs(inputs)


    inputs = get_conf()
    filtered = filter_and_upload.expand(data=inputs)
    loaded = load_csv_to_postgres.expand_kwargs(inputs)

    filtered >> loaded

    trigger = TriggerDagRunOperator(
        task_id="trigger_downstream_next_dag",
        trigger_dag_id="reassignment_dag",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"triggered_by": "filter_bhavcopies"}
    )

    loaded >> trigger

