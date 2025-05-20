from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.models import Variable
import subprocess
import zipfile
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests
import os
import sys

s3_bucket = Variable.get("bhavcopies_s3")

default_args = {
    'start_date': datetime(2025, 5, 19),
}

market_segments = ['nsefno', 'nsecm', 'bsefno', 'bsecm']  

def download_bhavcopies(ms, date, run_date):
    headers = {'User-Agent': 'Mozilla/5.0'}
    fmt_dt = datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')
    run_date_fmt = datetime.strptime(run_date, '%Y-%m-%d').strftime('%Y%m%d')

    url = ""
    if ms == 'nsefno':
        url = f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{fmt_dt}_F_0000.csv.zip"
    elif ms == "nsecm":
        url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{fmt_dt}_F_0000.csv.zip"
    elif ms == "bsecm":
        url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{fmt_dt}_F_0000.CSV"
    elif ms == "bsefno":
        url = f"https://www.bseindia.com/download/Bhavcopy/Derivative/BhavCopy_BSE_FO_0_0_0_{fmt_dt}_F_0000.CSV"
    else:
        raise ValueError(f"Unsupported market segment: {ms}")

    original_filename = url.split("/")[-1]

    if original_filename.endswith(".zip"):
        filename = original_filename
    elif original_filename.endswith(".CSV"):
        base, _ = os.path.splitext(original_filename)
        filename = base + ".csv"
    else:
        filename = original_filename

    s3_key = f"bhavcopy/{ms}/{run_date_fmt}/{filename}"

    print(f"Downloading {url} to s3://{s3_bucket}/{s3_key}")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url}, status code: {response.status_code}")

    local_path = f"/tmp/{filename}"
    with open(local_path, "wb") as f:
        f.write(response.content)
    
    if filename.endswith(".zip"):
        try:
            with zipfile.ZipFile(local_path, 'r') as zip_ref:
                zip_contents = zip_ref.namelist()
                for name in zip_contents:
                    if name.endswith('.csv'):
                        extracted_filename = name
                        zip_ref.extract(name, '/tmp/')  
                        print(f"Extracted CSV: {extracted_filename}")
                        local_path = f"/tmp/{extracted_filename}"  
                        s3_key = f"bhavcopy/{ms}/{run_date_fmt}/{extracted_filename}"  
                        break
                else:
                    raise RuntimeError("No CSV file found in the ZIP archive.")
        except zipfile.BadZipFile:
            raise RuntimeError(f"Downloaded file is not a valid zip file: {url}")

    try:
        subprocess.run([
            "aws", "s3", "cp", local_path, f"s3://{s3_bucket}/{s3_key}"
        ], check=True)
        print(f"Uploaded to s3://{s3_bucket}/{s3_key}")
    except subprocess.CalledProcessError as e:
        print(f"AWS CLI upload failed: {e}")
        sys.exit(0)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


with DAG(
    dag_id='download_bhavcopies',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=["bhavcopy", "dynamic"],
) as dag:

    @task
    def get_run_date(**context):
        return context["dag_run"].conf.get("run_date") or context["ds"]
        

    run_date_task = get_run_date()

    check_working_day = BashOperator(
        task_id='check_working_day',
        bash_command=(
            "holiday_check=$(python /opt/trading_cal.py -e nse -m is_holiday "
            "-f '%Y-%m-%d' -d {{ ti.xcom_pull(task_ids='get_run_date') }}); "
            "if [ \"$holiday_check\" = \"True\" ]; then exit 1; "
            "else echo 'Working day'; fi"
        )
    )

    get_prev_trading_date = BashOperator(
        task_id='get_prev_trading_date',
        bash_command=(
            "python /opt/trading_cal.py -d {{ ti.xcom_pull(task_ids='get_run_date') }} "
            "-f '%Y-%m-%d' -e nse"
        ),
        do_xcom_push=True,
    )

    @task
    def build_input_pairs(prevdate: str, run_date: str):
        return [{'market_segment': ms, 'date': prevdate, 'run_date': run_date} for ms in market_segments]

    @task
    def process_input(market_segment: str, date: str, run_date: str):
        download_bhavcopies(market_segment, date, run_date)

    run_date_task >> check_working_day >> get_prev_trading_date

    inputs = build_input_pairs(
        prevdate=get_prev_trading_date.output,
        run_date=run_date_task
    )

    process_input.expand_kwargs(inputs)

    #Trigger second DAG filter adn load to DB

    trigger = TriggerDagRunOperator(
        task_id ="trigger_downstream_dag",
        trigger_dag_id="filter_bhavcopies",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"triggered_by":"download_bhavcopies"}
    )
