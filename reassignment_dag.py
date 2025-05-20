from airflow.decorators import task
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'start_date': datetime(2025, 5, 19),
}

with DAG(
    dag_id='reassignment_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["bhavcopy", "reassignment"],
) as dag:

    @task
    def check_and_run_stored_proc(**context):
        run_date = context["dag_run"].conf.get("run_date") 
        if not run_date:
            raise ValueError("Run date is required in the DAG run configuration.")

        hook = PostgresHook(postgres_conn_id='psql_bhavcopy')
        conn = hook.get_conn()
        cursor = conn.cursor()

        check_query = """
        SELECT EXISTS (
            SELECT 1
            FROM formatted_bhavcopy fb
            JOIN reassignment_table rt ON fb.trading_session = rt.trading_session
            WHERE fb.trading_session = %s AND xc.trading_session = %s
        )
        """
        cursor.execute(check_query, (run_date, run_date))
        exists = cursor.fetchone()[0]

        if exists:
            print(f"Data available for {run_date}, running stored procedure")
            cursor.execute("CALL your_stored_procedure(%s)", (run_date,))
            conn.commit()
        else:
            print(f"Data not ready for {run_date}, skipping stored procedure.")

        cursor.close()
        conn.close()

    check_and_run_stored_proc()