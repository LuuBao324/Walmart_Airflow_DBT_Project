from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
import pendulum
import os

@dag(
        dag_id="orchestrate",
        schedule="0 11 * * *",
        catchup=True,
        start_date=pendulum.datetime(year=2026, month=11, day=3, tz="Asia/Ho_Chi_Minh")
)
def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host=os.environ.get("DATABRICKS_HOST"),
            token=os.environ.get("DATABRICKS_TOKEN")
        )

        job_trigger = ws.jobs.run_now(job_id="390364974550102")

        print(job_trigger)

        while True:
            job_run = ws.jobs.get_run(run_id=job_trigger.run_id)

            print(f"Job run state: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise RuntimeError(f"Job finished with result: {job_run.state.result_state}")
                
            time.sleep(5)
            return "CDC Ingestion Completed"

    @task.bash
    def clean_target():
        return "cd /opt/airflow/walmart_project && rm -rf target && rm -rf logs"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --models silver_t"
    )

    silver_technical_test = BashOperator(
        task_id="silver_technical_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_t"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_b"
    )

    silver_business_test = BashOperator(
        task_id="silver_business_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_b"
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/ephemeral"
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/fact"
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_test >> silver_business >> silver_business_test >> gold_ephemeral >> gold_dimensions >> gold_fact

orchestrate_dag = orchestrate()