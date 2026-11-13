from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="demo_ok",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "ok"],
)
def demo_ok():
    @task
    def dire_bonjour():
        print("Tout va bien")

    dire_bonjour()


demo_ok()
