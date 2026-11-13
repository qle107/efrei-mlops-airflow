from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="demo_ui",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "ui"],
)
def demo_ui():
    @task
    def extraire():
        return 42  # cette valeur devient un XCom

    @task
    def transformer(valeur):
        if valeur > 100:
            raise ValueError("Valeur trop grande")
        return valeur * 2

    @task
    def charger(valeur):
        print(f"Chargement de {valeur}")

    charger(transformer(extraire()))


demo_ui()
