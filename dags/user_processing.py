import csv
from datetime import datetime

from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

API_URL = "https://raw.githubusercontent.com/FatimaMoqran/airflow-datasets/refs/heads/main/fakeuser.json"
CSV_PATH = "/tmp/user_info.csv"


@dag
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                firstname VARCHAR(255),
                lastname VARCHAR(255),
                email VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests

        response = requests.get(API_URL)
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    @task
    def process_user(user_info):
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(CSV_PATH, "w", newline="") as f:
            # meme ordre que la table users (le dict recu par XCom n'est plus dans l'ordre)
            writer = csv.DictWriter(f, fieldnames=["id", "firstname", "lastname", "email", "created_at"])
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def save_user():
        # bonus : table temporaire + ON CONFLICT pour pouvoir relancer le DAG
        hook = PostgresHook(postgres_conn_id="postgres")
        with hook.get_conn() as conn, conn.cursor() as cur, open(CSV_PATH) as f:
            cur.execute("CREATE TEMP TABLE users_tmp (LIKE users) ON COMMIT DROP")
            cur.copy_expert("COPY users_tmp FROM STDIN WITH CSV HEADER", f)
            cur.execute("INSERT INTO users SELECT * FROM users_tmp ON CONFLICT (id) DO NOTHING")

    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    processed = process_user(user_info)
    create_table >> fake_user
    processed >> save_user()


