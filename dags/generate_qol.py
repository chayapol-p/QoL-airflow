from datetime import timedelta, date
import random
import csv

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator,
    S3CreateObjectOperator,
)

from utils.map import (
    random_route,
    load_bangkok_map,
    sample_shortest_path,
    map_to_normal_coor,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def generate_user_datas(N=10, **kwargs):
    qol_scores = [random.randint(1, 7) for i in range(N)]

    random_routes = random_route(**load_bangkok_map(), amounts=N)

    user_datas = [{"route": random_routes[i], "score": qol_scores[i]} for i in range(N)]
    return user_datas


def sampling_routes(n_sampling=30, **kwargs):
    ti = kwargs["ti"]
    user_datas = ti.xcom_pull(task_ids="generate_user_datas")

    for user_data in user_datas:
        sampling_route = sample_shortest_path(
            **load_bangkok_map(), route=user_data["route"], amouts=n_sampling
        )
        sampling_route_normal_coor = map_to_normal_coor(sampling_route)

        user_data["route"] = sampling_route_normal_coor

    return user_datas


def convert_user_data_to_csv(**kwargs):
    ti = kwargs["ti"]
    user_datas = ti.xcom_pull(task_ids="sampling_routes")

    keys = user_datas[0].keys()

    filename = f"{date.today()}_qol_datas.csv"

    with open(f"/tmp/{filename}", "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(user_datas)

    return filename


with DAG(
    "qol-generator",
    default_args=default_args,
    description="A generator of Qol score and path",
    schedule_interval="@daily",
) as dag:

    user_datas = PythonOperator(
        task_id="generate_user_datas",
        python_callable=generate_user_datas,
        provide_context=True,
    )

    sampled_user_datas = PythonOperator(
        task_id="sampling_routes",
        python_callable=sampling_routes,
        provide_context=True,
    )

    user_datas_csv = PythonOperator(
        task_id="convert_user_data_to_csv",
        python_callable=convert_user_data_to_csv,
        provide_context=True,
    )

    s3_upload = S3CreateObjectOperator(
        task_id="s3_upload_user_datas",
        s3_key=f"s3://chayapol.learning/qol_airflow/{user_datas_csv.output}",
        data=f"/tmp/{user_datas_csv.output}",
    )

    s3_file = S3ListOperator(
        task_id="list_3s_files",
        bucket="chayapol.learning",
        prefix="qol_airflow/",
        delimiter="/",
    )

    cleanup_file = BashOperator(
        task_id="list_files",
        bash_command="rm -f /tmp/*.csv",
    )

user_datas >> sampled_user_datas >> user_datas_csv >> s3_upload >> s3_file >> cleanup_file
