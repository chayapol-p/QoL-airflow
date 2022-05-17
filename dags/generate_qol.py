from datetime import timedelta, date
import random
import csv
import io

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

# set default args for each task
default_args = {
    "owner": "airflow", 
    "depends_on_past": False, # set to false for task can be run evenif the previous failed
    "start_date": airflow.utils.dates.days_ago(2), # set start to two day ago for the task to be run
    "retries": 1, # set maximum retries for each task to 1 time
    "retry_delay": timedelta(minutes=2), # set delay between failed task and new retry task
}


# This function will generate the user datas by random values
def generate_user_datas(N=10, **kwargs):
    # random N scores which are in range 1-7
    qol_scores = [random.randint(1, 7) for i in range(N)]

    # random origin and destination of each route in to list
    random_routes = random_route(**load_bangkok_map(), amounts=N)

    # prepare user data for the next task
    user_datas = [{"route": random_routes[i], "score": qol_scores[i]} for i in range(N)]

    # return user_data to be used in others task with Xcom
    return user_datas


# This function will sampling the points along the path for the corresponding origin and destination point
def sampling_routes(n_sampling=30, **kwargs):
    # take the user_datas from generate_user_datas task by Xcom
    ti = kwargs["ti"]
    user_datas = ti.xcom_pull(task_ids="generate_user_datas")

    # for each route, sampling the points and change the point coordinate to be lat and long
    for user_data in user_datas:
        # Sampling points part
        sampling_route = sample_shortest_path(
            **load_bangkok_map(), route=user_data["route"], amouts=n_sampling
        )
        # convert to lat and long coordinate system part
        sampling_route_normal_coor = map_to_normal_coor(sampling_route)

        # set user_datas.route to be sampling point instead of origin and destination point
        user_data["route"] = sampling_route_normal_coor

    # return user_data to be used in others task with Xcom
    return user_datas


# This function will prepare the user_datas into a csv format
def convert_user_data_to_csv(**kwargs):
    # take the user_datas from sampling_routes task by Xcom
    ti = kwargs["ti"]
    user_datas = ti.xcom_pull(task_ids="sampling_routes")

    # save the keys of user_datas to use as a csv column
    keys = user_datas[0].keys()

    # create csv formatted string part
    output_file = io.StringIO(newline="")
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(user_datas)

    return output_file.getvalue()


# Define dag in with context
# Most of task will set provide_context=True for use Xcom
with DAG(
    "qol-generator",
    default_args=default_args,
    description="A generator of Qol score and path",
    schedule_interval="@daily",
) as dag:

    # Create task for generate_user_datas
    user_datas = PythonOperator(
        task_id="generate_user_datas",
        python_callable=generate_user_datas,
        provide_context=True,
    )

    # Create task for sampling_routes
    sampled_user_datas = PythonOperator(
        task_id="sampling_routes",
        python_callable=sampling_routes,
        provide_context=True,
    )

    # Create task for convert_user_data_to_csv
    user_datas_csv = PythonOperator(
        task_id="convert_user_data_to_csv",
        python_callable=convert_user_data_to_csv,
        provide_context=True,
    )

    # Create task for upload file to s3
    s3_upload = S3CreateObjectOperator(
        task_id="s3_upload_user_datas",
        # define filename for this flow to the date, since we will run dags in daily
        s3_key=f"s3://chayapol.learning/qol_airflow/route_datas/{date.today()}.csv",
        data=user_datas_csv.output,
        replace=True, # set this to replace existed file when rerun, only for development
    )

    # Create task for listing the file in s3
    s3_file = S3ListOperator(
        task_id="list_3s_files",
        bucket="chayapol.learning",
        prefix="qol_airflow/route_datas/",
        delimiter="/",
    )

    # Create task for to cleanup the csv file
    cleanup_file = BashOperator(
        task_id="cleanup_files",
        bash_command="rm -f /tmp/*.csv",
    )

# define dependencies of each tasks
user_datas >> sampled_user_datas >> user_datas_csv >> s3_upload >> s3_file >> cleanup_file
