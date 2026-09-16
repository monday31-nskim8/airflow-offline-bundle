from airflow.sdk import dag, task
from datetime import datetime
import time

@dag(
    dag_id='pool_control_tutorial',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pool', 'study']
)
def my_pool_pipeline():

    # KMS API를 호출하는 태스크에 생성한 pool을 지정합니다.
    @task(pool='kms_api_pool')
    def call_kms_api(task_num):
        print(f"KMS API 호출 중... (태스크 번호: {task_num})")
        time.sleep(10) # 10초 동안 실행된다고 가정
        return True

    # 강제로 20개의 태스크를 병렬로 실행시켜 봅니다. (Dynamic Task Mapping 활용)
    call_kms_api.expand(task_num=list(range(1, 21)))

my_pool_pipeline()