# dags/slack_alert_dag.py
from airflow.sdk import dag, task
from datetime import datetime

# 방금 만든 공통 모듈에서 함수를 불러옵니다.
from common.slack_notifier import send_slack_alert

# 불러온 함수를 그대로 연결합니다.
default_args = {
    'owner': 'airflow',
    'on_failure_callback': send_slack_alert, 
}

@dag(
    dag_id='slack_alert_com_test_pipeline',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'alert']
)
def my_alert_pipeline():

    @task
    def fail_task():
        # 테스트용 에러 발생
        result = 10 / 0 

    fail_task()

my_alert_pipeline()