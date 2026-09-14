import pendulum
from airflow.decorators import dag, task
from datetime import datetime

kst = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id='modern_taskflow_etl',
    schedule='0 9 * * 1', # 매주 월요일 오전 9시
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    catchup=False, # 과거 누락분 자동 실행 방지
    tags=['study', 'etl']
)
def my_etl_pipeline():

    @task
    def extract():
        return {"user_id": 101} # 리턴값은 자동으로 다음 태스크로 전달됨

    @task
    def transform(raw_data: dict):
        raw_data['status'] = 'processed'
        return raw_data

    @task
    def load(final_data: dict):
        print(f"DB 적재 완료: {final_data}")

    raw = extract()
    processed = transform(raw)
    load(processed)

my_etl_pipeline()