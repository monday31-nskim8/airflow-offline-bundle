from airflow.sdk import dag, task
from datetime import datetime
import os

@dag(
    dag_id='wait_for_file_pipeline',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'sensor']
)
def my_sensor_pipeline():

    # 1. 최신 TaskFlow 방식 센서 (FileSensor 클래스 대신 사용)
    # mode='reschedule'을 주면 대기 중일 때 워커 자원을 반납하여 효율적입니다.
    @task.sensor(poke_interval=10, timeout=600, mode='reschedule')
    def wait_for_target_file() -> bool:
        filepath = '/opt/airflow/dags/data/target_data.csv'
        
        # 파일이 실제로 존재하는지 확인 (True면 다음 태스크로 넘어감, False면 다시 대기)
        return os.path.exists(filepath)

    # 2. 파일이 도착하면 실행될 데이터 처리 작업
    @task
    def process_arrived_file():
        print("✅ target_data.csv 파일이 도착했습니다!")
        print("데이터 정제 및 DB 적재 작업을 시작합니다...")

    # 실행 순서: 센서 함수가 True를 반환해야만 다음 작업이 실행됩니다.
    wait_for_target_file() >> process_arrived_file()

my_sensor_pipeline()