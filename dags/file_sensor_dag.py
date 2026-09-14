from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

@dag(
    dag_id='wait_for_file_pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'sensor']
)
def my_sensor_pipeline():

    # 1. 파일 센서 정의 (지정한 경로에 파일이 생길 때까지 대기)
    wait_for_csv = FileSensor(
        task_id='wait_for_target_file',
        # Docker 컨테이너 내부 기준의 절대 경로를 적어줍니다.
        filepath='/opt/airflow/dags/data/target_data.csv',
        poke_interval=10,  # 10초마다 파일이 있는지 확인 (찌르기)
        timeout=60 * 5,    # 5분(300초) 동안 파일이 안 오면 실패 처리
        mode='reschedule'  # 대기하는 동안 시스템 리소스(Worker)를 양보하는 필수 옵션
    )

    # 2. 파일이 도착하면 실행될 데이터 처리 작업
    @task
    def process_arrived_file():
        print("✅ target_data.csv 파일이 도착했습니다!")
        print("데이터 정제 및 DB 적재 작업을 시작합니다...")

    # 실행 순서: 센서가 통과되어야만 다음 태스크로 넘어갑니다.
    wait_for_csv >> process_arrived_file()

my_sensor_pipeline()