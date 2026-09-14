from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 파이썬으로 실행할 가상의 데이터 처리 로직
def process_data():
    print("데이터 추출(Extract) 및 변환(Transform)을 시작합니다...")
    print("데이터베이스에 적재(Load) 완료!")

# DAG의 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1, # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=1), # 재시도 대기 시간
}

# DAG 정의
with DAG(
    dag_id='my_first_batch_pipeline',
    default_args=default_args,
    description='나의 첫 번째 Airflow 배치 작업',
    # schedule_interval='@daily', # 매일 1회 실행
    schedule='@daily', # 매일 1회 실행
    start_date=datetime(2024, 1, 1),
    catchup=False, # 과거 누락된 배치를 한꺼번에 실행하지 않음
    tags=['sample', 'study'],
) as dag:

    # Task 1: Bash 명령어로 날짜 출력
    task_1 = BashOperator(
        task_id='print_current_date',
        bash_command='date',
    )

    # Task 2: 파이썬 함수 실행
    task_2 = PythonOperator(
        task_id='run_data_process',
        python_callable=process_data,
    )

    # Task 3: 완료 메시지 출력
    task_3 = BashOperator(
        task_id='print_finish',
        bash_command='echo "모든 배치 작업이 성공적으로 완료되었습니다."',
    )

    # Task 실행 순서 (의존성) 설정 : task_1 -> task_2 -> task_3 순서로 실행
    task_1 >> task_2 >> task_3

