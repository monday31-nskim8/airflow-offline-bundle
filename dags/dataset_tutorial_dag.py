from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime

# 1. Dataset 정의 (식별자 역할)
# 주의: URI 형태(file://, s3:// 등)로 적어야 하며, 실제 파일을 감시하는 것이 아니라 '이름표' 역할만 합니다.
my_target_dataset = Dataset("file://opt/airflow/dags/data/result.csv")


# ==========================================
# [파이프라인 A] 생산자 (Producer)
# ==========================================
@dag(
    dag_id='A_producer_pipeline',
    schedule_interval=None, # 수동 실행
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'dataset_producer']
)
def producer_pipeline():
    
    # 핵심 옵션: outlets=[데이터셋]
    # 이 태스크가 '성공'하면 Airflow에게 해당 데이터셋이 업데이트되었다고 알립니다.
    @task(outlets=[my_target_dataset])
    def create_csv_data():
        # 실제 파일 생성 로직
        with open("/opt/airflow/dags/data/result.csv", "w") as f:
            f.write("id,name\n1,Airflow_Student")
        print("✅ A 파이프라인: 파일 생성 및 Dataset 갱신 완료!")

    create_csv_data()

producer_pipeline()


# ==========================================
# [파이프라인 B] 소비자 (Consumer)
# ==========================================
@dag(
    dag_id='B_consumer_pipeline',
    # 핵심 옵션: schedule=[데이터셋]
    # 크론(Cron) 시간 대신 데이터셋을 스케줄로 지정합니다.
    schedule=[my_target_dataset], 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'dataset_consumer']
)
def consumer_pipeline():
    
    @task
    def read_csv_data():
        with open("/opt/airflow/dags/data/result.csv", "r") as f:
            content = f.read()
        print(f"✅ B 파이프라인: A가 만든 데이터를 즉시 인지하고 읽었습니다!\n{content}")

    read_csv_data()

consumer_pipeline()