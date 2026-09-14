import pendulum
from airflow.decorators import dag, task

# 한국 시간대(KST) 객체 생성
kst = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id='kst_schedule_dag',
    # 매주 월요일 아침 9시 (한국 시간 기준)
    schedule='0 9 * * 1', 
    # 시작 날짜에도 반드시 한국 시간대를 적용해야 합니다.
    start_date=pendulum.datetime(2024, 1, 1, tz=kst),
    catchup=False,
    tags=['study', 'schedule']
)
def my_schedule_pipeline():

    @task
    def print_time():
        print("한국 시간 기준 월요일 오전 9시에 실행되는 태스크입니다!")

    print_time()

my_schedule_pipeline()