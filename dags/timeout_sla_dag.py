from airflow.decorators import dag, task
from datetime import datetime, timedelta
import time

# 1. SLA 지연 시 실행될 콜백 함수 정의
# 파라미터(dag, task_list, blocking_task_list, slas, blocking_tis)는 Airflow가 자동으로 넘겨줍니다.
def my_sla_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"🚨 [SLA 경고] 다음 태스크들이 목표 시간 내에 완료되지 못했습니다: {task_list}")
    # 실무에서는 이곳에 이전 단계에서 만든 SlackWebhookHook 코드를 넣어 슬랙으로 알림을 보냅니다.

# 2. DAG 기본 설정에 콜백 함수 연결
default_args = {
    'owner': 'airflow',
    'sla_miss_callback': my_sla_alert,
}

@dag(
    dag_id='timeout_sla_tutorial',
    default_args=default_args,
    # schedule_interval='@daily',
    # 매 2분마다 자동으로 스케줄러가 실행하도록 설정
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2026, 9, 9), # 오늘 날짜로 맞춤
    catchup=False,
    tags=['study', 'timeout', 'sla']
)
def my_robust_pipeline():

    # A. Timeout 테스트 (10초 안에 안 끝나면 강제 종료)
    @task(execution_timeout=timedelta(seconds=10))
    def api_call_with_timeout():
        print("API 호출 시작...")
        # 고의로 30초 동안 멈춰있게 만듭니다.
        # 10초가 되는 순간 Airflow가 이 작업을 강제로 죽이고 에러 처리합니다.
        time.sleep(30) 
        print("이 메시지는 출력되지 않습니다.")

    # B. SLA 테스트 (DAG 실행 후 5분 안에 안 끝나면 알림 발생)
    # 실행 시간이 아닌 '목표 기한'을 의미하므로 에러를 내진 않습니다.
    # @task(sla=timedelta(minutes=5))
    # SLA 기한을 아주 짧게(1초) 줘서, 시작하자마자 무조건 지연 판정이 나도록 유도
    @task(sla=timedelta(seconds=1))
    def report_generation():
        print("리포트 생성 시작...")
        time.sleep(10)
        print("리포트 생성 완료!")

    api_call_with_timeout() >> report_generation()

my_robust_pipeline()