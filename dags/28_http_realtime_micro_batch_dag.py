import pendulum
import requests
from datetime import timedelta
from airflow.sdk import dag, task

# 한국 타임존 설정
kst_tz = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id='http_realtime_micro_batch',
    # 💡 매 1분마다 실행하는 크론(Cron) 표현식
    schedule='* * * * *', 
    start_date=pendulum.datetime(2026, 9, 16, tz=kst_tz),
    catchup=False,
    # 💡 실시간 배치의 핵심: 이전 1분 작업이 안 끝났으면 새 작업을 겹쳐서 돌리지 않고 대기시킴
    max_active_runs=1,
    tags=['tutorial', 'http', 'micro-batch', 'study']
)
def http_realtime_pipeline():

    # 💡 API 서버 장애로 인한 무한 대기 방지 (30초 넘으면 강제 실패)
    @task(execution_timeout=timedelta(seconds=30), retries=1)
    def fetch_realtime_data():
        print("🌐 실시간 API에서 데이터를 호출합니다...")
        
        # 실무에서는 사내 실시간 API URL을 입력합니다.
        # api_url = "http://internal-system/api/v1/metrics/current"
        # res = requests.get(api_url, timeout=10)
        # res.raise_for_status()
        # return res.json()

        # 테스트용 가상 데이터 반환
        return {
            "timestamp": pendulum.now("Asia/Seoul").to_iso8601_string(),
            "cpu_usage": 85.4,
            "active_users": 1024,
            "status": "warning"
        }

    @task
    def process_and_alert(payload: dict):
        current_time = payload.get("timestamp")
        status = payload.get("status")
        
        print(f"🕒 수집 시각: {current_time}")
        print(f"📊 현재 상태: {status}")
        
        # 실시간 데이터 분석 및 알림 조건 확인
        if status == "warning" or payload.get("cpu_usage") > 90.0:
            print("🚨 [경고] 시스템 부하가 높습니다! 슬랙(Slack) 알림 발송 로직 실행...")
        else:
            print("✅ 시스템 상태 정상.")

    # 의존성 연결 (데이터 추출 -> 분석 및 알림)
    realtime_data = fetch_realtime_data()
    process_and_alert(realtime_data)

# DAG 객체 생성
http_realtime_pipeline()