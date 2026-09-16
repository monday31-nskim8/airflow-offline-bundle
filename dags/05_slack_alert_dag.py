from airflow.sdk import dag, task
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from datetime import datetime

# 1. 실패 시 실행될 콜백(Callback) 함수 정의
def send_slack_alert(context):
    # context 딕셔너리에는 실패한 태스크의 모든 정보가 담겨 있습니다.
    ti = context.get('task_instance')
    
    # Slack으로 보낼 메시지 포맷팅
    slack_msg = f"""
    🚨 *Airflow 배치 실패 알림* 🚨
    *DAG:* `{ti.dag_id}`
    *Task:* `{ti.task_id}`
    *실행 시간:* `{context.get('logical_date')}`
    *로그 확인:* <{ti.log_url}|웹에서 로그 보기 링크>
    """
    
    # 웹 UI에 등록한 Connection ID로 Slack 연결
    hook = SlackWebhookHook(
        slack_webhook_conn_id='slack_alert_conn',
        text=slack_msg
    )
    
    # 메시지 전송
    hook.execute()

# 2. DAG의 기본 설정(default_args)에 콜백 함수 연결
default_args = {
    'owner': 'airflow',
    # 이 DAG 안의 어떤 태스크든 실패하면 send_slack_alert 함수를 실행합니다.
    'on_failure_callback': send_slack_alert,
}

@dag(
    dag_id='slack_alert_test_pipeline',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'alert']
)
def my_alert_pipeline():

    @task
    def success_task():
        print("이 태스크는 정상적으로 성공합니다.")

    @task
    def fail_task():
        print("일부러 에러를 발생시켜 Slack 알림을 테스트합니다.")
        # 0으로 나누기 에러(ZeroDivisionError)를 고의로 발생시킴
        result = 10 / 0 

    success_task() >> fail_task()

my_alert_pipeline()