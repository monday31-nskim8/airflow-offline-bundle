# dags/common/slack_notifier.py
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

def send_slack_alert(context):
    ti = context.get('task_instance')
    
    slack_msg = f"""
    🚨 *Airflow 배치 실패 공통 알림* 🚨
    *DAG:* `{ti.dag_id}`
    *Task:* `{ti.task_id}`
    *실행 시간:* `{context.get('logical_date')}`
    *로그 확인:* <{ti.log_url}|웹에서 로그 보기 링크>
    """
    
    hook = SlackWebhookHook(
        slack_webhook_conn_id='slack_alert_conn',
        text=slack_msg
    )
    
    hook.execute()