# dags/external_trigger_dag.py
from airflow.sdk import dag, task
import pendulum

kst_tz = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id='external_trigger_dag',
    schedule=None,  # 💡 스스로 실행되지 않고 외부 호출만 기다림
    start_date=pendulum.datetime(2026, 9, 1, tz=kst_tz),
    catchup=False,
    tags=['tutorial', 'api_trigger', 'study']
)
def external_trigger_pipeline():

    @task
    def process_external_event(**kwargs):
        # 💡 외부(API)에서 전달한 파라미터 데이터(conf)를 꺼냅니다.
        dag_run = kwargs.get('dag_run')
        external_data = dag_run.conf if dag_run.conf else {}
        
        # 외부에서 넘겨준 'file_name' 값을 추출 (없으면 'unknown' 반환)
        file_name = external_data.get('file_name', 'unknown_file.csv')
        trigger_source = external_data.get('source', 'unknown_source')
        
        print("🚀 외부 시스템으로부터 실행 요청을 받았습니다!")
        print(f"▶ 요청 출처: {trigger_source}")
        print(f"▶ 처리할 타겟 파일: {file_name}")
        
        # 여기서 전달받은 파일명을 이용해 파일 다운로드, DB 적재 등을 수행합니다.

    process_external_event()

external_trigger_pipeline()