from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id='dynamic_task_mapping_tutorial',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'dynamic_mapping']
)
def my_dynamic_pipeline():

    # 1. 대상 리스트를 생성하는 태스크 (실무에서는 DB 조회 후 리스트 반환)
    @task
    def get_user_ids():
        print("DB에서 처리할 대상 ID 리스트를 가져옵니다.")
        return [101, 102, 103, 104, 105]  # 5개의 요소 반환

    # 2. 단일 항목을 처리하는 템플릿 태스크
    @task
    def process_user(user_id, run_date):
        print(f"✅ 기준일({run_date}): 사용자 ID [{user_id}]의 데이터 처리를 완료했습니다!")
        return f"Success:{user_id}"

    # 3. 데이터 흐름 연결 (가장 중요한 부분)
    id_list = get_user_ids()
    
    # .partial(): 고정된 파라미터를 넘겨줄 때 사용 (모든 태스크가 동일하게 받음)
    # .expand(): 동적으로 쪼갤 리스트를 넘겨줄 때 사용 (리스트 개수만큼 태스크가 복제됨)
    process_user.partial(run_date="2026-09-07").expand(user_id=id_list)

my_dynamic_pipeline()