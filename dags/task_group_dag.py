from airflow.decorators import dag, task, task_group
from datetime import datetime

@dag(
    dag_id='task_group_tutorial_pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'task_group']
)
def my_grouping_pipeline():

    @task
    def extract_data():
        print("데이터 추출 완료")
        return "raw_data"

    # 1. 태스크 그룹 정의 (@task_group 데코레이터 사용)
    @task_group(group_id='transform_and_clean_group', tooltip='데이터 전처리 작업 모음')
    def transform_group(data):
        
        # 그룹 내부에서 돌아갈 태스크들을 정의합니다.
        @task
        def remove_nulls(raw):
            print(f"{raw}에서 결측치(Null) 값 제거")
            return "cleaned_data"

        @task
        def filter_invalid_rows(cleaned):
            print(f"{cleaned}에서 비정상 패턴 필터링")
            return "filtered_data"
        
        # 그룹 내부의 실행 순서 및 데이터 전달
        step1 = remove_nulls(data)
        step2 = filter_invalid_rows(step1)
        
        # 그룹의 최종 결과물을 밖으로 반환합니다.
        return step2 

    @task
    def load_data(final_data):
        print(f"✅ 최종 데이터 DB 적재 완료: {final_data}")

    # 2. 전체 파이프라인 흐름 연결
    # 내부 구조가 얼마나 복잡하든 밖에서 볼 때는 단 3줄로 정리됩니다!
    raw = extract_data()
    processed = transform_group(raw)
    load_data(processed)

my_grouping_pipeline()