from airflow.sdk import dag, task
from datetime import datetime
import random

@dag(
    dag_id='branching_tutorial_pipeline',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'branching']
)
def my_branching_pipeline():

    # 1. 분기 판단 태스크 (@task.branch)
    # 🚨 중요: 반드시 다음에 실행할 'task_id(문자열)'를 return 해야 합니다!
    @task.branch
    def choose_path():
        # 테스트를 위해 랜덤 값 사용 (실무에서는 DB 조회 결과나 날짜 등을 사용)
        is_weekend = random.choice([True, False])
        
        if is_weekend:
            print("조건: 주말입니다.")
            return 'weekend_process_task'  # 주말 태스크 ID 반환
        else:
            print("조건: 평일입니다.")
            return 'weekday_process_task'  # 평일 태스크 ID 반환

    # 2. 선택지 A (주말용 태스크)
    @task(task_id='weekend_process_task')
    def do_weekend_work():
        print("✅ 주말 배치 작업을 무사히 수행했습니다.")

    # 3. 선택지 B (평일용 태스크)
    @task(task_id='weekday_process_task')
    def do_weekday_work():
        print("✅ 평일 배치 작업을 무사히 수행했습니다.")

    # 4. 분기 이후에 다시 모여서 실행될 마무리 작업
    # 🚨 중요: trigger_rule을 변경하지 않으면 앞선 태스크 중 하나가 건너뛰어지면서 이 태스크도 실패(건너뜀) 처리됩니다.
    @task(trigger_rule='none_failed_min_one_success')
    def final_summary():
        print("마무리: 요일에 맞는 작업을 완료하고 파이프라인을 종료합니다.")

    # 5. 흐름 연결하기 (리스트 [ ] 를 사용해 분기 경로를 지정합니다)
    # branching_decision = choose_path()
    # weekend_path = do_weekend_work()
    # weekday_path = do_weekday_work()
    # summary = final_summary()

    # choose_path의 결과에 따라 weekend 또는 weekday 중 하나로 흐름이 나뉘고, 마지막에 합쳐짐
    # branching_decision >> [weekend_path, weekday_path] >> summary 

    # ⚠️ Airflow 3 문법
    choose_path() >> [do_weekend_work(), do_weekday_work()] >> final_summary() 

my_branching_pipeline()