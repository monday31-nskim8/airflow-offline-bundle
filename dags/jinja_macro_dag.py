import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

kst = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id='jinja_macro_tutorial',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2026, 9, 1, tz=kst),
    catchup=False,
    tags=['study', 'jinja']
)
def my_jinja_pipeline():

    # 1. 일반 Operator에서 Jinja 템플릿 쓰기 (문자열 안에 중괄호 사용)
    # bash_command, sql 등 템플릿이 허용된(templated field) 곳에서만 동작합니다.
    print_date_bash = BashOperator(
        task_id='print_with_bash',
        bash_command="""
        echo "✅ [Bash] 기준일: {{ ds }}"
        echo "✅ [Bash] 어제 날짜: {{ yesterday_ds }}"
        echo "✅ [Bash] 3일 전 날짜: {{ macros.ds_add(ds, -3) }}"
        echo "✅ [Bash] 생성될 파일명: backup_data_{{ ds_nodash }}.csv"
        """
    )

    # 2. 파이썬 함수(@task) 안에서 템플릿 변수 꺼내 쓰기
    # 함수 파라미터로 **kwargs 를 받으면, 그 안에 Airflow의 모든 매크로 변수가 딕셔너리 형태로 들어옵니다.
    @task
    def print_date_python(**kwargs):
        # kwargs 딕셔너리에서 원하는 변수 이름으로 꺼내 씁니다.
        current_date = kwargs['ds']
        yesterday = kwargs['yesterday_ds']
        file_suffix = kwargs['ds_nodash']
        
        print(f"🚀 [Python] 기준일: {current_date}")
        print(f"🚀 [Python] 어제 날짜: {yesterday}")
        print(f"🚀 [Python] DB 쿼리 예시: SELECT * FROM sales WHERE dt = '{yesterday}';")
        print(f"🚀 [Python] 생성될 파일명: report_{file_suffix}.xlsx")

    print_date_bash >> print_date_python()

my_jinja_pipeline()


# 2026-08-15를 기준으로 파이프라인을 실행 테스트
# docker compose exec airflow-scheduler airflow dags test jinja_macro_tutorial 2026-09-14