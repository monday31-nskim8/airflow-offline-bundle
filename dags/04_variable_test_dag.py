from airflow.sdk import dag, task
from airflow.models import Variable
# from airflow.models import Variable
# from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

# 아래처럼 DAG의 껍데기(전역 공간)에 Variable.get()을 적으면 절대 안 됩니다.
# ❌ 나쁜 예시 (절대 금지)
# api_key = Variable.get("my_secret_api_key")  # <--- 스케줄러를 마비시키는 주범

@dag(
    dag_id='variable_test_pipeline',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'variable']
)
def my_variable_pipeline():

    # 방법 A: 파이썬 함수(@task) 안에서 불러오기
    @task
    def call_api_with_variable():
        # Variable.get("키 이름") 으로 값을 가져옵니다.
        api_key = Variable.get("my_secret_api_key")
        
        print(f"DB에서 가져온 API 키를 사용해 요청을 보냅니다: {api_key}")
        # requests.get(url, headers={'Authorization': api_key}) 등 수행

    # 방법 B: 일반 Operator에서 Jinja 템플릿({{ }})으로 불러오기
    # 이 방식은 코드가 실행될 때 값을 바꿔치기 하므로 매우 효율적입니다.
    print_key = BashOperator(
        task_id='print_variable_bash',
        bash_command='echo "Bash에서 환경변수처럼 사용 가능: {{ var.value.my_secret_api_key }}"'
    )

    call_api_with_variable() >> print_key

my_variable_pipeline()