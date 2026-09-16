# 1. 최신 Airflow 3 SDK 임포트
from airflow.sdk import dag, task

# 2. Airflow 3부터 기본 오퍼레이터는 standard 프로바이더에서 불러옵니다.
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

@dag(
    dag_id='jinja_macro_tutorial',
    schedule='@daily', # ⚠️ schedule_interval 대신 무조건 schedule 사용
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=['tutorial', 'jinja', 'airflow3', 'study']
)
def jinja_tutorial_pipeline():

    # [Task 1] BashOperator에서 Jinja 템플릿 사용하기 (전통적인 방식)
    # execution_date 대신 논리적 기준일(ds)과 데이터 구간(data_interval)을 사용합니다.
    print_date_with_bash = BashOperator(
        task_id='print_date_with_bash',
        bash_command="""
        echo "=== Airflow 3 Jinja 매크로 출력 ==="
        echo "1. 기준일 (YYYY-MM-DD): {{ ds }}"
        echo "2. 기준일 (연속된숫자): {{ ds_nodash }}"
        echo "3. 데이터 처리 시작일: {{ data_interval_start }}"
        echo "4. 데이터 처리 종료일: {{ data_interval_end }}"
        echo "5. 사용자 정의 파라미터: {{ params.my_message }}"

        echo "=== 날짜 연산 매크로 추가 ==="
        echo "1. 기준일 (YYYY-MM-DD): {{ ds }}"
        
        # 💡 macros.ds_add(기준일_문자열, 더할_일수) 포맷을 사용합니다.
        echo "2. 기준일 3일 전: {{ macros.ds_add(ds, -3) }}"
        echo "3. 기준일 3일 후: {{ macros.ds_add(ds, 3) }}"
        
        echo "4. 사용자 정의 파라미터: {{ params.my_message }}"
        """,
        params={"my_message": "최신 버전 마이그레이션 성공!"}
    )

    # [Task 2] TaskFlow API(@task)에서 템플릿 변수 사용하기 (최신 파이썬 방식)
    # Airflow가 파라미터 이름(ds, logical_date 등)을 인식해서 값을 자동으로 꽂아줍니다.
    @task
    def print_date_with_python(ds=None, logical_date=None, **kwargs):
        print("=== TaskFlow API 컨텍스트 변수 출력 ===")
        print(f"1. 기준일 (ds): {ds}")
        print(f"2. 논리적 기준일 (logical_date): {logical_date}")
        
        # kwargs 안에도 모든 템플릿 정보가 딕셔너리로 들어있습니다.
        data_start = kwargs.get('data_interval_start')
        print(f"3. kwargs에서 꺼낸 시작일: {data_start}")

        print("=== TaskFlow API 컨텍스트 변수 출력 ===")
        print(f"1. 기준일 (ds 문자열): {ds}")
        
        # 💡 logical_date는 단순 문자열이 아닌 시간 객체(Pendulum)이므로 
        # add(), subtract() 함수로 아주 쉽게 날짜를 더하고 뺄 수 있습니다.
        date_minus_3 = logical_date.subtract(days=3).strftime('%Y-%m-%d')
        date_plus_3 = logical_date.add(days=3).strftime('%Y-%m-%d')
        
        print(f"2. 기준일 3일 전 (Python 연산): {date_minus_3}")
        print(f"3. 기준일 3일 후 (Python 연산): {date_plus_3}")
        
        # 다음 태스크나 파일명에 쓰기 위해 조합해서 반환
        return f"file_backup_{ds}.csv"

    # 실행 순서 연결
    print_date_with_bash >> print_date_with_python()

jinja_tutorial_pipeline()


# 2026-09-14를 기준으로 파이프라인을 실행 테스트
# docker compose exec airflow-scheduler airflow dags test jinja_macro_tutorial 2026-09-14