from airflow.sdk import dag, task
from datetime import datetime

# 💡 plugins 폴더에 있는 파이썬 파일명(my_legacy_api)으로 바로 import 합니다.
from my_legacy_api import LegacySystemOperator

@dag(
    dag_id='custom_operator_tutorial',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'custom_operator', 'airflow3']
)
def custom_operator_pipeline():

    # 나만의 연산자를 블록처럼 가져다 씁니다.
    call_legacy_system_1 = LegacySystemOperator(
        task_id='sync_user_data',
        endpoint='/api/v1/users/sync',
        # template_fields에 등록해두었기 때문에 Jinja 템플릿이 완벽하게 작동합니다.
        payload={
            # "request_date": "{{ ds }}",
            "system_code": "HR_SYS_01"
        }
    )

    call_legacy_system_2 = LegacySystemOperator(
        task_id='sync_salary_data',
        endpoint='/api/v1/salary/sync',
        payload={
            # "request_date": "{{ yesterday_ds }}",
            "system_code": "FIN_SYS_02"
        }
    )

    call_legacy_system_1 >> call_legacy_system_2

# DAG 객체 생성
custom_operator_pipeline()