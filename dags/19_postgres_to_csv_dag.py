from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime   
import csv
import os

@dag(
    dag_id='postgres_to_csv_export',
    schedule='@daily', # 매일 자정(00:00)에 자동 실행되도록 설정
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'export', 'postgres']
)
def my_csv_export_pipeline():

    @task
    def export_to_csv(**kwargs):
        # 1. Jinja 템플릿 변수를 통해 실행 기준일 가져오기
        logical_date = kwargs['ds']           # 예: 2026-09-08 (쿼리용)
        file_suffix = kwargs['ds_nodash']     # 예: 20260908 (파일명용)

        # 2. Postgres 연결 및 데이터 조회
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # 기준일에 해당하는 통계 데이터만 추출
        query = "SELECT id, run_date, event_type, event_count, created_at FROM daily_event_stats WHERE run_date = %s;"
        cursor.execute(query, (logical_date,))
        records = cursor.fetchall()

        if not records:
            print(f"⚠️ {logical_date} 일자의 통계 데이터가 없어 CSV를 생성하지 않습니다.")
            return

        # 커서(cursor.description)에서 컬럼(헤더) 이름 추출
        col_names = [desc[0] for desc in cursor.description]

        # 3. 파일 저장 경로 및 이름 지정
        # 이전에 만들어둔 dags/data 폴더를 타겟으로 합니다.
        output_dir = '/opt/airflow/dags/data'
        os.makedirs(output_dir, exist_ok=True) # 폴더가 없으면 생성
        file_path = f"{output_dir}/daily_report_{file_suffix}.csv"

        # 4. CSV 파일 쓰기
        with open(file_path, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(col_names) # 헤더(컬럼명) 기록
            writer.writerows(records)  # 실제 데이터 기록

        print(f"✅ CSV 파일 추출 성공! 저장 경로: {file_path} (총 {len(records)}건)")

    export_to_csv()

my_csv_export_pipeline()
