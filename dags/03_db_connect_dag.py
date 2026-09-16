from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

@dag(
    dag_id='postgres_query_test',
    schedule=None, # 수동으로만 실행하도록 설정
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'database']
)
def my_db_pipeline():

    # 1. 단순 쿼리 조회 (버전 확인)
    @task
    def check_db_version():
        # 웹 UI에서 등록한 커넥션 ID를 적어줍니다.
        hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        
        # get_records()로 쿼리 결과를 리스트 형태로 가져옵니다.
        records = hook.get_records("SELECT version();")
        
        print(f"✅ 접속된 PostgreSQL 버전: {records[0][0]}")

    # 2. 테이블 생성 및 데이터 삽입
    @task
    def insert_data():
        hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        
        sql = """
        CREATE TABLE IF NOT EXISTS study_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO study_users (name) VALUES ('Airflow 학습자');
        """
        # 데이터를 변경(INSERT, UPDATE, CREATE)할 때는 run()을 사용합니다.
        hook.run(sql)
        print("✅ 테이블 생성 및 샘플 데이터 삽입 완료!")

    # 3. 삽입된 데이터 조회
    @task
    def fetch_data():
        hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        
        records = hook.get_records("SELECT * FROM study_users;")
        print("✅ 테이블 조회 결과:")
        for row in records:
            print(row)

    # 실행 순서 지정
    check_db_version() >> insert_data() >> fetch_data()

# DAG 실행
my_db_pipeline()