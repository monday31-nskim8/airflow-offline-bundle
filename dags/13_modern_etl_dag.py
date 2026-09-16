from airflow.sdk import dag, task
from datetime import datetime
import pendulum # 💡 pendulum 라이브러리 추가

# 1. 한국 타임존 객체 생성
kst_tz = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id='modern_taskflow_etl',
    schedule='0 9 * * 1', # 매주 월요일 오전 9시
    # start_date=datetime(2026, 9, 1),
    # 2. start_date를 지정할 때 tzinfo 인자에 한국 타임존을 넣어줍니다.
    # (또는 pendulum.datetime(2026, 9, 1, tz="Asia/Seoul") 형태도 가능합니다.)
    start_date=pendulum.datetime(2026, 9, 1, tz=kst_tz),
    catchup=False,
    tags=['study', 'etl', 'airflow3']
)
def modern_etl_pipeline():

    # [Extract] 데이터 추출
    @task
    def extract_data():
        print("데이터를 추출합니다...")
        # 실무에서는 API 호출 결과나 DB 조회 결과를 반환합니다.
        raw_data = [
            {"id": 1, "name": "alice", "status": "active"},
            {"id": 2, "name": "bob", "status": "inactive"},
            {"id": 3, "name": "charlie", "status": "active"}
        ]
        return raw_data  # 자동으로 XCom을 통해 다음 태스크로 넘어갑니다.

    # [Transform] 데이터 정제
    @task
    def transform_data(data: list):
        print("추출된 데이터를 정제합니다...")
        processed_data = []
        for row in data:
            # 상태가 active인 유저만 필터링하고 이름을 대문자로 변환
            if row.get("status") == "active":
                processed_data.append({
                    "id": row["id"],
                    "name": row["name"].capitalize()
                })
        return processed_data

    # [Load] 데이터 적재
    @task
    def load_data(final_data: list):
        print(f"총 {len(final_data)}건의 데이터를 적재합니다.")
        for item in final_data:
            print(f"✅ 적재 완료: {item}")
        # 실무에서는 PostgresHook 등을 사용해 DB에 INSERT 합니다.

    # 3. 파이프라인 실행 흐름 (의존성 설정)
    # 별도의 '>>' 기호 없이 함수의 반환값을 변수로 받아 넘기기만 하면 
    # Airflow가 알아서 실행 순서와 데이터 전달을 처리합니다.
    extracted = extract_data()
    transformed = transform_data(extracted)
    load_data(transformed)

# DAG 객체 생성
modern_etl_pipeline()