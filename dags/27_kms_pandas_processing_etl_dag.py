from airflow.sdk import dag, task
from airflow.models import Variable
import pendulum
import requests
import pandas as pd

# 한국 타임존 설정
kst_tz = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id='kms_pandas_processing_etl',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 9, 1, tz=kst_tz),
    catchup=False,
    tags=['tutorial', 'kms', 'pandas', 'study']
)
def kms_pandas_etl():

    # 1. KMS에서 원본 메타데이터 추출 (Extract)
    @task(retries=2)
    def fetch_kms_metadata():
        print("KMS API에서 메타데이터 목록을 호출합니다.")
        
        # 실제 환경에서는 아래 주석을 해제하고 사용합니다.
        # api_token = Variable.get("kms_api_token")
        # headers = {"Authorization": f"Bearer {api_token}"}
        # res = requests.get("http://kms/api/documents/today", headers=headers)
        # res.raise_for_status()
        # raw_data = res.json()

        # [테스트용 가상 데이터] - 실무 API 반환값과 유사한 구조
        raw_data = [
            {"doc_no": "DOC-001", "subject": "보안 가이드", "writer": "김철수", "views": 150},
            {"doc_no": "DOC-002", "subject": "인프라 아키텍처", "writer": None, "views": 45},
            {"doc_no": "DOC-003", "subject": "오래된 규정 (폐기)", "writer": "이영희", "views": 12},
            {"doc_no": "DOC-004", "subject": "신규 프로젝트 기획안", "writer": "박민수", "views": 320},
        ]
        
        return raw_data

    # 2. Pandas를 활용한 강력한 데이터 정제 (Transform)
    @task
    def process_with_pandas(raw_data: list):
        # 2-1. JSON 리스트를 Pandas DataFrame으로 변환
        df = pd.DataFrame(raw_data)
        print("📊 [원본 데이터프레임 구조]")
        print(df.head())

        # 2-2. 결측치(NaN) 처리: 작성자가 없는 경우 'Unknown'으로 채우기
        df['writer'] = df['writer'].fillna('Unknown')

        # 2-3. 조건 필터링: 제목에 '(폐기)'가 들어간 문서는 제외
        df = df[~df['subject'].str.contains('\(폐기\)')]

        # 2-4. 컬럼명 표준화 (KMS 명칭 -> 우리 시스템 명칭)
        df = df.rename(columns={
            'doc_no': 'doc_id',
            'subject': 'title',
            'writer': 'author'
        })

        # 2-5. 파생 변수 추가: 조회수(views)가 100 이상이면 인기 문서로 분류
        df['is_popular'] = df['views'] >= 100

        print("\n✨ [정제 완료된 데이터프레임]")
        print(df.head())

        # 2-6. 다음 Task(동적 태스크 매핑 등)로 넘기기 위해 다시 딕셔너리 리스트로 변환
        # orient='records'는 [{"doc_id": "...", "title": "..."}, ...] 형태로 만들어줍니다.
        processed_data = df.to_dict(orient='records')
        
        return processed_data

    # 3. 정제된 데이터를 후속 작업으로 전달 (Load / Transfer)
    @task
    def load_processed_data(final_docs: list):
        print(f"총 {len(final_docs)}건의 정제된 데이터가 준비되었습니다.")
        for doc in final_docs:
            print(f"▶ 저장 준비 완료: {doc['doc_id']} - {doc['title']} (인기: {doc['is_popular']})")
        
        # 이 이후에 S3 전송이나 Postgres/ES 적재 태스크를 연결하면 됩니다.

    # 4. 파이프라인 의존성 연결
    raw_docs = fetch_kms_metadata()
    clean_docs = process_with_pandas(raw_docs)
    load_processed_data(clean_docs)

kms_pandas_etl()