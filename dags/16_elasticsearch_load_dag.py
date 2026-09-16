from airflow.sdk import dag, task
# from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
# 일반적인 커넥션 및 SQL 래퍼 기반 객체를 가져올 때
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchSQLHook

# 또는 순수 파이썬 네이티브 클라이언트를 직접 가져오고 싶을 때
# from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

from datetime import datetime
import requests
import json

@dag(
    dag_id='elasticsearch_load_test',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'elasticsearch']
)
def my_elastic_pipeline():

    @task
    def load_data_to_es():
        # 1. UI에 등록한 Connection ID로 Hook 생성
        es_hook = ElasticsearchPythonHook(elasticsearch_conn_id='my_elastic_conn')

        # AttributeError: 'ESConnection' object has no attribute 'index'
        # 2. 실제 ES와 통신할 수 있는 클라이언트 객체 가져오기 
        # es_client = hook.get_conn()

        # 💡 수정된 부분: get_conn() 뒤에 .es 를 붙여서 원본 클라이언트 객체를 추출합니다.
        es_client = es_hook.get_conn().es

        # 3. 적재할 가상의 샘플 데이터 딕셔너리 생성
        doc = {
            "user_id": 101,
            "event_type": "login",
            "platform": "windows",
            # Elasticsearch가 인식할 수 있도록 날짜를 ISO 포맷 문자열로 변환
            "timestamp": datetime.now().isoformat()
        }

        print(f"적재할 데이터: {json.dumps(doc, indent=2, ensure_ascii=False)}")

        # 4. Elasticsearch에 데이터 적재 (Index)
        # index 파라미터는 관계형 DB의 '테이블명'과 같은 역할입니다.
        # response = es_client.index(
        #     index="airflow_activity_log",
        #     document=sample_data
        # )

        # 💡 .index() 호출 직전에 .options()를 붙여 헤더를 강제로 조작합니다.
        response = es_client.options(
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        ).index(index="airflow_activity_log", document=doc)
        
        print(f"✅ 엘라스틱서치 적재 완료! 문서 고유 ID: {response['_id']}")

    @task
    def load_data_to_es2():
        # Elasticsearch의 REST API 엔드포인트 주소
        es_url = "http://host.docker.internal:9200/airflow_activity_log/_doc"
        
        doc = {"title": "보안 가이드", "author": "김철수"}
        
        # 순수 범용 JSON 헤더만 세팅 (버전 꼬리표 없음)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # requests 모듈을 이용해 직접 POST 요청 발송
        response = requests.post(es_url, headers=headers, data=json.dumps(doc))
        
        # 결과 확인
        response.raise_for_status() # 실패 시 에러 발생
        print("✅ REST API를 통한 ES 적재 완료:", response.json())

    load_data_to_es()
    # load_data_to_es2()

my_elastic_pipeline()
