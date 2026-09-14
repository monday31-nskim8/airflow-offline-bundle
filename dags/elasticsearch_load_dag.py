from airflow.decorators import dag, task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from datetime import datetime
import json

@dag(
    dag_id='elasticsearch_load_test',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'elasticsearch']
)
def my_elastic_pipeline():

    @task
    def load_data_to_es():
        # 1. UI에 등록한 Connection ID로 Hook 생성
        hook = ElasticsearchHook(elasticsearch_conn_id='my_elastic_conn')

        # AttributeError: 'ESConnection' object has no attribute 'index'
        # 2. 실제 ES와 통신할 수 있는 클라이언트 객체 가져오기 
        # es_client = hook.get_conn()

        # 💡 수정된 부분: get_conn() 뒤에 .es 를 붙여서 원본 클라이언트 객체를 추출합니다.
        es_client = hook.get_conn().es

        # 3. 적재할 가상의 샘플 데이터 딕셔너리 생성
        sample_data = {
            "user_id": 101,
            "event_type": "login",
            "platform": "windows",
            # Elasticsearch가 인식할 수 있도록 날짜를 ISO 포맷 문자열로 변환
            "timestamp": datetime.now().isoformat()
        }

        print(f"적재할 데이터: {json.dumps(sample_data)}")

        # 4. Elasticsearch에 데이터 적재 (Index)
        # index 파라미터는 관계형 DB의 '테이블명'과 같은 역할입니다.
        response = es_client.index(
            index="airflow_activity_log",
            document=sample_data
        )
        
        print(f"✅ 엘라스틱서치 적재 완료! 문서 고유 ID: {response['_id']}")

    load_data_to_es()

my_elastic_pipeline()
