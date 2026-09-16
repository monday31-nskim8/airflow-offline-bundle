from airflow.sdk import dag, task
# from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook

# 일반적인 커넥션 및 SQL 래퍼 기반 객체를 가져올 때
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchSQLHook

# 또는 순수 파이썬 네이티브 클라이언트를 직접 가져오고 싶을 때
# from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

from datetime import datetime
import json

@dag(
    dag_id='elasticsearch_aggregation_test',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'elasticsearch']
)
def my_elastic_search_pipeline():

    @task
    def fetch_and_aggregate():
        # 1. ES 커넥션 맺기
        es_hook = ElasticsearchSQLHook(elasticsearch_conn_id='my_elastic_conn')
        es_client = es_hook.get_conn().es

        # 2. ES Query DSL 작성 (event_type 필드 기준 그룹화)
        # 💡 핵심: size를 0으로 주면 원본 데이터는 가져오지 않고 통계 결과만 빠르게 반환받습니다.
        query = {
            "size": 0,
            "aggs": {
                "event_type_stats": {
                    "terms": {
                        # ES는 문자열을 기본적으로 text와 keyword로 이중 저장합니다. 
                        # 정확한 일치(그룹화)를 위해서는 .keyword를 붙여야 합니다.
                        "field": "event_type.keyword",
                        "size": 10 # 상위 10개 그룹만 추출
                    }
                }
            }
        }

        print(f"🚀 전송할 쿼리:\n{json.dumps(query, indent=2)}")

        # 3. 검색(Search) API 호출
        response = es_client.search(
            index="airflow_activity_log",
            body=query
        )

        # 4. 집계(Aggregations) 결과 파싱
        # response 딕셔너리 안에서 우리가 이름 붙인 'event_type_stats' 안의 'buckets'를 찾습니다.
        buckets = response.get("aggregations", {}).get("event_type_stats", {}).get("buckets", [])
        
        print("\n📊 [집계 결과 분석]")
        if not buckets:
            print("데이터가 없거나 집계된 결과가 없습니다.")
        
        for bucket in buckets:
            event_name = bucket['key']
            count = bucket['doc_count']
            print(f" - 이벤트 타입 [{event_name}]: {count}건")

        # 다음 태스크에서 사용할 수 있도록 결과 반환 (XCom 저장)
        return buckets

    # 5. 후속 처리 태스크 (이상 탐지 시뮬레이션)
    @task
    def check_alert_threshold(agg_results):
        for bucket in agg_results:
            # 만약 에러 로그가 100건 이상이면 슬랙 알림을 보낸다는 식의 분기 처리 가능
            if bucket['key'] == 'error' and bucket['doc_count'] > 100:
                print(f"🚨 심각한 에러 발생 급증! 즉시 슬랙 알림을 전송합니다. ({bucket['doc_count']}건)")
            else:
                print(f"✅ {bucket['key']} 지표 정상 범위 내 (건수: {bucket['doc_count']})")

    # 태스크 연결
    stats = fetch_and_aggregate()
    check_alert_threshold(stats)

my_elastic_search_pipeline()