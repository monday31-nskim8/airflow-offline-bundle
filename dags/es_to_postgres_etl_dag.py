from airflow.decorators import dag, task
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

@dag(
    dag_id='es_to_postgres_etl',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'etl']
)
def es_to_pg_pipeline():

    # [Extract & Transform] 1. ES에서 데이터 집계하기
    @task
    def extract_aggregated_data():
        es_hook = ElasticsearchHook(elasticsearch_conn_id='my_elastic_conn')
        es_client = es_hook.get_conn().es

        query = {
            "size": 0,
            "aggs": {
                "event_type_stats": {
                    "terms": {"field": "event_type.keyword", "size": 10}
                }
            }
        }

        response = es_client.search(index="airflow_activity_log", body=query)
        buckets = response.get("aggregations", {}).get("event_type_stats", {}).get("buckets", [])
        
        # 다음 태스크에서 처리하기 쉽도록 정제된 리스트(List of Dicts) 형태로 변환하여 반환합니다.
        parsed_data = [{"event_type": b['key'], "count": b['doc_count']} for b in buckets]
        
        print(f"✅ ES 집계 완료: {parsed_data}")
        return parsed_data

    # [Load] 2. PG 테이블에 적재하기
    # **kwargs를 통해 Airflow의 기본 매크로 변수(ds 등)를 가져옵니다.
    @task
    def load_to_postgres(stats_data, **kwargs):
        if not stats_data:
            print("적재할 데이터가 없습니다.")
            return

        logical_date = kwargs['ds'] # 파이프라인 기준일 (예: 2026-09-08)
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')

        # 2-1. 리포팅용 통계 테이블 생성 (없을 경우)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS daily_event_stats (
            id SERIAL PRIMARY KEY,
            run_date DATE NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            event_count INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg_hook.run(create_table_sql)

        # 2-2. 멱등성(Idempotency) 보장: 재실행 시 데이터가 중복으로 쌓이지 않도록 기존 날짜 데이터 삭제
        delete_sql = "DELETE FROM daily_event_stats WHERE run_date = %s;"
        pg_hook.run(delete_sql, parameters=(logical_date,))
        print(f"🗑️ 기준일({logical_date})의 기존 데이터를 삭제하여 중복을 방지했습니다.")

        # 2-3. 새로운 통계 데이터 삽입 (파라미터 바인딩으로 SQL 인젝션 방지)
        insert_sql = """
        INSERT INTO daily_event_stats (run_date, event_type, event_count) 
        VALUES (%s, %s, %s);
        """
        
        for stat in stats_data:
            pg_hook.run(
                insert_sql, 
                parameters=(logical_date, stat['event_type'], stat['count'])
            )
            print(f"💾 삽입 완료: [{stat['event_type']}] - {stat['count']}건")

    # 3. 태스크 연결 (함수 반환값을 다음 함수의 인자로 넣으면 자동으로 순서와 데이터가 연결됩니다)
    es_data = extract_aggregated_data()
    load_to_postgres(es_data)

es_to_pg_pipeline()