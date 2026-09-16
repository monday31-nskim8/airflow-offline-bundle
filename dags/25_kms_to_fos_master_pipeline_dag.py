from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import tempfile
import os

# 1. 지연 경고(SLA Miss) 콜백 함수
def sla_alert_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"🚨 [SLA 경고] 목표 시간 내 완료 실패: {task_list}")
    # 실무에서는 Slack Webhook 연동

default_args = {
    'owner': 'data_team',
    'sla_miss_callback': sla_alert_callback
}

@dag(
    dag_id='kms_to_fos_master_pipeline',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sample', 'project', 'kms', 'master', 'study']
)
def kms_sync_pipeline():

    # [Step 1] 이관 대상 문서 리스트 추출
    @task
    def get_target_documents():
        # 실무: KMS API에서 당일 업데이트된 리스트를 가져옴
        return [
            {"doc_id": "DOC-001", "title": "보안 가이드", "author": "김철수"},
            {"doc_id": "DOC-002", "title": "인프라 구성도", "author": "이영희"}
        ]

    # [Step 2] FOS 01 적재 (메모리 스트리밍 & 타임아웃 & Pool 제어)
    # execution_timeout: 10분 내에 안 끝나면 강제 종료
    # pool: 동시에 5개까지만 실행되도록 제어
    @task(pool='kms_api_pool', execution_timeout=timedelta(minutes=10))
    def transfer_to_fos(doc_info, target_date):
        doc_id = doc_info['doc_id']
        kms_url = f"http://kms.company.com/api/docs/{doc_id}/download"
        api_token = Variable.get("kms_api_token")
        
        fos_bucket = "fos-archive"
        fos_key = f"kms_docs/{target_date}/{doc_id}.pdf"

        # 1. 스트리밍 다운로드 (청크 단위로 임시 파일 기록)
        headers = {"Authorization": f"Bearer {api_token}"}
        with requests.get(kms_url, headers=headers, stream=True) as res:
            res.raise_for_status()
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                for chunk in res.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)
                tmp_path = tmp_file.name

        # 2. S3(FOS) 업로드 (replace=True 로 덮어쓰기 -> 멱등성 보장)
        try:
            s3_hook = S3Hook(aws_conn_id='fos_s3_conn')
            s3_hook.load_file(filename=tmp_path, key=fos_key, bucket_name=fos_bucket, replace=True)
            doc_info['fos_path'] = f"s3://{fos_bucket}/{fos_key}"
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path) # 사용 후 즉시 디스크 정리

        return doc_info

    # [Step 3] 메타데이터 DB 동기화 (UPSERT 방어 로직)
    # sla: DAG 논리적 시작 시간 기준 1시간 내에 통과 못하면 경고 발생
    @task(sla=timedelta(hours=1))
    def save_metadata_to_db(doc_info):
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        
        upsert_sql = """
        INSERT INTO kms_metadata (doc_id, title, author, fos_path)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (doc_id) 
        DO UPDATE SET
            title = EXCLUDED.title,
            author = EXCLUDED.author,
            fos_path = EXCLUDED.fos_path,
            synced_at = CURRENT_TIMESTAMP;
        """
        
        pg_hook.run(upsert_sql, parameters=(
            doc_info['doc_id'], doc_info['title'], doc_info['author'], doc_info['fos_path']
        ))

    # [흐름 연결] Dynamic Task Mapping 기반의 연쇄 병렬 처리
    target_docs = get_target_documents()
    
    # 1개의 리스트를 N개의 병렬 FOS 업로드 태스크로 확장 (run_date는 고정 변수로 주입)
    # 🚨 partial에 예약어(logical_date)를 억지로 넣으려다 에러 발생
    # uploaded_docs = transfer_to_fos.partial(logical_date="{{ ds_nodash }}").expand(doc_info=target_docs)
    
    # ✅ 시스템 예약어가 아니므로 partial()이 템플릿 변수를 정상적으로 넘겨줍니다!
    uploaded_docs = transfer_to_fos.partial(target_date="{{ ds_nodash }}").expand(doc_info=target_docs)

    # FOS 업로드가 끝난 N개의 결과를 다시 N개의 병렬 DB 적재 태스크로 확장
    save_metadata_to_db.expand(doc_info=uploaded_docs)

kms_sync_pipeline()