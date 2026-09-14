from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import tempfile
import os

@dag(
    dag_id='kms_to_fos_and_db_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['project', 'kms', 'fos', 'db', 'study']
)
def my_kms_sync_pipeline():

    # [Task 1] KMS에서 파일을 받아 FOS에 저장하고 메타데이터를 반환합니다.
    @task
    def transfer_to_fos(**kwargs):
        logical_date = kwargs['ds_nodash']
        doc_id = "DOC-20260908"
        
        # 1. KMS API에서 가져왔다고 가정하는 메타데이터
        metadata = {
            "doc_id": doc_id,
            "title": "2026년 하반기 보안 가이드라인",
            "author": "보안팀",
            "category": "규정",
            "kms_download_url": "http://kms.company.com/api/download/dummy"
        }

        fos_bucket = "fos-document-archive"
        fos_object_key = f"kms_backup/{logical_date}/{doc_id}.pdf"
        metadata['fos_path'] = f"s3://{fos_bucket}/{fos_object_key}" # 최종 저장 경로 기록

        print(f"🚀 [{metadata['title']}] 파일 다운로드 및 FOS 업로드를 시작합니다.")

        # (이전 단계에서 배운 스트리밍 다운로드 및 S3Hook 업로드 로직이 이곳에 들어갑니다)
        # ...
        print(f"✅ FOS 01 업로드 완료: {metadata['fos_path']}")

        # 2. 다음 태스크를 위해 메타데이터 딕셔너리를 통째로 반환합니다.
        return metadata

    # [Task 2] 앞선 태스크가 넘겨준 메타데이터를 DB에 저장합니다.
    @task
    def save_metadata_to_pg(doc_info):
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        
        # 1. 메타데이터를 저장할 테이블 생성 (기본키 설정이 핵심입니다)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS kms_document_metadata (
            doc_id VARCHAR(50) PRIMARY KEY,
            title VARCHAR(200),
            author VARCHAR(100),
            category VARCHAR(50),
            fos_path VARCHAR(500),
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg_hook.run(create_table_sql)

        # 2. UPSERT 쿼리 (멱등성 보장)
        # 이미 같은 doc_id가 존재하면 에러를 내지 않고 최신 정보로 덮어씁니다(UPDATE).
        upsert_sql = """
        INSERT INTO kms_document_metadata (doc_id, title, author, category, fos_path)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (doc_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            author = EXCLUDED.author,
            category = EXCLUDED.category,
            fos_path = EXCLUDED.fos_path,
            synced_at = CURRENT_TIMESTAMP;
        """
        
        pg_hook.run(
            upsert_sql,
            parameters=(
                doc_info['doc_id'],
                doc_info['title'],
                doc_info['author'],
                doc_info['category'],
                doc_info['fos_path']
            )
        )
        print(f"💾 DB 메타데이터 동기화 완료: {doc_info['doc_id']}")

    # 흐름 연결: Task 1의 리턴값을 Task 2의 파라미터로 넘겨줍니다.
    document_metadata = transfer_to_fos()
    save_metadata_to_pg(document_metadata)

my_kms_sync_pipeline()