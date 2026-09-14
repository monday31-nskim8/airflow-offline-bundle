from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import requests
import tempfile
import os

@dag(
    dag_id='kms_to_fos_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['project', 'kms', 'fos', 'study']
)
def my_kms_migration_pipeline():

    @task
    def transfer_document_to_fos(**kwargs):
        # 1. 메타데이터 및 설정 준비
        logical_date = kwargs['ds_nodash'] # 예: 20260908
        
        # (가정) KMS에서 가져올 문서의 ID와 다운로드 API URL
        doc_id = "DOC-998877"
        kms_download_url = f"http://kms.company.com/api/v1/documents/{doc_id}/download"
        kms_api_token = Variable.get("kms_api_token") # UI에서 등록한 API 키
        
        # FOS 01에 저장할 버킷명과 파일 경로(Object Key) 설계
        fos_bucket = "fos-document-archive"
        fos_object_key = f"kms_backup/{logical_date}/{doc_id}.pdf"

        print(f"🚀 KMS에서 문서({doc_id}) 다운로드를 시작합니다...")

        # 2. 스트리밍 방식으로 KMS 파일 다운로드 (메모리 폭발 방지)
        headers = {"Authorization": f"Bearer {kms_api_token}"}
        
        # stream=True 옵션이 가장 중요합니다.
        with requests.get(kms_download_url, headers=headers, stream=True) as response:
            response.raise_for_status() # 에러 발생 시 즉시 중단 (Airflow 재시도 유도)
            
            # 3. 로컬 디스크에 임시 파일(Temp File) 생성
            # 작업이 끝나면 운영체제가 알아서 이 임시 파일을 삭제합니다.
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                # 8KB(8192바이트)씩 잘라서 디스크에 기록합니다.
                for chunk in response.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)
                
                tmp_file_path = tmp_file.name
                print(f"✅ 임시 파일 저장 완료 (경로: {tmp_file_path})")

        # 4. FOS 01(S3)로 파일 업로드
        try:
            print(f"🚀 FOS 01 스토리지로 업로드를 시작합니다... (대상: {fos_object_key})")
            
            # UI에 등록한 Connection ID로 S3 통신
            s3_hook = S3Hook(aws_conn_id='fos_s3_conn')
            
            # S3에 임시 파일을 업로드 (replace=True로 멱등성 보장)
            s3_hook.load_file(
                filename=tmp_file_path,
                key=fos_object_key,
                bucket_name=fos_bucket,
                replace=True 
            )
            
            print("✅ FOS 01 적재가 완벽하게 끝났습니다!")
            
        finally:
            # 5. 혹시 모를 에러에 대비하여 임시 파일을 강제 삭제
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)

        # 다음 태스크(DB 메타데이터 적재 등)를 위해 파일 경로 반환
        return {"doc_id": doc_id, "fos_path": fos_object_key}

    # 태스크 실행
    transfer_document_to_fos()

my_kms_migration_pipeline()