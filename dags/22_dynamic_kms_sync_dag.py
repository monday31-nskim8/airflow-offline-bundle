from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id='dynamic_kms_to_fos_pipeline',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['project', 'dynamic_mapping', 'study']
)
def dynamic_sync_pipeline():

    # 1. 추출(Extract): 이관 대상 목록(100개)을 조회하여 리스트로 반환
    @task
    # @task(max_active_tis_per_dag=5) # **"한 번에 최대 5개씩만 병렬로 돌려라"**
    # @task(pool='kms_api_pool') # KMS API를 호출하는 태스크에 생성한 pool을 지정합니다.
    def get_target_documents():
        # 실제로는 KMS API를 호출해 ["DOC-001", "DOC-002", ...] 형태의 리스트를 받아옵니다.
        # 테스트를 위해 3개의 샘플 데이터를 반환합니다.
        print("KMS에서 오늘 이관할 문서 목록을 조회합니다.")
        return [
            {"doc_id": "DOC-101", "title": "2026 기획안", "author": "기획팀"},
            {"doc_id": "DOC-102", "title": "인프라 구성도", "author": "개발팀"},
            {"doc_id": "DOC-103", "title": "보안 점검표", "author": "보안팀"}
        ]

    # 2. 적재(Load 1): 개별 문서를 FOS에 업로드 (리스트 개수만큼 자동 복제됨)
    @task
    def transfer_to_fos(doc_info, run_date):
        # 개별 문서 1건을 처리하는 로직입니다.
        fos_path = f"s3://fos-document-archive/kms/{run_date}/{doc_info['doc_id']}.pdf"
        
        # (여기에 실제 S3Hook 파일 업로드 로직 배치)
        print(f"🚀 [{doc_info['title']}] 파일 FOS 업로드 완료: {fos_path}")
        
        # 메타데이터에 FOS 경로를 추가하여 반환합니다.
        doc_info['fos_path'] = fos_path
        return doc_info

    # 3. 적재(Load 2): 개별 문서의 메타데이터를 DB에 저장 (역시 자동 복제됨)
    @task
    def save_metadata_to_pg(doc_info):
        # 개별 문서 1건의 DB UPSERT 로직입니다.
        # (여기에 실제 PostgresHook DB 적재 로직 배치)
        print(f"💾 [{doc_info['doc_id']}] DB 메타데이터 저장 완료")

    # ==========================================
    # 파이프라인 조립 (마법이 일어나는 구간)
    # ==========================================
    
    # A. 대상 리스트를 가져옵니다.
    target_docs = get_target_documents()
    
    # B. 리스트를 펼쳐서(expand) 파일 업로드를 병렬로 실행합니다.
    # 고정된 값(run_date)은 partial로, 쪼갤 리스트(target_docs)는 expand로 넘깁니다.
    uploaded_docs = transfer_to_fos.partial(run_date="{{ ds_nodash }}").expand(doc_info=target_docs)
    
    # C. 🚀 연쇄 매핑 (Chaining) 🚀
    # B의 결과물(리스트)을 그대로 C의 expand에 던져주면, 
    # Airflow가 알아서 앞선 태스크와 1:1로 매핑하여 병렬로 DB에 적재합니다.
    save_metadata_to_pg.expand(doc_info=uploaded_docs)

dynamic_sync_pipeline()