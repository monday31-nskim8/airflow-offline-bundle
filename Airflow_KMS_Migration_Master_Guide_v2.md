# Apache Airflow 실전 마스터 가이드 (KMS 이관 프로젝트 & 트러블슈팅 통합본)

이 문서는 오프라인 환경에서 Airflow를 구축하고, 사내 지식관리시스템(KMS)의 대용량 문서를 파일 스토리지(FOS)로 안전하게 이관하며, 엘라스틱서치(Elasticsearch)와 PostgreSQL을 연동하는 엔터프라이즈급 데이터 파이프라인 구축 및 트러블슈팅 가이드입니다.

---

## 1. 오프라인(폐쇄망) 환경 필수 설정

인터넷이 차단된 환경에서는 온라인 PC에서 필수 패키지를 파일(`.whl`)로 다운로드하여 이동 후, Docker 이미지를 자체적으로 빌드해야 합니다.

### 1-1. 커스텀 Docker 이미지 빌드
온라인 망에서 `apache-airflow-providers-amazon`, `apache-airflow-providers-postgres`, `apache-airflow-providers-elasticsearch`, `requests` 등을 다운로드 후 오프라인으로 가져옵니다.

```dockerfile
# Dockerfile
FROM apache/airflow:2.9.2
COPY python-packages /python-packages
RUN pip install --no-index --find-links=/python-packages apache-airflow-providers-amazon apache-airflow-providers-postgres apache-airflow-providers-elasticsearch requests
```

### 1-2. PostgreSQL 포트 개방 (DBeaver 접속용)
`docker-compose.yaml` 파일에서 Postgres 포트를 열어두면 로컬 PC의 DBeaver에서 쉽게 DB 상태를 확인할 수 있습니다.
```yaml
  postgres:
    image: postgres:13
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    ports:
      - "5432:5432" # DBeaver 외부 접속 허용
```

---

## 2. Airflow 커넥션(Connection) 및 자원 제어(Pool)

코드 내에 비밀번호나 주요 설정을 하드코딩하지 않고, Airflow 웹 UI를 통해 전역적으로 관리합니다.

1. **DB 및 스토리지 커넥션 (Admin -> Connections)**
   * `fos_s3_conn` (Amazon Web Services): FOS 01 스토리지 액세스 키 및 Endpoint URL 입력
   * `my_postgres_conn` (Postgres): Host(`host.docker.internal`), Port(`5432`), Login/PW(`airflow`)
   * `my_elastic_conn` (Elasticsearch): Host(`host.docker.internal`), Schema(`http`), Port(`9200`)
2. **자원 보호용 Pool 생성 (Admin -> Pools)**
   * `kms_api_pool` (Slots: 5): KMS 서버 과부하 방지를 위해 동시 실행 태스크 수를 5개로 제한.

---

## 3. [핵심] KMS -> FOS -> DB 동기화 마스터 파이프라인

**스트리밍 다운로드(메모리 보호)**, **멱등성(재실행 안정성)**, **Dynamic Mapping(병렬 처리)**, **Timeout(무한 대기 방지)** 기술이 결합된 완성본입니다.

```python
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHookrt 
from datetime import datetime, timedelta
import requests
import tempfile
import os

# 1. 지연 경고(SLA Miss) 콜백 함수
def sla_alert_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"🚨 [SLA 경고] 목표 시간 내 완료 실패: {task_list}")
    # 실무에서는 이 위치에 Slack 연동 로직 추가

default_args = {
    'owner': 'data_team',
    'sla_miss_callback': sla_alert_callback
}

@dag(
    dag_id='kms_to_fos_master_pipeline',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def kms_sync_pipeline():

    @task
    def get_target_documents():
        return [{"doc_id": "DOC-001", "title": "보안 가이드", "author": "김철수"}]

    # execution_timeout: 10분 내에 완료되지 않으면 강제 종료 (서버 행 방지)
    # pool: 동시 실행 5개 제한
    @task(pool='kms_api_pool', execution_timeout=timedelta(minutes=10))
    def transfer_to_fos(doc_info, logical_date):
        doc_id = doc_info['doc_id']
        api_token = Variable.get("kms_api_token")
        fos_key = f"kms_docs/{logical_date}/{doc_id}.pdf"
        tmp_path = ""

        # 스트리밍 청크 다운로드
        headers = {"Authorization": f"Bearer {api_token}"}
        with requests.get(f"http://kms/api/{doc_id}", headers=headers, stream=True) as res:
            res.raise_for_status()
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                for chunk in res.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)
                tmp_path = tmp_file.name

        try:
            s3_hook = S3Hook(aws_conn_id='fos_s3_conn')
            # replace=True로 멱등성 보장
            s3_hook.load_file(filename=tmp_path, key=fos_key, bucket_name="fos-archive", replace=True)
            doc_info['fos_path'] = f"s3://fos-archive/{fos_key}"
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)

        return doc_info

    # sla: 파이프라인 논리 시작시간 기준 1시간 내 처리 안 되면 SLA 경고 발생
    @task(sla=timedelta(hours=1))
    def save_metadata_to_db(doc_info):
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        upsert_sql = """
        INSERT INTO kms_metadata (doc_id, title, fos_path) VALUES (%s, %s, %s)
        ON CONFLICT (doc_id) DO UPDATE SET title = EXCLUDED.title, fos_path = EXCLUDED.fos_path;
        """
        pg_hook.run(upsert_sql, parameters=(doc_info['doc_id'], doc_info['title'], doc_info['fos_path']))

    target_docs = get_target_documents()
    uploaded_docs = transfer_to_fos.partial(logical_date="{{ ds_nodash }}").expand(doc_info=target_docs)
    save_metadata_to_db.expand(doc_info=uploaded_docs)

kms_sync_pipeline()
```

---

## 4. 실전 트러블슈팅 (FAQ & Error 로그 해결)

### Error 1. Elasticsearch 연동 URL 파싱 에러
* **증상:** `ValueError: URL must include a 'scheme', 'host', and 'port' component (ie 'https://localhost:9200')`
* **원인:** 최신 ES 클라이언트의 엄격한 URL 검사 정책. Host 입력칸에 `http://`와 포트가 섞여서 파싱되지 않음.
* **해결:** 웹 UI Connection 설정에서 항목을 분리해 입력.
  * `Host`: `host.docker.internal` (http:// 제거)
  * `Schema`: `http`
  * `Port`: `9200`

### Error 2. ESConnection 객체 속성 에러
* **증상:** `AttributeError: 'ESConnection' object has no attribute 'index'`
* **원인:** Airflow `ElasticsearchHook.get_conn()`은 SQL 래퍼 객체를 반환하므로 네이티브 ES 함수(.index, .search)를 쓸 수 없음.
* **해결:** 반환된 객체 뒤에 `.es`를 붙여 네이티브 클라이언트를 추출.
  * 수정 전: `es_client = hook.get_conn()`
  * 수정 후: `es_client = hook.get_conn().es`

### Error 3. Postgres 시퀀스(Sequence) 충돌
* **증상:** `psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint "pg_type_typname_nsp_index"`
* **원인:** 테이블 재생성 시 기존 테이블에 물려 있던 `SERIAL`(자동 증가) 시퀀스 찌꺼기가 DB에 남아 있어 충돌 발생.
* **해결:** DB(DBeaver 또는 psql 터미널)에 접속하여 찌꺼기 강제 제거.
  * `DROP TABLE IF EXISTS [테이블명] CASCADE;`
  * `DROP SEQUENCE IF EXISTS [테이블명_id_seq] CASCADE;`

### Error 4. DBeaver에서 Postgres 접속 거부 (Connection Refused)
* **원인:** Airflow 공식 `docker-compose.yaml`은 보안상 Postgres 포트를 외부 호스트로 노출하지 않음.
* **해결:** `docker-compose.yaml`의 `postgres` 서비스 항목에 `ports: - "5432:5432"` 라인을 추가하고 컨테이너 재시작(`docker compose up -d`). DBeaver에서 `localhost:5432`, 계정 `airflow` / `airflow`로 접속.

### Error 5. AirflowTaskTimeout 에러
* **증상:** `airflow.exceptions.AirflowTaskTimeout: Timeout`
* **원인/해결:** 이것은 시스템 장애가 아닌 **정상적인 보호 기작(성공)**입니다. `@task(execution_timeout=...)`에 설정한 시간이 초과되어 서버 행(Hang)을 방지하고자 작업을 강제 킬(Kill)한 것입니다. 제한 시간을 늘리거나 실제 연동 속도를 튜닝해야 합니다.

### Error 6. SLA 콜백이 호출되지 않는 문제
* **증상:** 시간이 지연되었는데 슬랙/프린트 알림(`sla_miss_callback`)이 동작하지 않음.
* **원인 및 해결 (3대 체크리스트):**
  1. **수동 실행 여부:** SLA는 웹 UI에서 '▶(수동 실행)'하거나 터미널로 `test` 시 무시됩니다. 반드시 **스케줄러에 의한 자동 실행**이어야 작동합니다.
  2. **로그 위치 확인:** SLA 콜백은 Worker(태스크)가 아닌 Scheduler가 실행합니다. 태스크 로그 창이 아닌 터미널에서 `docker compose logs airflow-scheduler`를 확인해야 프린트 문구가 보입니다.
  3. **시간 계산 기준:** SLA 타이머는 작업 '시작 시간'이 아니라 파이프라인의 **'논리적 시작 기준일(Logical Date)'**부터 계산됩니다. 과거 Backfill 시 즉각 SLA Miss가 뜰 수 있습니다.
