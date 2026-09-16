# external_system_trigger.py (Airflow 밖의 다른 서버나 로컬 PC에서 실행)
import requests
import json

# 1. Airflow 웹 UI 주소 및 API 엔드포인트
# 형태: /api/v1/dags/{dag_id}/dagRuns
AIRFLOW_API_URL = "http://localhost:8080/api/v2/dags/external_trigger_dag/dagRuns"

# 기존 dagRuns 대신 dag_runs 로 변경
# AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags/external_trigger_dag/dag_runs"

# /api/v1/ 대신 /public/ 경로 사용
# AIRFLOW_API_URL = "http://localhost:8080/public/dags/external_trigger_dag/dagRuns"

# 2. Airflow 로그인 계정 정보 (기본값: airflow / airflow)
AUTH = ("airflow", "airflow")

# 3. Airflow DAG로 넘겨줄 동적 파라미터 (conf 딕셔너리 안에 작성)
payload = {
    "conf": {
        "file_name": "kms_document_999.pdf",
        "source": "KMS_File_Upload_Server"
    }
}

print("Airflow로 DAG 실행 요청을 전송합니다...")

# 4. HTTP POST 요청 발송
response = requests.post(
    AIRFLOW_API_URL,
    auth=AUTH,
    headers={"Content-Type": "application/json"},
    data=json.dumps(payload)
)

# 5. 결과 확인
if response.status_code == 200:
    print("✅ Airflow DAG가 성공적으로 실행되었습니다!")
    print("응답 데이터:", response.json())
else:
    print(f"❌ 실행 실패 (상태 코드: {response.status_code})")
    print("에러 내용:", response.text)