# plugins/my_legacy_api.py
from airflow.models.baseoperator import BaseOperator
import json

class LegacySystemOperator(BaseOperator):
    # 💡 핵심: Jinja 템플릿({{ ds }} 등)을 변환해서 받을 파라미터 이름을 지정합니다.
    template_fields = ('endpoint', 'payload')

    def __init__(self, endpoint: str, payload: dict, **kwargs):
        # 부모 클래스(BaseOperator)의 초기화 함수를 반드시 호출해야 합니다. (task_id 등을 처리)
        super().__init__(**kwargs)
        
        # DAG에서 넘겨받은 파라미터를 인스턴스 변수로 저장합니다.
        self.endpoint = endpoint
        self.payload = payload

    def execute(self, context):
        # 이 함수는 실제 태스크가 실행될 때 단 한 번 호출됩니다.
        # context 딕셔너리에는 실행 시간, task_id 등 Airflow의 모든 정보가 들어있습니다.
        
        print(f"🚀 사내 레거시 시스템({self.endpoint})으로 연결을 시도합니다...")
        
        # 실제로는 requests 모듈 등으로 사내 API를 호출하는 로직이 들어갑니다.
        formatted_payload = json.dumps(self.payload)
        print(f"전송할 데이터: {formatted_payload}")
        
        # 결과 검증 로직 시뮬레이션
        if "error" in formatted_payload:
            raise ValueError("레거시 시스템에서 에러를 반환했습니다.")
        
        print("✅ 데이터 전송이 성공적으로 완료되었습니다.")
        
        # return 한 값은 자동으로 XCom에 저장되어 다음 태스크가 쓸 수 있습니다.
        return f"Success_Response_from_{self.endpoint}"
