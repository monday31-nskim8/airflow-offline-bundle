from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

@dag(
    dag_id='docker_operator_tutorial',
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=['tutorial', 'docker', 'airflow3']
)
def docker_operator_pipeline():

    # DockerOperator는 @task 데코레이터 대신 클래스 객체 형태로 직접 호출합니다.
    # 독립된 Python 3.9 컨테이너를 띄워서 실행하는 태스크
    run_isolated_python = DockerOperator(
        task_id='run_python_3_9',
        # 1. 사용할 대상 이미지 (로컬에 로드되어 있어야 함) # 오프라인 환경이라면 호스트에 이 이미지가 미리 pull 되어 있어야 합니다.
        image='python:3.9-slim',

        # 2. 컨테이너 안에서 실행할 명령어
        command='echo "✅ Docker 내부에서 독립된 Python 컨테이너가 성공적으로 실행되었습니다!" && python --version',
        
        # 3. Airflow 컨테이너가 호스트 PC의 Docker 엔진과 통신하기 위한 주소 (필수)
        docker_url='unix://var/run/docker.sock',
        
        # 4. 네트워크 모드 (bridge 모드가 가장 무난함)
        network_mode='bridge',
        
        # 5. 작업이 끝나면 찌꺼기를 남기지 않고 컨테이너를 삭제할지 여부
        auto_remove='force',
        
        # 6. Docker in Docker 환경에서 경로 매핑 오류를 방지하기 위한 옵션
        mount_tmp_dir=False
    )

    run_isolated_python

docker_operator_pipeline()

# docker compose exec airflow-scheduler airflow tasks test docker_operator_tutorial run_python_3_9 2026-09-15