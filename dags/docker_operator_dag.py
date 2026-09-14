from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id='docker_operator_tutorial',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['study', 'docker']
) as dag:

    # 독립된 Python 3.9 컨테이너를 띄워서 실행하는 태스크
    run_isolated_python = DockerOperator(
        task_id='run_python_3_9',
        # 1. 사용할 대상 이미지 (로컬에 로드되어 있어야 함)
        image='python:3.9-slim',
        
        # 2. 컨테이너 안에서 실행할 명령어
        command='python -c "import sys; print(f\'✅ 격리된 컨테이너의 파이썬 버전: {sys.version}\')"',
        
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