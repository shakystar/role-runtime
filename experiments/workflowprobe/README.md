# SMS·LangGraph 연동을 검증하는 합성 테스트

`./scripts/go.sh build -o bin/workflow-probe ./experiments/workflowprobe`로 빌드한다. role-orchestrator/tests/test_workflow_probe.py의 Python 회귀 검사가 임시 상태 디렉터리에 이 프로그램을 실행한다. 모델을 호출하지 않고 실제 협력 저장소, 사서함, 임대와 영수증을 검사한다. 장애 주입을 위해 조기·오발송 SMS와 생산자 실행 차단을 포함한다. 서비스 API나 범용 실행기로 제공하지 않는다.

`observe`는 사서함과 발신함 상태를 대조·반영하고 생산자 완료 영수증, 수신함 식별자의 정확한 일치, 고정 합성 산출물의 SHA-256을 검사한다. 이 명령은 저장소 상태를 변경할 수 있다. 이미 테스트 소비자가 완료된 뒤 `consume`을 반복해도 결과는 같다. 호출마다 Go 저장소를 단독으로 열고 닫는다. 운영 중 동시 접근에는 각 호출이 저장소를 따로 여는 대신 기존 소유 프로세스가 제공하는 API가 필요하다.

성능·토큰·비용 측정과 범용 이벤트 집계는 구현하지 않았다. 측정 결과와 다음 단계는 role-orchestrator/docs/workflow-feasibility.md에 있다.
