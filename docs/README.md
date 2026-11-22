# 문서 색인

## 런타임 계약과 설계

| 찾는 내용 | 원본 |
| --- | --- |
| 목적, 용어, 초기 범위, 수용 기준 | [요구사항](requirements.md) |
| 역할·워커·상태의 분리와 자원 설정 | [런타임 설계](design/runtime.md) · [자원 산정](design/resource-sizing.md) |
| 영속 작업, 메시지, 대기·재개 | [협력 실행](design/cooperation.md) · [Codex 협력 실행](design/codex-cooperation.md) |
| Codex 실행과 외부 그래프 연결 | [어댑터](design/codex-adapter.md) · [JSONL 브리지](design/orchestrator-bridge.md) |
| 소켓 제어·영수증·수명주기 이벤트와 문서 인계 | [로컬 워크플로 제어](design/workflow-control.md) · [모델 문서 인계](model-workflow.md) |

## 결정과 계획

- [Go 우선 및 기존 하네스 재사용 결정](decisions/0001-go-runtime-and-harness-reuse.md) · [pi를 첫 어댑터로 채택한 결정](decisions/0002-pi-first-adapter.md) · [후보 조사](research/reuse-candidates.md) · [의존성과 출처](../THIRD_PARTY.md)
- [개발 계획과 단계별 상태](roadmap.md) · [메시지 후속 과제](design/messaging-backlog.md) · [메모리 페이징 후속 과제](design/paging-backlog.md)

## 검증과 실험

| 찾는 내용 | 원본 |
| --- | --- |
| 수용 조건과 재현 명령 | [검증 계획](validation.md) · [실험 실행 안내](../experiments/README.md) |
| 하네스 선택, 워커 풀, 자원 측정 | [Sprint 1 결과](research/sprint-1-results.md) · [Sprint 2 결과](research/sprint-2-results.md) · [자원 측정 결과](research/resource-results.md) |
| 협력 실행과 Codex 실모델 검증 | [협력 실행 결과](research/cooperation-results.md) · [Codex 파일럿](research/codex-pilot-results.md) · [heartbeat·문서 SMS](research/codex-heartbeat-sms-results.md) |
| 역할별 조사·심의·복구 | [사업 조직 실험 안내](../experiments/event-search/README.md) · [6역할·2워커 복구 기록](../experiments/event-search/target-review-team.md) |
| 외부 워크플로와 수신함 연동 | [합성 시험 프로그램](../experiments/workflowprobe/README.md) · [모델 문서 인계 검증](model-workflow.md) |

## 확인된 문서 공백

- 브리지 실행 시도 조회 명령은 [루트 README](../README.md)에 있지만, [브리지 설계 문서](design/orchestrator-bridge.md)에는 `absent`와 `observed`의 전체 응답 필드 및 출력 예제가 없다. 현재 구현은 [조회 코드](../cmd/runtime-bridge/inspection.go)에 있다.
- [자원 측정 결과](research/resource-results.md)는 합성 모델의 RSS·CPU·처리량을 기록한다. 같은 조건의 실모델 토큰 절감과 장기 메모리 상한을 확인한 검증 보고서는 없다.

## 문서 관리

이 저장소는 실행 계층의 계약과 검증 근거를 관리한다. 네 저장소의 통합 마일스톤 정본은 agent-eval의 `docs/milestones/vX.Y.Z/README.md`에 둔다. 작성 규칙은 [공통 문서 정책](https://github.com/shakystar/agent-collaboration-lab/blob/main/docs/documentation-policy.md)과 [마일스톤 양식](https://github.com/shakystar/agent-collaboration-lab/blob/main/templates/milestone.md)을 따른다. 두 링크는 비공개 저장소 접근 권한이 있어야 열 수 있다.
