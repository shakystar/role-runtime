# 협업 실행 계약

Go `engine.CooperativePool`이 영속 작업을 조회하여 K개 `StepExecutor`에 배정한다. 외부 공급자가 작업과 역할을 지정한다. 응답 메시지는 이미 제출된 작업을 깨우며, 새로운 응답 작업을 자동 생성하지 않는다. 기존 `engine.Pool`의 일회성 실행 계약은 유지한다. 두 풀로 같은 역할 세션을 동시에 실행하지 않는다.

## 대기와 재개

```text
외부 SubmitCooperative → ready → running → completed
                                  ↓
                      checkpoint + outbox + waiting
                                  ↓ 워커 반환
                       메시지 일치 → ready → running
```

`StepExecutor.Step`은 최종 `Transition`을 반환한다. `wait`가 있으면 체크포인트를 반드시 제공하고, 없으면 완료 결과 `text`가 필요하다. pi는 한 모델 턴이 `agent_settled`로 끝난 뒤 이 JSON을 파싱한다. 역할 지침에서 아래 프로토콜을 가르쳐야 한다. 기존 자유 텍스트 응답 어댑터를 자동으로 협업 모드로 바꾸지 않는다.

```json
{
  "checkpoint": "DTO 초안을 저장했다. QA 응답으로 검증을 이어간다.",
  "send": [{
    "id": "ask-42",
    "from": {"project":"demo","role":"dto","version":"v1","instance":"default"},
    "to": {"project":"demo","role":"qa","version":"v1","instance":"default"},
    "correlation":"review-42",
    "body":"DTO 초안 검증 요청"
  }],
  "wait": {
    "from": {"project":"demo","role":"qa","version":"v1","instance":"default"},
    "correlation":"review-42"
  }
}
```

재개 입력은 `Execution` JSON이다: 원래 `task`, 실행 `generation`, 저장한 `checkpoint`, 일치한 `inbox` 메시지. pi의 해당 역할 세션 기록도 복원한다. 완료 예시는 `{"text":"검증을 마쳤다"}`다. 중간 턴에 SMS를 주입하거나 LLM 호출 스택을 직렬화하지 않는다.

## 메시지와 원자성

주소는 프로젝트·역할·버전·인스턴스다. 버전은 정확히 일치해야 하고 프로젝트 간 전달은 거절한다. 에이전트의 발신 주소는 실행 역할과 같아야 한다. `Store.Deliver`는 신뢰된 외부 공급자용 내부 API이며 네트워크 인증 API가 아니다.

최소 메일박스는 `(수신 주소, 발신 주소, correlation)`마다 불변 메시지 하나를 저장한다. 동일 ID와 내용 재전달은 성공하고, 같은 슬롯의 다른 내용은 충돌이다. 요청마다 고유 correlation을 사용해야 한다. 여러 작업이 같은 슬롯을 기다리면 같은 메시지를 읽을 수 있으므로 전역 소비 큐나 임의 SMS 스트림으로 해석하지 않는다.

작업 파일 하나의 atomic rename으로 체크포인트·대기 조건·outbox를 함께 커밋한다. 이후 outbox를 메일박스에 전달하고 비운다. 일부 전달 후 중단되어도 재조회 시 멱등적으로 재전달한다. 대기 등록보다 메시지가 먼저 도착해도 조회 과정에서 찾는다. 재개 입력을 `ready` 기록에 저장하므로 깨우기 직후 재시작돼도 남는다. 메모리 알림에 의존하지 않는다.

현재는 단일 소유자 파일 저장소다. 작업 디렉터리를 32개 파일명씩 순회하고 실행 중인 최대 K개 레코드만 유지한다. 영속 대기열에는 역할별 goroutine이 없다. 완료 작업·메시지는 자동 삭제하지 않으므로 디스크 사용량과 조회 비용은 누적된다. 운영 규모에서는 인덱스·보존 정책·용량 제한이 필요하다.

상한: 발신 메시지 8개/턴, 메시지 body·체크포인트 각각 16KiB, 인코딩된 SMS 64KiB, 완료 텍스트 64KiB, 작업 레코드 512KiB. pi에 전달되는 전체 재개 입력은 기존 64KiB 입력 제한을 따른다. 이는 모델 토큰 예산이나 전체 RSS 상한이 아니다.

## Heartbeat와 실패

활성 워커에만 어댑터의 실제 RPC를 주기적으로 보내 성공한 경우 실행 임대를 갱신한다. pi는 `get_state`, Codex는 `thread/loaded/list`를 사용한다. 엔진 기본 주기는 1초, 응답 제한 2초, 임대 10초이며 Codex 데모는 각각 15초·10초·45초로 설정한다. 마지막 성공 시각은 작업 기록의 `lastHeartbeat`다. 작업 진척과는 다르다. 실행 단계의 경과시간 제한은 기본적으로 없으며 `StepTimeout > 0`을 명시할 때만 적용한다. 대기 중인 역할에는 heartbeat가 없다. [Codex 상태 관찰·SMS 계약](codex-cooperation.md)을 참고한다.

각 실행에 증가하는 generation을 부여한다. 완료와 임대 갱신은 현재 generation 및 만료 시각을 확인한다. 오래된 실행 결과는 거절한다. heartbeat 실패나 단계 시간 초과 시 실행을 취소하고 종료를 기다린 뒤 `uncertain`으로 남기고 역할 세션을 격리한다. 슬롯을 이전 실행이 끝나기 전에 재할당하지 않는다. 사용자 정의 StepExecutor도 취소와 Heartbeat timeout을 준수해야 한다.

런타임 재시작 시 `ready`·`waiting`은 복원하고, 남은 `running`은 세대를 올려 `uncertain`으로 격리한다. 자동 재실행이나 격리 해제 API는 제공하지 않는다. 운영자가 부작용과 프로세스 종료를 확인한 후 별도로 복구해야 한다. generation은 런타임의 결과·메시지 커밋을 보호하며 외부 도구 부작용을 되돌리지 않는다. 실행 중 강제 종료는 pi 프로세스 그룹을 이탈한 자식까지 보장하지 않는다.

이 단계는 단일 머신의 최소 협업 런타임이다. 분산 임대, 작업 진척 감지, 대기 timeout, 메시지 스트림, 운영자 복구 명령, DB 페이징은 후속 범위다.
