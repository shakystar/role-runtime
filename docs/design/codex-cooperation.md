# Codex의 heartbeat와 문서 완료 SMS

## 시간 제한 대신 상태 관찰

모델 턴과 CooperativePool step은 기본적으로 경과시간 제한을 두지 않는다. `TaskTimeout`/`StepTimeout` 또는 CLI `--timeout`에 명시적인 양수를 지정할 때만 기한을 적용한다. 사용자 취소·실제 프로세스 종료·프로토콜 오류는 계속 처리한다.

Codex 어댑터는 실행 중 15초마다 `thread/loaded/list`를 호출한다. 10초는 모델의 사고 시간 제한이 아니라 로컬 제어 RPC의 응답 기한이다. heartbeat 실패는 연결/프로세스 응답성 실패로 구분하며 모델의 사업 판단이 실패했다고 해석하지 않는다. 초기화·thread 활성화·turn 접수·unsubscribe에도 별도 제어 응답 기한이 있다.

`OnStatus`는 역할·작업·프로세스·실행 단계·경과시간·마지막 알림·마지막 진행 이벤트·마지막 heartbeat·thread 상태와 대기 사유를 제공한다. 모델 본문·추론 원문·인증 정보는 포함하지 않는다. 진행 이벤트 없이 heartbeat만 응답하면 응답 대기가 길어지는 상태로 관찰할 뿐 자동 취소·재실행하지 않는다. heartbeat가 정상이어도 모델 추론이 유의미하게 진행 중이라는 보장은 없다.

백엔드 `error` 알림은 알려진 오류 코드·HTTP 상태·재시도 여부·횟수·마지막 발생 시각만 노출한다. 이는 마지막 오류 이벤트의 기록이며 이후 heartbeat 성공으로 지우지 않는다. 원문 오류 메시지나 상세 설명은 노출하지 않는다. backend retry 여부와 로컬 RPC 응답성을 구분하며, Codex가 `turn/completed`의 failed 상태를 보내면 불확실 실행으로 종료한다. 런타임이 이를 자동 재시도하지 않는다.

정상 turn 완료 시 진행 중 heartbeat 응답을 기다린 후 슬롯을 반환한다. probe를 종료시키려고 context를 취소하면 공유 RPC 프로세스를 죽일 수 있으므로 stop과 cancel을 구별한다. CooperativePool의 외부 heartbeat는 실제 Codex 응답을 확인한 뒤 실행 lease를 갱신한다.

## 문서 의존성 협업

1. reviewer가 문서가 없음을 확인하고 발신자·버전·correlation을 지정해 wait와 체크포인트를 반환한다.
2. 모델 턴이 끝나고 런타임이 wait를 영속 저장한다. 대기 역할은 워커를 점유하지 않는다.
3. 외부 작업 공급자가 planner 작업을 접수한다. 같은 워커가 planner 컨텍스트로 문서를 작성한다.
4. planner는 문서와 완료 메시지를 Transition으로 제출한다. engine은 checkpoint/outbox를 저장하고 발신자·주소를 검증한 뒤 영속 mailbox로 전달한다.
5. 정확한 메시지를 기다리던 reviewer가 ready가 된다. 저장된 체크포인트와 inbox를 받아 검토를 이어간다.

이번 최소 버전은 짧은 문서 전체를 버전 고정 SMS body에 담는다. 따라서 문서 산출물은 영속 mailbox 본문이다. 별도 파일에 먼저 문서를 저장했다고 주장하지 않는다. 큰 문서에는 별도 artifact 저장소의 불변 ID·digest를 메시지에 싣고, artifact 저장 후 outbox 공개 순서를 보장하는 추가 구현이 필요하다.

`internal/codexcooperation`은 Execution을 JSON으로 전달하고 Transition 구조화 출력을 받아 기존 engine 검증·커밋 경로에 넘긴다. 모델이 문서를 기다리며 장시간 열린 turn을 유지하는 방식은 아니다. 도구 실행 없이도 역할이 반환하는 제어 메시지로 협력한다.

## 실제 모델 실행 명령

```sh
./scripts/go.sh build -o bin/codex-collaborate ./cmd/codex-collaborate
./bin/codex-collaborate --state artifacts/codex-sms-new > artifacts/codex-sms-report.json 2> artifacts/codex-sms-monitor.log
```

state는 새 경로여야 하며 부모 디렉터리는 존재해야 한다. 기본 워커 1개, 역할 2개, 예상 step 3개다. 문서가 없는 reviewer를 먼저 실행하고 실제 waiting 기록 이후에만 planner를 접수한다. 모델이 작성한 문서의 지역·첫 고객 과제·고유 운영 규칙이 재개한 검토 결과에 정확히 포함되는지 검사한다. 문서 내용은 사전 고정하지 않는다.

명시적으로 `--structured-output=false`를 지정하면 backend 출력 스키마 대신 프롬프트 JSON 모드를 사용한다. 두 모드 모두 호스트의 엄격한 Transition 검사와 engine의 발신자·주소·내용 제한을 유지한다. 자동 모드 전환은 없다. backend 스키마 유무에 따른 단일 대조 성공만으로 연결 실패의 원인을 단정하지 않는다.

현재 도구 정책은 advisory다. 실제 파일 쓰기·외부 연락·광고 집행은 수행하지 않는다. 프로세스 재시작 SMS 복구는 기존 engine/pi의 별도 검증이 있으며, 이 Codex 명령의 기본 실행은 한 프로세스의 대기·교체·재개 검증이다.

## 시스템 시각 변경과 lease

활성 Store는 최대 64개 실행의 lease 만료를 단조 시계로 판단한다. JSON deadline/lastHeartbeat는 벽시계 감사 기록이며 실행 만료의 권위 있는 시계가 아니다. 벽시계가 앞으로 이동해도 현재 프로세스의 정상 heartbeat를 만료시키지 않는다. 실제 TTL 만료·세대 변경은 계속 거절한다. 완료·대기·격리 시 활성 lease를 해제하며, 재시작은 이 메모리 상태를 복원하지 않고 실행 중이던 작업을 불확정 격리한다. 이는 로컬 단일 소유자 저장소의 계약이며 분산 lease 구현이 아니다.
