# Codex app-server 어댑터

설치된 codex-cli 0.166.0의 `app-server generate-json-schema` 출력으로 프로토콜을 확인했다. 기본 실행은 `codex app-server --stdio`이며, 별도 하네스를 재작성하지 않는다. 생성 스키마는 `scratch/codex-schema`에 두며 커밋하지 않는다.

## 경계

- `internal/codexrpc`: JSONL initialize/initialized, ID별 요청·응답, 서버 알림, 프로세스 그룹 종료·회수. 기본 레코드 1MiB, pending 8개, 알림 64개, stderr 16KiB 상한. 초과·프로토콜 오류·진행 중 요청 취소는 프로세스를 종료한다. 서버가 보낸 승인 요청은 자동 허용하지 않고 오류 응답한다.
- `internal/codexadapter`: 하나의 워커가 하나의 app-server를 재사용한다. 역할 키별 thread ID·workspace·지침 digest를 저장하고 start/resume한다. turn 시작 승인과 완료를 구분하며 동일 thread/turn의 완료만 수용한다.
- `engine.Store.ResolveDefinitions`: 프로젝트·역할·격리 상태만 확인한다. 기존 pi `Resolve`의 세션 처리는 유지하고, Codex가 pi JSONL 파일을 만들지 않게 분리했다.
- `cmd/codex-pilot`: 사업 안건과 단계 의존성을 공급하는 외부 드라이버다. 실행·역할 직렬화·큐 상한·task receipt는 실제 `engine.Pool`이 담당한다.

매 turn 전 active task intent를 원자적으로 저장한다. 성공 완료와 unsubscribe 후에만 지운다. 실패·취소·시작 응답 유실이나 재시작 시 남은 intent는 역할을 격리한다. 자동 재실행으로 부작용이 중복되지 않게 한다. 같은 Store의 동시 소유 방지는 기존 flock, 같은 역할 실행 직렬화는 Pool을 사용한다.

역할 thread는 작업이 끝나면 unsubscribe하며 `thread_unload_delay_secs=0`을 설정한다. unsubscribe 응답은 구독 해제를 뜻하며 RSS 즉시 회수나 모델 KV 캐시 이전을 입증하지 않는다. 영속 thread transcript는 Codex가 관리하며 DB 페이징은 없다.

## 첫 지원 범위

`Role.Tools=["advisory"]`만 받는다. 공급된 자료로 판단하는 사전 심의용이며 쓰기·외부 연락·도구 작업은 지침으로 금지한다. read-only sandbox 및 approvalPolicy=never, shell/apps/plugins/browser/computer/image 등 비활성화 설정, config/read에서 확인한 MCP 서버별 비활성화를 적용한다. 관찰된 외부 도구 실행 알림도 실패로 처리한다.

이는 모든 내장 도구에 대한 완전한 allowlist나 OS 보안 격리가 아니다. 이미 실행된 도구를 알림으로 탐지하는 것은 부작용을 되돌리지 못한다. 사용자 전역 설정·인증은 Codex가 읽는다. 설정 값·인증 원문을 보고서에 출력하지 않는다. 실제 개발용 쓰기 권한·도구 정책과 CooperativePool heartbeat/SMS 연결은 아직 지원하지 않는다.

`Usage.Scope=successful-turn-last-notification`은 성공한 turn의 마지막 토큰 알림 합계다. 완료된 turn 수와 측정된 turn 수를 함께 기록하며 실패 이후 Known=false를 유지한다. 실패·취소 비용, 모든 내부 모델 요청, 전체 thread 누적 비용을 완전히 계측한 것이 아니다. 원화·달러 비용 또는 토큰 절감률로 환산하지 않는다.

## 실행

```sh
./scripts/go.sh build -o bin/codex-pilot ./cmd/codex-pilot
./bin/codex-pilot --state artifacts/codex-pilot-new --workers 2 > artifacts/codex-pilot-new-report.json
```

새 state 디렉터리가 필요하다. 설치된 Codex의 로그인과 모델 연결을 사용하며 실제 모델 작업 7건을 실행한다. 기본 모델은 사용자 설치 설정을 따르고 명시적으로 변경하려면 `--model`을 지정한다. 원시 모델 출력·task 입력·thread 참조는 ignored artifacts와 Codex의 로컬 저장소에 남고 공개 커밋하지 않는다.

실행 순서: 고객·재무·운영 독립 검토 3건 → 모든 워커 및 Store 종료·재개 → 동일 역할 반론 검토 3건 → 의장 결정 1건. 검토 역할마다 서로 다른 난수 확인값을 첫 turn에만 주고 두 번째 입력에서는 제외한다. 재시작 후 같은 thread와 확인값이 복원되는지 드라이버가 검사한다. 다른 역할에 전달하는 의견에서는 확인값을 제거한다. 최대 워커는 2개, 마지막 심의는 1개다.

이는 외부에서 정한 단계별 작업열이다. 에이전트가 스스로 작업 DAG를 만들거나, 역할 40개를 모두 실행하거나, 역할별 서로 다른 모델을 사용한 실험은 아니다.
