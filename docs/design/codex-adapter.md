# Codex app-server 어댑터

설치된 codex-cli 0.166.0의 `app-server generate-json-schema` 출력으로 프로토콜을 확인했다. 기본 실행은 `codex app-server --stdio`이며, 별도 하네스를 재작성하지 않는다. 생성 스키마는 `scratch/codex-schema`에 두며 커밋하지 않는다.

독립 구현은 [codex-adapter](https://github.com/shakystar/codex-adapter)에 있다. Go API와 언어 독립 JSON CLI를 제공한다. 이 저장소의 `internal/codexadapter`는 역할 정의를 공개 Request로 변환하고 engine 격리 기록을 연결하는 얇은 바인딩이다. 로컬 replace 없이 게시된 커밋 2e2a0da를 go.mod에 고정했다.

계정 사전 검사 `account/read(refreshToken=false)`가 실패하면 모델 작업 전에 오류를 반환한다. RPC 원문 오류는 노출하지 않고 알려진 오류만 `workspace_routing_timeout` 등으로 분류한다. 계정 조회 성공은 모델 실행 성공을 보장하지 않는다.

## 경계

- 외부 `github.com/shakystar/codex-adapter/rpc`: JSONL initialize/initialized, ID별 요청·응답, 서버 알림, 프로세스 그룹 종료·회수. 기본 레코드 1MiB, pending 8개, 알림 64개, stderr 16KiB 상한. 초과·프로토콜 오류·진행 중 요청 취소는 프로세스를 종료한다. 서버가 보낸 승인 요청은 자동 허용하지 않고 오류 응답한다.
- 외부 `github.com/shakystar/codex-adapter`: 하나의 워커가 하나의 app-server를 재사용한다. 역할 키별 thread ID·workspace·지침 digest를 저장하고 start/resume한다. turn 시작 승인과 완료를 구분하며 동일 thread/turn의 완료만 수용한다.
- `engine.Store.ResolveDefinitions`: 프로젝트·역할·격리 상태만 확인한다. 기존 pi `Resolve`의 세션 처리는 유지하고, Codex가 pi JSONL 파일을 만들지 않게 분리했다.
- `cmd/codex-pilot`: 사업 안건과 단계 의존성을 공급하는 외부 드라이버다. 실행·역할 직렬화·큐 상한·task receipt는 실제 `engine.Pool`이 담당한다.

매 turn 전 active task intent를 원자적으로 저장한다. 성공 완료와 unsubscribe 후에만 지운다. 실패·취소·시작 응답 유실이나 재시작 시 남은 intent는 역할을 격리한다. 자동 재실행으로 부작용이 중복되지 않게 한다. 같은 Store의 동시 소유 방지는 기존 flock, 같은 역할 실행 직렬화는 Pool을 사용한다.

역할 thread는 작업이 끝나면 unsubscribe하며 `thread_unload_delay_secs=0`을 설정한다. unsubscribe 응답은 구독 해제를 뜻하며 RSS 즉시 회수나 모델 KV 캐시 이전을 입증하지 않는다. 영속 thread transcript는 Codex가 관리하며 DB 페이징은 없다.

## 첫 지원 범위

`Role.Tools=["advisory"]`는 공급된 자료로 판단하는 사전 심의용이다. 쓰기·외부 연락·도구 작업은 지침으로 금지한다. read-only sandbox 및 approvalPolicy=never, shell/apps/plugins/browser/computer/image 등 비활성화 설정, config/read에서 확인한 MCP 서버별 비활성화를 적용한다. 관찰된 외부 도구 실행 알림도 실패로 처리한다.

`Role.Tools=["web-research"]`는 웹 조사용이다. thread start/resume마다 `web_search=live`를 설정하고 `webSearch` 항목만 추가 허용한다. advisory는 같은 프로세스에서도 `web_search=disabled`를 명시한다. 두 프로필을 섞거나 다른 도구를 지정하면 거절한다. 읽은 외부 자료는 지침이 아닌 데이터로 취급하도록 하고, 로그인·게시·연락·파일 변경을 허용하지 않는다.

웹 완료 이벤트는 thread/turn에 맞춰 관찰하며 item ID로 중복 집계하지 않는다. 한 turn에서 최대 256개 ID만 보관한다. 상태 콜백에는 완료 수와 search/openPage/findInPage 종류별 수만 추가하고 검색어·URL·본문은 넣지 않는다. 검색 호출 수는 검색어 개수와 같지 않다. 완료 이벤트가 페이지 접근 성공 또는 인용 정확성을 보증하지는 않는다. 자료의 접근 수준과 근거 요약은 모델의 산출물에 따로 남긴다.

이는 모든 내장 도구에 대한 완전한 allowlist나 OS 보안 격리가 아니다. 이미 실행된 도구를 알림으로 탐지하는 것은 부작용을 되돌리지 못한다. 사용자 전역 설정·인증은 Codex가 읽는다. 설정 값·인증 원문을 보고서에 출력하지 않는다. 실제 개발용 쓰기 권한·도구 정책은 아직 지원하지 않는다. [heartbeat 관찰 및 CooperativePool SMS 연결](codex-cooperation.md)을 별도 추가했다.

`Usage.Scope=successful-turn-last-notification`은 성공한 turn의 마지막 토큰 알림 합계다. 완료된 turn 수와 측정된 turn 수를 함께 기록하며 실패 이후 Known=false를 유지한다. 실패·취소 비용, 모든 내부 모델 요청, 전체 thread 누적 비용을 완전히 계측한 것이 아니다. 원화·달러 비용 또는 토큰 절감률로 환산하지 않는다.

## 실행

```sh
./scripts/go.sh build -o bin/codex-pilot ./cmd/codex-pilot
./bin/codex-pilot --state artifacts/codex-pilot-new --workers 2 > artifacts/codex-pilot-new-report.json
```

새 안건은 `--proposal experiments/event-search/regional-agenda.md`로 공급한다. 입력 파일은 비어 있지 않은 16KiB 이하 문서로 제한한다. 안건의 출처 확인은 외부 작업 공급자가 담당하며 advisory 역할이 직접 웹을 조사했다고 주장하지 않는다. 새 state 실행은 이전 회의 thread를 이어 쓰는 것이 아니라 명시적인 이전 결정 요약을 공급한 새 회의다. 한 회의 안의 독립 검토→교차 검토에서는 역할 thread를 복원한다.

파일럿은 `turn/start.outputSchema`로 checkpoint·recommendation·risk 세 문자열만 허용하는 JSON 스키마를 전달한다. 사용자 안건이 길어지자 모델이 추가 설명 필드를 생성해 드라이버가 중단된 실제 실패를 계기로 보완했다. 어댑터의 `Config.OutputSchema`는 선택 사항이며 파일럿의 최종 파싱·필드 검사도 유지한다.

새 state 디렉터리가 필요하다. 설치된 Codex의 로그인과 모델 연결을 사용하며 실제 모델 작업 7건을 실행한다. 기본 모델은 사용자 설치 설정을 따르고 명시적으로 변경하려면 `--model`을 지정한다. 원시 모델 출력·task 입력·thread 참조는 ignored artifacts와 Codex의 로컬 저장소에 남고 공개 커밋하지 않는다.

실행 순서: 고객·재무·운영 독립 검토 3건 → 모든 워커 및 Store 종료·재개 → 동일 역할 반론 검토 3건 → 의장 결정 1건. 검토 역할마다 서로 다른 난수 확인값을 첫 turn에만 주고 두 번째 입력에서는 제외한다. 재시작 후 같은 thread와 확인값이 복원되는지 드라이버가 검사한다. 다른 역할에 전달하는 의견에서는 확인값을 제거한다. 최대 워커는 2개, 마지막 심의는 1개다.

이는 외부에서 정한 단계별 작업열이다. 에이전트가 스스로 작업 DAG를 만들거나, 역할 40개를 모두 실행하거나, 역할별 서로 다른 모델을 사용한 실험은 아니다.

## Failure responsibility contract — 2026-11-19

The reusable adapter now owns sanitized failure classification and bounded account
preflight recovery. Its default is up to three startup attempts under one startup
budget; only classified transient routing/rate-limit failures qualify. Turn submission
never automatically retries. Local inspection preserves uncertain/acknowledged/completed
turn evidence without claiming current remote state or clearing interrupted intents.

The runtime wrapper consumes `FailureOf` and does not quarantine a role session for
`not_started` errors. Unknown, started, completed-with-error and unclassified failures
remain conservative. Pool receipts retain generic `result.failure` fields: version,
stage, code, execution and retryable. The engine uses a small error interface and has
no dependency on Codex's implementation. Cancellation status and existing failed-task
receipt rules remain intact: retry eligibility does not delete a receipt or automatically
requeue work. Startup factory failures are returned to the caller; the lazy bridge
captures them as task failure metadata. Cooperative execution also receives metadata
through the wrapper's result.

This is minimum integration, not an automated task recovery API. The bridge v1 error
envelope is unchanged; rich failure evidence is in the local task receipt. A future
versioned orchestrator recovery interface can consume it without changing responsibility.

Validation: adapter synthetic stdio failures cover bounded preflight recovery, auth
failure, lost acknowledgment and cleanup loss. Runtime tests verify quarantine policy,
wrapped error extraction and durable failure metadata for all four states, with no
reexecution on repeated failed task submission. No upstream routing repair is claimed.
