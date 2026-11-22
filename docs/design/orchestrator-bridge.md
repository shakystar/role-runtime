# 실험용 LangGraph 브리지

2026-11-19에 `cmd/runtime-bridge`가 독립 Python [role-orchestrator](https://github.com/shakystar/role-orchestrator)에 순차 JSONL 인터페이스를 제공하도록 구현했다. 정식 채택이나 공개 API 안정성을 보장하기 전의 파일럿이다.

외부 그래프가 제안·검토·수정 결정과 SQLite 체크포인트를 관리한다. 브리지는 변경할 수 없는 역할·프로젝트 정의를 등록하고 워커 한 개, 큐 슬롯 한 개로 `engine.Pool`에서 작업을 실행한다. 브리지가 소유한 실행기 하나를 여러 제출에서 빌려 쓰고 종료 시 닫는다. `--backend fixture`는 지정한 합성 응답을 반환한다. `--backend codex`는 기존 codex-adapter와 자문 도구를 사용하며 heartbeat와 상태를 보고한다. 핵심 엔진 변경은 없었다.

같은 작업 ID로 다시 요청하면 입력과 역할 정의가 일치하는 완료 영수증을 재사용한다. 입력이나 역할 정의가 바뀌거나 기존 영수증이 실패 또는 손상 상태면 요청을 거부한다. 실행 여부가 불확실한 모델 작업은 자동 재실행하지 않는다. 저장소 간 트랜잭션과 외부 부작용의 정확히 한 번 실행은 보장하지 않는다.

```sh
./scripts/go.sh build -o bin/runtime-bridge ./cmd/runtime-bridge
./bin/runtime-bridge --state artifacts/bridge-new --backend fixture
```

기본 프로토콜은 **bridge v1**이다. 줄마다 정수 `schema_version: 1`과 문자열 `run_id`, `attempt_id`, `traceparent`, `id`, `role`, `instruction`, `input`을 담은 JSON 객체 하나를 전송한다. 미등록·중복·누락 필드와 대소문자가 틀린 필드를 거부한다. 버전 없는 요청에는 `--allow-legacy`와 별도 상태 디렉터리가 필요하다. 합성 입력은 응답 객체 `fixture`를 담은 JSON 문자열이다. 응답에는 스키마·실행·시도·작업 식별값과 `status`, `text`, `cached`, `stats`가 있으며, 민감한 정보를 제거한 `error`/`error_code`를 선택적으로 포함한다. 한 줄은 128 KiB, 작업 입력은 64 KiB, 실행기 출력은 256 KiB까지다. 상태는 stderr에, 응답만 stdout에 쓴다. 전체 프로토콜과 Python 명령은 role-orchestrator에 있다.

Go fmt/vet와 `go test -race ./...`를 통과했다. 브리지 검사에서는 저장소를 다시 연 후 성공 영수증 재사용, 입력·역할 충돌, 불확실하거나 손상된 영수증 거부를 확인했다. Python 검사에서는 실제 브리지 재시작·재개, 캐시 사용, 수정 없는 수락, 미해결 검토 횟수 제한, 예산 소진을 확인했다. 실제 Codex는 Python을 두 번 실행하는 동안 워커 한 개에서 3개 역할의 작업 4건을 완료했고 수정은 한 번이었다. 성공 영수증을 다시 조회할 때 모델은 실행되지 않았다. 자료는 합성 데이터였다. 여행 수요와 범용 에이전트 자율성, SMS로 작업 재개, 병렬 워커, 동적 그래프, 운영 환경 복구는 이 파일럿의 검증 범위 밖이다.

실제 영수증 재조회 중 어댑터가 캐시된 작업에도 Codex를 즉시 기동해 제한된 환경에서 `codex RPC stdout closed`로 실패했다. 이후 백엔드를 실제 실행 시점에 초기화하도록 변경했다. 회귀 검사에서 캐시된 영수증은 실행기 생성 함수를 호출하지 않음을 확인했고, 존재하지 않는 Codex 실행 파일을 지정해도 실제 완료 출력을 다시 읽었다. 처음 실패한 검사는 성공에 포함하지 않았다.

## 버전 계약과 프로세스 간 추적

Python 소유 프로세스는 실행·추적 식별자를 보존하고 클라이언트가 요청을 전달할 때마다 새 스팬을 만든다. Go는 W3C 컨텍스트를 읽어 `runtime.request`를 만들고 실행기를 실제로 호출할 때만 `runtime.execute`를 만든다. 캐시 조회에는 실행 스팬이 없다. 프로토콜·실행·추적 식별자의 연결과 작업별 시도 의도를 영속 저장한다. 의도만 남았거나 영수증이 실패 상태면 후속 실행을 차단한다. 자동 재시도와 저장소 간 트랜잭션은 없다.

로컬 로그는 메타데이터만 보관하며 한도는 8 MiB다. `ROLE_TRACE_ENDPOINT`(루프백 HTTP 전용)와 `ROLE_TRACE_AUTH`로 OTLP 내보내기를 선택할 수 있다. Python은 두 값을 명령 인수에 넣지 않고 자식 프로세스 환경으로 전달한다. 내보내기에 실패해도 작업 영수증은 유지된다. 큐 한도로 관측 데이터가 유실될 수 있고, 영속 전송·재전송은 보장하지 않는다.

새 버전의 첫 실모델 점검은 계정 사전 확인 중 `workspace_routing_timeout`으로 실패했다. 이 과정에서 생성에 실패한 실행기가 Go 인터페이스에 구체 타입을 가진 nil 값으로 남는 문제를 발견했다. 이제 초기화에 성공한 실행기만 저장하며 회귀 검사로 안전한 종료를 확인했다. 상위 서비스 라우팅 장애는 별개다. 전체 통신·소유권 규칙은 role-orchestrator의 `docs/protocol.md`, 양쪽에서 실행하는 공통 입력 벡터는 `cmd/runtime-bridge/testdata/v1-vectors.json`과 Python 측 대응 파일에 있다.
