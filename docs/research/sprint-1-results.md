# Sprint 1 — 역할 전환 재사용 실험

상태: 기능 실험 완료, 후속 검증 범위 명시. 연구 범위는 역할 전환과 완료된 작업 상태의 복원이다.

## 환경과 고정 버전

| 대상 | 실험/검토 버전 | 소스 또는 무결성 근거 |
| --- | --- | --- |
| Go | 1.26.11, darwin/amd64 | 공식 archive SHA256 `394baae971332f2bc783d6e2004ae57101479bd3b06ed8209a3fe3570d4ce498` |
| Node / npm | 24.16.0 / 11.13.0 | 로컬 실행 환경 |
| pi coding agent | npm 1.5.2 | package-lock.json의 배포 integrity; 검토 소스 `1f6f319bb28992a40b99c147fffb23b65a23129d` |
| Eino | v0.9.23 | tag 및 Go module origin `35480319ee8f6f75f612b4dce0bd5f4747b37730` |
| Letta 안내 저장소 | main 소스 검토만 | `8816d3a8516893bfdc0fa14bfb1235226012e295` |
| Letta Code | manifest 0.34.63, 소스 검토만 | `d3f8093b4291f8f9c44cee730c87dd3b9c307cf1` |

pi npm metadata는 gitHead를 반환하지 않았다. npm 설치물은 lockfile integrity로, 소스 검토는 별도 commit SHA로 고정한다. 둘을 같은 커밋에서 빌드했다고 단정하지 않는다.

Go 1.27.4로도 첫 Eino 기능 실험은 통과했지만 Sonic v1.15.0이 Go 1.27에서 `encoding/json` fallback 경고를 출력했다. 비교 환경을 지원 범위에 맞추기 위해 Go 1.26.11을 채택했다. 시스템 도구체인을 수정하지 않고 프로젝트의 무시되는 `scratch/` 경로에 설치했다.

## 실제 검증 범위

외부 LLM 대신 합성 모델을 연결했지만 세션 전환, 이벤트 전달, 도구 실행, 저장과 복원은 실제 하네스를 사용했다. [공통 fixture](../../experiments/fixtures/role-switch.json)로 A → B → A, 프로젝트 간 격리, 프로세스 재시작을 검사한다.

| 항목 | pi 1.5.2 | Eino v0.9.23 |
| --- | --- | --- |
| 같은 프로세스의 A → B → A | 통과: RPC session 교체 | 통과: 같은 worker에서 Runner 재구성 |
| 같은 프로젝트 디렉터리에서 역할 교체 | 통과: 세션의 역할 참조로 지침 적용 | 통과: activation별 Instruction 적용 |
| 모델에 전달한 이전 역할 지침·대화 배제 | 통과 | 통과 |
| 모델에 노출되는 도구 목록 교체 | 명시적 확장 allowlist 적용 후 통과 | 실행별 ToolsConfig 적용 후 통과 |
| 실제 역할 도구 실행 | 통과 | 통과 |
| 프로젝트별 동일 역할 기억 분리 | 통과 | 통과 |
| 새 OS 프로세스에서 완료된 기록 복원 | pi session JSONL로 통과 | 실험 소유 JSON transcript로 통과 |
| 하네스 내부 실행 중 checkpoint 복구 | 미검증 | 미검증 |
| 메모리 상한 / 장시간 누수 / 비용 | 미측정 | 미측정 |

메시지·도구·프로젝트 경로의 기능적 격리를 검증했다. OS 보안 경계나 임의의 확장 상태까지 격리한다고 주장하지 않는다.

## 발견한 제약

### pi의 defaultTools는 확장 도구 allowlist가 아니다

첫 테스트에서 `defaultTools: ["dto_probe"]`를 설정했지만 `dto_probe`와 `qa_probe`가 모두 모델에 노출됐다. 이 동작을 별도 비교 테스트로 남겼다.

포크 없이 공개 확장 API로 해결했다.

1. 세션 custom entry에 역할 참조 보존.
2. `session_start`에서 해당 역할 참조 확인.
3. `setActiveTools([role.tool])`로 활성 도구 명시.
4. `before_agent_start`에서 해당 역할의 지침 적용.

고정된 프로젝트 설정 파일을 역할마다 덮어쓰지 않는다. 같은 프로젝트 디렉터리의 서로 다른 역할 세션이 각자 컨텍스트를 구성한다.

### Eino의 완료 상태 저장은 직접 구성했다

이 실험은 Runner 실행이 끝난 후 메시지 기록을 저장해 다음 activation에 주입한다. Eino의 interrupt/checkpoint/resume API는 소스에서 확인했지만 실행 중 복구 실험은 하지 않았다. 역할별 파일 경로와 도구의 작업 디렉터리도 어댑터가 관리한다.

### Letta의 현재 구현은 이전 V1 서버와 다르다

기존 `letta-ai/letta`의 main은 현재 구현을 `letta-ai/letta-code`로 안내하며 V1 서버를 archive로 분리했다. 따라서 이전의 DB 기반 V1 설명을 현재 구현의 독립 메모리 API로 가정할 수 없다.

현재 소스에서 Git 기반 MemFS, memory constraints, worktree 기반 작업 경로를 확인했다. `memory-worktree.ts`는 인증·backend proxy·operation 관리에 연결돼 있으므로, 독립 라이브러리처럼 바로 추출할 수 있다고 판단하지 않았다. 이번 스프린트에서 실행 의존성으로 추가하지 않았고, Sprint 4의 별도 메모리 실험 후보로 보존한다.

## 결정

검사 결과: Go formatting, `go vet ./...`, `go test -race -count=1 ./...` 통과. pi는 기존 도구 노출 동작을 확인하는 비교 테스트와 역할 격리·복원 테스트를 통과했다. Eino의 `TestProcessResume`는 독립 실행 시 skip하고 부모 테스트가 새 프로세스로 실행할 때 검증한다.

Sprint 2의 첫 어댑터는 **Go 런타임 + pi RPC**로 진행한다. 역할 교체와 저장된 세션 재개가 공개 확장 지점에서 동작했고, 코딩 하네스를 재작성하지 않는 방향에 맞는다. Eino는 Go 내부 실행의 비교군과 대체 후보로 유지한다. 성능 우위에 의한 결정은 아니다.

[ADR 0002](../decisions/0002-pi-first-adapter.md)에 선택 이유, 어댑터 계약과 재검토 조건을 기록했다.

## 재현과 남은 위험

[실험 실행 방법](../../experiments/README.md)을 따른다. 원시 개인 데이터나 API 자격 증명을 사용하지 않는다.

다음 스프린트에서는 Go RPC 어댑터, K=4 실행 제한, 취소 완료 처리, 역할 버전 검증, 큐·버퍼 상한을 검증한다. 현재 테스트가 증명한 것은 작은 합성 시나리오의 기능적 전환이다. 실제 LLM 성능, 장기 파이프라인 메모리 효율, 선택적 기억 로딩의 이점은 후속 실험 없이는 주장하지 않는다.

## 소스 근거

- [pi RPC 명령 타입](https://github.com/earendil-works/pi/blob/1f6f319bb28992a40b99c147fffb23b65a23129d/packages/coding-agent/src/modes/rpc/rpc-types.ts)
- [pi 세션 runtime 교체](https://github.com/earendil-works/pi/blob/1f6f319bb28992a40b99c147fffb23b65a23129d/packages/coding-agent/src/core/agent-session-runtime.ts)
- [pi SDK 옵션과 도구 선택](https://github.com/earendil-works/pi/blob/1f6f319bb28992a40b99c147fffb23b65a23129d/packages/coding-agent/src/core/sdk.ts)
- [Eino Runner](https://github.com/cloudwego/eino/blob/35480319ee8f6f75f612b4dce0bd5f4747b37730/adk/runner.go)
- [Letta 이전 서버 안내](https://github.com/letta-ai/letta/blob/8816d3a8516893bfdc0fa14bfb1235226012e295/README.md)
- [Letta Code README](https://github.com/letta-ai/letta-code/blob/d3f8093b4291f8f9c44cee730c87dd3b9c307cf1/README.md)
- [MemFS constraints](https://github.com/letta-ai/letta-code/blob/d3f8093b4291f8f9c44cee730c87dd3b9c307cf1/src/memory-constraints.ts)
- [MemFS worktree](https://github.com/letta-ai/letta-code/blob/d3f8093b4291f8f9c44cee730c87dd3b9c307cf1/src/agent/memory-worktree.ts)
