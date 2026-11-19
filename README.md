# role-runtime

수십 개의 전문 역할 컨텍스트를 소수의 실행 워커에 교체 로드하는 런타임.

외부에서 **무엇을 할지**와 **어떤 역할로 실행할지**를 지정한다. 런타임은 해당 역할의 지침·도구·작업 상태를 불러와 실행하고, 재개 가능한 상태를 저장한 뒤 워커를 반환한다.

예를 들어 DTO, 컨트롤러 와이어링, DB 설계, 테스트 픽처, QA 역할 40개를 정의하더라도 실행 워커는 4개로 유지한다. 역할마다 프로세스를 상주시킬 필요 없이, 실행 시점에 필요한 컨텍스트만 구성하는 것이 목표다.

**현재 상태: Go RPC·제한된 워커 풀에 heartbeat, 영속 SMS, 대기·재개를 구현했다. 400역할 실행, 역할 간 요청·응답, 워커 수별 자원 사용량을 실제 pi와 합성 모델로 검증했다. 생산용 런타임은 아직 아니다.**

독립 [codex-adapter](https://github.com/shakystar/codex-adapter)의 Go API와 JSON CLI를 재사용한다. Codex app-server용 [Go 어댑터](docs/design/codex-adapter.md)로 역할 thread를 교체·복원한다. [heartbeat·문서 SMS 검증](docs/research/codex-heartbeat-sms-results.md)에서 실제 워커 1개로 문서 대기·작성·전달·검토 재개를 통과했다. 임의 모델 턴 제한 대신 응답성·진행·백엔드 재시도를 관찰한다. 쓰기 도구와 자원 효율 검증은 후속 범위다.

[실제 실행 결과](docs/research/codex-pilot-results.md): 최종 파일럿 7/7 작업 성공, 최대 2워커, 재시작 후 역할 복원 3/3. 중간 시간 초과 실행도 함께 기록했다.

[6역할·2워커 복구 검증](experiments/event-search/target-review-team.md): 2026-11-19 실제 모델로 6역할 완료, SMS 대기·재개 5회, 원문 참조 보존을 통과했다. 로컬 lease는 단조 시계로 만료를 판정하며, macOS 장시간 검증에는 프로세스 단위 `caffeinate -i`를 사용했다. 이전 실패와 검증 한계도 함께 기록했다.

## 실행 모델

```text
외부 작업 공급자 — 작업·역할 지정
             │
             ▼
       제한된 작업 접수 / 실행 슬롯 관리
             │
       ┌─────┼─────┐
     워커 1 워커 2 워커 3 … K개
       └─────┼─────┘
             ↕
   역할 정의 / 프로젝트별 상태 / 산출물 저장소
```

워커의 역할은 고정되지 않는다. 같은 워커가 DTO → QA → DTO 순서로 실행될 수 있으며, 다시 DTO로 돌아왔을 때 해당 프로젝트의 저장된 상태를 복원한다. CPU의 컨텍스트 스위칭은 설계 비유다. 모델 내부 상태나 KV 캐시를 그대로 이동한다는 의미는 아니다.

## 검증할 가설

- 역할과 파이프라인 단계 수가 늘어도 활성 작업 수는 설정한 K 이하로 유지할 수 있다.
- 역할 전환 시 이전 역할의 지침·도구·기억이 다음 역할에 섞이지 않는다.
- 필요한 상태를 저장·복원하면 전체 대화를 상주시킬 필요가 줄어든다.
- 기존 하네스의 제한된 동시 실행보다 유의미한 이점을 제공할 수 있다.

역할 400개와 워커 1·2·4·8·12개의 합성 실행에서 프로세스 트리 RSS·CPU·처리량을 측정했다. [결과와 적용 범위](docs/research/resource-results.md)를 참고한다. 실제 모델의 토큰 절감과 역할별 상주 프로세스 대비 절감률은 검증하지 않았다.

## 기술 방향

핵심 런타임은 Go로 진행하며, 첫 백엔드는 **pi RPC + 역할 확장**을 사용한다. 하네스 전체를 재작성하지 않는다. [선택 근거](docs/decisions/0002-pi-first-adapter.md)는 기능 검증에 기반하며 성능 우위를 의미하지 않는다.

- **pi 1.5.2:** 실제 RPC에서 A → B → A, 동일 프로젝트의 역할·도구 격리, 재시작 후 복원 통과. Go RPC 어댑터와 역할 전환 확장을 구현했다.
- **Eino v0.9.23:** 같은 합성 fixture 통과. Go 내부 실행 비교군과 대체 후보로 유지한다.
- **MemGPT / Letta:** 현재 구현이 Letta Code로 이동했음을 확인했다. 기억 계층의 독립적 재사용은 후속 실험으로 남긴다.

pi의 `defaultTools`만으로는 확장 도구가 제한되지 않았다. 세션 활성화 시 명시적인 도구 allowlist를 적용하는 확장으로 해결했고, 기존 동작도 비교 테스트에 남겼다.

## 문서

실제 실험은 [사업 조직 운영](experiments/event-search/README.md)이다. 40개 전문 역할의 절차를 설계하고 협업 에이전트로 G0·G1 준비를 진행한 뒤, Welcome to Korea 안건을 Codex 어댑터와 실제 Pool로 실행했다. 전체 40개 역할 실행과 실제 고객 조사는 아직 진행하지 않았다.

| 문서 | 내용 |
| --- | --- |
| [요구사항](docs/requirements.md) | 문제 정의, 용어, 범위, 수용 기준 |
| [재사용 조사](docs/research/reuse-candidates.md) | 확인된 근거, 미확인 항목, 후보별 실험 |
| [설계 초안](docs/design/runtime.md) | 역할·작업·워커 분리와 전환 절차 |
| [검증 계획](docs/validation.md) | 비교군, 실험 조건, 측정 지표, 실패 기준 |
| [개발 계획](docs/roadmap.md) | 스프린트별 산출물과 종료 기준 |
| [기술 결정 0001](docs/decisions/0001-go-runtime-and-harness-reuse.md) | Go 우선 선택과 하네스 재사용 전략 |
| [Sprint 1 결과](docs/research/sprint-1-results.md) | 고정 버전, 실행 근거, 발견한 제약 |
| [기술 결정 0002](docs/decisions/0002-pi-first-adapter.md) | pi 첫 어댑터 선택과 최소 계약 |
| [외부 구성요소](THIRD_PARTY.md) | 라이선스 원문과 사용 경계 |

## 개발 상태

Go 1.26.11과 Node 24.16.0에서 검증했다. 자원 계측과 전체 검사에는 Python 3 표준 라이브러리도 사용한다. 외부 LLM 키 없이 실제 하네스에 합성 공급자를 연결해 검사한다. 설치·실행 상세는 [실험 안내](experiments/README.md)를 따른다.

```sh
./scripts/go.sh mod download
npm --prefix experiments/pi ci --ignore-scripts --no-audit --no-fund
./scripts/check.sh
```

핵심 코드는 `internal/`, CLI는 `cmd/role-runtime/`, pi 역할 확장은 `adapters/pi/`에 있다.

```sh
./scripts/go.sh run ./cmd/role-runtime demo --roles 40 --workers 4 --rounds 2 --queue 8
./scripts/go.sh test -race -tags integration -count=1 ./internal/pispike
```

데모는 실제 pi에 합성 모델을 연결한다. `--fresh`는 작업마다 프로세스를 생성하는 비교군이다. `--state`를 지정하면 기록을 보존하며 같은 작업 ID의 재실행은 거절한다.

[Sprint 2 결과](docs/research/sprint-2-results.md)에 검증 범위와 한계를 기록했다. [협업 실행 계약](docs/design/cooperation.md)과 [heartbeat·SMS 검증 결과](docs/research/cooperation-results.md)를 추가했다. [DB 기반 메모리 페이징](docs/design/paging-backlog.md)은 후속 단계이며 메모리·토큰 절감은 아직 검증하지 않았다.

협업 데모와 전체 통합 검사는 다음과 같이 실행한다.

```sh
./scripts/go.sh run ./cmd/role-runtime cooperate-demo
ROLE_RUNTIME_INTEGRATION=1 ./scripts/check.sh
```

SMS는 역할 주소와 correlation별 영속 메시지다. 대기는 모델 턴을 끝내고 체크포인트를 저장하는 협력적 방식이다. 장애 난 실행은 자동 재시도하지 않고 격리한다.

경주 공식 자료를 공급하는 [발견 → 검증 → 심의 실험](experiments/event-search/gyeongju-evidence-pipeline.md)은 세 역할의 문서 대기와 두 번의 SMS 전달을 연결한다. 웹 수집은 외부 공급자가 담당하며, 역할은 제공된 근거를 검토한다.

후속 [커뮤니티 → 데이터 조사 → 심의](experiments/event-search/community-data-pipeline.md)는 `web-research` 역할이 직접 검색한다. 실제 3역할·1워커·두 SMS 전달을 통과했다. 커뮤니티와 공식 데이터 설명을 조사했으며 실제 데이터 행 수집은 아직 완료하지 못했다.

동시 워커는 `--workers`로 지정한다. 이 장비의 짧은 합성 작업에서는 2개, 도구 대기 250ms 조건에서는 4개가 효율적인 후보였다. 실제 작업 특성에 따라 다시 측정해야 한다. 자원 예산과 추천 계산은 [자원 설정 설계](docs/design/resource-sizing.md)에 설명했다.

```sh
./scripts/go.sh build -o bin/role-runtime ./cmd/role-runtime
python3 scripts/benchmark.py --plan matrix --memory-budget-mib 4096 --cpu-budget-cores 6
```

프로젝트 라이선스는 미정이다. 외부 코드를 복사하거나 배포하기 전에 대상 버전의 라이선스와 고지 의무를 검토한다.
