# Sprint 1 실험

실제 pi/Eino 하네스에 결정적인 합성 모델과 무해한 테스트 도구를 연결한다. 유료 API 호출이나 모델 키 없이 **실제로 모델 경계에 전달되는 지침·도구·기록**을 검사한다. 모델의 추론 품질이나 작업 성공률을 측정하는 실험은 아니다.

## 준비

- Go **1.26.11** (Eino의 Sonic v1.15.0 지원 범위에 맞춤).
- Node.js **24.16.0**에서 검증. pi 패키지 요구사항은 Node >=22.19.0.
- npm **11.13.0**에서 검증.
- 최초 의존성 설치에만 네트워크가 필요하다.

저장소 루트에서:

```sh
./scripts/go.sh mod download
npm --prefix experiments/pi ci --ignore-scripts --no-audit --no-fund
./scripts/check.sh
```

`scripts/go.sh`는 PATH의 Go 또는 `ROLE_RUNTIME_GO`로 지정한 바이너리를 사용한다. 프로젝트 내부 `scratch/toolchain/go/bin/go`도 지원한다. Go 도구체인 자체는 커밋하지 않는다. 캐시는 `scratch/` 아래에 두고 별도 모듈 경계로 `go test ./...` 탐색에서 제외한다.

개별 실행:

```sh
./scripts/go.sh test -v -count=1 ./experiments/eino
npm --prefix experiments/pi test
```

## 공통 시나리오

[fixtures/role-switch.json](fixtures/role-switch.json)을 두 구현이 함께 사용한다.

1. 프로젝트 one의 DTO 역할 실행.
2. **같은 프로젝트 디렉터리**에서 QA 역할로 교체. DTO 기록·지침·도구가 노출되지 않아야 함.
3. DTO로 복귀. DTO의 이전 기록은 복원되고 QA 기록은 포함되지 않아야 함.
4. 프로젝트 two의 DTO 실행. 같은 역할이라도 프로젝트 one의 기록은 없어야 함.
5. **새 OS 프로세스**에서 프로젝트 one의 DTO 복원. 첫 번째·세 번째 작업 기록이 복원돼야 함.

각 단계에서 모델 → 테스트 도구 → 모델 순환을 실제 하네스가 수행한다. 도구 목록과 실행된 도구, 작업 디렉터리도 검사한다. 원시 캡처와 세션은 임시 디렉터리에 저장하고 테스트가 끝나면 정리한다.

## pi

[pi/spike.test.mjs](pi/spike.test.mjs)는 실제 설치된 pi CLI를 RPC 모드로 실행한다. 현재 드라이버는 실험용 Node 코드다. **Go RPC 어댑터 구현은 Sprint 2 범위**다.

- `switch_session`으로 같은 프로세스의 활성 세션을 교체한다.
- 세션 custom entry에 프로젝트·역할 참조를 넣고 확장이 활성화 시 역할 지침과 도구를 적용한다.
- `prompt` 수락과 완료를 구별하고 `agent_settled`까지 기다린다.
- `defaultTools`만으로 확장 도구가 제한되지 않는 비교 테스트를 포함한다. 이 테스트의 PASS는 양쪽 도구가 노출된다는 기존 동작의 재현이다.
- 역할별 allowlist를 `setActiveTools`로 적용한 경우에만 격리 테스트를 통과한다.
- CLI의 자동 네트워크 활동과 프로젝트 밖 리소스 발견을 끄고 합성 공급자만 사용한다.

세션별 지침을 적용하는 확장도 테스트 코드이며, 저장된 역할 정보는 신뢰된 합성 입력이다. 제품에서는 역할 참조의 검증·버전 고정과 프로젝트 필수 규칙을 포함한 컨텍스트 조립이 추가로 필요하다.

## Eino

[eino/spike_test.go](eino/spike_test.go)는 실제 `ChatModelAgent`와 `Runner`, 도구 루프를 실행한다. 같은 Go worker 객체가 매번 역할 정의와 디스크 기록으로 실행 객체를 재구성한다.

- 모델 경계에서 메시지와 실제 tool 옵션을 캡처한다.
- 도구는 프로젝트 경로에 묶여 합성 파일을 읽는다. 작업 디렉터리 적용은 하네스가 아닌 어댑터의 책임이다.
- 완료된 turn의 기록을 실험 코드가 JSON으로 저장한다.
- 새 테스트 프로세스로 같은 저장 파일을 읽어 복원한다.
- 손상된 저장 파일을 조용히 무시하지 않고 실패하는지 검사한다.

**Eino의 실행 중 interrupt/checkpoint/resume를 검증한 것은 아니다.** 현재 방식은 완료 경계의 transcript 저장·재구성이다.

## 범위 밖

RAM 절감률, GC 후 실제 메모리 회수, 장시간 캐시 증가, 취소 중인 외부 도구 종료, 악의적 도구 호출, 장애 도중 exactly-once, 토큰 예산, 전체 대화 페이징, 유료 모델 품질은 아직 검증하지 않았다. 두 후보의 테스트 실행 시간을 성능 비교 수치로 사용하지 않는다.

## 자원 계측

`scripts/benchmark.py`는 Python 3 표준 라이브러리로 실제 Go+pi 프로세스 트리를 측정한다. 별도 모델 키는 필요하지 않다. 프로세스 조회가 가능한 macOS/Linux에서 실행하며 측정 중 다른 테스트를 동시에 실행하지 않는다.

```sh
./scripts/go.sh build -o bin/role-runtime ./cmd/role-runtime
python3 scripts/benchmark.py --plan matrix
python3 scripts/benchmark.py --plan extended --output artifacts/resource-extended.json
```

데모의 `--payload-bytes`는 합성 입력 크기, `--tool-delay-ms`는 합성 도구 대기 시간을 바꾼다. 결과 필드와 한계는 [자원 설정 설계](../docs/design/resource-sizing.md), 측정 결과는 [보고서](../docs/research/resource-results.md)를 따른다. 자동 워커 확장·축소는 구현하지 않았다.
