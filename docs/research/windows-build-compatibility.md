# Windows workflow 빌드 오류 수정

2026-11-20. Go 1.26.11, Windows/amd64.

`internal/control/server.go`가 `syscall.Flock`, `LOCK_EX`, `LOCK_NB`를 OS 구분 없이 참조해 workflow 컴파일이 실패했습니다. control owner 잠금을 OS별 파일로 분리했습니다. Linux/macOS의 기존 파일 잠금과 close 해제를 유지하며, 다른 OS에서는 명시적 미지원 오류를 반환합니다. state store의 지원 OS 정책은 변경하지 않았습니다.

검증 결과:

- `go build ./cmd/runtime-workflow`: Windows 빌드 통과.
- `go test ./internal/control -run '^TestUnsupportedOwnerLockFailsClosed$' -count=1`: 통과. 파일을 생성하거나 잠금 없이 성공하지 않음을 확인했습니다.
- 같은 검사에 `-race` 적용: 통과.
- `go vet ./internal/control ./cmd/runtime-workflow`: 통과.
- Linux/amd64, Darwin/amd64 control 테스트 교차 컴파일: 통과. 해당 OS에서 테스트를 실행한 것은 아닙니다.
- 생성된 바이너리를 fixture backend로 실행: 기존 `state store currently supports Linux/macOS only` 오류로 종료했습니다. 모델 호출은 없습니다.

Windows 빌드 복구와 Windows workflow 실행 지원은 별개입니다. Unix socket·파일 권한·영속 저장·Python orchestrator의 `fcntl`을 포함하는 전체 Windows 지원은 이번 수정에 포함하지 않습니다. Linux/macOS 잠금 배타성·해제 회귀 검사를 추가했으며, 해당 OS에서의 실행 검증은 남습니다.
