# 외부 구성요소 기록

이 문서는 첫 실험의 직접 의존성과 소스 검토 대상을 기록한다. 프로젝트 자체 라이선스는 아직 미정이다. 이 문서가 모든 전이 의존성의 라이선스 검토나 배포 고지를 대신하지는 않는다.

| 구성요소 | 버전 / 검토 commit | upstream 라이선스 | 사용 범위 |
| --- | --- | --- | --- |
| pi coding agent / pi-ai | npm 1.5.2; 소스 검토 `1f6f319bb28992a40b99c147fffb23b65a23129d` | MIT | 실험에서 설치 및 공개 SDK/RPC 사용; pi-schema는 전이 의존성 |
| codex-adapter | `2e2a0da8b056397e2a85eade967f2ea436ed188c` | MIT | 독립 Go API 및 JSON CLI. role-runtime의 기존 구현에서 분리; RPC·세션·heartbeat·계정 사전 검사. pi는 대체 경로로 유지 |
| Eino | v0.9.23 / `35480319ee8f6f75f612b4dce0bd5f4747b37730` | Apache-2.0 | Go 실험 모듈 의존성 |
| Letta 안내 저장소 | `8816d3a8516893bfdc0fa14bfb1235226012e295` | Apache-2.0 | README 및 라이선스 검토만 |
| Letta Code | manifest 0.34.63 / `d3f8093b4291f8f9c44cee730c87dd3b9c307cf1` | Apache-2.0 | 메모리 계층 소스 검토만; 실행 의존성 없음 |
| 설치된 Codex CLI | 0.166.0; 타겟 재심 진단에 설치된 0.169.1 추가 비교 | 해당 설치물의 라이선스 원문 재검토 미완료 | 로컬 app-server RPC와 생성 스키마 사용. 바이너리·리소스를 복사하거나 재배포하지 않음. 기존 pi 어댑터 유지 |

확인한 원문:

- [pi LICENSE](https://github.com/earendil-works/pi/blob/1f6f319bb28992a40b99c147fffb23b65a23129d/LICENSE)
- [Eino LICENSE-APACHE](https://github.com/cloudwego/eino/blob/35480319ee8f6f75f612b4dce0bd5f4747b37730/LICENSE-APACHE)
- [Letta LICENSE](https://github.com/letta-ai/letta/blob/8816d3a8516893bfdc0fa14bfb1235226012e295/LICENSE)
- [Letta Code LICENSE](https://github.com/letta-ai/letta-code/blob/d3f8093b4291f8f9c44cee730c87dd3b9c307cf1/LICENSE)

실험 코드는 공개 API를 호출하도록 작성했으며 upstream 구현 파일을 복사해 포함하지 않았다. 다운로드한 저장소, 도구체인, node_modules와 모듈 캐시는 커밋하지 않는다. 설치물은 package-lock.json과 go.sum으로 고정한다.

배포물을 만들 때는 포함되는 직접·전이 구성요소의 저작권/라이선스 원문과 NOTICE 유무를 버전별로 확인하고 필요한 고지를 함께 보존한다. 현재 단계에서는 배포용 번들·바이너리를 생성하거나 재배포하지 않았다.
