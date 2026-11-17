# Codex heartbeat·문서 SMS 실제 검증

2026-11-18. 실제 로컬 Codex와 기존 Go CooperativePool을 사용했다. [비식별 지표](codex-heartbeat-sms-metrics.json)와 [실행 계약](../design/codex-cooperation.md)을 참고한다. 모델 본문과 세션 원문은 ignored artifacts에만 보존한다.

## 최종 결과

SMS-03: **33.40초, 워커 1개, 프로세스 시작 1회, 3 steps, 2 completed, 1 suspended, 1 resumed, maxActive 1, 실행 lease heartbeat 3회.** 모델 토큰 알림은 성공 turn의 마지막 알림 합계이며 전체 비용을 의미하지 않는다.

1. reviewer를 먼저 실행했다. 문서가 없으므로 실제 모델이 wait와 체크포인트를 반환했고 waiting 기록을 확인했다.
2. 그 이후에만 planner를 접수했다. 같은 워커와 프로세스가 다른 역할 thread에서 짧은 지역 제안 문서를 생성했다.
3. 생성 문서 JSON 전체를 버전·발신자·수신자·correlation이 있는 영속 SMS로 전달했다.
4. reviewer가 generation 2로 재개했다. 문서의 지역·첫 고객 과제·모델이 새로 작성한 고유 운영 규칙 3개를 실제 검토에 정확히 반영했다.
5. 영속 mailbox와 완료된 리뷰를 대조했고 CLI 전체 결과가 completed=true였다.

대기 모델 turn을 열린 채 유지한 것이 아니다. turn을 끝내고 checkpoint를 저장해 슬롯을 반환했다. 이번 문서 저장소는 SMS body이며 별도 파일 저장 후 알림을 보낸 구조라고 주장하지 않는다.

## 감시로 확인한 지연과 장애

이전 2분 제한은 초기 연결 실험용으로 임의 설정했던 값이었다. 모델/step 전체의 기본 기한을 제거하고, 로컬 RPC 응답성·실제 진행 알림·thread 상태·백엔드 재시도 코드를 분리했다.

진단 실행은 **185.16초 후 정상 완료**했다. 첫 출력 진행 이벤트는 약 170.02초, 관찰된 heartbeat는 11회, `responseStreamDisconnected` 오류 알림은 7회였다. heartbeat가 살아 있어도 모델 스트림에 장애가 있을 수 있었고, 백엔드 재시도 뒤 출력이 회복되었다. 임의 2분 기한이면 이 완료를 기다리지 못했을 것이다.

backend 출력 스키마를 끈 단일 대조 실행도 60.68초에 성공했다. 시간과 서비스 상태가 통제된 반복 실험이 아니므로 속도 우위나 구조화 출력이 장애 원인이라고 결론내리지 않는다. 최종 SMS-03은 backend schema를 켠 상태에서 성공했다.

## 실패 기록과 수정

- SMS-01: 로컬 heartbeat는 지속 성공했으나 Codex가 turn failed를 반환했다. 219.05초, completed/suspended/resumed는 모두 0이다. 작업을 격리했고 성공으로 재분류하지 않았다. 이 이전 바이너리는 백엔드 오류 코드 감시를 추가하기 전이므로 오류 코드 0회라고 해석하지 않는다.
- SMS-02: 실제 3 steps·2 completed·1 suspended·1 resumed와 문서 반영은 성공했다. 하지만 CLI가 완료 receipt에도 Inbox가 남아 있어야 한다고 잘못 검사해 completed=false를 반환했다. 엔진은 완료 시 임시 Inbox를 지우며 영속 mailbox는 남긴다. 검증기를 주소·correlation으로 mailbox를 읽고 리뷰와 대조하도록 수정했다. 기존 결과를 덮어쓰지 않고 별도 재검증 증거와 SMS-03 성공을 남겼다.
- 상태 감시 초기에 서로 다른 알림의 status 형상을 하나로 파싱하는 오류를 실제 실행에서 발견했다. 지원 메서드를 구분하고 status 알림에서만 해당 형상을 해석하도록 고쳤으며 회귀검사를 추가했다.
- 지역 심의의 첫 실행에서는 모델이 응답 필드를 추가해 호스트 JSON 검사에 실패했다. 출력 스키마 전달을 추가했고, 이후 백엔드 연결 장애·알림 파싱 오류를 별도 기록했다. 최종 [지역 심의](../../experiments/event-search/regional-decision.md)는 7/7 성공했다.

## 검증 범위

포맷·vet·Go race·Python·pi 기존 검사 통과. 추가 fake 검사에는 heartbeat 중 무출력 대기, 완료와 probe 경합, 알림 drain, 실제 프로세스 장애·사용자 취소, 오류 원문 비노출, 스키마 전달, SMS envelope·shape·크기 제한, step 기본 deadline 부재가 포함된다. 최종 CLI 변경은 컴파일·vet와 실제 SMS-03으로 확인했다.

아직 검증하지 않은 것은 큰 문서 artifact 저장·참조의 원자성, Codex SMS의 프로세스 재시작 복구 통합 실험, 40역할 실제 조직, 운영 중 자동 작업 분해, 대규모 자원 효율 및 실제 사업 수요다. 기존 engine/pi의 재시작 검증을 Codex SMS 재시작 검증으로 대신하지 않는다.
