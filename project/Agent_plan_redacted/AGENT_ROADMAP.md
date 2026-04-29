# Agent Roadmap

이 문서는 Rule-Based 지표 산정 파이프라인 이후에 붙일 Agent 확장 계획입니다. 실제 제출 코드는 아직 Rule-Based 중심이며, Agent는 아래 순서로 단계적으로 붙이는 것을 목표로 합니다.

## Phase 1. 입력 계약 정리

- Rule-Based 결과를 `company`, `year`, `field_id`, `rule_value`, `evidence`, `confidence` 단위 record로 내보냅니다.
- Agent 입력은 전체 셀이 아니라 실패, 누락, 낮은 신뢰도, 출처 충돌 record로 제한합니다.
- 정답 파일을 Agent 입력으로 넣지 않습니다.

## Phase 2. 보완 수집 Agent

- 공식 원천, 문서, 표, 웹 원문 후보를 탐색합니다.
- 후보 원천마다 URL, 문서명, 발행 주체, 수집 시각, 관련 문장 또는 표 위치를 저장합니다.
- 후보값은 최종값이 아니라 `candidate`로만 저장합니다.

## Phase 3. 검증 Agent

- Rule-Based 값과 후보 근거를 비교해 `correct`, `incorrect`, `unknown`, `needs_source`로 판정합니다.
- `유/무`, `건수`, `합계` 계열은 기본값 placeholder를 자동 정답으로 보지 않습니다.
- 검증 불가 항목은 반드시 `unknown` 또는 `needs_source`로 남깁니다.

## Phase 4. 사람 검토 UI

- Agent 판정 결과를 workbook 주석, review table, dashboard 형태로 보여줍니다.
- 사람이 승인한 후보만 Rule-Based rule 또는 source adapter 개선 대상으로 보냅니다.
- Agent가 직접 최종 workbook을 덮어쓰지 않습니다.

## Phase 4-4. 리포트 작성 Agent

- 검증된 산출값과 근거만 사용해 기업별 또는 전체 요약 초안을 작성합니다.
- 모든 주요 문장에는 출처, 근거 record, confidence를 연결합니다.
- 불확실한 항목은 확정 표현 대신 검토 필요 항목으로 표시합니다.

## 운영 원칙

- Rule-Based first: 계산은 규칙 기반으로 유지합니다.
- Evidence first: 근거 없는 Agent 답변은 사용하지 않습니다.
- Human approval: 최종 반영은 사람 또는 결정적 검증기를 거칩니다.
- Traceable output: 후보값, 판정, 출처, 오류 유형을 모두 추적 가능하게 남깁니다.
