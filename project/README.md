# Project Evidence

이 폴더는 ESG 지표산정 자동화 Agent 프로젝트의 공개 제출용 보조 자료입니다.

실제 운영 코드, 지표 산식, 가중치, 원본 데이터, 정답 데이터, 기관/기업 식별 정보는 포함하지 않습니다. 공개 가능한 범위에서 프로젝트 구조, 설계 의도, Agent 확장 계획만 설명합니다.

`Rulebased_code_redacted/`는 실행 가능한 원본 코드가 아니라, 대외비를 제거한 pseudocode 성격의 구조 증빙 자료입니다. 코드의 모듈 배치와 책임 분리는 보여주되, 산정 로직을 복원할 수 있는 수치, 등급, 산식, 원천 정보는 `1`, `2`, `3`, `4`, `TXT_REDACTED`, `SRC_REDACTED` 같은 placeholder로 통일했습니다.

## Contents

| Path | Purpose |
| --- | --- |
| `Agent_plan_redacted/` | Agent 보완 수집, 검증 보조, 리포트 작성 계획 |
| `Rulebased_code_redacted/` | Rule-Based 계산 파트의 공개용 pseudocode/구조 증빙 |

## Public Scope

- Rule-Based first 구조
- Agent 보완 수집과 검증 흐름
- 모듈 경계와 데이터 흐름
- 공개 가능한 수준의 pseudocode

## Excluded Scope

- 실제 계산 산식
- 점수표, 임계값, 가중치
- 원본 수집 데이터와 정답 데이터
- 내부 설정, API 키, 서버 경로
- 실제 기관명, 기업명, 원천 URL

## Generative AI Disclosure

본 문서 정리에는 생성형 AI를 활용했으며, 공개 전 민감정보 포함 여부를 검토했습니다.
