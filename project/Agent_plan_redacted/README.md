# Agent 보완·검증 파트 계획서

`Planning Document` · `Agent Extension` · `Rule-Based First` · `Redacted`

이 폴더는 외부 의뢰 과제로 진행 중인 지표 산정 자동화 시스템에서 **향후 Agent가 담당할 보완·검증 영역**을 설명하기 위한 문서입니다.

현재 공개 제출 자료의 중심은 `Rulebased_code_redacted`의 **Rule-Based 수집 및 계산 구조 설명**입니다. 실제 운영 코드는 산식과 내부 규칙 보호를 위해 포함하지 않으며, 이 문서에서는 Agent를 어떤 역할로 확장할 계획인지와 어떤 원칙을 지켜야 하는지만 정리합니다.

민감한 기관명, 실제 원천명, 지표명, 세부 산식, URL, 서버 경로, 모델명은 포함하지 않았습니다.

## 전체 시스템 위치

```text
Rule-Based 수집 및 계산
        ↓
Agent 보완 수집
        ↓
Agent 검증 보조
        ↓
리포트 초안 작성
        ↓
사람 최종 검토
```

Agent는 Rule-Based 계산을 대체하지 않습니다. 계산 책임은 명시적 규칙과 검증 가능한 데이터 파이프라인에 두고, Agent는 사람이 확인해야 하는 누락 자료, 근거 부족, 오류 의심 항목을 정리하는 보조 계층으로 둡니다.

## Agent가 맡을 역할

| 역할 | 설명 |
| --- | --- |
| 보완 수집 | Rule-Based가 찾지 못한 자료의 후보 원천을 탐색 |
| 근거 정리 | URL, 문서명, 발행 주체, 관련 문장 또는 표 위치를 구조화 |
| 검증 보조 | 산출값과 근거가 일치하는지 `correct`, `incorrect`, `unknown`, `needs_source`로 판정 |
| 오류 설명 | 파서 오류, source 오류, mapping 오류, rule 오류, missing data를 구분 |
| 리포트 초안 | 검증된 결과와 근거를 바탕으로 사람이 수정 가능한 요약 생성 |

## Agent가 하면 안 되는 것

- 근거 없이 값을 새로 만들어내기
- 정답 파일을 보고 값을 맞추기
- Rule-Based 결과를 자동으로 덮어쓰기
- 출처 검증 없이 후보값을 최종값으로 반영하기
- 특정 회사나 특정 연도에만 맞춘 예외 로직 만들기
- LLM 응답을 검증 없이 신뢰하기

## 계획된 처리 흐름

1. Rule-Based 파이프라인이 회사, 연도, 지표, 산출값, 중간값, 근거 record를 생성합니다.
2. Agent는 실패 또는 신뢰도 낮은 record만 입력으로 받습니다.
3. Agent는 후보 원천을 탐색하고 근거 문장 또는 표 위치를 저장합니다.
4. 후보값은 별도 `candidate` 계층에만 저장합니다.
- 결정적 검증기 또는 사람이 승인하기 전까지 최종 workbook에는 반영하지 않습니다.
- 검증 결과는 사람이 읽을 수 있는 review 문서 또는 workbook 주석으로 제공합니다.

## 출력 설계

Agent 결과는 아래와 같은 구조를 목표로 합니다.

```json
{
  "company": "COMPANY_REDACTED",
  "year": "YEAR",
  "field_id": "FIELD_REDACTED",
  "rule_value": "VALUE_FROM_RULE",
  "verdict": "correct | incorrect | unknown | needs_source",
  "suggested_value": "CANDIDATE_VALUE",
  "confidence": "1",
  "supporting_sources": ["SRC_REDACTED"],
  "quoted_evidence": ["TEXT_REDACTED"],
  "next_action": "HUMAN_REVIEW_OR_RULE_UPDATE"
}
```

## 기술 방향

| 영역 | 후보 |
| --- | --- |
| Agent workflow | LangGraph 또는 경량 Python state machine |
| LLM 연결 | OpenAI-compatible endpoint 또는 로컬 LLM 서버 |
| API 계층 | FastAPI |
| 상태 저장 | SQLite 우선, 필요 시 PostgreSQL |
| 문서 검색 | Qdrant 또는 Chroma 기반 vector search |
| 브라우저 자동화 | Playwright |
| 프론트엔드 | React 기반 검토 UI |
| 검증 스키마 | Pydantic |

위 항목은 구현 계획이며, 현재 공개 제출 코드는 Rule-Based 중심입니다.

## 성공 기준

- Agent가 계산값을 직접 만들어내지 않는다.
- 모든 후보값에 출처와 근거 위치가 붙는다.
- `unknown`, `needs_source`가 최종값으로 조용히 반영되지 않는다.
- 사람이 검토할 수 있는 형태로 오류 이유와 다음 조치를 남긴다.
- Rule-Based 수정이 필요한 항목과 단순 source 부족 항목을 구분한다.

## 현재 제출 범위

이번 공개 제출에서 Agent는 코드가 아니라 **계획 문서**로만 포함합니다. Rule-Based 파트도 실행 가능한 원본 코드가 아니라 공개 가능한 구조 설명 중심으로 제공합니다.
