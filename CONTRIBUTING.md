# Contributing

이 저장소는 AIOSS 과제 산출물을 공개 가능한 범위에서 관리하기 위한 저장소입니다.

## Contribution Scope

기여는 다음 범위로 제한합니다.

- 과제별 README 및 제출 문서 정리
- GitHub Issues, Project, Pull Request, Wiki 운영 문서 개선
- 공개 가능한 아키텍처, 워크플로우, 검증 절차 문서 개선
- 대외비 정보를 포함하지 않는 예제 코드와 자동화 스크립트 개선

다음 내용은 기여 대상에서 제외합니다.

- ESG 지표 산식
- 내부 계산 규칙
- 원본 데이터
- 정답 데이터
- 고객 또는 단체 요구사항의 비공개 세부 내용

## Branch Strategy

작업은 `main`에 직접 커밋하지 않고 목적에 맞는 브랜치에서 진행합니다.

```text
feature/<task-name>
fix/<bug-name>
docs/<document-name>
```

## Commit Convention

커밋 메시지는 Conventional Commits 형식을 따릅니다.

```text
docs: update assignment README
feat: add metrics dashboard renderer
fix: correct broken wiki link
```

## Pull Request Guide

PR에는 다음 내용을 포함합니다.

- 변경 목적
- 주요 변경 파일
- 검토가 필요한 부분
- 공개 저장소에 민감정보가 포함되지 않았다는 확인

## Review Tags

리뷰 피드백은 중요도를 명확히 하기 위해 다음 태그를 사용합니다.

| Tag | Meaning |
| --- | --- |
| `[MUST]` | 병합 전 반드시 해결해야 하는 문제 |
| `[SHOULD]` | 병합 전 반영을 권장하는 개선 |
| `[NITS]` | 사소한 문구, 형식, 정리 제안 |

## Generative AI Disclosure

생성형 AI로 코드 또는 문서를 작성한 경우, 해당 과제 README에 생성형 AI 활용 사실을 명시합니다.
