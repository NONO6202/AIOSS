# GitHub Flow 수행 기록

## Branch

- Base branch: `main`
- Feature branch: `feature/week04-collaboration-workflow`

## Commit Convention

이번 과제 작업에는 Conventional Commits 형식을 적용한다.

```text
docs: add week04 collaboration workflow assignment
```

## Workflow

1. `main` 최신 상태에서 feature 브랜치를 생성한다.
2. 과제 산출물을 feature 브랜치에서 작성한다.
3. Conventional Commits 형식으로 커밋한다.
4. 원격 브랜치에 push한다.
5. GitHub Pull Request를 생성한다.
6. PR 본문에서 변경 목적, 변경 파일, 검토 포인트를 명확히 작성한다.
7. `[MUST]`, `[SHOULD]` 태그를 사용해 구조화된 리뷰 피드백을 남긴다.

## Pull Request

- PR: [#11 docs: add week04 collaboration workflow assignment](https://github.com/NONO6202/AIOSS/pull/11)
- Base: `main`
- Head: `feature/week04-collaboration-workflow`

## PR 체크리스트

- [x] 작업 브랜치가 `feature/` prefix를 사용한다.
- [x] 커밋 메시지가 Conventional Commits 형식이다.
- [x] 과제 제출 문서가 `week04/` 아래에 모여 있다.
- [x] 선택과제 파일은 생성하지 않았다.
- [x] 공개 저장소에 대외비 ESG 산식, 원본 데이터, 정답 데이터가 포함되지 않았다.
