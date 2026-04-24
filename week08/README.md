# Week 08: Workflow Optimization

## 과제 내용

Matrix 확장 테스트를 적용하고 Reusable Workflow 및 Composite Action을 만들어 중복을 제거한다. 캐싱 전후 실행 시간을 측정해 개선률을 수치로 보고한다. 브랜치/PR 조건 및 변경 파일 감지를 활용해 선택적 배포 파이프라인을 구현한다.

## 제출 링크

- Optimized CI Workflow: https://github.com/NONO6202/AIOSS/blob/main/.github/workflows/ci.yml
- Reusable Workflow: https://github.com/NONO6202/AIOSS/blob/main/.github/workflows/reusable-node-check.yml
- Composite Action: https://github.com/NONO6202/AIOSS/blob/main/.github/actions/setup-node-project/action.yml
- Actions 실행 내역: https://github.com/NONO6202/AIOSS/actions/workflows/ci.yml
- 최적화 비교 리포트: [CACHE_REPORT.md](CACHE_REPORT.md)

## 필수 요구사항 충족

| 요구사항 | 상태 | 근거 |
| --- | --- | --- |
| Matrix 확장 테스트 | 완료 | OS `ubuntu-latest`, `macos-latest`, `windows-latest`와 Node.js `20`, `22`, `24` 조합 |
| Reusable Workflow | 완료 | `.github/workflows/reusable-node-check.yml` |
| Composite Action | 완료 | `.github/actions/setup-node-project/action.yml` |
| 캐싱 전후 비교 리포트 | 작성 | `week08/CACHE_REPORT.md` |
| 브랜치/PR 조건 | 완료 | deploy job은 `main` 또는 수동 실행에서만 수행 |
| 변경 파일 감지 | 완료 | `scripts/detect_changed_files.mjs`와 `detect-changes` job |
| 선택적 배포 | 완료 | 관련 파일 변경 시에만 deploy preview artifact 생성 |

## 구조 변경 요약

1. Node.js 설치와 `npm ci`를 composite action으로 분리했다.
2. build/test 공통 로직을 reusable workflow로 분리했다.
3. test matrix를 2x2에서 3x3으로 확장했다.
4. 변경 파일 감지 결과를 deploy 조건에 연결했다.
5. build artifact를 test/deploy job에서 다운로드해 검증한다.

## 생성형 AI 활용

본 워크플로우와 문서 작성 및 정리 과정에는 생성형 AI를 활용했습니다. 최종 내용은 직접 검토하고 수정했습니다.
