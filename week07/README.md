# Week 07: GitHub Actions

## 과제 내용

Node.js 프로젝트에 CI 워크플로우를 구성해 Lint/Test를 자동 실행한다. Matrix 전략으로 버전/OS 조합 테스트를 적용하고, Secrets로 민감정보를 안전하게 주입한다. Build -> Test -> Deploy 의존성과 아티팩트 업로드/다운로드를 포함한 복합 워크플로우를 구성한다.

## 제출 링크

- Workflow YAML: https://github.com/NONO6202/AIOSS/blob/main/.github/workflows/ci.yml
- Actions 실행 내역: https://github.com/NONO6202/AIOSS/actions/workflows/ci.yml

## 필수 요구사항 충족

| 요구사항 | 상태 | 근거 |
| --- | --- | --- |
| Node.js CI 구성 | 완료 | `package.json`, `.github/workflows/ci.yml` |
| Lint 자동 실행 | 완료 | `npm run lint` |
| Test 자동 실행 | 완료 | `npm test` |
| Matrix 전략 | 완료 | OS `ubuntu-latest`, `macos-latest`와 Node.js `20`, `22` 조합 |
| Secrets 안전 주입 | 완료 | `${{ secrets.GITHUB_TOKEN }}`를 환경변수로 주입하고 값은 출력하지 않음 |
| Build -> Test -> Deploy 의존성 | 완료 | `test`는 `build` 이후, `deploy`는 `test` 이후 실행 |
| Artifact 업로드/다운로드 | 완료 | `ci-build-output` 업로드, test/deploy job에서 다운로드 |

## Workflow 구조

1. `build`: Node.js 20에서 lint와 report build를 수행하고 CI artifact를 업로드한다.
2. `test`: Matrix 조합별로 artifact를 다운로드한 뒤 lint/test를 실행한다.
3. `deploy`: main 또는 수동 실행에서 build artifact를 다운로드해 deploy preview artifact를 만든다.

## 보안 메모

Workflow는 Secret 값 자체를 출력하거나 artifact에 저장하지 않는다. 제출 문서에는 secret 존재 여부와 주입 방식만 기록한다.

## 생성형 AI 활용

본 워크플로우와 문서 작성 및 정리 과정에는 생성형 AI를 활용했습니다. 최종 내용은 직접 검토하고 수정했습니다.
